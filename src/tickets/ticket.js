import { apiClient, endpoints } from "../service/api.js";

function normalizeRef(ref) {
    return ref.toString().trim().toUpperCase();
}

function pad2(x) {
    return String(x).padStart(2, "0");
}

function parseKoboHour(raw) {
    const s = String(raw ?? "").trim();
    const m = s.match(
        /^([01]\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?(?:\.\d{1,3})?(Z|[+\-]\d{2}:\d{2})?$/
    );
    if (!m) return null;

    const HH = m[1];
    const MM = m[2];
    const SS = String(m[3] ?? "00").padStart(2, "0");
    const offset = m[4] || null;

    return { HH, MM, SS, offset, raw: s };
}

function buildDolibarrDatetime(ymd, t) {
    return `${ymd} ${t.HH}:${t.MM}:${t.SS}`;
}

function secOfDay(t) {
    return Number(t.HH) * 3600 + Number(t.MM) * 60 + Number(t.SS);
}

async function findTicketByRef(ref) {
    const target = normalizeRef(ref);
    const limit = 50;
    let page = 0;
    const MAX_PAGES = 500;

    console.log("[findTicketByRef] target:", target);

    while (page < MAX_PAGES) {
        console.log("[findTicketByRef] page:", page);

        const res = await apiClient.get(endpoints.ticketsEndpoint, { params: { limit, page } });
        const data = res.data;

        console.log("[findTicketByRef] got:", Array.isArray(data) ? data.length : typeof data);

        if (!Array.isArray(data) || data.length === 0) return null;

        const found = data.find((t) => normalizeRef(t.ref) === target);
        if (found) {
            console.log("[findTicketByRef] FOUND ticket.id:", found.id, "ref:", found.ref);
            return found;
        }

        page++;
    }

    return null;
}

async function closeTicket(ticketId) {
    const closeStatus = Number(process.env.DOLIBARR_CLOSE_STATUS || 8);
    const url = `${endpoints.ticketsEndpoint}/${ticketId}`;
    console.log("[closeTicket] url:", url, "fk_statut:", closeStatus);
    const r = await apiClient.put(url, { fk_statut: closeStatus });
    console.log("[closeTicket] status:", r?.status);
}

async function setTicketHours(ticketId, valueInicio, valueFinal) {
    const url = `${endpoints.ticketsEndpoint}/${ticketId}`;

    console.log("[setTicketHours] url:", url);
    console.log("[setTicketHours] valueInicio:", valueInicio);
    console.log("[setTicketHours] valueFinal:", valueFinal);

    const current = await apiClient.get(url);
    const currentOptions = current?.data?.array_options || {};

    console.log("[setTicketHours] current.array_options:", currentOptions);

    const nextOptions = { ...currentOptions };
    nextOptions.options_horadeinicio = valueInicio;
    nextOptions.options_horafinal = valueFinal;

    console.log("[setTicketHours] sending.array_options:", nextOptions);

    const putRes = await apiClient.put(url, { array_options: nextOptions });
    console.log("[setTicketHours] put.status:", putRes?.status);

    const after = await apiClient.get(url);
    console.log("[setTicketHours] after.array_options:", after?.data?.array_options || null);

    return after?.data?.array_options || null;
}

function pickFromManoObra(body) {
    const rows = Array.isArray(body?.mano_obra) ? body.mano_obra : [];
    console.log("[pickFromManoObra] rows.length:", rows.length);

    if (!rows.length) return null;

    const parsed = rows
        .map((r, idx) => {
            const dia = r?.["mano_obra/dia"];
            const mes = r?.["mano_obra/mes"];
            const anio = r?.["mano_obra/anio"];
            const deRaw = r?.["mano_obra/hora_de"];
            const aRaw = r?.["mano_obra/hora_a"];

            const ymd =
                anio && mes && dia ? `${String(anio).trim()}-${pad2(mes)}-${pad2(dia)}` : null;

            const tDe = parseKoboHour(deRaw);
            const tA = parseKoboHour(aRaw);

            console.log("[pickFromManoObra] row", idx, {
                dia,
                mes,
                anio,
                ymd,
                deRaw,
                aRaw,
                tDe,
                tA,
            });

            if (!ymd || !tDe || !tA) return null;

            return { idx, ymd, tDe, tA };
        })
        .filter(Boolean);

    console.log("[pickFromManoObra] parsed.valid:", parsed.length);

    if (!parsed.length) return null;

    let earliest = parsed[0];
    for (const p of parsed) {
        if (secOfDay(p.tDe) < secOfDay(earliest.tDe)) earliest = p;
    }

    let latest = parsed[0];
    for (const p of parsed) {
        if (secOfDay(p.tA) > secOfDay(latest.tA)) latest = p;
    }

    const ymd = earliest.ymd;
    const tDe = earliest.tDe;
    const tA = latest.tA;

    console.log("[pickFromManoObra] chosen:", { ymd, tDe, tA });

    return { ymd, tDe, tA };
}

export async function runCerrarTicket(req, res) {
    try {
        const body = req.body || {};

        console.log("[runCerrarTicket] body keys:", Object.keys(body));
        console.log("[runCerrarTicket] body:", JSON.stringify(body));

        const ticketRef = body.ticket_ref || null;
        console.log("[runCerrarTicket] ticketRef:", ticketRef);

        if (!ticketRef) return res.status(200).json({ status: "SIN ticket_ref" });

        const ticket = await findTicketByRef(ticketRef);
        if (!ticket) return res.status(200).json({ ticketRef, status: "NO EXISTE" });

        await closeTicket(ticket.id);

        const pick = pickFromManoObra(body);

        if (!pick) {
            console.log("[runCerrarTicket] NO HOURS UPDATE: mano_obra no trae fecha/hora bien");
            return res.status(200).json({
                ticketRef,
                ticketId: ticket.id,
                status: "CERRADO",
                hoursUpdated: false,
                motivo: "mano_obra sin dia/mes/anio o hora_de/hora_a válidos",
            });
        }

        const inicioStr = buildDolibarrDatetime(pick.ymd, pick.tDe);
        let finalStr = buildDolibarrDatetime(pick.ymd, pick.tA);

        console.log("[runCerrarTicket] inicioStr:", inicioStr);
        console.log("[runCerrarTicket] finalStr(before):", finalStr);

        if (secOfDay(pick.tA) <= secOfDay(pick.tDe)) {
            console.log("[runCerrarTicket] cruzo medianoche, sumando 1 dia al finalStr");
            const [Y, M, D] = pick.ymd.split("-").map(Number);
            const next = new Date(Y, M - 1, D + 1);
            const ymd2 = `${next.getFullYear()}-${pad2(next.getMonth() + 1)}-${pad2(next.getDate())}`;
            finalStr = buildDolibarrDatetime(ymd2, pick.tA);
        }

        console.log("[runCerrarTicket] finalStr(after):", finalStr);

        let array_options_after = null;
        let hoursError = null;

        try {
            array_options_after = await setTicketHours(ticket.id, inicioStr, finalStr);
        } catch (e) {
            hoursError = e?.response?.data || e.message || String(e);
            console.log("[runCerrarTicket] setTicketHours ERROR:", hoursError);
        }

        const ok =
            array_options_after?.options_horadeinicio === inicioStr &&
            array_options_after?.options_horafinal === finalStr;

        console.log("[runCerrarTicket] hoursUpdated:", ok);
        console.log("[runCerrarTicket] array_options_after:", array_options_after);

        return res.status(200).json({
            ticketRef,
            ticketId: ticket.id,
            status: "CERRADO",
            hoursUpdated: ok,
            enviado: { inicioStr, finalStr },
            array_options_after,
            hoursError,
        });
    } catch (error) {
        const err = error?.response?.data || error.message || String(error);
        console.log("[runCerrarTicket] FATAL:", err);
        return res.status(500).json({ error: err });
    }
}
