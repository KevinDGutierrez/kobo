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

    return { HH, MM, SS, offset };
}

function secOfDay(t) {
    return Number(t.HH) * 3600 + Number(t.MM) * 60 + Number(t.SS);
}

function toEpochSeconds(ymd, t) {
    const m = String(ymd || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return null;

    const y = Number(m[1]);
    const mo = Number(m[2]);
    const d = Number(m[3]);

    const utcMs = Date.UTC(y, mo - 1, d, Number(t.HH), Number(t.MM), Number(t.SS), 0);

    if (t.offset && t.offset !== "Z") {
        const sign = t.offset.startsWith("-") ? -1 : 1;
        const [oh, om] = t.offset.slice(1).split(":").map(Number);
        const offMin = sign * (oh * 60 + om);
        return Math.floor((utcMs - offMin * 60000) / 1000);
    }

    if (t.offset === "Z") return Math.floor(utcMs / 1000);

    const localMs = new Date(y, mo - 1, d, Number(t.HH), Number(t.MM), Number(t.SS), 0).getTime();
    return Math.floor(localMs / 1000);
}

function epochToUtcString(epochSec) {
    const d = new Date(Number(epochSec) * 1000);
    const y = d.getUTCFullYear();
    const mo = pad2(d.getUTCMonth() + 1);
    const da = pad2(d.getUTCDate());
    const hh = pad2(d.getUTCHours());
    const mm = pad2(d.getUTCMinutes());
    const ss = pad2(d.getUTCSeconds());
    return `${y}-${mo}-${da} ${hh}:${mm}:${ss}`;
}

function toUtcDatetimeFromKobo(ymd, t, defaultOffset = "-06:00") {
    const t2 = { ...t, offset: t.offset || defaultOffset };
    const epoch = toEpochSeconds(ymd, t2);
    if (epoch == null) return null;
    return { epoch, utcStr: epochToUtcString(epoch) };
}

async function findTicketByRef(ref) {
    const target = normalizeRef(ref);
    const limit = 50;
    let page = 0;
    const MAX_PAGES = 500;

    while (page < MAX_PAGES) {
        const res = await apiClient.get(endpoints.ticketsEndpoint, { params: { limit, page } });
        const data = res.data;

        if (!Array.isArray(data) || data.length === 0) return null;

        const found = data.find((t) => normalizeRef(t.ref) === target);
        if (found) return found;

        page++;
    }

    return null;
}

async function closeTicket(ticketId) {
    const closeStatus = Number(process.env.DOLIBARR_CLOSE_STATUS || 8);
    await apiClient.put(`${endpoints.ticketsEndpoint}/${ticketId}`, { fk_statut: closeStatus });
}

async function setTicketHours(ticketId, valueInicio, valueFinal) {
    const url = `${endpoints.ticketsEndpoint}/${ticketId}`;

    const current = await apiClient.get(url);
    const currentOptions = current?.data?.array_options || {};
    const nextOptions = { ...currentOptions };

    nextOptions.options_horadeinicio = valueInicio;
    nextOptions.options_horafinal = valueFinal;

    await apiClient.put(url, { array_options: nextOptions });

    const after = await apiClient.get(url);
    return after?.data?.array_options || null;
}

function pickFromManoObra(body) {
    const rows = Array.isArray(body?.mano_obra) ? body.mano_obra : [];
    if (!rows.length) return null;

    const parsed = rows
        .map((r) => {
            const dia = r?.["mano_obra/dia"];
            const mes = r?.["mano_obra/mes"];
            const anio = r?.["mano_obra/anio"];
            const deRaw = r?.["mano_obra/hora_de"];
            const aRaw = r?.["mano_obra/hora_a"];

            const ymd =
                anio && mes && dia ? `${String(anio).trim()}-${pad2(mes)}-${pad2(dia)}` : null;

            const tDe = parseKoboHour(deRaw);
            const tA = parseKoboHour(aRaw);

            if (!ymd || !tDe || !tA) return null;
            return { ymd, tDe, tA };
        })
        .filter(Boolean);

    if (!parsed.length) return null;

    let earliest = parsed[0];
    for (const p of parsed) {
        if (secOfDay(p.tDe) < secOfDay(earliest.tDe)) earliest = p;
    }

    let latest = parsed[0];
    for (const p of parsed) {
        if (secOfDay(p.tA) > secOfDay(latest.tA)) latest = p;
    }

    return { ymd: earliest.ymd, tDe: earliest.tDe, tA: latest.tA };
}

export async function runCerrarTicket(req, res) {
    try {
        const body = req.body || {};
        const ticketRef = body.ticket_ref || null;

        if (!ticketRef) return res.status(200).json({ status: "SIN ticket_ref" });

        const ticket = await findTicketByRef(ticketRef);
        if (!ticket) return res.status(200).json({ ticketRef, status: "NO EXISTE" });

        await closeTicket(ticket.id);

        const pick = pickFromManoObra(body);
        if (!pick) {
            return res.status(200).json({
                ticketRef,
                ticketId: ticket.id,
                status: "CERRADO",
                hoursUpdated: false,
            });
        }

        const start = toUtcDatetimeFromKobo(pick.ymd, pick.tDe);
        const end = toUtcDatetimeFromKobo(pick.ymd, pick.tA);

        if (!start || !end) {
            return res.status(200).json({
                ticketRef,
                ticketId: ticket.id,
                status: "CERRADO",
                hoursUpdated: false,
            });
        }

        let inicioStr = start.utcStr;
        let finalStr = end.utcStr;

        if (end.epoch <= start.epoch) {
            finalStr = epochToUtcString(end.epoch + 86400);
        }

        const array_options_after = await setTicketHours(ticket.id, inicioStr, finalStr);

        const ok =
            array_options_after?.options_horadeinicio === inicioStr &&
            array_options_after?.options_horafinal === finalStr;

        return res.status(200).json({
            ticketRef,
            ticketId: ticket.id,
            status: "CERRADO",
            hoursUpdated: ok,
        });
    } catch (error) {
        return res.status(500).json({
            error: error?.response?.data || error.message || String(error),
        });
    }
}
