import { apiClient, endpoints } from "../service/api.js";

function normalizeRef(ref) {
    return ref.toString().trim().toUpperCase();
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

function pad2(x) {
    return String(x).padStart(2, "0");
}

function buildYMDFromKobo(body) {
    const anio = body.anio ?? body?.datos_tecnico?.anio;
    const mes = body.mes ?? body?.datos_tecnico?.mes;
    const dia = body.dia ?? body?.datos_tecnico?.dia;

    if (!anio || !mes || !dia) return null;
    return `${String(anio).trim()}-${pad2(mes)}-${pad2(dia)}`;
}

function buildDolibarrDatetime(ymd, t) {
    return `${ymd} ${t.HH}:${t.MM}:${t.SS}`;
}

function toEpochSeconds(ymd, t) {
    const m = ymd.match(/^(\d{4})-(\d{2})-(\d{2})$/);
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

    if (t.offset === "Z") {
        return Math.floor(utcMs / 1000);
    }

    const localMs = new Date(y, mo - 1, d, Number(t.HH), Number(t.MM), Number(t.SS), 0).getTime();
    return Math.floor(localMs / 1000);
}

async function setTicketHours(ticketId, valueInicio, valueFinal) {
    const current = await apiClient.get(`${endpoints.ticketsEndpoint}/${ticketId}`);
    const currentOptions = current?.data?.array_options || {};
    const nextOptions = { ...currentOptions };

    nextOptions.options_horadeinicio = valueInicio;
    nextOptions.options_horafinal = valueFinal;

    await apiClient.put(`${endpoints.ticketsEndpoint}/${ticketId}`, {
        array_options: nextOptions,
    });

    const after = await apiClient.get(`${endpoints.ticketsEndpoint}/${ticketId}`);
    return after.data?.array_options || null;
}

export async function runCerrarTicket(req, res) {
    try {
        const body = req.body || {};

        const ticketRef = body.ticket_ref || body?.datos_tecnico?.ticket_ref || null;
        if (!ticketRef) return res.status(200).json({ status: "SIN ticket_ref" });

        const ticket = await findTicketByRef(ticketRef);
        if (!ticket) return res.status(200).json({ ticketRef, status: "NO EXISTE" });

        await closeTicket(ticket.id);

        const horaDeRaw = body.hora_de ?? body?.datos_tecnico?.hora_de ?? null;
        const horaARaw = body.hora_a ?? body?.datos_tecnico?.hora_a ?? null;

        const ymd = buildYMDFromKobo(body);
        const tDe = parseKoboHour(horaDeRaw);
        const tA = parseKoboHour(horaARaw);

        let hoursUpdated = false;
        let method = null;
        let array_options_after = null;
        let hoursError = null;

        if (!ymd || !tDe || !tA) {
            return res.status(200).json({
                ticketRef,
                ticketId: ticket.id,
                status: "CERRADO",
                hoursUpdated: false,
                motivo: "FALTAN dia/mes/anio o hora_de/hora_a o formato inválido",
                recibido: { ymd, horaDeRaw, horaARaw },
            });
        }

        const inicioStr = buildDolibarrDatetime(ymd, tDe);
        const finalStr = buildDolibarrDatetime(ymd, tA);

        try {
            const ao = await setTicketHours(ticket.id, inicioStr, finalStr);
            array_options_after = ao;
            if (
                ao?.options_horadeinicio === inicioStr &&
                ao?.options_horafinal === finalStr
            ) {
                hoursUpdated = true;
                method = "datetime_string";
            }
        } catch (e) {
            hoursError = e?.response?.data || e.message || String(e);
        }

        if (!hoursUpdated) {
            try {
                const inicioTs = toEpochSeconds(ymd, tDe);
                const finalTs = toEpochSeconds(ymd, tA);

                const ao2 = await setTicketHours(ticket.id, inicioTs, finalTs);
                array_options_after = ao2;

                if (
                    ao2?.options_horadeinicio === inicioTs ||
                    ao2?.options_horadeinicio === String(inicioTs)
                ) {
                    hoursUpdated = true;
                    method = "epoch_seconds";
                }
            } catch (e2) {
                hoursError = e2?.response?.data || e2.message || String(e2);
            }
        }

        return res.status(200).json({
            ticketRef,
            ticketId: ticket.id,
            status: "CERRADO",
            hoursUpdated,
            method,
            enviado: { inicioStr, finalStr },
            array_options_after,
            hoursError,
        });
    } catch (error) {
        return res.status(500).json({
            error: error?.response?.data || error.message || String(error),
        });
    }
}
