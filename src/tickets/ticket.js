import { apiClient, endpoints } from "../service/api.js";

function normalizeRef(ref) {
    return ref.toString().trim().toUpperCase();
}

function parseDateYMD(dateStr) {
    const m = String(dateStr || "").trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return null;
    return { y: Number(m[1]), mo: Number(m[2]), d: Number(m[3]) };
}

function parseTimeHMS(timeStr) {
    const m = String(timeStr || "")
        .trim()
        .match(/^([01]\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?$/);
    if (!m) return null;
    return { h: Number(m[1]), mi: Number(m[2]), s: Number(m[3] || 0) };
}

function toUnixSeconds(value, fallbackDateStr) {
    if (value == null || value === "") return null;

    if (typeof value === "number" && Number.isFinite(value)) {
        return value > 1e12 ? Math.floor(value / 1000) : Math.floor(value);
    }

    const s = String(value).trim();

    if (/[zZ]|[+\-]\d{2}:\d{2}$/.test(s) && !Number.isNaN(Date.parse(s))) {
        return Math.floor(new Date(s).getTime() / 1000);
    }

    {
        const m = s.match(
            /^(\d{4})-(\d{2})-(\d{2})[ T]([01]\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?$/
        );
        if (m) {
            const y = Number(m[1]),
                mo = Number(m[2]),
                d = Number(m[3]),
                h = Number(m[4]),
                mi = Number(m[5]),
                sec = Number(m[6] || 0);
            return Math.floor(new Date(y, mo - 1, d, h, mi, sec).getTime() / 1000);
        }
    }

    {
        const t = parseTimeHMS(s);
        const d = parseDateYMD(fallbackDateStr);
        if (t && d) {
            return Math.floor(
                new Date(d.y, d.mo - 1, d.d, t.h, t.mi, t.s).getTime() / 1000
            );
        }
    }

    const parsed = Date.parse(s);
    if (!Number.isNaN(parsed)) return Math.floor(parsed / 1000);

    return null;
}

async function findTicketByRef(ref) {
    const target = normalizeRef(ref);
    const limit = 50;
    let page = 0;
    const MAX_PAGES = 500;

    while (page < MAX_PAGES) {
        const res = await apiClient.get(endpoints.ticketsEndpoint, {
            params: { limit, page },
        });

        const data = res.data;

        if (!Array.isArray(data) || data.length === 0) return null;

        const found = data.find((t) => normalizeRef(t.ref) === target);
        if (found) return found;

        page++;
    }

    return null;
}

async function closeTicketAndSetHours(ticketId, horaInicioTs, horaFinTs) {
    const closeStatus = Number(process.env.DOLIBARR_CLOSE_STATUS || 8);

    const current = await apiClient.get(`${endpoints.ticketsEndpoint}/${ticketId}`);
    const currentOptions = current?.data?.array_options || {};

    const nextOptions = { ...currentOptions };

    if (horaInicioTs != null) nextOptions.options_horadeinicio = String(horaInicioTs);
    if (horaFinTs != null) nextOptions.options_horafinal = String(horaFinTs);

    await apiClient.put(`${endpoints.ticketsEndpoint}/${ticketId}`, {
        fk_statut: closeStatus,
        array_options: nextOptions,
    });
}

export async function runCerrarTicket(req, res) {
    try {
        const body = req.body || {};

        const ticketRef = body.ticket_ref || body?.datos_tecnico?.ticket_ref || null;
        if (!ticketRef) {
            return res.status(200).json({ status: "SIN ticket_ref" });
        }

        const fecha =
            body.fecha || body.date || body?.datos_tecnico?.fecha || body?.datos_tecnico?.date || null;

        const horaInicioRaw =
            body.hora_inicio ||
            body.hora_inicio_visita ||
            body.horadeinicio ||
            body?.datos_tecnico?.hora_inicio ||
            body?.datos_tecnico?.hora_inicio_visita ||
            body?.datos_tecnico?.horadeinicio ||
            null;

        const horaFinRaw =
            body.hora_fin ||
            body.hora_fin_visita ||
            body.horafinal ||
            body?.datos_tecnico?.hora_fin ||
            body?.datos_tecnico?.hora_fin_visita ||
            body?.datos_tecnico?.horafinal ||
            null;

        const horaInicioTs = toUnixSeconds(horaInicioRaw, fecha);
        const horaFinTs = toUnixSeconds(horaFinRaw, fecha);

        const ticket = await findTicketByRef(ticketRef);
        if (!ticket) {
            return res.status(200).json({ ticketRef, status: "NO EXISTE" });
        }

        await closeTicketAndSetHours(ticket.id, horaInicioTs, horaFinTs);

        return res.status(200).json({
            ticketRef,
            ticketId: ticket.id,
            status: "CERRADO + HORAS ACTUALIZADAS",
            horadeinicio_ts: horaInicioTs,
            horafinal_ts: horaFinTs,
        });
    } catch (error) {
        return res.status(500).json({
            error: error?.response?.data || error.message || String(error),
        });
    }
}
