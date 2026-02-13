import { apiClient, endpoints } from "../service/api.js";

function normalizeRef(ref) {
    return ref.toString().trim().toUpperCase();
}

function parseTimeWithOffset(raw) {
    const s = String(raw || "").trim();

    const m = s.match(
        /^([01]\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?(?:\.(\d{1,3}))?(Z|[+\-]\d{2}:\d{2})?$/
    );
    if (!m) return null;

    const h = Number(m[1]);
    const mi = Number(m[2]);
    const sec = Number(m[3] || 0);
    const ms = Number((m[4] || "0").padEnd(3, "0"));
    const tz = m[5] || null;

    let offsetMin = null;
    if (tz === "Z") offsetMin = 0;
    else if (tz) {
        const sign = tz.startsWith("-") ? -1 : 1;
        const [oh, om] = tz.slice(1).split(":").map(Number);
        offsetMin = sign * (oh * 60 + om);
    }

    return { h, mi, sec, ms, offsetMin };
}

function ymdFromTicketTs(ticketTsSeconds, offsetMin) {
    if (!ticketTsSeconds) return null;
    const utcMs = Number(ticketTsSeconds) * 1000;

    const localMs = offsetMin != null ? utcMs + offsetMin * 60000 : utcMs;

    const d = new Date(localMs);
    const y = d.getUTCFullYear();
    const mo = String(d.getUTCMonth() + 1).padStart(2, "0");
    const day = String(d.getUTCDate()).padStart(2, "0");
    return `${y}-${mo}-${day}`;
}

function toUnixSecondsFromDateAndTime(ymd, timeObj) {
    if (!ymd || !timeObj) return null;

    const m = ymd.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return null;

    const y = Number(m[1]);
    const mo = Number(m[2]);
    const d = Number(m[3]);

    const { h, mi, sec, ms, offsetMin } = timeObj;

    const localUtcMs = Date.UTC(y, mo - 1, d, h, mi, sec, ms);
    const utcMs = offsetMin != null ? localUtcMs - offsetMin * 60000 : localUtcMs;

    return Math.floor(utcMs / 1000);
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

    if (horaInicioTs != null) nextOptions.options_horadeinicio = horaInicioTs;
    if (horaFinTs != null) nextOptions.options_horafinal = horaFinTs;

    await apiClient.put(`${endpoints.ticketsEndpoint}/${ticketId}`, {
        fk_statut: closeStatus,
        array_options: nextOptions,
    });

    const after = await apiClient.get(`${endpoints.ticketsEndpoint}/${ticketId}`);
    return after.data;
}

export async function runCerrarTicket(req, res) {
    try {
        const body = req.body || {};

        const ticketRef = body.ticket_ref || body?.datos_tecnico?.ticket_ref || null;
        if (!ticketRef) return res.status(200).json({ status: "SIN ticket_ref" });

        const ticket = await findTicketByRef(ticketRef);
        if (!ticket) return res.status(200).json({ ticketRef, status: "NO EXISTE" });

        const horaDeRaw =
            body.de ||
            body.hora_de ||
            body?.detalle_mano_obra?.de ||
            body?.detalle_mano_obra?.De ||
            body?.datos_tecnico?.de ||
            body?.datos_tecnico?.hora_de ||
            null;

        const horaARaw =
            body.a ||
            body.hora_a ||
            body?.detalle_mano_obra?.a ||
            body?.detalle_mano_obra?.A ||
            body?.datos_tecnico?.a ||
            body?.datos_tecnico?.hora_a ||
            null;

        const tDe = parseTimeWithOffset(horaDeRaw);
        const tA = parseTimeWithOffset(horaARaw);

        const ticketTs =
            ticket.date_creation || ticket.datec || ticket?.date_creation_timestamp || null;

        const offsetMin = tDe?.offsetMin ?? tA?.offsetMin ?? null;

        const ymd = ymdFromTicketTs(ticketTs, offsetMin);

        const horaInicioTs = toUnixSecondsFromDateAndTime(ymd, tDe);
        const horaFinTs = toUnixSecondsFromDateAndTime(ymd, tA);

        if (horaInicioTs == null || horaFinTs == null) {
            return res.status(200).json({
                ticketRef,
                ticketId: ticket.id,
                status: "NO SE PUDO PARSEAR HORAS",
                recibido: { horaDeRaw, horaARaw, ymd, tDe, tA },
            });
        }

        const updatedTicket = await closeTicketAndSetHours(
            ticket.id,
            horaInicioTs,
            horaFinTs
        );

        return res.status(200).json({
            ticketRef,
            ticketId: ticket.id,
            status: "CERRADO + HORAS ACTUALIZADAS",
            base_date_ymd: ymd,
            horadeinicio_ts: horaInicioTs,
            horafinal_ts: horaFinTs,
            array_options_after: updatedTicket.array_options,
        });
    } catch (error) {
        return res.status(500).json({
            error: error?.response?.data || error.message || String(error),
        });
    }
}
