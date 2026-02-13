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

function pickTicketYMD(ticket) {
    for (const k of ["date_creation", "datec", "date_closure", "date_close"]) {
        const v = ticket?.[k];
        if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}/.test(v)) return v.slice(0, 10);
    }

    for (const k of ["date_creation", "datec"]) {
        const v = ticket?.[k];
        if (typeof v === "number" && Number.isFinite(v)) {
            const d = new Date(v * 1000);
            const y = d.getFullYear();
            const mo = String(d.getMonth() + 1).padStart(2, "0");
            const day = String(d.getDate()).padStart(2, "0");
            return `${y}-${mo}-${day}`;
        }
    }

    const d = new Date();
    const y = d.getFullYear();
    const mo = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${y}-${mo}-${day}`;
}

function toUnixSecondsFromYMDAndTime(ymd, t) {
    if (!ymd || !t) return null;
    const m = ymd.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return null;

    const y = Number(m[1]);
    const mo = Number(m[2]);
    const d = Number(m[3]);

    const localUtcMs = Date.UTC(y, mo - 1, d, t.h, t.mi, t.sec, t.ms);
    const utcMs = t.offsetMin != null ? localUtcMs - t.offsetMin * 60000 : localUtcMs;
    return Math.floor(utcMs / 1000);
}

function pickDeA(body) {
    let de =
        body.de ?? body.De ?? body.hora_de ?? body.horaDe ?? body?.detalle_mano_obra_de ?? body?.detalle_mano_obra?.de;
    let a =
        body.a ?? body.A ?? body.hora_a ?? body.horaA ?? body?.detalle_mano_obra_a ?? body?.detalle_mano_obra?.a;

    de = de ?? body?.datos_tecnico?.de ?? body?.datos_tecnico?.hora_de ?? body?.datos_tecnico?.horaDe;
    a = a ?? body?.datos_tecnico?.a ?? body?.datos_tecnico?.hora_a ?? body?.datos_tecnico?.horaA;

    const rows =
        body?.detalle_mano_obra_rows ||
        body?.detalle_mano_obra ||
        body?.detalleManoObra ||
        body?.datos_tecnico?.detalle_mano_obra ||
        null;

    if (Array.isArray(rows) && rows.length) {
        const parsed = rows
            .map((r) => ({ de: parseTimeWithOffset(r?.de ?? r?.De), a: parseTimeWithOffset(r?.a ?? r?.A) }))
            .filter((x) => x.de && x.a);

        if (parsed.length) {
            const toSecDay = (t) => t.h * 3600 + t.mi * 60 + t.sec;
            const earliest = parsed.reduce((p, c) => (toSecDay(c.de) < toSecDay(p.de) ? c : p), parsed[0]);
            const latest = parsed.reduce((p, c) => (toSecDay(c.a) > toSecDay(p.a) ? c : p), parsed[0]);
            de = de ?? rows[0]?.de ?? rows[0]?.De;
            a = a ?? rows[0]?.a ?? rows[0]?.A;
            return { deRaw: earliest.de, aRaw: latest.a, rawDeStr: de, rawAStr: a, fromRows: true };
        }
    }

    return { deRaw: de, aRaw: a, rawDeStr: de, rawAStr: a, fromRows: false };
}

async function setTicketHours(ticketId, horaInicioTs, horaFinTs) {
    const current = await apiClient.get(`${endpoints.ticketsEndpoint}/${ticketId}`);
    const currentOptions = current?.data?.array_options || {};
    const nextOptions = { ...currentOptions };

    nextOptions.options_horadeinicio = horaInicioTs;
    nextOptions.options_horafinal = horaFinTs;

    await apiClient.put(`${endpoints.ticketsEndpoint}/${ticketId}`, {
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

        await closeTicket(ticket.id);

        const ymd = pickTicketYMD(ticket);
        const pick = pickDeA(body);

        const tDe = typeof pick.deRaw === "object" ? pick.deRaw : parseTimeWithOffset(pick.deRaw);
        const tA = typeof pick.aRaw === "object" ? pick.aRaw : parseTimeWithOffset(pick.aRaw);

        let hoursUpdated = false;
        let hoursError = null;
        let arrayOptionsAfter = null;

        if (tDe && tA) {
            let inicioTs = toUnixSecondsFromYMDAndTime(ymd, tDe);
            let finTs = toUnixSecondsFromYMDAndTime(ymd, tA);

            if (inicioTs != null && finTs != null && finTs <= inicioTs) finTs += 86400;

            try {
                const updated = await setTicketHours(ticket.id, inicioTs, finTs);
                hoursUpdated = true;
                arrayOptionsAfter = updated.array_options;
            } catch (e) {
                hoursError = e?.response?.data || e.message || String(e);
            }
        }

        return res.status(200).json({
            ticketRef,
            ticketId: ticket.id,
            status: "CERRADO",
            hoursUpdated,
            base_date_ymd: ymd,
            received_de: pick.rawDeStr,
            received_a: pick.rawAStr,
            hoursError,
            array_options_after: arrayOptionsAfter,
        });
    } catch (error) {
        return res.status(500).json({
            error: error?.response?.data || error.message || String(error),
        });
    }
}
