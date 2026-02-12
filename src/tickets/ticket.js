import { apiClient, endpoints } from "../service/api.js";

function normalizeRef(ref) {
    return ref.toString().trim().toUpperCase();
}

async function findTicketByRef(ref) {
    const target = normalizeRef(ref);
    const limit = 50;
    let page = 0;

    // Evita loops infinitos si algo raro pasa
    const MAX_PAGES = 500;

    while (page < MAX_PAGES) {
        const res = await apiClient.get(endpoints.ticketsEndpoint, {
            params: { limit, page },
        });

        const data = res.data;

        if (!Array.isArray(data) || data.length === 0) {
            return null;
        }

        const found = data.find((t) => normalizeRef(t.ref) === target);
        if (found) return found;

        page++;
    }

    return null;
}

async function closeTicket(ticketId) {
    const closeStatus = Number(process.env.DOLIBARR_CLOSE_STATUS || 8);

    await apiClient.put(`${endpoints.ticketsEndpoint}/${ticketId}`, {
        fk_statut: closeStatus,
    });
}

/**
 * Handler para POST /run
 */
export async function runCerrarTicket(req, res) {
    try {
        const body = req.body || {};

        const ticketRef =
            body.ticket_ref ||
            body?.datos_tecnico?.ticket_ref ||
            null;

        if (!ticketRef) {
            return res.status(200).json({ status: "SIN ticket_ref" });
        }

        const ticket = await findTicketByRef(ticketRef);

        if (!ticket) {
            return res.status(200).json({
                ticketRef,
                status: "NO EXISTE",
            });
        }

        await closeTicket(ticket.id);

        return res.status(200).json({
            ticketRef,
            ticketId: ticket.id,
            status: "CERRADO",
        });
    } catch (error) {
        return res.status(500).json({
            error: error?.response?.data || error.message || String(error),
        });
    }
}
