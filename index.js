import express from "express";
import axios from "axios";

const app = express();
const PORT = process.env.PORT || 8080;

app.use(express.json());

const DOLIBARR_API_URL = "https://app.sen.com.gt:25443/api/index.php";
const DOLIBARR_API_KEY = "quk5j73GFHUL0F1vZk5l6PhR4t4D8Vvr";

const dolibarr = axios.create({
  baseURL: DOLIBARR_API_URL,
  timeout: 50000,
  headers: {
    DOLAPIKEY: DOLIBARR_API_KEY,
    Accept: "application/json",
    "User-Agent": "KoBo-Dolibarr-Integration/1.0"
  }
});

function normalizeRef(ref) {
  return ref.toString().trim().toUpperCase();
}

async function findTicketByRef(ref) {
  const target = normalizeRef(ref);
  const limit = 50;
  let page = 0;

  while (true) {
    const res = await dolibarr.get("/tickets", {
      params: { limit, page }
    });

    if (!Array.isArray(res.data) || res.data.length === 0) {
      return null;
    }

    const found = res.data.find(
      t => normalizeRef(t.ref) === target
    );

    if (found) return found;

    page++;
  }
}

async function closeTicket(ticketId) {
  await dolibarr.put(`/tickets/${ticketId}`, {
    fk_statut: 8
  });
}

app.get("/", (_, res) => {
  res.send("KoBo → Dolibarr service running");
});

app.post("/run", async (req, res) => {
  try {
    const s = req.body;

    const ticketRef =
      s.ticket_ref ||
      s?.datos_tecnico?.ticket_ref ||
      null;

    if (!ticketRef) {
      return res.json({ status: "SIN ticket_ref" });
    }

    const ticket = await findTicketByRef(ticketRef);

    if (!ticket) {
      return res.json({
        ticketRef,
        status: "NO EXISTE"
      });
    }

    await closeTicket(ticket.id);

    res.json({
      ticketRef,
      ticketId: ticket.id,
      status: "CERRADO"
    });
  } catch (error) {
    res.status(500).json({
      error: error.response?.data || error.message
    });
  }
})

app.listen(PORT, () => {
  console.log(`KoBo → Dolibarr service listening on port ${PORT}`);
});