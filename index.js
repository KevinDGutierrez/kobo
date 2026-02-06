import express from "express";
import axios from "axios";

const app = express();
const PORT = process.env.PORT || 8080;

const KOBO_TOKEN = "f295306d3c5728fc520bb928e40530d034f71100";
const ASSET_UID = "aU7Ss6syzzmPJBACQobF4Q";

const DOLIBARR_API_URL = "https://app.sen.com.gt:25443/api/index.php";
const DOLIBARR_API_KEY = "quk5j73GFHUL0F1vZk5l6PhR4t4D8Vvr";

const KOBO_URL = `https://kf.kobotoolbox.org/api/v2/assets/${ASSET_UID}/data/`;

const dolibarr = axios.create({
  baseURL: DOLIBARR_API_URL,
  timeout: 20000,
  headers: {
    DOLAPIKEY: DOLIBARR_API_KEY,
    Accept: "application/json",
    "User-Agent": "KoBo-Dolibarr-Integration/1.0",
  }
});

async function getKoboSubmissions() {
  const res = await axios.get(KOBO_URL, {
    headers: {
      Authorization: `Token ${KOBO_TOKEN}`,
    }
  });

  return res.data?.results || [];
}

async function findTicketByRef(ref) {
  const res = await dolibarr.get("/tickets", {
    params: {
      limit: 100,
    }
  });

  if (!Array.isArray(res.data)) return null;

  return res.data.find((t) => t.ref === ref) || null;
}

async function closeTicket(ticketId) {
  await dolibarr.put(`/tickets/${ticketId}`, {
    fk_statut: 8
  });
}

app.get("/", (_, res) => {
  res.send("KoBo → Dolibarr service running");
});

app.get("/run", async (_, res) => {
  const report = [];

  try {
    const submissions = await getKoboSubmissions();

    for (const s of submissions) {
      const ticketRef =
        s.ticket_ref ||
        s?.datos_tecnico?.ticket_ref ||
        null;

      if (!ticketRef) continue;

      const ticket = await findTicketByRef(ticketRef);

      if (!ticket) {
        report.push({
          ticketRef,
          status: "NO EXISTE",
        });
        continue;
      }

      await closeTicket(ticket.id);

      report.push({
        ticketRef,
        ticketId: ticket.id,
        status: "CERRADO",
      });
    }

    res.json({
      processed: report.length,
      results: report,
    });
  } catch (error) {
    res.status(500).json({
      error: error.response?.data || error.message,
    });
  }
})

app.listen(PORT, () => {
  console.log(`KoBo → Dolibarr service listening on port ${PORT}`);
});