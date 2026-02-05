import express from "express";
import axios from "axios";

const app = express();
const PORT = process.env.PORT || 8080;

/* ===== CONFIG ===== */

// KoBo
const KOBO_TOKEN = process.env.KOBO_TOKEN;
const ASSET_UID = process.env.ASSET_UID;
const KOBO_URL = `https://kf.kobotoolbox.org/api/v2/assets/${ASSET_UID}/data/`;

// Dolibarr
const DOLIBARR_API_URL = "https://app.sen.com.gt:25443/api/index.php";
const DOLIBARR_API_KEY = process.env.DOLIBARR_API_KEY;

/* ================= */

const dolibarr = axios.create({
  baseURL: DOLIBARR_API_URL,
  headers: {
    DOLAPIKEY: DOLIBARR_API_KEY,
    "Content-Type": "application/json"
  }
});

async function getKoboSubmissions() {
  const res = await axios.get(KOBO_URL, {
    headers: { Authorization: `Token ${KOBO_TOKEN}` }
  });
  return res.data.results || [];
}

async function findTicketByRef(ref) {
  const res = await dolibarr.get("/tickets", {
    params: { sqlfilters: `(t.ref:=:${ref})` }
  });
  return res.data.length ? res.data[0] : null;
}

async function closeTicket(ticketId) {
  await dolibarr.put(`/tickets/${ticketId}`, { status: 3 });
}

/* ===== ENDPOINT ===== */
app.get("/", (req, res) => {
  res.send("KoBo → Dolibarr service running");
});

app.get("/run", async (req, res) => {
  try {
    const submissions = await getKoboSubmissions();

    for (const s of submissions) {
      if (!s.ticket_ref) continue;

      const ticket = await findTicketByRef(s.ticket_ref);
      if (!ticket) continue;

      await closeTicket(ticket.id);
    }

    res.json({ status: "OK", processed: submissions.length });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`Service running on port ${PORT}`);
});
