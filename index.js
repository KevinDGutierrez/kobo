import express from "express";
import axios from "axios";

const app = express();
const PORT = process.env.PORT || 8080;

/* ================== CONFIG ================== */

const KOBO_TOKEN = process.env.KOBO_TOKEN;
const ASSET_UID = process.env.ASSET_UID;
const DOLIBARR_API_KEY = process.env.DOLIBARR_API_KEY;

const KOBO_URL = `https://kf.kobotoolbox.org/api/v2/assets/${ASSET_UID}/data/`;
const DOLIBARR_API_URL = "https://app.sen.com.gt:25443/api/index.php";

/* ================== VALIDACIÓN DE ENV ================== */

console.log("[BOOT] Starting service...");
console.log("[BOOT] ENV CHECK", {
  hasKoboToken: !!KOBO_TOKEN,
  hasAssetUid: !!ASSET_UID,
  hasDolibarrKey: !!DOLIBARR_API_KEY
});

if (!KOBO_TOKEN || !ASSET_UID || !DOLIBARR_API_KEY) {
  console.error("[FATAL] Missing required environment variables");
  process.exit(1);
}

/* ================== CLIENTES ================== */

const dolibarr = axios.create({
  baseURL: DOLIBARR_API_URL,
  headers: {
    DOLAPIKEY: DOLIBARR_API_KEY,
    "Content-Type": "application/json"
  },
  timeout: 15000
});

/* ================== FUNCIONES ================== */

async function getKoboSubmissions() {
  console.log("[KOBO] Fetching submissions...");
  const res = await axios.get(KOBO_URL, {
    headers: { Authorization: `Token ${KOBO_TOKEN}` },
    timeout: 15000
  });
  console.log("[KOBO] Submissions fetched:", res.data.count);
  return res.data.results || [];
}

async function findTicketByRef(ref) {
  console.log("[DOLIBARR] Searching ticket:", ref);
  const res = await dolibarr.get("/tickets", {
    params: { sqlfilters: `(t.ref:=:${ref})` }
  });
  if (!res.data.length) {
    console.warn("[DOLIBARR] Ticket not found:", ref);
    return null;
  }
  return res.data[0];
}

async function closeTicket(ticketId, ref) {
  console.log("[DOLIBARR] Closing ticket:", ref, `(ID ${ticketId})`);
  await dolibarr.put(`/tickets/${ticketId}`, { status: 8 });
  console.log("[DOLIBARR] Ticket closed:", ref);
}

/* ================== ENDPOINTS ================== */

app.get("/", (req, res) => {
  res.send("KoBo → Dolibarr service running");
});

app.get("/run", async (req, res) => {
  const startedAt = new Date().toISOString();
  console.log("[RUN] Job started at", startedAt);

  let processed = 0;
  let closed = 0;

  try {
    const submissions = await getKoboSubmissions();

    for (const s of submissions) {
      processed++;

      if (!s.ticket_ref) {
        console.warn("[RUN] Submission without ticket_ref, skipping");
        continue;
      }

      const ticket = await findTicketByRef(s.ticket_ref);
      if (!ticket) continue;

      await closeTicket(ticket.id, s.ticket_ref);
      closed++;
    }

    console.log("[RUN] Job finished", {
      processed,
      closed
    });

    res.json({
      status: "OK",
      processed,
      closed
    });

  } catch (err) {
    console.error("[RUN] ERROR", {
      message: err.message,
      stack: err.stack
    });

    res.status(500).json({
      status: "ERROR",
      message: err.message
    });
  }
});

/* ================== START ================== */

app.listen(PORT, () => {
  console.log(`[BOOT] Service listening on port ${PORT}`);
});
