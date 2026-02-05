import express from "express";
import axios from "axios";

const app = express();
const PORT = 8080;

/* ================== CONFIG (PRUEBAS) ================== */

// ⚠️ SOLO PARA PRUEBAS
const KOBO_TOKEN = "f295306d3c5728fc520bb928e40530d034f71100";
const ASSET_UID = "aU7Ss6syzzmPJBACQobF4Q";

const DOLIBARR_API_URL = "https://app.sen.com.gt:25443/api/index.php";
const DOLIBARR_API_KEY = "quk5j73GFHUL0F1vZk5l6PhR4t4D8Vvr";

const KOBO_URL = `https://kf.kobotoolbox.org/api/v2/assets/${ASSET_UID}/data/`;

/* ===================================================== */

console.log("========== BOOT ==========");
console.log("[BOOT] Service starting...");
console.log("[BOOT] KOBO_TOKEN:", KOBO_TOKEN);
console.log("[BOOT] ASSET_UID:", ASSET_UID);
console.log("[BOOT] DOLIBARR_API_KEY:", DOLIBARR_API_KEY);
console.log("==========================");

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
  console.log("[KOBO] Requesting submissions...");
  console.log("[KOBO] URL:", KOBO_URL);

  const res = await axios.get(KOBO_URL, {
    headers: {
      Authorization: `Token ${KOBO_TOKEN}`
    },
    timeout: 15000
  });

  console.log("[KOBO] HTTP STATUS:", res.status);
  console.log("[KOBO] TOTAL SUBMISSIONS:", res.data.count);

  return res.data.results || [];
}

async function findTicketByRef(ref) {
  console.log("[DOLIBARR] Searching ticket with ref:", ref);

  const res = await dolibarr.get("/tickets", {
    params: {
      sqlfilters: `(t.ref:=:${ref})`
    }
  });

  console.log("[DOLIBARR] Tickets found:", res.data.length);

  return res.data.length ? res.data[0] : null;
}

async function closeTicket(ticketId, ref) {
  console.log(`[DOLIBARR] Closing ticket ${ref} (ID ${ticketId})`);

  const res = await dolibarr.put(`/tickets/${ticketId}`, {
    status: 3
  });

  console.log("[DOLIBARR] Close response status:", res.status);
}

/* ================== ENDPOINTS ================== */

app.get("/", (req, res) => {
  res.send("KoBo → Dolibarr service running (TEST MODE)");
});

app.get("/run", async (req, res) => {
  console.log("========== RUN START ==========");

  let processed = 0;
  let closed = 0;

  try {
    const submissions = await getKoboSubmissions();

    for (const s of submissions) {
      processed++;

      console.log("[RUN] Submission:", s._id || "NO_ID");

      if (!s.ticket_ref) {
        console.warn("[RUN] ticket_ref missing, skipping");
        continue;
      }

      console.log("[RUN] ticket_ref:", s.ticket_ref);

      const ticket = await findTicketByRef(s.ticket_ref);
      if (!ticket) {
        console.warn("[RUN] Ticket not found in Dolibarr");
        continue;
      }

      await closeTicket(ticket.id, s.ticket_ref);
      closed++;
    }

    console.log("========== RUN END ==========");
    console.log("[RUN] Processed:", processed);
    console.log("[RUN] Closed:", closed);

    res.json({
      status: "OK",
      processed,
      closed
    });

  } catch (err) {
    console.error("========== RUN ERROR ==========");
    console.error(err.message);
    console.error(err.stack);

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
