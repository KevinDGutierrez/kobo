import express from "express";
import axios from "axios";

const app = express();
const PORT = 8080;

/* ================== CONFIG (PRUEBAS) ================== */

// ⚠️ SOLO PARA PRUEBAS – NO USAR ASÍ EN PRODUCCIÓN
const KOBO_TOKEN = "f295306d3c5728fc520bb928e40530d034f71100";
const ASSET_UID = "aU7Ss6syzzmPJBACQobF4Q";

const DOLIBARR_API_URL = "https://app.sen.com.gt:25443/api/index.php";
const DOLIBARR_API_KEY = "quk5j73GFHUL0F1vZk5l6PhR4t4D8Vvr";

const KOBO_URL = `https://kf.kobotoolbox.org/api/v2/assets/${ASSET_UID}/data/`;

/* ===================================================== */

/* ================== BOOT LOGS ================== */
console.log("========== BOOT ==========");
console.log("[BOOT] Service starting...");
console.log("[BOOT] KOBO_URL:", KOBO_URL);
console.log("[BOOT] DOLIBARR_API_URL:", DOLIBARR_API_URL);
console.log("[BOOT] PORT:", PORT);
console.log("==========================");

/* ================== AXIOS CLIENT ================== */
const dolibarr = axios.create({
  baseURL: DOLIBARR_API_URL,
  headers: {
    DOLAPIKEY: DOLIBARR_API_KEY,
    "Content-Type": "application/json"
  },
  timeout: 20000
});

/* ================== FUNCIONES ================== */

async function getKoboSubmissions() {
  console.log("------ KOBO FETCH START ------");

  const res = await axios.get(KOBO_URL, {
    headers: { Authorization: `Token ${KOBO_TOKEN}` },
    timeout: 20000
  });

  console.log("[KOBO] HTTP STATUS:", res.status);
  console.log("[KOBO] COUNT:", res.data?.count);

  if (!Array.isArray(res.data.results)) {
    console.error("[KOBO] results is NOT an array");
    return [];
  }

  console.log("[KOBO] FIRST ITEM SAMPLE:", res.data.results[0]);
  console.log("------ KOBO FETCH END ------");

  return res.data.results;
}

async function findTicketByRef(ref) {
  console.log("------ DOLIBARR SEARCH START ------");
  console.log("[DOLIBARR] Searching ticket ref:", ref);

  const res = await dolibarr.get("/tickets", {
    params: {
      sqlfilters: `(t.ref:=:${ref})`
    }
  });

  console.log("[DOLIBARR] HTTP STATUS:", res.status);
  console.log("[DOLIBARR] RAW RESPONSE:", res.data);

  if (!Array.isArray(res.data) || res.data.length === 0) {
    console.warn("[DOLIBARR] Ticket NOT found:", ref);
    return null;
  }

  console.log("[DOLIBARR] Ticket FOUND:", res.data[0]);
  console.log("------ DOLIBARR SEARCH END ------");

  return res.data[0];
}

async function closeTicket(ticketId, ref) {
  console.log("------ DOLIBARR UPDATE START ------");
  console.log(`[DOLIBARR] Updating fk_statut=8 for ticket REF=${ref} ID=${ticketId}`);
  console.log("[DOLIBARR] Endpoint:", `/tickets/${ticketId}`);

  const res = await dolibarr.put(
    `/tickets/${ticketId}`,
    {
      fk_statut: 8
    }
  );

  console.log("[DOLIBARR] HTTP STATUS:", res.status);
  console.log("[DOLIBARR] RESPONSE:", res.data);
  console.log("------ DOLIBARR UPDATE END ------");
}

/* ================== ENDPOINTS ================== */

app.get("/", (req, res) => {
  res.send("KoBo → Dolibarr service running (DEBUG MODE)");
});

app.get("/run", async (req, res) => {
  console.log("========== RUN START ==========");
  console.log("[RUN] Triggered at:", new Date().toISOString());

  let processed = 0;
  let closed = 0;

  try {
    const submissions = await getKoboSubmissions();
    console.log("[RUN] Submissions received:", submissions.length);

    for (const s of submissions) {
      processed++;
      console.log("---- SUBMISSION LOOP ----");
      console.log("[RUN] Submission ID:", s._id);

      if (!s.ticket_ref) {
        console.warn("[RUN] ticket_ref NOT FOUND in submission");
        continue;
      }

      console.log("[RUN] ticket_ref:", s.ticket_ref);

      const ticket = await findTicketByRef(s.ticket_ref);
      if (!ticket) continue;

      await closeTicket(ticket.id, s.ticket_ref);
      closed++;
    }

    console.log("========== RUN END ==========");
    console.log("[RUN] PROCESSED:", processed);
    console.log("[RUN] CLOSED:", closed);

    res.json({ status: "OK", processed, closed });
  } catch (err) {
    console.error("========== RUN FATAL ERROR ==========");
    console.error(err.stack);
    res.status(500).json({ error: err.message });
  }
});

/* ================== START ================== */

app.listen(PORT, () => {
  console.log(`[BOOT] Service listening on port ${PORT}`);
});
