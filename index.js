import express from "express";
import axios from "axios";

const app = express();
const PORT = process.env.PORT || 8080;

/* ===== CONFIG PRUEBAS ===== */
const KOBO_TOKEN = "f295306d3c5728fc520bb928e40530d034f71100";
const ASSET_UID = "aU7Ss6syzzmPJBACQobF4Q";

const DOLIBARR_API_URL = "https://app.sen.com.gt:25443/api/index.php";
const DOLIBARR_API_KEY = "quk5j73GFHUL0F1vZk5l6PhR4t4D8Vvr";

const KOBO_URL = `https://kf.kobotoolbox.org/api/v2/assets/${ASSET_UID}/data/`;
/* ========================= */

const dolibarr = axios.create({
  baseURL: DOLIBARR_API_URL,
  headers: {
    DOLAPIKEY: DOLIBARR_API_KEY,
    "Content-Type": "application/json",
  },
  timeout: 15000,
});

/* ===== FUNCIONES ===== */

async function getKoboSubmissions() {
  const res = await axios.get(KOBO_URL, {
    headers: { Authorization: `Token ${KOBO_TOKEN}` },
  });
  return Array.isArray(res.data?.results) ? res.data.results : [];
}

async function findTicketByRef(ref) {
  const res = await dolibarr.get("/tickets", {
    params: { sqlfilters: `(t.ref:=:${ref})` },
  });
  return res.data?.length ? res.data[0] : null;
}

/**
 * Workflow REAL: 0 → 1 → 2 → 3 → 8
 */
async function applyWorkflow(ticketId) {
  // 1 = Leído
  await dolibarr.post(`/tickets/${ticketId}/setstatus`, null, {
    params: { status: 1 },
  });

  // 2 = Asignado
  await dolibarr.post(`/tickets/${ticketId}/setstatus`, null, {
    params: { status: 2 },
  });

  // 3 = En progreso
  await dolibarr.post(`/tickets/${ticketId}/setstatus`, null, {
    params: { status: 3 },
  });

  // 8 = Cerrado / Resuelto
  await dolibarr.post(`/tickets/${ticketId}/setstatus`, null, {
    params: { status: 8 },
  });
}

/* ===== ENDPOINTS ===== */

app.get("/", (_, res) => {
  res.send("KoBo → Dolibarr workflow service running");
});

app.get("/run", async (_, res) => {
  let processed = 0;
  let closed = 0;

  try {
    const submissions = await getKoboSubmissions();

    for (const s of submissions) {
      processed++;
      if (!s.ticket_ref) continue;

      // 🔑 SOLO usamos la referencia
      const ticket = await findTicketByRef(s.ticket_ref);
      if (!ticket) continue;

      // 🔥 Aplicar workflow correcto
      await applyWorkflow(ticket.id);
      closed++;
    }

    res.json({ status: "OK", processed, closed });
  } catch (err) {
    res.status(500).json({
      status: "ERROR",
      message: err.response?.data || err.message,
    });
  }
});

/* ===== START ===== */
app.listen(PORT, () => {
  console.log(`Service listening on port ${PORT}`);
});