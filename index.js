import express from "express";
import axios from "axios";

const app = express();
const PORT = process.env.PORT || 8080;

/* ========= CONFIG PRUEBAS ========= */

const KOBO_TOKEN = "f295306d3c5728fc520bb928e40530d034f71100";
const ASSET_UID  = "aU7Ss6syzzmPJBACQobF4Q";

const DOLIBARR_API_URL = "https://app.sen.com.gt:25443/api/index.php";
const DOLIBARR_API_KEY = "quk5j73GFHUL0F1vZk5l6PhR4t4D8Vvr";

const KOBO_URL = `https://kf.kobotoolbox.org/api/v2/assets/${ASSET_UID}/data/`;

/* ================================= */

const dolibarr = axios.create({
  baseURL: DOLIBARR_API_URL,
  headers: {
    DOLAPIKEY: DOLIBARR_API_KEY,
    "Content-Type": "application/json"
  },
  timeout: 15000
});

/* ========= FUNCIONES ========= */

async function getKoboSubmissions() {
  const res = await axios.get(KOBO_URL, {
    headers: { Authorization: `Token ${KOBO_TOKEN}` }
  });
  return Array.isArray(res.data.results) ? res.data.results : [];
}

async function findTicketByRef(ref) {
  const res = await dolibarr.get("/tickets", {
    params: { sqlfilters: `(t.ref:=:${ref})` }
  });
  return res.data?.length ? res.data[0] : null;
}

/**
 * Aplica workflow correcto desde cualquier estado
 * 0 → 1 → 3 → 8
 */
async function applyWorkflow(ticketId, currentStatus) {
  const steps = [];

  if (currentStatus === 0) steps.push(1, 3, 8);
  else if (currentStatus === 1) steps.push(3, 8);
  else if (currentStatus === 2) steps.push(3, 8);
  else if (currentStatus === 3) steps.push(8);
  else return;

  for (const status of steps) {
    await dolibarr.post(
      `/tickets/${ticketId}/setstatus`,
      null,
      { params: { status } }
    );
  }
}

/* ========= ENDPOINTS ========= */

app.get("/", (_, res) => {
  res.send("KoBo → Dolibarr workflow service running");
});

app.get("/run", async (_, res) => {
  let processed = 0;
  let updated = 0;

  try {
    const submissions = await getKoboSubmissions();

    for (const s of submissions) {
      processed++;
      if (!s.ticket_ref) continue;

      const ticket = await findTicketByRef(s.ticket_ref);
      if (!ticket) continue;

      const currentStatus = Number(ticket.fk_statut);
      await applyWorkflow(ticket.id, currentStatus);
      updated++;
    }

    res.json({
      status: "OK",
      processed,
      updated
    });

  } catch (err) {
    res.status(500).json({
      status: "ERROR",
      message: err.response?.data || err.message
    });
  }
});

/* ========= START ========= */

app.listen(PORT, () => {
  console.log(`Service listening on port ${PORT}`);
});