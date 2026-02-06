import express from "express";
import axios from "axios";

const app = express();
const PORT = process.env.PORT || 8080;

/* ===== CONFIG PRUEBAS (NO PRODUCCIÓN) ===== */
const KOBO_TOKEN = "f295306d3c5728fc520bb928e40530d034f71100";
const ASSET_UID = "aU7Ss6syzzmPJBACQobF4Q";

const DOLIBARR_API_URL = "https://app.sen.com.gt:25443/api/index.php";
const DOLIBARR_API_KEY = "quk5j73GFHUL0F1vZk5l6PhR4t4D8Vvr";

const KOBO_URL = `https://kf.kobotoolbox.org/api/v2/assets/${ASSET_UID}/data/`;
/* ========================================= */

const dolibarr = axios.create({
  baseURL: DOLIBARR_API_URL,
  headers: {
    DOLAPIKEY: DOLIBARR_API_KEY,
    "Content-Type": "application/json",
  },
  timeout: 20000,
});

async function getKoboSubmissions() {
  const res = await axios.get(KOBO_URL, {
    headers: { Authorization: `Token ${KOBO_TOKEN}` },
    timeout: 20000,
  });
  return Array.isArray(res.data?.results) ? res.data.results : [];
}

async function findTicketByRef(ref) {
  const res = await dolibarr.get("/tickets", {
    params: { sqlfilters: `(t.ref:=:${ref})` },
  });
  return Array.isArray(res.data) && res.data.length ? res.data[0] : null;
}

/**
 * PRUEBA BRUTA: forzar estado con PUT
 * Nota: algunos Dolibarr requieren incluir más campos o no permiten saltos.
 */
async function forceStatusWithPut(ticketId, status) {
  // Intento 1: PUT con fk_statut
  return dolibarr.put(`/tickets/${ticketId}`, { fk_statut: status });
}

app.get("/", (_, res) => {
  res.send("KoBo → Dolibarr (PUT fk_statut test) running");
});

app.get("/run", async (_, res) => {
  let processed = 0;
  let updated = 0;

  try {
    const submissions = await getKoboSubmissions();

    for (const s of submissions) {
      processed++;
      const ref = s.ticket_ref;
      if (!ref) continue;

      const ticket = await findTicketByRef(ref);
      if (!ticket) continue;

      // Forzar a CERRADO/RESUELTO (8)
      await forceStatusWithPut(ticket.id, 8);
      updated++;
    }

    res.json({ status: "OK", processed, updated });
  } catch (err) {
    res.status(500).json({
      status: "ERROR",
      message: err.response?.data || err.message,
    });
  }
});

app.listen(PORT, () => {
  console.log(`Service listening on port ${PORT}`);
});