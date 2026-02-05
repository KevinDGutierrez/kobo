import axios from "axios";

/* ================== CONFIG ================== */

// KoBo
const KOBO_TOKEN = "f295306d3c5728fc520bb928e40530d034f71100";
const ASSET_UID = "aU7Ss6syzzmPJBACQobF4Q";
const KOBO_URL = `https://kf.kobotoolbox.org/api/v2/assets/${ASSET_UID}/data/`;

// Dolibarr
const DOLIBARR_API_URL = "https://app.sen.com.gt:25443/api/index.php";
const DOLIBARR_API_KEY = "quk5j73GFHUL0F1vZk5l6PhR4t4D8Vvr";

/* ============================================ */

const dolibarr = axios.create({
  baseURL: DOLIBARR_API_URL,
  headers: {
    "DOLAPIKEY": DOLIBARR_API_KEY,
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
    params: {
      sqlfilters: `(t.ref:=:${ref})`
    }
  });
  return res.data.length > 0 ? res.data[0] : null;
}

async function closeTicket(ticketId) {
  await dolibarr.put(`/tickets/${ticketId}`, { status: 8 });
}

async function main() {
  console.log("Buscando reportes KoBo...");
  const submissions = await getKoboSubmissions();

  for (const s of submissions) {
    const ticketRef = s.ticket_ref;
    if (!ticketRef) continue;

    console.log("Procesando ticket:", ticketRef);

    const ticket = await findTicketByRef(ticketRef);
    if (!ticket) {
      console.log("Ticket no encontrado en Dolibarr");
      continue;
    }

    await closeTicket(ticket.id);
    console.log(`Ticket ${ticketRef} FINALIZADO`);
  }
}

main().catch(err => {
  console.error("Error:", err.response?.data || err.message);
});
