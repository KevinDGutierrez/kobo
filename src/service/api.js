import axios from "axios";

const DOLIBARR_API_URL = process.env.DOLIBARR_API_URL;
const DOLIBARR_API_KEY = process.env.DOLIBARR_API_KEY;

const baseURL = (DOLIBARR_API_URL || "").replace(/\/+$/, "");

if (baseURL && !baseURL.endsWith("/api/index.php")) {
  console.log(
    `[WARN] DOLIBARR_API_URL debería terminar en /api/index.php. Actual: ${baseURL}`
  );
}

export const apiClient = axios.create({
  baseURL,
  timeout: 50000,
  decompress: false,
  headers: {
    DOLAPIKEY: DOLIBARR_API_KEY,
    Accept: "application/json",
    "Content-Type": "application/json",
    "Accept-Encoding": "identity",
    "User-Agent": "KoBo-Dolibarr-Integration/1.0",
  },
});

export const endpoints = {
  thirdpartiesEndpoint: "/thirdparties",
  usersEndpoint: "/users",
  ticketsEndpoint: "/tickets",
  agendaEventsEndpoint: "/agendaevents",
};
