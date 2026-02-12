import axios from "axios";

const DOLIBARR_API_URL = process.env.DOLIBARR_API_URL;
const DOLIBARR_API_KEY = process.env.DOLIBARR_API_KEY;

export const apiClient = axios.create({
  baseURL: DOLIBARR_API_URL,
  timeout: 50000,
  headers: {
    DOLAPIKEY: DOLIBARR_API_KEY,
    Accept: "application/json",
    "Content-Type": "application/json",
    "User-Agent": "KoBo-Dolibarr-Integration/1.0",
  },
});

export const endpoints = {
  ticketsEndpoint: "/tickets",
  agendaEventsEndpoint: '/agendaevents'
};