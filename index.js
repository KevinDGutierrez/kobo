import express from "express";
import { runCerrarTicket } from "./src/tickets/ticket.js";

const app = express();
const PORT = process.env.PORT || 8080;

app.use(express.json());

app.get("/", (_, res) => res.send("KoBo → Dolibarr service running"));
app.get("/healthz", (_, res) => res.status(200).send("ok"));

app.post("/run", runCerrarTicket);

app.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
});
