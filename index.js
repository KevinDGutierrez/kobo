import express from "express";
import { runCerrarTicket } from "./src/tickets/ticket.js";
import { crearVisita } from "./src/visit/visit.js";

const app = express();
const PORT = process.env.PORT || 8080;

app.use(
  express.json({
    type: "*/*",
    verify: (req, _res, buf) => {
      req.rawBody = buf?.toString("utf8");
    },
  })
);

app.use(express.urlencoded({ extended: true }));

app.get("/", (_, res) => res.send("KoBo → Dolibarr service running"));
app.get("/healthz", (_, res) => res.status(200).send("ok"));

app.post("/run", runCerrarTicket);
app.post("/visit/run", crearVisita);

app.listen(PORT, () => console.log(`Listening on port ${PORT}`));
