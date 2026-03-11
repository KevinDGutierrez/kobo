import crypto from "crypto";
import axios from "axios";
import { apiClient, endpoints } from "../service/api.js";
import { sendNoClientEmail, getEmailForSubmitter } from "../service/email-sender.js";

const DEBUG = false;
const RAW_LOG_MAX = 4000;

function asArray(data) {
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.rows)) return data.rows;
  return [];
}

function norm(x) {
  return String(x ?? "").trim().toUpperCase();
}

function normLogin(x) {
  return String(x ?? "").trim().toLowerCase();
}

function normText(s) {
  return String(s ?? "")
    .toLowerCase()
    .trim()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function splitTokens(s) {
  const t = normText(s);
  return t ? t.split(" ").filter(Boolean) : [];
}

function parseYesNo(v) {
  const s = String(v ?? "").trim().toLowerCase();
  return s === "si" || s === "sí" || s === "yes" || s === "true" || s === "1";
}

function isClienteNuevo(v) {
  const s = normText(v);
  return s === "nuevo" || s === "cliente nuevo" || s === "cliente_nuevo";
}

function normalizePhone(p) {
  return String(p ?? "").replace(/[^\d+]/g, "").trim();
}

function normEmail(e) {
  return String(e ?? "").trim().toLowerCase();
}

function truncate128(s) {
  const x = String(s ?? "").trim();
  if (!x) return "";
  return x.length > 128 ? x.slice(0, 128) : x;
}

function getValue(obj, key) {
  if (!obj) return undefined;
  if (Object.prototype.hasOwnProperty.call(obj, key)) return obj[key];
  if (key.includes(".")) {
    return key.split(".").reduce((acc, k) => {
      if (acc && Object.prototype.hasOwnProperty.call(acc, k)) return acc[k];
      return undefined;
    }, obj);
  }
  return undefined;
}

function firstNonEmpty(obj, keys) {
  for (const k of keys) {
    const v = getValue(obj, k);
    if (v !== undefined && v !== null && String(v).trim() !== "") return v;
  }
  return null;
}

function pickRawFields(obj, keys) {
  const out = {};
  for (const k of keys) out[k] = getValue(obj, k);
  return out;
}

function parseGeoPoint(raw) {
  if (raw == null) return null;

  if (Array.isArray(raw) && raw.length >= 2) {
    const lat = Number(raw[0]);
    const lon = Number(raw[1]);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
    return { lat, lon };
  }

  const s = String(raw).trim().replace(/,/g, " ");
  const parts = s.split(/\s+/).filter(Boolean);
  if (parts.length < 2) return null;

  const lat = Number(parts[0]);
  const lon = Number(parts[1]);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

  return { lat, lon };
}

function uniqJoin(parts, sep = ", ") {
  const out = [];
  const seen = new Set();
  for (const p of parts) {
    const v = String(p ?? "").trim();
    if (!v) continue;
    const key = v.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(v);
  }
  return out.join(sep);
}

function pickBestZona(addr) {
  try {
    const s = JSON.stringify(addr || {});
    const re = /zona\s*(\d{1,2})/gi;
    let m;
    const nums = [];
    while ((m = re.exec(s))) nums.push(Number(m[1]));
    if (!nums.length) return null;
    const best = Math.max(...nums);
    return `Zona ${best}`;
  } catch {
    return null;
  }
}

function buildGtAddress(addr) {
  if (!addr) return null;

  const road = addr.road || addr.pedestrian || addr.footway || addr.path || "";
  const house = addr.house_number || "";
  const suburb = addr.neighbourhood || addr.suburb || addr.quarter || "";
  const city =
    addr.city || addr.town || addr.village || addr.municipality || "Ciudad de Guatemala";
  const state = addr.state || "Guatemala";

  const zona = pickBestZona(addr);
  const line1 = uniqJoin([road, house], " ").trim();

  const parts = [
    line1,
    zona,
    suburb && (!zona || suburb.toLowerCase() !== zona.toLowerCase()) ? suburb : null,
    city,
    state,
  ];

  return uniqJoin(parts);
}

async function reverseGeocode(lat, lon) {
  const provider = (process.env.GEOCODE_PROVIDER || "nominatim").toLowerCase();

  if (provider === "google") {
    const key = process.env.GOOGLE_MAPS_API_KEY;
    if (!key) return null;

    const url = "https://maps.googleapis.com/maps/api/geocode/json";
    const r = await axios.get(url, { params: { latlng: `${lat},${lon}`, key } });
    return r?.data?.results?.[0]?.formatted_address || null;
  }

  const email = process.env.NOMINATIM_EMAIL || "no-reply@example.com";
  const url = "https://nominatim.openstreetmap.org/reverse";

  const r = await axios.get(url, {
    params: { format: "jsonv2", lat, lon, zoom: 18, addressdetails: 1 },
    headers: { "User-Agent": `kobo-dolibarr-integration/1.0 (${email})` },
    timeout: 10000,
  });

  const addr = r?.data?.address || null;
  const pretty = buildGtAddress(addr);
  return pretty || r?.data?.display_name || null;
}

function logRid() { }

function scoreByQuery(candidateNameRaw, queryRaw) {
  const cand = normText(candidateNameRaw);
  const q = normText(queryRaw);
  if (!cand || !q) return 0;

  const qTokens = splitTokens(q);
  const cTokens = new Set(splitTokens(cand));

  let score = 0;

  if (cand.includes(q)) score += 500;
  if (cand.startsWith(q)) score += 200;

  for (const tk of qTokens) {
    if (cTokens.has(tk)) score += 120;
    else score -= 250;
  }

  if (qTokens.length && qTokens.every((t) => cTokens.has(t))) score += 200;

  return score;
}

function parseSpanishDateToUnix(raw) {
  if (!raw) return null;

  if (typeof raw === "number" && Number.isFinite(raw)) {
    return Math.floor(raw);
  }

  const s = String(raw).trim();
  if (!s) return null;

  const iso = Date.parse(s);
  if (!Number.isNaN(iso)) return Math.floor(iso / 1000);

  const cleaned = s.toLowerCase().replace(/\./g, "").replace(/\s+/g, " ").trim();

  const months = {
    ene: 0,
    enero: 0,
    feb: 1,
    febrero: 1,
    mar: 2,
    marzo: 2,
    abr: 3,
    abril: 3,
    may: 4,
    mayo: 4,
    jun: 5,
    junio: 5,
    jul: 6,
    julio: 6,
    ago: 7,
    agosto: 7,
    sep: 8,
    sept: 8,
    septiembre: 8,
    oct: 9,
    octubre: 9,
    nov: 10,
    noviembre: 10,
    dic: 11,
    diciembre: 11,
  };

  const m = cleaned.match(/^(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(\d{4})$/i);
  if (!m) return null;

  const day = Number(m[1]);
  const mon = months[m[2]];
  const year = Number(m[3]);

  if (!Number.isFinite(day) || mon == null || !Number.isFinite(year)) return null;

  const d = new Date(year, mon, day, 12, 0, 0);
  return Math.floor(d.getTime() / 1000);
}

function parseDateFlexibleToUnix(raw) {
  if (!raw) return null;

  if (typeof raw === "number" && Number.isFinite(raw)) {
    return Math.floor(raw);
  }

  const s = String(raw).trim();
  if (!s) return null;

  const direct = Date.parse(s);
  if (!Number.isNaN(direct)) return Math.floor(direct / 1000);

  const es = parseSpanishDateToUnix(s);
  if (es) return es;

  return null;
}

function mapOpportunityStatusToId(label) {
  const s = normText(label);
  if (!s) return null;
  if (s === "prospeccion" || s === "prospección") return 1;
  if (s === "cualificacion" || s === "cualificación") return 2;
  if (s === "presupuesto") return 3;
  if (s === "negociacion" || s === "negociación") return 4;
  if (s === "ganado") return 6;
  if (s === "perdido") return 7;
  return null;
}

function generateOpportunityRef() {
  const now = new Date();
  const yy = String(now.getFullYear()).slice(-2);
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  const hh = String(now.getHours()).padStart(2, "0");
  const mi = String(now.getMinutes()).padStart(2, "0");
  const ss = String(now.getSeconds()).padStart(2, "0");
  const rnd = Math.random().toString(36).slice(2, 6).toUpperCase();
  return `PJ${yy}${mm}${dd}${hh}${mi}${ss}${rnd}`;
}

function getCurrentUnixTime() {
  return Math.floor(Date.now() / 1000);
}

async function findThirdpartyByRef(ref) {
  const target = norm(ref);

  try {
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.code_client:=:${encodeURIComponent(
      target
    )})`;

    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const exact = list.find((t) => norm(t?.code_client) === target || norm(t?.ref) === target);

    if (exact) return exact;
  } catch (e) {
    if (e?.response?.status === 404) return null;
  }

  const limit = 50;
  let page = 0;

  while (true) {
    try {
      const res = await apiClient.get(endpoints.thirdpartiesEndpoint, { params: { limit, page } });
      const list = asArray(res.data);
      if (!list.length) return null;

      const found = list.find((t) => norm(t?.code_client) === target || norm(t?.ref) === target);
      if (found) return found;

      page++;
      if (page > 300) return null;
    } catch {
      return null;
    }
  }
}

async function findThirdpartyByNameSmart(nombre) {
  const query = String(nombre ?? "").trim();
  if (!query) return null;

  function pickBest(list) {
    const scored = list
      .map((t) => {
        const n = String(t?.nom ?? t?.name ?? "").trim();
        return { t, score: scoreByQuery(n, query) };
      })
      .filter((x) => x.score > 0)
      .sort((a, b) => b.score - a.score);

    if (!scored.length) return null;

    const best = scored[0];
    const second = scored[1];
    const MIN = 250;
    const GAP = 120;

    if (best.score < MIN) return null;
    if (second && best.score - second.score < GAP) return null;

    return best.t;
  }

  try {
    const like = `%${query}%`;
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.nom:like:${encodeURIComponent(like)})`;

    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const best = pickBest(list);

    if (best) return best;
  } catch { }

  const limit = 50;
  let page = 0;
  let best = null;
  let bestScore = 0;
  let secondBest = 0;

  while (true) {
    try {
      const res = await apiClient.get(endpoints.thirdpartiesEndpoint, { params: { limit, page } });
      const list = asArray(res.data);
      if (!list.length) break;

      for (const t of list) {
        const n = String(t?.nom ?? t?.name ?? "").trim();
        const s = scoreByQuery(n, query);
        if (s > bestScore) {
          secondBest = bestScore;
          bestScore = s;
          best = t;
        } else if (s > secondBest) {
          secondBest = s;
        }
      }

      page++;
      if (page > 300) break;
    } catch {
      return null;
    }
  }

  const MIN = 250;
  const GAP = 120;
  const ok = bestScore >= MIN && bestScore - secondBest >= GAP;

  if (ok) return best;
  return null;
}

async function findThirdpartyByNameExact(nombre) {
  const query = String(nombre ?? "").trim();
  if (!query) return null;

  try {
    const like = `%${query}%`;
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.nom:like:${encodeURIComponent(like)})`;

    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const exact = list.find((t) => normText(t?.nom ?? t?.name) === normText(query));

    return exact || null;
  } catch (e) {
    if (e?.response?.status === 404) return null;
    return null;
  }
}

async function createThirdpartyIfNew({
  clienteTipo,
  nombreClienteNuevo,
  correoClienteNuevo,
  numeroClienteNuevo,
  locationText,
  user,
}) {
  if (!isClienteNuevo(clienteTipo)) {
    return { attempted: false, created: false, tercero: null, reason: "NO_ES_CLIENTE_NUEVO" };
  }

  if (!nombreClienteNuevo) {
    return { attempted: true, created: false, tercero: null, reason: "SIN_NOMBRE_CLIENTE_NUEVO" };
  }

  const found = await findThirdpartyByNameExact(nombreClienteNuevo);

  if (found) {
    return { attempted: true, created: false, tercero: found, reason: "YA_EXISTE" };
  }

  const payload = {
    name: String(nombreClienteNuevo).trim(),
    address: String(locationText ?? "").trim(),
    phone: String(numeroClienteNuevo ?? "").trim(),
    email: String(correoClienteNuevo ?? "").trim(),
    client: 1,
    code_client: "auto",
  };

  if (user?.id) payload.commercial_id = Number(user.id);

  try {
    const createResponse = await apiClient.post(endpoints.thirdpartiesEndpoint, payload);
    const socid = createResponse.data;
    const details = await apiClient.get(`${endpoints.thirdpartiesEndpoint}/${socid}`);
    const tercero = details.data || null;

    return { attempted: true, created: true, tercero, reason: "CREADO" };
  } catch {
    return {
      attempted: true,
      created: false,
      tercero: null,
      reason: "ERROR_CREANDO_CLIENTE_NUEVO",
    };
  }
}

async function findUserByLogin(login) {
  const target = normLogin(login);

  try {
    const url = `${endpoints.usersEndpoint}?sqlfilters=(t.login:=:${encodeURIComponent(login)})`;

    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const exact = list.find((u) => normLogin(u?.login) === target);

    if (exact) return exact;
  } catch (e) {
    if (e?.response?.status === 404) return null;
  }

  const limit = 50;
  let page = 0;

  while (true) {
    try {
      const res = await apiClient.get(endpoints.usersEndpoint, { params: { limit, page } });
      const list = asArray(res.data);
      if (!list.length) return null;

      const found = list.find((u) => normLogin(u?.login) === target);
      if (found) return found;

      page++;
      if (page > 300) return null;
    } catch {
      return null;
    }
  }
}

async function listContactsBySocid(socid) {
  if (!endpoints?.contactsEndpoint) return [];
  if (!socid || !Number.isFinite(Number(socid))) return [];

  try {
    const url = `${endpoints.contactsEndpoint}?sqlfilters=(fk_soc:=:${socid})`;
    const res = await apiClient.get(url);
    return asArray(res.data);
  } catch (e) {
    if (e?.response?.status === 404) return [];
    return [];
  }
}

function contactMatches(existing, desired) {
  const exEmail = normEmail(existing?.email);
  const exPhone = normalizePhone(existing?.phone);
  const exFirst = normText(existing?.firstname);
  const exLast = normText(existing?.lastname);

  const dEmail = normEmail(desired?.email);
  const dPhone = normalizePhone(desired?.phone);
  const dFirst = normText(desired?.firstname);
  const dLast = normText(desired?.lastname);

  if (dEmail && exEmail && dEmail === exEmail) return true;
  if (dPhone && exPhone && dPhone === exPhone) return true;
  if (dFirst && dLast && exFirst === dFirst && exLast === dLast) return true;

  return false;
}

async function ensureContactIfRequested({ body, tercero }) {
  if (!endpoints?.contactsEndpoint) {
    return { attempted: false, done: false, created: false, reason: "SIN_ENDPOINT_CONTACTS" };
  }

  const wants = firstNonEmpty(body, [
    "dolibarr/contacto_cliente",
    "dolibarr.contacto_cliente",
    "contacto_cliente",
    "dolibarr/contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "contacto_cliente_00",
  ]);

  if (!parseYesNo(wants)) {
    return { attempted: false, done: false, created: false, reason: "NO_SOLICITADO" };
  }

  const firstname = String(
    firstNonEmpty(body, [
      "datos_persona/nombre_contacto",
      "datos_persona.nombre_contacto",
      "nombre_contacto",
      "dolibarr/nombre_contacto",
      "dolibarr.nombre_contacto",
    ]) ?? ""
  ).trim();

  const lastname = String(
    firstNonEmpty(body, [
      "datos_persona/apellido_contacto",
      "datos_persona.apellido_contacto",
      "apellido_contacto",
      "dolibarr/apellido_contacto",
      "dolibarr.apellido_contacto",
    ]) ?? ""
  ).trim();

  const phone = String(
    firstNonEmpty(body, [
      "datos_persona/numero_contacto",
      "datos_persona.numero_contacto",
      "numero_contacto",
      "dolibarr/numero_contacto",
      "dolibarr.numero_contacto",
    ]) ?? ""
  ).trim();

  const email = String(
    firstNonEmpty(body, [
      "datos_persona/correo_contacto",
      "datos_persona.correo_contacto",
      "correo_contacto",
      "dolibarr/correo_contacto",
      "dolibarr.correo_contacto",
    ]) ?? ""
  ).trim();

  const desired = {
    firstname: firstname || "N/D",
    lastname: lastname || "N/D",
    phone,
    email,
  };

  const hasSomething =
    desired.firstname !== "N/D" ||
    desired.lastname !== "N/D" ||
    desired.phone ||
    desired.email;

  if (!hasSomething) {
    return { attempted: true, done: false, created: false, reason: "SIN_DATOS_CONTACTO" };
  }

  const socid = tercero?.id ? Number(tercero.id) : null;

  if (socid && Number.isFinite(socid)) {
    const existing = await listContactsBySocid(socid);
    const exists = existing.some((c) => contactMatches(c, desired));
    if (exists) {
      return { attempted: true, done: true, created: false, reason: "YA_EXISTE", linkedSocid: socid };
    }
  }

  try {
    const payload = {
      firstname: desired.firstname,
      lastname: desired.lastname,
      phone: desired.phone,
      email: desired.email,
    };

    if (socid && Number.isFinite(socid)) {
      payload.fk_soc = socid;
      payload.socid = socid;
    }

    const r = await apiClient.post(endpoints.contactsEndpoint, payload);

    return {
      attempted: true,
      done: true,
      created: true,
      contactId: r?.data ?? null,
      linkedSocid: socid ?? null,
    };
  } catch {
    return { attempted: true, done: true, created: false, reason: "ERROR_CREANDO" };
  }
}

async function createOpportunityIfRequested({ body, tercero, note, user }) {
  const wants = firstNonEmpty(body, [
    "ventas_oportunidad/quiere_oportunidad",
    "ventas_oportunidad.quiere_oportunidad",
    "quiere_oportunidad",
    "dolibarr/quiere_oportunidad",
    "dolibarr.quiere_oportunidad",
  ]);

  if (!parseYesNo(wants)) {
    return { attempted: false, done: false, created: false, reason: "NO_SOLICITADO" };
  }

  if (!endpoints?.projectsEndpoint) {
    return { attempted: true, done: false, created: false, reason: "SIN_ENDPOINT_PROJECTS" };
  }

  const title =
    firstNonEmpty(body, [
      "ventas_oportunidad/oportunidad_titulo",
      "ventas_oportunidad.oportunidad_titulo",
      "oportunidad_titulo",
      "dolibarr/oportunidad_titulo",
      "dolibarr.oportunidad_titulo",
    ]) || note || "Oportunidad";

  const statusLabel = firstNonEmpty(body, [
    "ventas_oportunidad/oportunidad_estado",
    "ventas_oportunidad.oportunidad_estado",
    "oportunidad_estado",
    "dolibarr/oportunidad_estado",
    "dolibarr.oportunidad_estado",
  ]);

  const dateEndRaw = firstNonEmpty(body, [
    "ventas_oportunidad/oportunidad_fecha_final",
    "ventas_oportunidad.oportunidad_fecha_final",
    "oportunidad_fecha_final",
    "dolibarr/oportunidad_fecha_final",
    "dolibarr.oportunidad_fecha_final",
    "fecha_fin",
    "end",
  ]);

  const oppAmount = firstNonEmpty(body, [
    "ventas_oportunidad/oportunidad_importe",
    "ventas_oportunidad.oportunidad_importe",
    "oportunidad_importe",
    "dolibarr/oportunidad_importe",
    "dolibarr.oportunidad_importe",
  ]);

  const budgetAmount = firstNonEmpty(body, [
    "ventas_oportunidad/oportunidad_presupuesto",
    "ventas_oportunidad.oportunidad_presupuesto",
    "oportunidad_presupuesto",
    "dolibarr/oportunidad_presupuesto",
    "dolibarr.oportunidad_presupuesto",
    "presupuesto",
  ]);

  const fkOppStatus = mapOpportunityStatusToId(statusLabel);
  const nowUnix = getCurrentUnixTime();
  const dateEndUnix = parseDateFlexibleToUnix(dateEndRaw);
  const generatedRef = generateOpportunityRef();

  try {
    const payload = {
      ref: generatedRef,
      title: String(title).trim(),
      description: String(note ?? "").trim(),
      status: 1,
      usage_opportunity: 1,
      date_start: nowUnix,
    };

    if (tercero?.id) payload.socid = Number(tercero.id);
    if (fkOppStatus != null) payload.opp_status = fkOppStatus;
    if (dateEndUnix) payload.date_end = dateEndUnix;
    if (oppAmount != null && String(oppAmount).trim() !== "") {
      payload.opp_amount = String(oppAmount).trim();
    }
    if (budgetAmount != null && String(budgetAmount).trim() !== "") {
      payload.budget_amount = String(budgetAmount).trim();
    }
    if (user?.id) payload.user_resp_id = Number(user.id);

    const r = await apiClient.post(endpoints.projectsEndpoint, payload);

    return {
      attempted: true,
      done: true,
      created: true,
      projectId: r?.data ?? null,
      ref: generatedRef,
      oppStatusId: fkOppStatus ?? null,
      dateStartUnix: nowUnix,
      dateEndUnix: dateEndUnix ?? null,
    };
  } catch {
    return { attempted: true, done: true, created: false, reason: "ERROR_CREANDO_OPORTUNIDAD" };
  }
}

export async function crearVisita(req, res) {
  const rid = crypto.randomUUID();

  const result = {
    rid,
    status: "PROCESADO",
    terceroModo: "SIN_CLIENTE",
    thirdpartyId: null,
    thirdpartyName: null,
    thirdpartyRef: null,
    nombreCliente: null,
    numeroCliente: null,
    asesorLogin: null,
    userId: null,
    userLogin: null,
    location: null,
    clienteNuevo: null,
    contact: null,
    oportunidad: null,
    event: null,
    warnings: [],
    errors: [],
  };

  try {
    const body = req.body || {};

    const clienteTipo = firstNonEmpty(body, [
      "dolibarr/cliente_nuevo",
      "dolibarr.cliente_nuevo",
      "cliente_nuevo",
      "tipo_cliente",
    ]);

    const preguntaCodigo = firstNonEmpty(body, [
      "dolibarr/pregunta_codigo",
      "dolibarr.pregunta_codigo",
      "pregunta_codigo",
      "dolibarr/tiene_ref",
      "dolibarr.tiene_ref",
      "tiene_ref",
    ]);

    const thirdpartyRef = firstNonEmpty(body, [
      "dolibarr/thirdparty_ref",
      "dolibarr.thirdparty_ref",
      "thirdparty_ref",
      "dolibarr/thirdparty_ref_001",
      "dolibarr.thirdparty_ref_001",
      "thirdparty_ref_001",
      "codigo_cliente",
      "cliente_codigo",
    ]);

    const nombreCliente = firstNonEmpty(body, [
      "dolibarr/nombre_cliente",
      "dolibarr.nombre_cliente",
      "nombre_cliente",
      "cliente_nombre",
      "nom",
    ]);

    const numeroCliente = firstNonEmpty(body, [
      "dolibarr/numero_cliente",
      "dolibarr.numero_cliente",
      "numero_cliente",
      "telefono_cliente",
    ]);

    const nombreClienteNuevo = firstNonEmpty(body, [
      "dolibarr/nombre_cliente_nuevo",
      "dolibarr.nombre_cliente_nuevo",
      "nombre_cliente_nuevo",
      "nuevo_nombre_cliente",
      "name",
    ]);

    const correoClienteNuevo = firstNonEmpty(body, [
      "dolibarr/correo_cliente_nuevo",
      "dolibarr.correo_cliente_nuevo",
      "correo_cliente_nuevo",
      "nuevo_correo_cliente",
      "email",
    ]);

    const numeroClienteNuevo = firstNonEmpty(body, [
      "dolibarr/numero_cliente_nuevo",
      "dolibarr.numero_cliente_nuevo",
      "numero_cliente_nuevo",
      "nuevo_numero_cliente",
      "phone",
    ]);

    const asesorLogin = firstNonEmpty(body, [
      "dolibarr/asesor_login",
      "dolibarr.asesor_login",
      "asesor_login",
      "login",
    ]);

    const note =
      firstNonEmpty(body, [
        "dolibarr/descripcion",
        "dolibarr.descripcion",
        "descripcion",
        "note",
      ]) || "";

    const ubicacionRaw = firstNonEmpty(body, [
      "datos_visita/ubicacion_gps",
      "datos_visita.ubicacion_gps",
      "ubicacion_gps",
      "_geolocation",
      "ubicacion",
      "gps_inicio",
    ]);

    result.thirdpartyRef = thirdpartyRef || null;
    result.nombreCliente = nombreCliente || nombreClienteNuevo || null;
    result.numeroCliente = numeroCliente || numeroClienteNuevo || null;
    result.asesorLogin = asesorLogin || null;

    if (!asesorLogin) {
      result.errors.push("SIN asesor_login");
      return res.status(200).json(result);
    }

    const user = await findUserByLogin(asesorLogin);
    if (!user) {
      result.errors.push("USUARIO NO EXISTE (login exacto)");
      return res.status(200).json(result);
    }

    result.userId = user.id ?? null;
    result.userLogin = user.login ?? null;

    let locationText = firstNonEmpty(body, [
      "ubicacion_texto",
      "ubicacion_direccion",
      "direccion",
      "location_text",
    ]);

    if (!locationText) {
      const gp = parseGeoPoint(ubicacionRaw);
      if (gp) {
        try {
          locationText = (await reverseGeocode(gp.lat, gp.lon)) || null;
        } catch {
          result.warnings.push("ERROR_REVERSE_GEOCODE");
        }
      }
    }

    result.location = truncate128(locationText);

    let tercero = null;
    let terceroModo = "SIN_CLIENTE";

    try {
      if (isClienteNuevo(clienteTipo)) {
        result.clienteNuevo = await createThirdpartyIfNew({
          clienteTipo,
          nombreClienteNuevo,
          correoClienteNuevo,
          numeroClienteNuevo,
          locationText,
          user,
        });

        if (result.clienteNuevo?.tercero) {
          tercero = result.clienteNuevo.tercero;
          terceroModo = result.clienteNuevo.created
            ? "CLIENTE_NUEVO_CREADO"
            : "CLIENTE_NUEVO_YA_EXISTIA";
        } else if (result.clienteNuevo?.attempted) {
          result.warnings.push(result.clienteNuevo.reason || "FALLO_CREAR_CLIENTE_NUEVO");
        }
      } else {
        if (parseYesNo(preguntaCodigo) && thirdpartyRef) {
          tercero = await findThirdpartyByRef(thirdpartyRef);
          if (tercero) {
            terceroModo = "ASOCIADO_POR_CODIGO";
          } else {
            result.warnings.push("CLIENTE_NO_ENCONTRADO_POR_CODIGO");
          }
        } else if (nombreCliente) {
          tercero = await findThirdpartyByNameSmart(nombreCliente);
          if (tercero) {
            terceroModo = "ASOCIADO_POR_NOMBRE_PARCIAL";
          } else {
            result.warnings.push("CLIENTE_NO_ENCONTRADO_POR_NOMBRE");
          }
        } else {
          result.warnings.push("SIN_DATOS_DE_CLIENTE");
        }
      }
    } catch {
      result.errors.push("ERROR_FLUJO_TERCERO");
    }

    result.terceroModo = terceroModo;
    result.thirdpartyId = tercero?.id ?? null;
    result.thirdpartyName = tercero ? tercero.nom || tercero.name || tercero.ref || null : null;

    try {
      result.contact = await ensureContactIfRequested({ body, tercero });
      if (result.contact?.reason && !result.contact?.created && result.contact?.attempted) {
        result.warnings.push(`CONTACT_${result.contact.reason}`);
      }
    } catch {
      result.errors.push("ERROR_FLUJO_CONTACTO");
    }

    try {
      result.oportunidad = await createOpportunityIfRequested({ body, tercero, note, user });
      if (
        result.oportunidad?.reason &&
        !result.oportunidad?.created &&
        result.oportunidad?.attempted
      ) {
        result.warnings.push(`OPORTUNIDAD_${result.oportunidad.reason}`);
      }
    } catch {
      result.errors.push("ERROR_FLUJO_OPORTUNIDAD");
    }

    try {
      const now = getCurrentUnixTime();

      const agendaPayload = {
        userownerid: Number(user.id),
        type_code: "AC_RDV",
        label: "Visita de ventas",
        note,
        datep: now,
        datef: now,
        location: truncate128(locationText),
      };

      if (tercero?.id) agendaPayload.socid = Number(tercero.id);

      const created = await apiClient.post(endpoints.agendaEventsEndpoint, agendaPayload);

      result.event = {
        created: true,
        eventId: created.data,
      };

      if (terceroModo === "SIN_CLIENTE") {
        try {
          const to = await getEmailForSubmitter({ body, user, rid, firstNonEmpty });

          if (to) {
            await sendNoClientEmail(to, {
              userLogin: user.login,
              eventId: created.data,
              nombreCliente: nombreCliente || nombreClienteNuevo,
              thirdpartyRef,
            });
          } else {
            result.warnings.push("NO_HAY_EMAIL_PARA_NOTIFICAR");
          }
        } catch {
          result.warnings.push("ERROR_ENVIO_CORREO");
        }
      }
    } catch {
      result.event = { created: false, reason: "ERROR_CREANDO_EVENTO" };
      result.errors.push("ERROR_FLUJO_EVENTO");
    }

    result.status = result.event?.created ? "VISITA CREADA" : "PROCESADO_CON_ERRORES";

    return res.status(200).json(result);
  } catch (error) {
    const data = error?.response?.data;

    return res.status(500).json({
      rid,
      status: "ERROR_GENERAL",
      error: data || error.message || String(error),
    });
  }
}