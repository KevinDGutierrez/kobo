import crypto from "crypto";
import axios from "axios";
import { apiClient, endpoints } from "../service/api.js";
import { sendNoClientEmail, getEmailForSubmitter } from "../service/email-sender.js";

const DEBUG = process.env.DEBUG_VISIT === "1";
const RAW_LOG_MAX = 800;


function asArray(data) {
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.rows)) return data.rows;
  return [];
}

function norm(x) { return String(x ?? "").trim().toUpperCase(); }
function normLogin(x) { return String(x ?? "").trim().toLowerCase(); }

function getValue(obj, key) {
  if (!obj) return undefined;
  if (Object.prototype.hasOwnProperty.call(obj, key)) return obj[key];
  if (key.includes("/")) { 
    return key.split("/").reduce((acc, k) => (acc && acc[k] !== undefined ? acc[k] : undefined), obj);
  }
  if (key.includes(".")) {
    return key.split(".").reduce((acc, k) => (acc && acc[k] !== undefined ? acc[k] : undefined), obj);
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

function truncate128(s) {
  const x = String(s ?? "").trim();
  if (!x) return "";
  return x.length > 128 ? x.slice(0, 128) : x;
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
  } catch { return null; }
}

function buildGtAddress(addr) {
  if (!addr) return null;
  const road = addr.road || addr.pedestrian || addr.footway || addr.path || "";
  const house = addr.house_number || "";
  const suburb = addr.neighbourhood || addr.suburb || addr.quarter || "";
  const city = addr.city || addr.town || addr.village || addr.municipality || "Ciudad de Guatemala";
  const state = addr.state || "Guatemala";
  const zona = pickBestZona(addr);
  const line1 = uniqJoin([road, house], " ").trim();
  const parts = [line1, zona, suburb && (!zona || suburb.toLowerCase() !== zona.toLowerCase()) ? suburb : null, city, state];
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

async function findThirdpartyByRef(ref, rid) {
  const target = norm(ref);
  try {
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.code_client:=:${encodeURIComponent(target)})`;
    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const exact = list.find((t) => norm(t?.code_client) === target || norm(t?.ref) === target);
    if (exact) return exact;
  } catch (e) { if (e?.response?.status !== 404) console.log(`[VISIT ${rid}] Ref search error`, e.message); }

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
    } catch (e) { return null; }
  }
}

function normText(s) {
  return String(s ?? "").toLowerCase().trim().replace(/[^\p{L}\p{N}\s]/gu, " ").replace(/\s+/g, " ").trim();
}

function splitTokens(s) {
  const t = normText(s);
  return t ? t.split(" ").filter(Boolean) : [];
}

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

async function findThirdpartyByNameSmart(nombre, rid) {
  const query = String(nombre ?? "").trim();
  if (!query) return null;
  function pickBest(list) {
    const scored = list.map((t) => {
      const n = String(t?.nom ?? t?.name ?? "").trim();
      return { t, name: n, score: scoreByQuery(n, query) };
    }).filter((x) => x.score > 0).sort((a, b) => b.score - a.score);
    if (!scored.length) return null;
    const best = scored[0];
    const second = scored[1];
    if (best.score < 250) return null;
    if (second && best.score - second.score < 120) return null;
    return best.t;
  }
  try {
    const like = `%${query}%`;
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.nom:like:${encodeURIComponent(like)})`;
    const res = await apiClient.get(url);
    const best = pickBest(asArray(res.data));
    if (best) return best;
  } catch (e) { }

  let page = 0;
  let best = null; let bestScore = 0; let secondBest = 0;
  while (true) {
    try {
      const res = await apiClient.get(endpoints.thirdpartiesEndpoint, { params: { limit: 50, page } });
      const list = asArray(res.data);
      if (!list.length) break;
      for (const t of list) {
        const s = scoreByQuery(String(t?.nom ?? t?.name ?? ""), query);
        if (s > bestScore) { secondBest = bestScore; bestScore = s; best = t; }
        else if (s > secondBest) { secondBest = s; }
      }
      page++; if (page > 300) break;
    } catch (e) { break; }
  }
  if (bestScore >= 250 && bestScore - secondBest >= 120) return best;
  return null;
}

async function findUserByLogin(login, rid) {
  const target = normLogin(login);
  try {
    const url = `${endpoints.usersEndpoint}?sqlfilters=(t.login:=:${encodeURIComponent(login)})`;
    const res = await apiClient.get(url);
    const exact = asArray(res.data).find((u) => normLogin(u?.login) === target);
    if (exact) return exact;
  } catch (e) { }
  let page = 0;
  while (true) {
    try {
      const res = await apiClient.get(endpoints.usersEndpoint, { params: { limit: 50, page } });
      const list = asArray(res.data);
      if (!list.length) return null;
      const found = list.find((u) => normLogin(u?.login) === target);
      if (found) return found;
      page++; if (page > 300) return null;
    } catch (e) { return null; }
  }
}

async function crearContactoAdicional(body, terceroId, rid) {
  const wants = firstNonEmpty(body, ["contacto_cliente_00", "dolibarr/contacto_cliente_00", "DATOS_PARA_DOLIBARR/contacto_cliente_00"]);
  if (!String(wants ?? "").toLowerCase().startsWith("s")) return { status: "NO_SOLICITADO" };

  const payload = {
    socid: terceroId || 0,
    firstname: firstNonEmpty(body, ["nombre_contacto", "datos_persona/nombre_contacto"]),
    lastname: firstNonEmpty(body, ["apellido_contacto", "datos_persona/apellido_contacto"]) || "N/D",
    phone: firstNonEmpty(body, ["numero_contacto", "datos_persona/numero_contacto"]),
    email: firstNonEmpty(body, ["correo_contacto", "datos_persona/correo_contacto"]),
  };

  try {
    const created = await apiClient.post(endpoints.contactsEndpoint, payload);
    return { status: "CREADO", id: created.data };
  } catch (e) {
    console.log(`[VISIT ${rid}] Error creando contacto:`, e.message);
    return { status: "ERROR" };
  }
}

export async function crearVisita(req, res) {
  const rid = crypto.randomUUID();
  try {
    const body = req.body || {};

    const thirdpartyRef = firstNonEmpty(body, ["thirdparty_ref_001", "DATOS_PARA_DOLIBARR/thirdparty_ref_001", "thirdparty_ref"]);
    const nombreCliente = firstNonEmpty(body, ["nombre_cliente", "DATOS_PARA_DOLIBARR/nombre_cliente"]);
    const asesorLogin = firstNonEmpty(body, ["asesor_login", "DATOS_PARA_DOLIBARR/asesor_login"]);
    const note = firstNonEmpty(body, ["descripcion", "DATOS_PARA_DOLIBARR/descripcion"]) || "";
    const ubicacionRaw = firstNonEmpty(body, ["ubicacion_gps", "_geolocation"]);

    if (!asesorLogin) return res.status(200).json({ status: "SIN asesor_login" });

    const user = await findUserByLogin(asesorLogin, rid);
    if (!user) return res.status(200).json({ status: "USUARIO NO EXISTE", asesorLogin });

    let tercero = null;
    let terceroModo = "SIN_CLIENTE";

    if (thirdpartyRef) {
      tercero = await findThirdpartyByRef(thirdpartyRef, rid);
      if (tercero) terceroModo = "ASOCIADO_POR_CODIGO";
    } 
    if (!tercero && nombreCliente) {
      tercero = await findThirdpartyByNameSmart(nombreCliente, rid);
      if (tercero) terceroModo = "ASOCIADO_POR_NOMBRE_PARCIAL";
    }

    if (!tercero) {
      try {
        const mailTo = await getEmailForSubmitter(body) || process.env.ADMIN_EMAIL;
        await sendNoClientEmail({
          to: mailTo,
          cliente: nombreCliente || "Desconocido",
          ref: thirdpartyRef || "N/A",
          vendedor: asesorLogin,
          rid
        });
      } catch (e) { console.log(`[VISIT ${rid}] Error mail:`, e.message); }
    }

    const contactoInfo = await crearContactoAdicional(body, tercero?.id, rid);

    let locationText = firstNonEmpty(body, ["ubicacion_texto", "direccion"]);
    if (!locationText) {
      const gp = parseGeoPoint(ubicacionRaw);
      if (gp) {
        const addr = await reverseGeocode(gp.lat, gp.lon);
        locationText = addr;
      }
    }

    const now = Math.floor(Date.now() / 1000);
    const payloadVisita = {
      userownerid: Number(user.id),
      type_code: "AC_RDV",
      label: "Visita de ventas",
      note,
      datep: now,
      datef: now,
      location: truncate128(locationText),
      socid: tercero?.id ? Number(tercero.id) : null
    };

    const created = await apiClient.post(endpoints.agendaEventsEndpoint, payloadVisita);

    return res.status(200).json({
      status: "VISITA CREADA",
      eventId: created.data,
      terceroModo,
      thirdpartyId: tercero?.id ?? null,
      contactoStatus: contactoInfo.status,
      contactoId: contactoInfo.id ?? null,
      asesorLogin,
      location: payloadVisita.location
    });

  } catch (error) {
    console.log(`[VISIT ${rid}] ERROR:`, error.response?.data || error.message);
    return res.status(500).json({ error: error.message, rid });
  }
}