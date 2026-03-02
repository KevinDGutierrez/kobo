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

function norm(x) {
  return String(x ?? "").trim().toUpperCase();
}

function normLogin(x) {
  return String(x ?? "").trim().toLowerCase();
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

function logRid(rid, msg, obj) {
  if (!DEBUG) return;
  try {
    const safe = obj === undefined ? "" : ` ${JSON.stringify(obj).slice(0, RAW_LOG_MAX)}`;
    console.log(`[VISIT ${rid}] ${msg}${safe}`);
  } catch {
    console.log(`[VISIT ${rid}] ${msg}`);
  }
}

/* ================================
   TERCEROS
================================ */

async function findThirdpartyByRef(ref, rid) {
  const target = norm(ref);

  try {
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.code_client:=:${encodeURIComponent(
      target
    )})`;

    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const exact = list.find(
      (t) => norm(t?.code_client) === target || norm(t?.ref) === target
    );
    if (exact) return exact;
  } catch (e) {
    if (e?.response?.status === 404) return null;
  }

  const limit = 50;
  let page = 0;

  while (true) {
    try {
      const res = await apiClient.get(endpoints.thirdpartiesEndpoint, {
        params: { limit, page },
      });
      const list = asArray(res.data);
      if (!list.length) return null;

      const found = list.find(
        (t) => norm(t?.code_client) === target || norm(t?.ref) === target
      );
      if (found) return found;

      page++;
      if (page > 300) return null;
    } catch {
      return null;
    }
  }
}

/* ================================
   (AQUÍ VIENE TODO TU CÓDIGO
   DE BÚSQUEDA POR NOMBRE,
   USUARIOS, CONTACTOS ETC.
   — EXACTAMENTE IGUAL —
   NO LO REPITO PARA NO
   CORTAR POR LÍMITE DE MENSAJE)
================================ */

/* ================================
   FUNCIÓN PRINCIPAL
================================ */

export async function crearVisita(req, res) {
  const rid = crypto.randomUUID();

  try {
    const body = req.body || {};

    const thirdpartyRef = firstNonEmpty(body, [
      "thirdparty_ref",
      "tercero_ref",
      "dolibarr/thirdparty_ref",
      "dolibarr/tercero_ref",
      "dolibarr.thirdparty_ref",
      "dolibarr.tercero_ref",
      "datos_visita/thirdparty_ref",
      "datos_visita/tercero_ref",
      "datos_visita.thirdparty_ref",
      "datos_visita.tercero_ref",
      "codigo_cliente",
      "codigo_del_cliente",
      "cliente_codigo",
      "code_client",
      "dolibarr/codigo_cliente",
      "dolibarr.codigo_cliente",
      "datos_para_dolibarr/codigo_cliente",
      "datos_para_dolibarr.codigo_cliente",
    ]);

    const nombreCliente = firstNonEmpty(body, [
      "nombre_cliente",
      "cliente_nombre",
      "nom",
      "dolibarr/nombre_cliente",
      "dolibarr/nom",
      "dolibarr.nombre_cliente",
      "dolibarr.nom",
      "datos_visita/nombre_cliente",
      "datos_visita.nombre_cliente",
    ]);

    const asesorLogin = firstNonEmpty(body, [
      "asesor_login",
      "login",
      "dolibarr/asesor_login",
      "dolibarr/login",
      "dolibarr.asesor_login",
      "dolibarr.login",
      "datos_visita/asesor_login",
      "datos_visita/login",
      "datos_visita.asesor_login",
      "datos_visita.login",
    ]);

    if (!asesorLogin)
      return res.status(200).json({ status: "SIN asesor_login" });

    const user = await findUserByLogin(asesorLogin, rid);
    if (!user)
      return res
        .status(200)
        .json({ asesorLogin, status: "USUARIO NO EXISTE (login exacto)" });

    let tercero = null;
    let terceroModo = "SIN_CLIENTE";

    /* 🔥 CORRECCIÓN APLICADA AQUÍ */
    if (thirdpartyRef) {
      tercero = await findThirdpartyByRef(thirdpartyRef, rid);

      if (tercero) {
        terceroModo = "ASOCIADO_POR_CODIGO";
      } else if (nombreCliente) {
        tercero = await findThirdpartyByNameSmart(nombreCliente, rid);
        if (tercero)
          terceroModo = "ASOCIADO_POR_NOMBRE_PARCIAL_FALLBACK";
      }
    } else if (nombreCliente) {
      tercero = await findThirdpartyByNameSmart(nombreCliente, rid);
      if (tercero)
        terceroModo = "ASOCIADO_POR_NOMBRE_PARCIAL";
    }

    /* RESTO DE TU FUNCIÓN EXACTAMENTE IGUAL */

    return res.status(200).json({
      status: "VISITA CREADA",
      terceroModo,
      thirdpartyId: tercero?.id ?? null,
      asesorLogin,
      userId: user.id,
    });
  } catch (error) {
    return res.status(500).json({
      rid,
      error: error?.response?.data || error.message || String(error),
    });
  }
}