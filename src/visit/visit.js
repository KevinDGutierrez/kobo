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
    const best = r?.data?.results?.[0]?.formatted_address || null;
    return best;
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
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.code_client:=:${encodeURIComponent(
      target
    )})`;
    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const exact = list.find((t) => norm(t?.code_client) === target || norm(t?.ref) === target);
    if (exact) return exact;
  } catch (e) {
    console.log(
      `[VISIT ${rid}] thirdparty search(ref sql) ERROR:`,
      e?.response?.status,
      JSON.stringify(e?.response?.data || e.message)
    );
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
    } catch (e) {
      console.log(
        `[VISIT ${rid}] thirdparty paging(ref) ERROR:`,
        e?.response?.status,
        JSON.stringify(e?.response?.data || e.message)
      );
      return null;
    }
  }
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
    const scored = list
      .map((t) => {
        const n = String(t?.nom ?? t?.name ?? "").trim();
        return { t, name: n, score: scoreByQuery(n, query) };
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
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.nom:like:${encodeURIComponent(
      like
    )})`;
    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const best = pickBest(list);
    if (best) return best;
  } catch (e) {
    console.log(
      `[VISIT ${rid}] thirdparty search(name like) ERROR:`,
      e?.response?.status,
      JSON.stringify(e?.response?.data || e.message)
    );
    if (e?.response?.status === 404) return null;
  }

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
    } catch (e) {
      console.log(
        `[VISIT ${rid}] thirdparty paging(name) ERROR:`,
        e?.response?.status,
        JSON.stringify(e?.response?.data || e.message)
      );
      return null;
    }
  }

  const MIN = 250;
  const GAP = 120;
  if (bestScore >= MIN && bestScore - secondBest >= GAP) return best;

  return null;
}

async function findUserByLogin(login, rid) {
  const target = normLogin(login);

  try {
    const url = `${endpoints.usersEndpoint}?sqlfilters=(t.login:=:${encodeURIComponent(login)})`;
    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const exact = list.find((u) => normLogin(u?.login) === target);
    if (exact) return exact;
  } catch (e) {
    console.log(
      `[VISIT ${rid}] user search(sql) ERROR:`,
      e?.response?.status,
      JSON.stringify(e?.response?.data || e.message)
    );
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
    } catch (e) {
      console.log(
        `[VISIT ${rid}] user paging ERROR:`,
        e?.response?.status,
        JSON.stringify(e?.response?.data || e.message)
      );
      return null;
    }
  }
}

function parseYesNo(v) {
  const s = String(v ?? "").trim().toLowerCase();
  return s === "si" || s === "sí" || s === "yes" || s === "true" || s === "1";
}

function normalizePhone(p) {
  return String(p ?? "").replace(/[^\d+]/g, "").trim();
}

function normEmail(e) {
  return String(e ?? "").trim().toLowerCase();
}

async function listContactsBySocid(socid, rid) {
  const limit = 100;
  let page = 0;
  const all = [];

  while (true) {
    try {
      const url = `${endpoints.contactsEndpoint}?sqlfilters=(fk_soc:=:${socid})`;
      const res = await apiClient.get(url, { params: { limit, page } });
      const list = asArray(res.data);
      if (!list.length) break;
      all.push(...list);

      if (list.length < limit) break;
      page++;
      if (page > 50) break;
    } catch (e) {
      if (e?.response?.status === 404) return [];
      console.log(
        `[VISIT ${rid}] contacts list ERROR:`,
        e?.response?.status,
        JSON.stringify(e?.response?.data || e.message)
      );
      return [];
    }
  }

  return all;
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

async function ensureContactIfRequested({ body, tercero, rid }) {
  if (!tercero?.id) return { done: false, reason: "NO_TERCERO" };
  if (!endpoints?.contactsEndpoint) return { done: true, created: false, reason: "SIN_ENDPOINT_CONTACTS" };

  const wants = firstNonEmpty(body, [
    "contacto_cliente_00",
    "dolibarr/contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "datos_visita/contacto_cliente_00",
    "datos_visita.contacto_cliente_00",
    "datos_para_dolibarr/contacto_cliente_00",
    "datos_para_dolibarr.contacto_cliente_00",
  ]);

  if (!parseYesNo(wants)) return { done: false, reason: "NO_SOLICITADO" };

  const firstname = firstNonEmpty(body, [
    "nombre_contacto",
    "datos_persona/nombre_contacto",
    "datos_persona.nombre_contacto",
    "dolibarr/nombre_contacto",
    "dolibarr.nombre_contacto",
  ]);

  const lastname = firstNonEmpty(body, [
    "apellido_contacto",
    "datos_persona/apellido_contacto",
    "datos_persona.apellido_contacto",
    "dolibarr/apellido_contacto",
    "dolibarr.apellido_contacto",
  ]);

  const phone = firstNonEmpty(body, [
    "numero_contacto",
    "datos_persona/numero_contacto",
    "datos_persona.numero_contacto",
    "dolibarr/numero_contacto",
    "dolibarr.numero_contacto",
  ]);

  const email = firstNonEmpty(body, [
    "correo_contacto",
    "datos_persona/correo_contacto",
    "datos_persona.correo_contacto",
    "dolibarr/correo_contacto",
    "dolibarr.correo_contacto",
  ]);

  const hasSomething =
    String(firstname ?? "").trim() ||
    String(lastname ?? "").trim() ||
    String(phone ?? "").trim() ||
    String(email ?? "").trim();

  if (!hasSomething) return { done: false, reason: "SIN_DATOS_CONTACTO" };

  const socid = Number(tercero.id);
  const desired = {
    firstname: String(firstname ?? "").trim(),
    lastname: String(lastname ?? "").trim(),
    phone: String(phone ?? "").trim(),
    email: String(email ?? "").trim(),
  };

  const contacts = await listContactsBySocid(socid, rid);
  const exists = contacts.some((c) => contactMatches(c, desired));
  if (exists) return { done: true, created: false, reason: "YA_EXISTE" };

  try {
    const payload = {
      fk_soc: socid,
      socid: socid,
      firstname: desired.firstname,
      lastname: desired.lastname || "N/D",
      phone: desired.phone,
      email: desired.email,
    };

    const r = await apiClient.post(endpoints.contactsEndpoint, payload);
    return { done: true, created: true, contactId: r?.data ?? null };
  } catch (e) {
    console.log(
      `[VISIT ${rid}] create contact ERROR:`,
      e?.response?.status,
      JSON.stringify(e?.response?.data || e.message)
    );
    return { done: true, created: false, reason: "ERROR_CREANDO" };
  }
}

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
      "code_client",
      "cliente_codigo",
      "datos_para_dolibarr/codigo_cliente",
      "datos_para_dolibarr.codigo_cliente",
      "dolibarr/codigo_cliente",
      "dolibarr.codigo_cliente",
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

    const label = "Visita de ventas";

    const note =
      firstNonEmpty(body, ["note", "descripcion", "dolibarr/descripcion", "dolibarr.descripcion"]) ||
      "";

    const ubicacionRaw = firstNonEmpty(body, [
      "ubicacion_gps",
      "gps_inicio",
      "ubicacion",
      "_geolocation",
    ]);

    if (!asesorLogin) return res.status(200).json({ status: "SIN asesor_login" });

    const user = await findUserByLogin(asesorLogin, rid);
    if (!user) return res.status(200).json({ asesorLogin, status: "USUARIO NO EXISTE (login exacto)" });

    let tercero = null;
    let terceroModo = "SIN_CLIENTE";

    if (thirdpartyRef) {
      tercero = await findThirdpartyByRef(thirdpartyRef, rid);
      if (tercero) terceroModo = "ASOCIADO_POR_CODIGO";
    } else if (nombreCliente) {
      tercero = await findThirdpartyByNameSmart(nombreCliente, rid);
      if (tercero) terceroModo = "ASOCIADO_POR_NOMBRE_PARCIAL";
    }

    const contactResult = await ensureContactIfRequested({ body, tercero, rid });

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
          const addr = await reverseGeocode(gp.lat, gp.lon);
          locationText = addr || null;
        } catch (e) {
          console.log(`[VISIT ${rid}] reverseGeocode ERROR:`, e?.message || String(e));
        }
      }
    }

    const now = Math.floor(Date.now() / 1000);

    const payload = {
      userownerid: Number(user.id),
      type_code: "AC_RDV",
      label,
      note,
      datep: now,
      datef: now,
      location: truncate128(locationText),
    };

    if (tercero?.id) payload.socid = Number(tercero.id);

    const created = await apiClient.post(endpoints.agendaEventsEndpoint, payload);

    if (terceroModo === "SIN_CLIENTE") {
      try {
        const to = await getEmailForSubmitter({ body, user, rid, firstNonEmpty });

        if (to) {
          await sendNoClientEmail(to, {
            userLogin: user.login,
            eventId: created.data,
            nombreCliente,
            thirdpartyRef,
          });
        } else {
          console.log(`[VISIT ${rid}] SIN_CLIENTE pero no hay email para notificar`);
        }
      } catch (e) {
        console.log(`[VISIT ${rid}] sendNoClientEmail ERROR:`, e?.message || String(e));
      }
    }

    return res.status(200).json({
      status: "VISITA CREADA",
      eventId: created.data,
      terceroModo,
      thirdpartyId: tercero?.id ?? null,
      thirdpartyName: tercero ? (tercero.nom || tercero.name || tercero.ref || null) : null,
      thirdpartyRef: thirdpartyRef || null,
      nombreCliente: nombreCliente || null,
      asesorLogin,
      userId: user.id,
      userLogin: user.login,
      location: payload.location,
      contact: contactResult,
    });
  } catch (error) {
    const status = error?.response?.status;
    const data = error?.response?.data;

    console.log(`[VISIT ${rid}] ERROR status=${status} message=${error?.message}`);
    if (data) console.log(`[VISIT ${rid}] ERROR data=`, JSON.stringify(data));

    return res.status(500).json({
      rid,
      error: data || error.message || String(error),
    });
  }
}