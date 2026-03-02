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

function logRid(rid, msg, obj) {
  if (!DEBUG) return;
  try {
    const safe = obj === undefined ? "" : ` ${JSON.stringify(obj).slice(0, RAW_LOG_MAX)}`;
    console.log(`[VISIT ${rid}] ${msg}${safe}`);
  } catch {
    console.log(`[VISIT ${rid}] ${msg}`);
  }
}

async function findThirdpartyByRef(ref, rid) {
  const target = norm(ref);

  try {
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.code_client:=:${encodeURIComponent(
      target
    )})`;
    logRid(rid, "thirdparty search(sql) ->", { url, target });

    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const exact = list.find((t) => norm(t?.code_client) === target || norm(t?.ref) === target);

    logRid(rid, "thirdparty search(sql) result", {
      rows: list.length,
      found: Boolean(exact),
      foundId: exact?.id ?? null,
      foundCode: exact?.code_client ?? null,
      foundRef: exact?.ref ?? null,
    });

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
      logRid(rid, "thirdparty paging(ref) page", { page, limit });

      const res = await apiClient.get(endpoints.thirdpartiesEndpoint, { params: { limit, page } });
      const list = asArray(res.data);
      if (!list.length) return null;

      const found = list.find((t) => norm(t?.code_client) === target || norm(t?.ref) === target);
      if (found) {
        logRid(rid, "thirdparty paging(ref) FOUND", {
          id: found?.id,
          code_client: found?.code_client,
          ref: found?.ref,
        });
        return found;
      }

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
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.nom:like:${encodeURIComponent(like)})`;
    logRid(rid, "thirdparty search(name like) ->", { url });

    const res = await apiClient.get(url);
    const list = asArray(res.data);

    const best = pickBest(list);
    logRid(rid, "thirdparty search(name like) result", {
      rows: list.length,
      found: Boolean(best),
      foundId: best?.id ?? null,
    });

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
      logRid(rid, "thirdparty paging(name) page", { page, limit });

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
  const ok = bestScore >= MIN && bestScore - secondBest >= GAP;
  logRid(rid, "thirdparty paging(name) best", {
    ok,
    bestScore,
    secondBest,
    bestId: best?.id ?? null,
  });

  if (ok) return best;
  return null;
}

async function findUserByLogin(login, rid) {
  const target = normLogin(login);

  try {
    const url = `${endpoints.usersEndpoint}?sqlfilters=(t.login:=:${encodeURIComponent(login)})`;
    logRid(rid, "user search(sql) ->", { url, login });

    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const exact = list.find((u) => normLogin(u?.login) === target);

    logRid(rid, "user search(sql) result", {
      rows: list.length,
      found: Boolean(exact),
      userId: exact?.id ?? null,
    });

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
      logRid(rid, "user paging page", { page, limit });

      const res = await apiClient.get(endpoints.usersEndpoint, { params: { limit, page } });
      const list = asArray(res.data);
      if (!list.length) return null;

      const found = list.find((u) => normLogin(u?.login) === target);
      if (found) {
        logRid(rid, "user paging FOUND", { id: found?.id, login: found?.login });
        return found;
      }

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
  if (!endpoints?.contactsEndpoint) return [];
  if (!socid || !Number.isFinite(Number(socid))) return [];

  try {
    const url = `${endpoints.contactsEndpoint}?sqlfilters=(fk_soc:=:${socid})`;
    logRid(rid, "contacts list ->", { url });

    const res = await apiClient.get(url);
    const list = asArray(res.data);
    logRid(rid, "contacts list result", { rows: list.length });

    return list;
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
  if (!endpoints?.contactsEndpoint) {
    logRid(rid, "contact skip", { reason: "SIN_ENDPOINT_CONTACTS" });
    return { done: true, created: false, reason: "SIN_ENDPOINT_CONTACTS" };
  }

  const wants = firstNonEmpty(body, [
    "contacto_cliente_00",
    "dolibarr/contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "datos_visita/contacto_cliente_00",
    "datos_visita.contacto_cliente_00",
    "dolibarr/contacto_cliente_00",
    "dolibarr.contacto_cliente_00",
    "contacto_cliente_00",
  ]);

  logRid(rid, "contact wants raw", { wants });

  if (!parseYesNo(wants)) {
    logRid(rid, "contact skip", { reason: "NO_SOLICITADO" });
    return { done: false, reason: "NO_SOLICITADO" };
  }

  const firstname = firstNonEmpty(body, [
    "datos_persona/nombre_contacto",
    "datos_persona.nombre_contacto",
    "nombre_contacto",
    "dolibarr/nombre_contacto",
    "dolibarr.nombre_contacto",
  ]);

  const lastname = firstNonEmpty(body, [
    "datos_persona/apellido_contacto",
    "datos_persona.apellido_contacto",
    "apellido_contacto",
    "dolibarr/apellido_contacto",
    "dolibarr.apellido_contacto",
  ]);

  const phone = firstNonEmpty(body, [
    "datos_persona/numero_contacto",
    "datos_persona.numero_contacto",
    "numero_contacto",
    "dolibarr/numero_contacto",
    "dolibarr.numero_contacto",
  ]);

  const email = firstNonEmpty(body, [
    "datos_persona/correo_contacto",
    "datos_persona.correo_contacto",
    "correo_contacto",
    "dolibarr/correo_contacto",
    "dolibarr.correo_contacto",
  ]);

  logRid(rid, "contact fields", { firstname, lastname, phone, email, terceroId: tercero?.id ?? null });

  const hasSomething =
    String(firstname ?? "").trim() ||
    String(lastname ?? "").trim() ||
    String(phone ?? "").trim() ||
    String(email ?? "").trim();

  if (!hasSomething) {
    logRid(rid, "contact skip", { reason: "SIN_DATOS_CONTACTO" });
    return { done: false, reason: "SIN_DATOS_CONTACTO" };
  }

  const desired = {
    firstname: String(firstname ?? "").trim(),
    lastname: String(lastname ?? "").trim() || "N/D",
    phone: String(phone ?? "").trim(),
    email: String(email ?? "").trim(),
  };

  const socid = tercero?.id ? Number(tercero.id) : null;

  if (socid && Number.isFinite(socid)) {
    const existing = await listContactsBySocid(socid, rid);
    const exists = existing.some((c) => contactMatches(c, desired));
    if (exists) {
      logRid(rid, "contact exists", { socid, reason: "YA_EXISTE" });
      return { done: true, created: false, reason: "YA_EXISTE" };
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

    logRid(rid, "contact create payload", payload);

    const r = await apiClient.post(endpoints.contactsEndpoint, payload);
    logRid(rid, "contact created", { contactId: r?.data ?? null });

    return { done: true, created: true, contactId: r?.data ?? null, linkedSocid: socid ?? null };
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

    const tieneRef = firstNonEmpty(body, [
      "dolibarr/tiene_ref",
      "dolibarr.tiene_ref",
      "tiene_ref",
    ]);

    const thirdpartyRef = firstNonEmpty(body, [
      "dolibarr/thirdparty_ref_001",
      "dolibarr.thirdparty_ref_001",
      "thirdparty_ref_001",
      "dolibarr/thirdparty_ref",
      "dolibarr.thirdparty_ref",
      "thirdparty_ref",
      "tercero_ref",
      "codigo_cliente",
      "codigo_del_cliente",
      "cliente_codigo",
      "code_client",
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

    logRid(rid, "IN keys", {
      asesorLogin,
      tieneRef,
      thirdpartyRef,
      nombreCliente,
      numeroCliente,
      contacto_cliente_00: firstNonEmpty(body, [
        "dolibarr/contacto_cliente_00",
        "dolibarr.contacto_cliente_00",
        "contacto_cliente_00",
      ]),
    });

    if (!asesorLogin) return res.status(200).json({ status: "SIN asesor_login" });

    const user = await findUserByLogin(asesorLogin, rid);
    if (!user) return res.status(200).json({ asesorLogin, status: "USUARIO NO EXISTE (login exacto)" });

    let tercero = null;
    let terceroModo = "SIN_CLIENTE";

    if (parseYesNo(tieneRef) && thirdpartyRef) {
      tercero = await findThirdpartyByRef(thirdpartyRef, rid);
      if (tercero) terceroModo = "ASOCIADO_POR_CODIGO";
    } else if (nombreCliente) {
      tercero = await findThirdpartyByNameSmart(nombreCliente, rid);
      if (tercero) terceroModo = "ASOCIADO_POR_NOMBRE_PARCIAL";
    }

    logRid(rid, "TERCERO result", {
      terceroModo,
      terceroId: tercero?.id ?? null,
      terceroRef: tercero?.ref ?? null,
      terceroCode: tercero?.code_client ?? null,
      terceroNom: tercero?.nom ?? tercero?.name ?? null,
    });

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
      label: "Visita de ventas",
      note,
      datep: now,
      datef: now,
      location: truncate128(locationText),
    };

    if (tercero?.id) payload.socid = Number(tercero.id);

    logRid(rid, "agenda payload", payload);

    const created = await apiClient.post(endpoints.agendaEventsEndpoint, payload);

    if (terceroModo === "SIN_CLIENTE") {
      try {
        const to = await getEmailForSubmitter({ body, user, rid, firstNonEmpty });
        logRid(rid, "email(to) resolve", { to });

        if (to) {
          await sendNoClientEmail(to, {
            userLogin: user.login,
            eventId: created.data,
            nombreCliente,
            thirdpartyRef,
            numeroCliente,
            contact: contactResult,
          });
          logRid(rid, "email SENT", { to, eventId: created.data });
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
      numeroCliente: numeroCliente || null,
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