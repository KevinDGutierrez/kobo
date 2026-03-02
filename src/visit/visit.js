import crypto from "crypto";
import axios from "axios";
import { apiClient, endpoints } from "../service/api.js";
import { sendNoClientEmail, getEmailForSubmitter } from "../service/email-sender.js";

const DEBUG = process.env.DEBUG_VISIT === "1" || true; // Forzamos debug para ver errores de mapeo
const RAW_LOG_MAX = 800;

// --- FUNCIONES DE UTILIDAD (TU LÓGICA ORIGINAL) ---
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
  if (key.includes("/")) { // Soporte para la estructura de Kobo con "/"
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

function logRid(rid, msg, obj) {
  if (!DEBUG) return;
  const safe = obj === undefined ? "" : ` ${JSON.stringify(obj).slice(0, RAW_LOG_MAX)}`;
  console.log(`[VISIT ${rid}] ${msg}${safe}`);
}

// --- FUNCIONES DE BÚSQUEDA (TU LÓGICA COMPLETA DE LA VERSIÓN 1) ---

async function findThirdpartyByRef(ref, rid) {
  const target = norm(ref);
  try {
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.code_client:=:${encodeURIComponent(target)})`;
    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const exact = list.find((t) => norm(t?.code_client) === target || norm(t?.ref) === target);
    if (exact) return exact;
  } catch (e) {
    if (e?.response?.status !== 404) console.log(`[VISIT ${rid}] search ref error:`, e.message);
  }
  // Paginación si no se encuentra por SQL directo
  let page = 0;
  while (page < 50) {
    const res = await apiClient.get(endpoints.thirdpartiesEndpoint, { params: { limit: 100, page } });
    const list = asArray(res.data);
    if (!list.length) break;
    const found = list.find((t) => norm(t?.code_client) === target || norm(t?.ref) === target);
    if (found) return found;
    page++;
  }
  return null;
}

// ... (Aquí se asumen findThirdpartyByNameSmart y findUserByLogin iguales a tu versión funcional)
// Para ahorrar espacio las incluimos conceptualmente, pero usa exactamente tus bloques de código de la Versión 1

// --- NUEVA LÓGICA DE CONTACTOS Y CORREOS ---

async function gestionarContacto(body, terceroId, rid) {
  const wants = firstNonEmpty(body, ["contacto_cliente_00", "DATOS_PARA_DOLIBARR/contacto_cliente_00"]);
  const s = String(wants ?? "").trim().toLowerCase();
  const si = (s === "si" || s === "sí" || s === "yes");

  if (!si) return { status: "NO_SOLICITADO" };

  const payload = {
    socid: terceroId || 0, // Si no hay tercero, se intenta crear huérfano (socid 0)
    firstname: firstNonEmpty(body, ["datos_persona/nombre_contacto", "nombre_contacto"]),
    lastname: firstNonEmpty(body, ["datos_persona/apellido_contacto", "apellido_contacto"]) || "N/D",
    phone: firstNonEmpty(body, ["datos_persona/numero_contacto", "numero_contacto"]),
    email: firstNonEmpty(body, ["datos_persona/correo_contacto", "correo_contacto"]),
  };

  try {
    const res = await apiClient.post(endpoints.contactsEndpoint, payload);
    return { status: "CREADO", id: res.data };
  } catch (e) {
    logRid(rid, "Error creando contacto", e.response?.data || e.message);
    return { status: "ERROR" };
  }
}

// --- EXPORT PRINCIPAL ---

export async function crearVisita(req, res) {
  const rid = crypto.randomUUID();
  try {
    const body = req.body || {};

    // MAPEO ROBUSTO (Combina ambos formatos de Kobo)
    const thirdpartyRef = firstNonEmpty(body, [
      "DATOS_PARA_DOLIBARR/thirdparty_ref_001",
      "thirdparty_ref_001",
      "thirdparty_ref"
    ]);
    const nombreCliente = firstNonEmpty(body, [
      "DATOS_PARA_DOLIBARR/nombre_cliente",
      "nombre_cliente"
    ]);
    const asesorLogin = firstNonEmpty(body, [
      "DATOS_PARA_DOLIBARR/asesor_login",
      "asesor_login"
    ]);

    logRid(rid, "INICIO PROCESO", { asesorLogin, thirdpartyRef, nombreCliente });

    if (!asesorLogin) return res.status(200).json({ status: "ERROR", message: "SIN asesor_login" });

    // 1. Validar Usuario
    const user = await findUserByLogin(asesorLogin, rid);
    if (!user) return res.status(200).json({ status: "ERROR", message: "USUARIO NO EXISTE" });

    // 2. Buscar Tercero con tu lógica de la Versión 1
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

    // 3. Lógica de CORREO si el cliente no existe
    if (!tercero) {
      logRid(rid, "Cliente no encontrado, enviando notificación...");
      try {
        const mailTo = await getEmailForSubmitter(body) || process.env.ADMIN_EMAIL;
        await sendNoClientEmail({
          to: mailTo,
          cliente: nombreCliente || "Desconocido",
          ref: thirdpartyRef || "N/A",
          vendedor: asesorLogin,
          rid
        });
      } catch (e) { logRid(rid, "Error email", e.message); }
    }

    // 4. Lógica de CONTACTO (Funciona siempre, tenga tercero o no)
    const contactoRes = await gestionarContacto(body, tercero?.id, rid);

    // 5. Crear Evento en Agenda (Tu payload original)
    const now = Math.floor(Date.now() / 1000);
    const payloadVisita = {
      userownerid: Number(user.id),
      type_code: "AC_RDV",
      label: "Visita de ventas",
      note: firstNonEmpty(body, ["DATOS_PARA_DOLIBARR/descripcion", "descripcion"]) || "",
      datep: now,
      datef: now,
      socid: tercero?.id ? Number(tercero.id) : null
    };

    const created = await apiClient.post(endpoints.agendaEventsEndpoint, payloadVisita);

    return res.status(200).json({
      status: "VISITA CREADA",
      eventId: created.data,
      terceroModo,
      thirdpartyId: tercero?.id || null,
      contactoStatus: contactoRes.status,
      contactoId: contactoRes.id || null,
      rid
    });

  } catch (error) {
    console.log(`[VISIT ${rid}] ERROR CRÍTICO:`, error.message);
    return res.status(500).json({ error: error.message, rid });
  }
}