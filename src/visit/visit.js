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
  return undefined;
}

function firstNonEmpty(obj, keys) {
  for (const k of keys) {
    const v = getValue(obj, k);
    if (v !== undefined && v !== null && String(v).trim() !== "") return v;
  }
  return null;
}

async function findThirdpartyByRef(ref, rid) {
  const target = norm(ref);
  try {
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.code_client:=:'${target}')`;
    const res = await apiClient.get(url);
    const list = asArray(res.data);
    const exact = list.find(t => norm(t.code_client) === target || norm(t.ref) === target);
    if (exact) return exact;
  } catch (e) {
    console.log(`[VISIT ${rid}] Error ref search:`, e.message);
  }
  return null; 
}

async function ensureContactIfRequested({ body, tercero, rid }) {
  if (!tercero?.id) return { done: false, reason: "NO_TERCERO" };

  const wants = firstNonEmpty(body, ["contacto_cliente_00", "DATOS_PARA_DOLIBARR/contacto_cliente_00"]);
  
  const s = String(wants ?? "").toLowerCase();
  const isYes = s === "si" || s === "sí" || s === "true";

  if (!isYes) return { done: false, reason: "NO_SOLICITADO" };

  const payload = {
    socid: tercero.id,
    firstname: firstNonEmpty(body, ["datos_persona/nombre_contacto", "nombre_contacto"]),
    lastname: firstNonEmpty(body, ["datos_persona/apellido_contacto", "apellido_contacto"]) || "N/D",
    phone: firstNonEmpty(body, ["datos_persona/numero_contacto", "numero_contacto"]),
    email: firstNonEmpty(body, ["datos_persona/correo_contacto", "correo_contacto"]),
  };

  try {
    const r = await apiClient.post(endpoints.contactsEndpoint, payload);
    return { done: true, created: true, contactId: r.data };
  } catch (e) {
    return { done: true, created: false, error: e.message };
  }
}

export async function crearVisita(req, res) {
  const rid = crypto.randomUUID();
  try {
    const body = req.body || {};

    const thirdpartyRef = firstNonEmpty(body, [
      "DATOS_PARA_DOLIBARR/thirdparty_ref_001", 
      "thirdparty_ref_001",
      "codigo_cliente"
    ]);

    const nombreCliente = firstNonEmpty(body, ["nombre_cliente", "DATOS_PARA_DOLIBARR/nombre_cliente"]);
    const asesorLogin = firstNonEmpty(body, ["asesor_login", "DATOS_PARA_DOLIBARR/asesor_login"]);
    const descripcion = firstNonEmpty(body, ["descripcion", "DATOS_PARA_DOLIBARR/descripcion"]);
    const ubicacionRaw = firstNonEmpty(body, ["ubicacion_gps", "_geolocation"]);

    if (!asesorLogin) return res.status(200).json({ status: "FALTA_ASESOR_LOGIN" });

    const user = await findUserByLogin(asesorLogin, rid);
    if (!user) return res.status(200).json({ status: "USUARIO_NO_EXISTE", login: asesorLogin });

    let tercero = null;
    let terceroModo = "NO_ENCONTRADO";

    if (thirdpartyRef) {
      tercero = await findThirdpartyByRef(thirdpartyRef, rid);
      if (tercero) terceroModo = "ASOCIADO_POR_CODIGO";
    }

    if (!tercero && nombreCliente) {
      tercero = await findThirdpartyByNameSmart(nombreCliente, rid);
      if (tercero) terceroModo = "ASOCIADO_POR_NOMBRE";
    }

    if (!tercero) {
      const submitterEmail = await getEmailForSubmitter(body);
      await sendNoClientEmail({
        to: submitterEmail || process.env.ADMIN_EMAIL,
        clienteNombre: nombreCliente,
        asesor: asesorLogin,
        rid
      });
    }

    let contactoResult = null;
    if (tercero) {
      contactoResult = await ensureContactIfRequested({ body, tercero, rid });
    }

    const now = Math.floor(Date.now() / 1000);
    const payloadEvento = {
      userownerid: user.id,
      type_code: "AC_RDV",
      label: "Visita de ventas",
      note: descripcion || "Sin descripción",
      datep: now,
      datef: now,
      socid: tercero?.id || null,
    };

    const created = await apiClient.post(endpoints.agendaEventsEndpoint, payloadEvento);

    return res.status(200).json({
      status: "OK",
      eventId: created.data,
      terceroModo,
      contacto: contactoResult
    });

  } catch (error) {
    console.error(`[VISIT ${rid}] CRITICAL ERROR:`, error.message);
    return res.status(500).json({ error: error.message, rid });
  }
}