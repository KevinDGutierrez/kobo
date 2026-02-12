import crypto from "crypto";
import { apiClient, endpoints } from "../service/api.js";

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
    return String(x ?? "").trim();
}

// Lee keys directas, dot notation (a.b.c) y keys con "/" (dolibarr/thirdparty_ref)
function getValue(obj, key) {
    if (!obj) return undefined;

    // key directa (incluye keys con "/")
    if (Object.prototype.hasOwnProperty.call(obj, key)) return obj[key];

    // dot notation
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

async function findThirdpartyByRef(ref, rid) {
    const target = norm(ref);

    // 1) Intento directo por code_client (Ref del tercero)
    try {
        const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.code_client:=:${encodeURIComponent(target)})`;

        if (DEBUG) console.log(`[VISIT ${rid}] thirdparty search url1: ${url}`);

        const res = await apiClient.get(url);
        const list = asArray(res.data);

        console.log(`[VISIT ${rid}] thirdparty search1 results: ${list.length}`);

        if (list.length) return list[0];
    } catch (e) {
        console.log(
            `[VISIT ${rid}] thirdparty search1 ERROR:`,
            e?.response?.status,
            JSON.stringify(e?.response?.data || e.message)
        );
    }

    // 2) Fallback paginando
    const limit = 50;
    let page = 0;

    while (true) {
        if (DEBUG) console.log(`[VISIT ${rid}] thirdparty paging page=${page}`);

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
    }
}

async function findUserByLogin(login, rid) {
    const target = normLogin(login);

    // 1) Intento por sqlfilters login
    try {
        const url = `${endpoints.usersEndpoint}?sqlfilters=(t.login:=:${encodeURIComponent(target)})`;

        if (DEBUG) console.log(`[VISIT ${rid}] user search url1: ${url}`);

        const res = await apiClient.get(url);
        const list = asArray(res.data);

        console.log(`[VISIT ${rid}] user search1 results: ${list.length}`);

        if (list.length) return list[0];
    } catch (e) {
        console.log(
            `[VISIT ${rid}] user search1 ERROR:`,
            e?.response?.status,
            JSON.stringify(e?.response?.data || e.message)
        );
    }

    // 2) Fallback paginando
    const limit = 50;
    let page = 0;

    while (true) {
        if (DEBUG) console.log(`[VISIT ${rid}] user paging page=${page}`);

        const res = await apiClient.get(endpoints.usersEndpoint, {
            params: { limit, page },
        });

        const list = asArray(res.data);
        if (!list.length) return null;

        const found = list.find((u) => String(u?.login ?? "").trim() === target);
        if (found) return found;

        page++;
        if (page > 300) return null;
    }
}

/**
 * POST /visit/run
 */
export async function crearVisita(req, res) {
    const rid = crypto.randomUUID();

    try {
        console.log(`[VISIT ${rid}] START /visit/run`);
        console.log(`[VISIT ${rid}] content-type: ${req.headers["content-type"]}`);

        const body = req.body || {};

        // Logs útiles: qué claves vienen realmente
        const keys = Object.keys(body);
        console.log(`[VISIT ${rid}] body keys count=${keys.length} sample=${keys.slice(0, 30).join(", ")}`);

        // Si habilitas DEBUG_VISIT=1 y tu index.js guarda rawBody, lo verás aquí
        if (DEBUG && req.rawBody) {
            console.log(`[VISIT ${rid}] rawBody(0..${RAW_LOG_MAX}): ${String(req.rawBody).slice(0, RAW_LOG_MAX)}`);
        } else if (DEBUG && !req.rawBody) {
            console.log(`[VISIT ${rid}] rawBody: (no disponible - revisa middleware en index.js)`);
        }

        // ✅ Soporta:
        // - thirdparty_ref (raíz)
        // - dolibarr.thirdparty_ref (anidado)
        // - dolibarr/thirdparty_ref (aplanado con "/")
        // - datos_visita.* (por si tu form lo manda ahí)
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

        const label =
            firstNonEmpty(body, [
                "label",
                "titulo",
                "dolibarr/label",
                "dolibarr/titulo",
                "dolibarr.label",
                "dolibarr.titulo",
                "datos_visita/label",
                "datos_visita/titulo",
                "datos_visita.label",
                "datos_visita.titulo",
            ]) || (thirdpartyRef ? `Visita - ${thirdpartyRef}` : "Visita");

        const note =
            firstNonEmpty(body, [
                "note",
                "descripcion",
                "dolibarr/descripcion",
                "dolibarr.descripcion",
                "datos_visita/descripcion",
                "datos_visita.descripcion",
            ]) || "";

        console.log(`[VISIT ${rid}] extracted thirdpartyRef=${thirdpartyRef} asesorLogin=${asesorLogin}`);

        if (!thirdpartyRef) return res.status(200).json({ status: "SIN tercero_ref" });
        if (!asesorLogin) return res.status(200).json({ status: "SIN asesor_login" });

        // Buscar tercero
        const tercero = await findThirdpartyByRef(thirdpartyRef, rid);
        console.log(`[VISIT ${rid}] thirdparty found? ${!!tercero} id=${tercero?.id} code_client=${tercero?.code_client}`);

        if (!tercero) {
            return res.status(200).json({ thirdpartyRef, status: "TERCERO NO EXISTE" });
        }

        // Buscar usuario por login
        const user = await findUserByLogin(asesorLogin, rid);
        console.log(`[VISIT ${rid}] user found? ${!!user} id=${user?.id} login=${user?.login}`);

        if (!user) {
            return res.status(200).json({ asesorLogin, status: "USUARIO NO EXISTE" });
        }

        // Fecha automática (unix timestamp en segundos)
        const now = Math.floor(Date.now() / 1000);

        const payload = {
            socid: Number(tercero.id),
            userownerid: Number(user.id),
            type_code: "AC_RDV",
            label,
            note,
            datep: now,
            datef: now, // mismo valor en inicio y fin
        };

        console.log(`[VISIT ${rid}] POST ${endpoints.agendaEventsEndpoint} payload=`, JSON.stringify(payload));

        const created = await apiClient.post(endpoints.agendaEventsEndpoint, payload);

        console.log(`[VISIT ${rid}] CREATED response status=${created.status} data=`, JSON.stringify(created.data));

        return res.status(200).json({
            status: "VISITA CREADA",
            eventId: created.data,
            thirdpartyRef,
            thirdpartyId: tercero.id,
            asesorLogin,
            userId: user.id,
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