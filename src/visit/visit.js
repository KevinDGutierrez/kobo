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
    return String(x ?? "").trim().toLowerCase();
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

    // 1) Intento por sqlfilters code_client
    try {
        const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.code_client:=:${encodeURIComponent(
            target
        )})`;

        if (DEBUG) console.log(`[VISIT ${rid}] thirdparty search url1: ${url}`);

        const res = await apiClient.get(url);
        const list = asArray(res.data);

        console.log(`[VISIT ${rid}] thirdparty search1 results: ${list.length}`);

        // ✅ Match exacto (NO devolver el primero)
        const exact = list.find(
            (t) => norm(t?.code_client) === target || norm(t?.ref) === target
        );

        if (DEBUG && list.length) {
            console.log(
                `[VISIT ${rid}] thirdparty search1 sample:`,
                list
                    .slice(0, 10)
                    .map((t) => `${t?.id}:${t?.code_client || ""}:${t?.name || ""}`)
                    .join(" | ")
            );
        }

        if (exact) return exact;

        console.log(
            `[VISIT ${rid}] thirdparty search1: NO exact match for "${target}", will fallback paging`
        );
    } catch (e) {
        console.log(
            `[VISIT ${rid}] thirdparty search1 ERROR:`,
            e?.response?.status,
            JSON.stringify(e?.response?.data || e.message)
        );
    }

    // 2) Fallback paginando (match exacto)
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
        const url = `${endpoints.usersEndpoint}?sqlfilters=(t.login:=:${encodeURIComponent(
            login
        )})`;

        if (DEBUG) console.log(`[VISIT ${rid}] user search url1: ${url}`);

        const res = await apiClient.get(url);
        const list = asArray(res.data);

        console.log(`[VISIT ${rid}] user search1 results: ${list.length}`);

        // ✅ Match exacto por login (NO devolver el primero)
        const exact = list.find(
            (u) => normLogin(u?.login) === target
        );

        if (DEBUG && list.length) {
            console.log(
                `[VISIT ${rid}] user search1 logins sample:`,
                list
                    .slice(0, 15)
                    .map((u) => `${u?.id}:${u?.login || ""}:${u?.firstname || ""} ${u?.lastname || ""}`)
                    .join(" | ")
            );
        }

        if (exact) return exact;

        console.log(
            `[VISIT ${rid}] user search1: NO exact match for login="${target}", will fallback paging`
        );
    } catch (e) {
        console.log(
            `[VISIT ${rid}] user search1 ERROR:`,
            e?.response?.status,
            JSON.stringify(e?.response?.data || e.message)
        );
    }

    // 2) Fallback paginando (match exacto)
    const limit = 50;
    let page = 0;

    while (true) {
        if (DEBUG) console.log(`[VISIT ${rid}] user paging page=${page}`);

        const res = await apiClient.get(endpoints.usersEndpoint, {
            params: { limit, page },
        });

        const list = asArray(res.data);
        if (!list.length) return null;

        const found = list.find((u) => normLogin(u?.login) === target);
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
        const keys = Object.keys(body);
        console.log(
            `[VISIT ${rid}] body keys count=${keys.length} sample=${keys
                .slice(0, 30)
                .join(", ")}`
        );

        if (DEBUG && req.rawBody) {
            console.log(
                `[VISIT ${rid}] rawBody(0..${RAW_LOG_MAX}): ${String(req.rawBody).slice(
                    0,
                    RAW_LOG_MAX
                )}`
            );
        }

        // KoBo manda keys aplanadas como "dolibarr/thirdparty_ref" y "dolibarr/asesor_login"
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
            ]) || (thirdpartyRef ? `Visita - ${thirdpartyRef}` : "Visita");

        const note =
            firstNonEmpty(body, [
                "note",
                "descripcion",
                "dolibarr/descripcion",
                "dolibarr.descripcion",
            ]) || "";

        console.log(
            `[VISIT ${rid}] extracted thirdpartyRef=${thirdpartyRef} asesorLogin=${asesorLogin}`
        );

        if (!thirdpartyRef)
            return res.status(200).json({ status: "SIN thirdparty_ref" });
        if (!asesorLogin)
            return res.status(200).json({ status: "SIN asesor_login" });

        // Buscar tercero (match exacto)
        const tercero = await findThirdpartyByRef(thirdpartyRef, rid);
        console.log(
            `[VISIT ${rid}] thirdparty found? ${!!tercero} id=${tercero?.id} code_client=${tercero?.code_client} name=${tercero?.name}`
        );

        if (!tercero) {
            return res
                .status(200)
                .json({ thirdpartyRef, status: "TERCERO NO EXISTE" });
        }

        // Buscar usuario por login (match exacto)
        const user = await findUserByLogin(asesorLogin, rid);
        console.log(
            `[VISIT ${rid}] user found? ${!!user} id=${user?.id} login=${user?.login}`
        );

        if (!user) {
            return res
                .status(200)
                .json({ asesorLogin, status: "USUARIO NO EXISTE (login exacto)" });
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
            datef: now,
        };

        console.log(
            `[VISIT ${rid}] POST ${endpoints.agendaEventsEndpoint} payload=`,
            JSON.stringify(payload)
        );

        const created = await apiClient.post(endpoints.agendaEventsEndpoint, payload);

        console.log(
            `[VISIT ${rid}] CREATED response status=${created.status} data=`,
            JSON.stringify(created.data)
        );

        return res.status(200).json({
            status: "VISITA CREADA",
            eventId: created.data,
            thirdpartyRef,
            thirdpartyId: tercero.id,
            thirdpartyName: tercero.name,
            asesorLogin,
            userId: user.id,
            userLogin: user.login,
        });
    } catch (error) {
        const status = error?.response?.status;
        const data = error?.response?.data;

        console.log(
            `[VISIT ${rid}] ERROR status=${status} message=${error?.message}`
        );
        if (data) console.log(`[VISIT ${rid}] ERROR data=`, JSON.stringify(data));

        return res.status(500).json({
            rid,
            error: data || error.message || String(error),
        });
    }
}