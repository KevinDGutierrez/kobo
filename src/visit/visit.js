import crypto from "crypto";
import { apiClient, endpoints } from "../service/api.js";

const DEBUG = process.env.DEBUG_VISIT === "1";

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

async function findThirdpartyByRef(ref, rid) {
    const target = norm(ref);

    // 1) Intento directo por code_client (normalmente la "Ref" del tercero)
    try {
        const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.code_client:=:${encodeURIComponent(
            target
        )})`;

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
        if (page > 300) return null; // evita loop infinito
    }
}

async function findUserByLogin(login, rid) {
    const target = normLogin(login);

    // 1) Intento por sqlfilters login
    try {
        const url = `${endpoints.usersEndpoint}?sqlfilters=(t.login:=:${encodeURIComponent(
            target
        )})`;

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

        const body = req.body || {};

        // ✅ KoBo está enviando los campos dentro del grupo "dolibarr"
        const dol = body?.dolibarr || {};
        const visita = body?.datos_visita || {};

        if (DEBUG) {
            console.log(`[VISIT ${rid}] BODY:`, JSON.stringify(body));
            console.log(`[VISIT ${rid}] dol keys:`, Object.keys(dol));
            console.log(`[VISIT ${rid}] visita keys:`, Object.keys(visita));
        }

        const thirdpartyRef =
            body?.thirdparty_ref ||
            dol?.thirdparty_ref ||
            body?.tercero_ref ||
            dol?.tercero_ref ||
            visita?.thirdparty_ref ||
            visita?.tercero_ref ||
            null;

        const asesorLogin =
            body?.asesor_login ||
            dol?.asesor_login ||
            body?.login ||
            dol?.login ||
            visita?.asesor_login ||
            visita?.login ||
            null;

        const label =
            body?.label ||
            dol?.label ||
            body?.titulo ||
            dol?.titulo ||
            (thirdpartyRef ? `Visita - ${thirdpartyRef}` : "Visita");

        const note =
            body?.note ||
            body?.descripcion ||
            dol?.descripcion ||
            visita?.descripcion ||
            "";

        console.log(
            `[VISIT ${rid}] extracted thirdpartyRef=${thirdpartyRef} asesorLogin=${asesorLogin}`
        );

        if (!thirdpartyRef) return res.status(200).json({ status: "SIN tercero_ref" });
        if (!asesorLogin) return res.status(200).json({ status: "SIN asesor_login" });

        // Buscar tercero
        const tercero = await findThirdpartyByRef(thirdpartyRef, rid);
        console.log(
            `[VISIT ${rid}] thirdparty found? ${!!tercero} id=${tercero?.id} code_client=${tercero?.code_client}`
        );

        if (!tercero) {
            return res.status(200).json({ thirdpartyRef, status: "TERCERO NO EXISTE" });
        }

        // Buscar usuario por login
        const user = await findUserByLogin(asesorLogin, rid);
        console.log(
            `[VISIT ${rid}] user found? ${!!user} id=${user?.id} login=${user?.login}`
        );

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
