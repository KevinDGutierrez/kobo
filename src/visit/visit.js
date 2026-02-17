import crypto from "crypto";
import axios from "axios";
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
            `[VISIT ${rid}] thirdparty search1 ERROR:`,
            e?.response?.status,
            JSON.stringify(e?.response?.data || e.message)
        );
    }

    const limit = 50;
    let page = 0;

    while (true) {
        const res = await apiClient.get(endpoints.thirdpartiesEndpoint, { params: { limit, page } });
        const list = asArray(res.data);
        if (!list.length) return null;

        const found = list.find((t) => norm(t?.code_client) === target || norm(t?.ref) === target);
        if (found) return found;

        page++;
        if (page > 300) return null;
    }
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
            `[VISIT ${rid}] user search1 ERROR:`,
            e?.response?.status,
            JSON.stringify(e?.response?.data || e.message)
        );
    }

    const limit = 50;
    let page = 0;

    while (true) {
        const res = await apiClient.get(endpoints.usersEndpoint, { params: { limit, page } });
        const list = asArray(res.data);
        if (!list.length) return null;

        const found = list.find((u) => normLogin(u?.login) === target);
        if (found) return found;

        page++;
        if (page > 300) return null;
    }
}

export async function crearVisita(req, res) {
    const rid = crypto.randomUUID();

    try {
        const body = req.body || {};
        const keys = Object.keys(body);
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
            firstNonEmpty(body, ["note", "descripcion", "dolibarr/descripcion", "dolibarr.descripcion"]) ||
            "";

        const ubicacionRaw = firstNonEmpty(body, ["ubicacion_gps", "gps_inicio", "ubicacion", "_geolocation"]);

        if (!thirdpartyRef) return res.status(200).json({ status: "SIN thirdparty_ref" });
        if (!asesorLogin) return res.status(200).json({ status: "SIN asesor_login" });

        const tercero = await findThirdpartyByRef(thirdpartyRef, rid);

        if (!tercero) return res.status(200).json({ thirdpartyRef, status: "TERCERO NO EXISTE" });

        const user = await findUserByLogin(asesorLogin, rid);

        if (!user) return res.status(200).json({ asesorLogin, status: "USUARIO NO EXISTE (login exacto)" });

        let locationText = firstNonEmpty(body, ["ubicacion_texto", "ubicacion_direccion", "direccion", "location_text"]);
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
            socid: Number(tercero.id),
            userownerid: Number(user.id),
            type_code: "AC_RDV",
            label,
            note,
            datep: now,
            datef: now,
            location: truncate128(locationText),
        };

        const created = await apiClient.post(endpoints.agendaEventsEndpoint, payload);

        return res.status(200).json({
            status: "VISITA CREADA",
            eventId: created.data,
            thirdpartyRef,
            thirdpartyId: tercero.id,
            thirdpartyName: tercero.name,
            asesorLogin,
            userId: user.id,
            userLogin: user.login,
            location: payload.location,
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