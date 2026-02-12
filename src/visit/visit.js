import { apiClient, endpoints } from '../service/api.js';

function asArray(data) {
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.rows)) return data.rows;
  return [];
}

function norm(x) {
  return String(x ?? '').trim().toUpperCase();
}
function normLogin(x) {
  return String(x ?? '').trim();
}

async function findThirdpartyByRef(ref) {
  const target = norm(ref);

  try {
    const url = `${endpoints.thirdpartiesEndpoint}?sqlfilters=(t.code_client:=:${encodeURIComponent(target)})`;
    const res = await apiClient.get(url);
    const list = asArray(res.data);
    if (list.length) return list[0];
  } catch {}

  const limit = 50;
  let page = 0;
  while (true) {
    const res = await apiClient.get(endpoints.thirdpartiesEndpoint, { params: { limit, page } });
    const list = asArray(res.data);
    if (!list.length) return null;

    const found = list.find(t => norm(t?.code_client) === target || norm(t?.ref) === target);
    if (found) return found;

    page++;
  }
}

async function findUserByLogin(login) {
  const target = normLogin(login);

  try {
    const url = `${endpoints.usersEndpoint}?sqlfilters=(t.login:=:${encodeURIComponent(target)})`;
    const res = await apiClient.get(url);
    const list = asArray(res.data);
    if (list.length) return list[0];
  } catch {}

  const limit = 50;
  let page = 0;
  while (true) {
    const res = await apiClient.get(endpoints.usersEndpoint, { params: { limit, page } });
    const list = asArray(res.data);
    if (!list.length) return null;

    const found = list.find(u => String(u?.login ?? '').trim() === target);
    if (found) return found;

    page++;
  }
}

export async function crearVisita(req, res) {
  try {
    const body = req.body;

    const thirdpartyRef =
      body?.thirdparty_ref ||
      body?.tercero_ref ||
      body?.datos_visita?.thirdparty_ref ||
      body?.datos_visita?.tercero_ref ||
      null;

    const asesorLogin =
      body?.asesor_login ||
      body?.login ||
      body?.datos_visita?.asesor_login ||
      body?.datos_visita?.login ||
      null;

    if (!thirdpartyRef) return res.json({ status: 'SIN tercero_ref' });
    if (!asesorLogin) return res.json({ status: 'SIN asesor_login' });

    const tercero = await findThirdpartyByRef(thirdpartyRef);
    if (!tercero) return res.json({ thirdpartyRef, status: 'TERCERO NO EXISTE' });

    const user = await findUserByLogin(asesorLogin);
    if (!user) return res.json({ asesorLogin, status: 'USUARIO NO EXISTE' });

    const now = Math.floor(Date.now() / 1000);

    const payload = {
      socid: Number(tercero.id),
      userownerid: Number(user.id),
      type_code: 'AC_RDV',
      label: body?.label || body?.titulo || `Visita - ${thirdpartyRef}`,
      note: body?.note || body?.descripcion || '',
      datep: now,
      datef: now
    };

    const created = await apiClient.post(endpoints.agendaEventsEndpoint, payload);

    return res.json({
      status: 'VISITA CREADA',
      thirdpartyRef,
      thirdpartyId: tercero.id,
      asesorLogin,
      userId: user.id,
      eventId: created.data
    });
  } catch (error) {
    return res.status(500).json({
      error: error?.response?.data || error?.message || String(error)
    });
  }
}
