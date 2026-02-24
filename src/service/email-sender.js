import nodemailer from "nodemailer";
import { apiClient, endpoints } from "./api.js";

const transporter = nodemailer.createTransport({
  host: "smtp.gmail.com",
  port: 465,
  secure: true,
  auth: {
    user: "senia@sencomca.com",
    pass: process.env.GMAIL_PASS,
  },
});

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

export function buildNoClientHtml({ userLogin, eventId, nombreCliente, thirdpartyRef }) {
  const nc = escapeHtml(nombreCliente || "N/D");
  const ref = escapeHtml(thirdpartyRef || "N/D");
  const ev = escapeHtml(eventId);

  return `
    <div style="font-family: Arial, sans-serif; line-height:1.6; color:#333">
      <h2 style="margin:0 0 12px 0;">Registro de visita guardado (sin cliente asociado)</h2>

      <p>Querido usuario <strong>${escapeHtml(userLogin)}</strong>,</p>

      <p>
        Le notificamos que su evento fue <strong>registrado correctamente</strong>, pero
        <strong>no se encontró ningún cliente</strong> para asociarlo en Dolibarr.
      </p>

      <div style="background:#f7f7f7; padding:12px; border-radius:8px; margin:14px 0;">
        <p style="margin:0;"><strong>Event ID:</strong> ${ev}</p>
        <p style="margin:0;"><strong>Código recibido:</strong> ${ref}</p>
        <p style="margin:0;"><strong>Nombre recibido:</strong> ${nc}</p>
      </div>

      <p>
        Verifique los datos del cliente (Código/Nombre) y envíe otro formulario cuando tenga los datos correctos,
        para que el registro quede asociado al cliente en cuestión.
      </p>

      <p style="margin-top:18px; font-size:12px; color:#777;">
        Este mensaje fue enviado automáticamente. Por favor, no responda a esta dirección.
      </p>
    </div>
  `;
}

export async function getEmailForSubmitter({ body, user, rid, firstNonEmpty }) {
  const fromKobo = firstNonEmpty(body, [
    "lx_user/email",
    "lx_user.email",
    "lx_user->email",
    "lx_user_email",
    "user/email",
    "user.email",
    "email",
    "correo",
  ]);
  if (fromKobo) return String(fromKobo).trim();

  if (user?.email) return String(user.email).trim();

  try {
    const r = await apiClient.get(`${endpoints.usersEndpoint}/${user.id}`);
    const email = r?.data?.email || r?.data?.mail || null;
    return email ? String(email).trim() : null;
  } catch (e) {
    console.log(
      `[VISIT ${rid}] getEmailForSubmitter ERROR:`,
      e?.response?.status,
      JSON.stringify(e?.response?.data || e.message)
    );
    return null;
  }
}

export async function sendNoClientEmail(to, { userLogin, eventId, nombreCliente, thirdpartyRef }) {
  if (!to) return;

  const mailOptions = {
    from: "senia@sencomca.com",
    to,
    subject: `Visita registrada sin cliente asociado (Evento ${eventId})`,
    html: buildNoClientHtml({ userLogin, eventId, nombreCliente, thirdpartyRef }),
  };

  await transporter.sendMail(mailOptions);
}