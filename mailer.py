"""
mailer.py — JobBot Cold Email Engine
Motor de envío de correos fríos con rotación de contenido, adjuntos dinámicos
y rate limiting para cuidar la reputación del remitente.

Cambios v1.1:
  - _make_message_id(): extrae dominio del email del remitente en lugar de
    parsear SMTP_HOST con split() — evita IndexError con hosts de 1 segmento
  - Import de ResultadoScoring eliminado (no se usaba en este módulo)

Python: 3.11+
Dependencias: stdlib únicamente (smtplib, email, os, random, time, logging)
"""

from __future__ import annotations

import logging
import os
import random
import re
import smtplib
import time
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from pathlib import Path
from typing import Optional

from db_manager import (
    esta_en_cooldown,
    get_contactos_by_empresa,
    get_empresas_listas_para_envio,
    registrar_envio,
    actualizar_estado_envio,
)

logger = logging.getLogger("jobbot.mailer")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
SLEEP_MIN_SEGUNDOS: int = 180
SLEEP_MAX_SEGUNDOS: int = 480

CV_DIR = Path(__file__).parent / "cvs"
CV_MAP: dict[str, str] = {
    "CV_Tech":     "CV_Tech.pdf",
    "CV_Admin_IT": "CV_Admin_IT.pdf",
}

SENDER_NAME: str = os.getenv("SENDER_NAME", "Alaska")

ASUNTOS: tuple[str, ...] = (
    "Búsqueda laboral — Administración con perfil IT | {nombre_empresa}",
    "Postulación espontánea: Gestión administrativa y soporte técnico",
    "Candidatura — Perfil híbrido Admin/IT para {nombre_empresa}",
    "Interés en sumarme al equipo de {nombre_empresa}",
    "Postulación: Secretariado técnico y soporte de sistemas",
    "CV adjunto — Administración IT | Disponibilidad inmediata",
    "{nombre_empresa} — Candidatura espontánea, perfil admin-técnico",
)

CUERPOS: tuple[str, ...] = (
    """\
Buenos días,

Mi nombre es {nombre_remitente} y me comunico para dejar mi candidatura espontánea \
en {nombre_empresa}.

Mi perfil combina administración de oficina con conocimientos técnicos en soporte IT, \
lo que me permite no solo gestionar tareas operativas y de secretariado, sino también \
resolver incidencias de sistemas, administrar accesos y documentar procesos internos \
de forma autónoma.

Adjunto mi CV para que puedan evaluarlo con detenimiento. Quedo a disposición ante \
cualquier consulta.

{firma}""",

    """\
Hola,

Les escribo desde Mar del Plata para compartir mi perfil con {nombre_empresa}.

Cuento con experiencia en gestión administrativa, atención a proveedores y clientes, \
manejo de herramientas de oficina y un fuerte componente técnico: soporte de primer \
nivel, scripting para automatizar tareas repetitivas y administración básica de redes.

Es un perfil que suele ser difícil de encontrar en el mercado local, por lo que me \
pareció interesante acercarles mi CV directamente.

Muchas gracias por su tiempo.

{firma}""",

    """\
Estimado equipo de {nombre_empresa}:

Me dirijo a ustedes para postularme de forma espontánea. Soy técnico administrativo \
con orientación IT, radicado en Mar del Plata y con disponibilidad inmediata.

Entre mis habilidades principales se encuentran: organización de documentación y \
archivos, coordinación de agenda, soporte técnico a usuarios, mantenimiento preventivo \
de equipos y automatización de reportes con Python y scripts de shell.

Adjunto mi CV en formato PDF. Estoy disponible para una entrevista en el horario \
que mejor les convenga.

Saludos cordiales,

{firma}""",

    """\
Buenas tardes,

Mi nombre es {nombre_remitente}. Encontré información sobre {nombre_empresa} y me \
resultó interesante la posibilidad de sumarme al equipo.

Tengo experiencia cubriendo roles que históricamente se dividen en dos personas: \
el administrativo y el de soporte técnico. Puedo redactar informes, coordinar con \
proveedores y al mismo tiempo diagnosticar una falla de red o automatizar una tarea \
con un script. Para una PyME, eso representa eficiencia real.

Si les parece relevante el perfil, con gusto ampliamos información.

{firma}""",

    """\
Hola equipo de {nombre_empresa},

Les escribo para dejar mi CV ante la posibilidad de que necesiten reforzar el área \
administrativa o de soporte técnico.

Soy una persona con perfil híbrido: manejo fluido de herramientas ofimáticas, \
redacción de comunicaciones formales, gestión de facturación y a la vez conocimientos \
de redes, sistemas operativos y automatización de procesos. Resido en Mar del Plata \
y tengo disponibilidad completa.

Adjunto mi currículum. Muchas gracias por considerar mi postulación.

{firma}""",
)

FIRMA_TEMPLATE: str = """\
{nombre_remitente}
Mar del Plata, Buenos Aires
📧 {email_remitente}
🔗 {github_user}.github.io/  |  linkedin.com/in/{linkedin_user}

PD: Este correo y su adjunto fueron generados con JobBot, \
una herramienta de automatización de búsqueda laboral que desarrollé en Python. \
Podés ver el código en: github.com/{github_user}/jobbot"""


# ---------------------------------------------------------------------------
# Configuración SMTP
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ConfigSMTP:
    host:         str
    port:         int
    user:         str
    password:     str
    sender_name:  str
    github_user:  str
    linkedin_user: str

    @classmethod
    def from_env(cls) -> "ConfigSMTP":
        required = ("SMTP_HOST", "SMTP_USER", "SMTP_PASS")
        missing  = [v for v in required if not os.getenv(v)]
        if missing:
            raise EnvironmentError(
                f"Variables de entorno faltantes: {', '.join(missing)}. "
                "Configuralas en start_bot.sh antes de ejecutar el bot."
            )
        return cls(
            host=os.environ["SMTP_HOST"],
            port=int(os.getenv("SMTP_PORT", "587")),
            user=os.environ["SMTP_USER"],
            password=os.environ["SMTP_PASS"],
            sender_name=os.getenv("SENDER_NAME", "Alaska"),
            github_user=os.getenv("GITHUB_USER", "tu-usuario"),
            linkedin_user=os.getenv("LINKEDIN_USER", "tu-perfil"),
        )


# ---------------------------------------------------------------------------
# Message-ID robusto
# ---------------------------------------------------------------------------

def _make_message_id(smtp_user: str, smtp_host: str) -> str:
    """
    Genera un Message-ID válido usando el dominio del email del remitente.

    Estrategia de fallback en orden:
      1. Dominio del email del remitente (user@dominio.com → dominio.com)  ← preferido
      2. SMTP_HOST si tiene al menos 2 segmentos (smtp.ejemplo.com → ejemplo.com)
      3. Literal "jobbot.local" como último recurso

    Esto elimina el IndexError que ocurría con hosts de un solo segmento
    (ej: SMTP_HOST="localhost") al hacer split(".")[-2].

    Args:
        smtp_user: Dirección de email del remitente (SMTP_USER).
        smtp_host: Hostname del servidor SMTP (SMTP_HOST).

    Returns:
        String de Message-ID con formato RFC 2822 válido.
    """
    # Intento 1: dominio del email del remitente — siempre tiene el formato correcto
    if "@" in smtp_user:
        domain = smtp_user.split("@", 1)[1].strip()
        if "." in domain:
            return make_msgid(domain=domain)

    # Intento 2: construir dominio desde SMTP_HOST
    parts = smtp_host.split(".")
    if len(parts) >= 2:
        domain = ".".join(parts[-2:])   # smtp.gmail.com → gmail.com
        return make_msgid(domain=domain)

    # Intento 3: fallback seguro
    return make_msgid(domain="jobbot.local")


# ---------------------------------------------------------------------------
# Adjunto dinámico
# ---------------------------------------------------------------------------

def _preparar_adjunto(perfil_cv: str, nombre_empresa: str) -> tuple[bytes, str]:
    if perfil_cv not in CV_MAP:
        raise ValueError(f"Perfil de CV desconocido: '{perfil_cv}'. Válidos: {list(CV_MAP)}")

    archivo_fuente = CV_DIR / CV_MAP[perfil_cv]
    if not archivo_fuente.exists():
        raise FileNotFoundError(
            f"CV fuente no encontrado: {archivo_fuente}. "
            f"Asegurate de que exista en '{CV_DIR}'."
        )

    nombre_limpio = re.sub(r'[^\w\s-]', '', nombre_empresa).strip().replace(' ', '_')
    perfil_label  = perfil_cv.replace("CV_", "")
    filename      = f"CV_{SENDER_NAME}_{perfil_label}_{nombre_limpio}.pdf"
    pdf_bytes     = archivo_fuente.read_bytes()

    logger.debug(
        "Adjunto preparado | fuente=%s | filename=%s | size=%d bytes",
        archivo_fuente.name, filename, len(pdf_bytes),
    )
    return pdf_bytes, filename


# ---------------------------------------------------------------------------
# Construcción del EmailMessage
# ---------------------------------------------------------------------------

def _construir_email(
    config: ConfigSMTP,
    destinatario: str,
    nombre_empresa: str,
    perfil_cv: str,
) -> tuple[EmailMessage, str]:
    firma = FIRMA_TEMPLATE.format(
        nombre_remitente=config.sender_name,
        email_remitente=config.user,
        github_user=config.github_user,
        linkedin_user=config.linkedin_user,
    )

    asunto = random.choice(ASUNTOS).format(nombre_empresa=nombre_empresa)
    cuerpo = random.choice(CUERPOS).format(
        nombre_remitente=config.sender_name,
        nombre_empresa=nombre_empresa,
        firma=firma,
    )

    msg = EmailMessage()
    msg["From"]       = f"{config.sender_name} <{config.user}>"
    msg["To"]         = destinatario
    msg["Subject"]    = asunto
    msg["Date"]       = formatdate(localtime=True)
    msg["Message-ID"] = _make_message_id(config.user, config.host)  # ← usa helper robusto

    msg.set_content(cuerpo, charset="utf-8")

    pdf_bytes, filename = _preparar_adjunto(perfil_cv, nombre_empresa)
    msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf", filename=filename)

    return msg, asunto


# ---------------------------------------------------------------------------
# Motor SMTP
# ---------------------------------------------------------------------------

def _enviar_via_smtp(config: ConfigSMTP, msg: EmailMessage) -> bool:
    try:
        with smtplib.SMTP(config.host, config.port, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(config.user, config.password)
            smtp.send_message(msg)

        logger.info("Email enviado | to=%s | subject='%s'", msg["To"], msg["Subject"][:60])
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error("Error de autenticación SMTP | host=%s", config.host)
    except smtplib.SMTPRecipientsRefused as exc:
        logger.warning("Destinatario rechazado | to=%s | %s", msg["To"], exc)
    except smtplib.SMTPException as exc:
        logger.error("Error SMTP | %s", exc)
    except TimeoutError:
        logger.error("Timeout SMTP | host=%s:%d", config.host, config.port)
    except OSError as exc:
        logger.error("Error de red | %s", exc)

    return False


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def procesar_envios_pendientes(
    min_score: int = 55,
    limite_empresas: int = 50,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Obtiene empresas aptas, verifica cooldown, construye y envía correos,
    registra resultados en DB y aplica rate limiting entre envíos.

    Args:
        min_score:       Score mínimo para considerar una empresa.
        limite_empresas: Máximo de empresas a procesar en esta ejecución.
        dry_run:         Si True, construye el email pero NO lo envía ni registra.

    Returns:
        {'procesadas': N, 'enviadas': N, 'omitidas': N, 'errores': N}
    """
    config   = ConfigSMTP.from_env()
    metricas = {"procesadas": 0, "enviadas": 0, "omitidas": 0, "errores": 0}

    if dry_run:
        logger.warning("=== MODO DRY-RUN: no se enviará ningún correo real ===")

    empresas = get_empresas_listas_para_envio(min_score=min_score, limit=limite_empresas)
    if not empresas:
        logger.info("No hay empresas aptas (min_score=%d).", min_score)
        return metricas

    logger.info("Empresas aptas: %d", len(empresas))
    es_primer_envio = True

    for empresa in empresas:
        empresa_id = empresa["id"]
        nombre     = empresa["nombre"]
        dominio    = empresa["dominio"]
        perfil_cv  = empresa["perfil_cv"] or "CV_Admin_IT"
        score      = empresa["score"]
        metricas["procesadas"] += 1

        logger.info("--- Procesando | empresa='%s' | dominio=%s | score=%d ---", nombre, dominio, score)

        # Doble check de cooldown (race condition safety)
        if esta_en_cooldown(empresa_id):
            logger.info("En cooldown de envío, omitiendo | empresa='%s'", nombre)
            metricas["omitidas"] += 1
            continue

        # Selección del contacto de máxima prioridad
        contactos       = get_contactos_by_empresa(empresa_id)
        contactos_email = [
            c for c in contactos
            if c["tipo"] in ("RRHH", "General") and "@" in c["email_o_link"]
        ]
        if not contactos_email:
            logger.info("Sin emails disponibles, omitiendo | empresa='%s'", nombre)
            metricas["omitidas"] += 1
            continue

        contacto_objetivo = sorted(contactos_email, key=lambda c: c["prioridad"])[0]
        if contacto_objetivo["prioridad"] > 3:
            logger.info("Prioridad %d demasiado baja | empresa='%s'", contacto_objetivo["prioridad"], nombre)
            metricas["omitidas"] += 1
            continue

        destinatario = contacto_objetivo["email_o_link"]

        # Construir email
        try:
            msg, asunto_usado = _construir_email(config, destinatario, nombre, perfil_cv)
        except (FileNotFoundError, ValueError) as exc:
            logger.error("No se pudo construir el email | empresa='%s' | %s", nombre, exc)
            metricas["errores"] += 1
            continue

        # Rate limiting
        if not es_primer_envio and not dry_run:
            sleep_seg = random.randint(SLEEP_MIN_SEGUNDOS, SLEEP_MAX_SEGUNDOS)
            logger.info("Rate limit: %d seg (~%.1f min)…", sleep_seg, sleep_seg / 60)
            time.sleep(sleep_seg)

        # Envío real o simulado
        if dry_run:
            logger.info(
                "[DRY-RUN] OK | to=%s | subject='%s' | cv=%s",
                destinatario, asunto_usado[:60], perfil_cv,
            )
            metricas["enviadas"] += 1
            es_primer_envio = False
            continue

        exito = _enviar_via_smtp(config, msg)

        if exito:
            envio_id = registrar_envio(
                empresa_id=empresa_id,
                cv_enviado=CV_MAP.get(perfil_cv, perfil_cv),
                asunto_usado=asunto_usado,
                estado="enviado",
            )
            logger.info("✓ Enviado | empresa='%s' | to=%s | envio_id=%d", nombre, destinatario, envio_id)
            metricas["enviadas"] += 1
        else:
            registrar_envio(
                empresa_id=empresa_id,
                cv_enviado=CV_MAP.get(perfil_cv, perfil_cv),
                asunto_usado=asunto_usado,
                estado="rebotado",
            )
            logger.warning("✗ Fallo | empresa='%s' | to=%s", nombre, destinatario)
            metricas["errores"] += 1

        es_primer_envio = False

    logger.info(
        "=== Campaña finalizada | procesadas=%d | enviadas=%d | omitidas=%d | errores=%d ===",
        metricas["procesadas"], metricas["enviadas"], metricas["omitidas"], metricas["errores"],
    )
    return metricas


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="JobBot — Motor de envío de emails")
    parser.add_argument("--dry-run",   action="store_true", help="Auditoría sin envío real")
    parser.add_argument("--min-score", type=int, default=55, help="Score mínimo (default: 55)")
    parser.add_argument("--limite",    type=int, default=20, help="Máximo de empresas (default: 20)")
    args = parser.parse_args()

    metricas = procesar_envios_pendientes(
        min_score=args.min_score,
        limite_empresas=args.limite,
        dry_run=args.dry_run,
    )
    print(f"\nResultado: {metricas}")
    