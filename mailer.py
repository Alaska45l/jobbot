"""
mailer.py — JobBot Cold Email Engine
Motor de envío de correos fríos con CVs dinámicos compilados con Typst.

Python: 3.11+
Dependencias: utils/cv_builder.py, typst CLI, stdlib (smtplib, asyncio, email…)
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from config import SENDER_NAME, SMTP_JITTER_MIN_S, SMTP_JITTER_MAX_S
from utils.cv_builder import compilar_cv_dinamico, CVCompilationError
from db_manager import (
    esta_en_cooldown,
    get_contactos_by_empresa,
    get_empresas_listas_para_envio,
    registrar_envio,
)

logger = logging.getLogger("jobbot.mailer")

# ---------------------------------------------------------------------------
# Constantes de contenido
# ---------------------------------------------------------------------------

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

Me dirijo a ustedes para postularme de forma espontánea. Soy técnica administrativa \
con orientación IT, radicada en Mar del Plata y con disponibilidad inmediata.

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
Email: {email_remitente}
Web: {github_user}.github.io/  |  linkedin.com/in/{linkedin_user}

PD: Este correo y su adjunto fueron generados con JobBot, \
una herramienta de automatización de búsqueda laboral que desarrollé en Python. \
Podés ver el código en: github.com/{github_user}/jobbot"""


# ---------------------------------------------------------------------------
# Configuración SMTP
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ConfigSMTP:
    host:          str
    port:          int
    user:          str
    password:      str
    sender_name:   str
    github_user:   str
    linkedin_user: str

    @classmethod
    def from_env(cls) -> "ConfigSMTP":
        required = ("SMTP_HOST", "SMTP_USER", "SMTP_PASS")
        missing  = [v for v in required if not os.getenv(v)]
        if missing:
            raise EnvironmentError(
                f"Variables de entorno faltantes: {', '.join(missing)}. "
                "Configuralas en start_bot.sh antes de ejecutar el bot."
                "Configuralas en el archivo .env antes de ejecutar el bot."
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
# Helpers internos
# ---------------------------------------------------------------------------

def _make_message_id(smtp_user: str, smtp_host: str) -> str:
    """Genera un Message-ID válido priorizando el dominio del email del remitente."""
    if "@" in smtp_user:
        domain = smtp_user.split("@", 1)[1].strip()
        if "." in domain:
            return make_msgid(domain=domain)
    parts = smtp_host.split(".")
    if len(parts) >= 2:
        return make_msgid(domain=".".join(parts[-2:]))
    return make_msgid(domain="jobbot.local")


def _derivar_keywords(perfil_cv: str, rubro: Optional[str]) -> list[str]:
    """
    Infiere las keywords relevantes para el CV dinámico a partir del perfil
    de CV y el rubro de la empresa, sin requerir datos adicionales en la DB.

    La lógica es intencional e independiente del módulo de scoring:
    mailer.py conoce el perfil final (CV_Tech / CV_Admin_IT) y el rubro
    detectado — con eso alcanza para personalizar el CV en forma útil.

    Args:
        perfil_cv: 'CV_Tech' | 'CV_Admin_IT'
        rubro:     Sector detectado durante el scraping (puede ser None).

    Returns:
        Lista de keywords ordenadas por relevancia para ese rubro.
    """
    # Bases por perfil
    base_tech = [
        "Python", "Linux", "Redes TCP/IP", "Soporte IT",
        "Git", "Scripting Bash", "Administración de sistemas", "Docker",
    ]
    base_admin = [
        "Microsoft 365", "Gestión documental", "Facturación AFIP",
        "Atención al cliente", "Tango Gestión", "Soporte IT",
        "Automatización de procesos", "Administración",
    ]

    keywords = base_tech if perfil_cv == "CV_Tech" else base_admin

    # Enriquecimiento por rubro — sobrescribe la base con keywords más específicas
    if rubro:
        r = rubro.lower()
        if any(t in r for t in ("software", "sistemas", "saas", "devops", "dev", "qa")):
            keywords = [
                "Python", "QA / Testing", "Git", "APIs REST",
                "Linux", "CI/CD", "Soporte IT", "Scripting",
            ]
        elif any(t in r for t in ("clínica", "salud", "médico", "laboratorio", "sanatorio")):
            keywords = [
                "Historia clínica digital", "Gestión de turnos",
                "Microsoft 365", "Soporte IT", "Facturación a obras sociales",
                "Administración sanitaria", "Atención al paciente",
            ]
        elif any(t in r for t in ("contable", "estudio", "auditoría", "impositivo")):
            keywords = [
                "AFIP / ARCA", "Factura electrónica", "Tango Gestión",
                "Excel avanzado", "Soporte IT", "Gestión documental",
                "Liquidación de sueldos",
            ]
        elif any(t in r for t in ("inmobiliaria", "propiedades", "real estate")):
            keywords = [
                "CRM inmobiliario", "Gestión documental", "Microsoft 365",
                "Atención al cliente", "Administración de contratos",
                "Soporte IT",
            ]
        elif any(t in r for t in ("logística", "transporte", "distribuidora")):
            keywords = [
                "Gestión de flota", "Excel avanzado", "Soporte IT",
                "Administración", "Tracking de envíos", "Facturación",
            ]

    return keywords


def _enviar_via_smtp(config: ConfigSMTP, msg: EmailMessage) -> bool:
    """
    Envía un email vía SMTP con TLS. Función sincrónica — se llama desde
    asyncio.to_thread para no bloquear el event loop durante la conexión.
    """
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
# Pipeline de construcción del email — async
# ---------------------------------------------------------------------------

async def _preparar_adjunto_dinamico(
    nombre_empresa: str,
    perfil_cv: str,
    rubro: Optional[str],
    sender_name: str,
) -> tuple[bytes, str]:
    """
    Compila el CV personalizado para esta empresa y devuelve los bytes
    del PDF junto con el nombre de archivo sugerido para el adjunto.

    Args:
        nombre_empresa: Nombre de la empresa (va en la carta y en el filename).
        perfil_cv:      'CV_Tech' | 'CV_Admin_IT'
        rubro:          Sector detectado (para enriquecer keywords).
        sender_name:    Nombre del remitente (para el filename).

    Returns:
        (pdf_bytes, filename) donde filename tiene el formato:
        CV_Alaska_TechMDP_SRL.pdf

    Raises:
        CVCompilationError: Si la compilación falla (propagada hacia arriba).
    """
    keywords = _derivar_keywords(perfil_cv, rubro)

    logger.debug(
        "Preparando adjunto dinámico | empresa='%s' | perfil=%s | keywords=%s",
        nombre_empresa, perfil_cv, ", ".join(keywords),
    )

    pdf_bytes = await compilar_cv_dinamico(nombre_empresa, keywords)

    # Nombre de archivo limpio y descriptivo
    nombre_limpio = re.sub(r'[^\w\s-]', '', nombre_empresa).strip().replace(' ', '_')
    filename      = f"CV_{sender_name}_{nombre_limpio}.pdf"

    return pdf_bytes, filename


def _render_template(template: str, **kwargs: str) -> str:
    class _SafeMap(dict):
        def __missing__(self, key: str) -> str:
            logger.warning(
                "Placeholder desconocido '{%s}' en plantilla de email — "
                "posiblemente proveniente del nombre de empresa. Se preserva literal.",
                key,
            )
            return "{" + key + "}"

    try:
        return template.format_map(_SafeMap(kwargs))
    except (ValueError, AttributeError) as exc:
        # Malformed placeholder like {!invalid} — fall back to the template as-is
        logger.error(
            "Error irrecuperable en plantilla | %s: %s | template='%s…'",
            type(exc).__name__, exc, template[:60],
        )
        return template


async def _construir_email(
    config: ConfigSMTP,
    destinatario: str,
    nombre_empresa: str,
    perfil_cv: str,
    rubro: Optional[str],
) -> tuple[EmailMessage, str]:
    """
    Construye el EmailMessage completo con cuerpo y CV adjunto.

    Es async porque necesita await de _preparar_adjunto_dinamico(),
    que a su vez await-ea la compilación de Typst.

    Args:
        config:         Configuración SMTP con credenciales del remitente.
        destinatario:   Dirección de email destino.
        nombre_empresa: Nombre de la empresa (se inyecta en asunto y cuerpo).
        perfil_cv:      Perfil de CV para derivar keywords.
        rubro:          Sector de la empresa (enriquece keywords).

    Returns:
        (EmailMessage listo para enviar, asunto usado para registrar en DB)
    """
    firma = FIRMA_TEMPLATE.format(
        nombre_remitente=config.sender_name,
        email_remitente=config.user,
        github_user=config.github_user,
        linkedin_user=config.linkedin_user,
    )

    asunto = _render_template(
        random.choice(ASUNTOS),
        nombre_empresa=nombre_empresa,
    )
    cuerpo = _render_template(
        random.choice(CUERPOS),
        nombre_remitente=config.sender_name,
        nombre_empresa=nombre_empresa,
        firma=firma,
    )

    msg = EmailMessage()
    msg["From"]       = f"{config.sender_name} <{config.user}>"
    msg["To"]         = destinatario
    msg["Subject"]    = asunto
    msg["Date"]       = formatdate(localtime=True)
    msg["Message-ID"] = _make_message_id(config.user, config.host)
    msg.set_content(cuerpo, charset="utf-8")

    # Compilar y adjuntar el CV dinámico
    pdf_bytes, filename = await _preparar_adjunto_dinamico(
        nombre_empresa, perfil_cv, rubro, config.sender_name,
    )
    msg.add_attachment(
        pdf_bytes,
        maintype="application",
        subtype="pdf",
        filename=filename,
    )

    logger.debug(
        "Email construido | to=%s | subject='%s' | adjunto=%s (%d bytes)",
        destinatario, asunto[:50], filename, len(pdf_bytes),
    )
    return msg, asunto


# ---------------------------------------------------------------------------
# Pipeline principal — ahora async
# ---------------------------------------------------------------------------

async def procesar_envios_pendientes(
    min_score: int = 55,
    limite_empresas: int = 50,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Obtiene empresas aptas, verifica cooldown, compila CVs dinámicos,
    construye y envía correos, y registra resultados en la DB.

    CAMBIO v2.0: función async.
    El jitter usa await asyncio.sleep() — no bloquea el event loop
    durante las esperas de 3–8 minutos entre envíos.

    Args:
        min_score:       Score mínimo para considerar una empresa apta.
        limite_empresas: Máximo de empresas a procesar en esta ejecución.
        dry_run:         Si True, compila el CV pero NO envía ni registra.

    Returns:
        {'procesadas': N, 'enviadas': N, 'omitidas': N, 'errores': N}
    """
    config   = ConfigSMTP.from_env()
    metricas = {"procesadas": 0, "enviadas": 0, "omitidas": 0, "errores": 0}

    if dry_run:
        logger.warning("=== MODO DRY-RUN: no se enviará ningún correo real ===")

    empresas = await asyncio.to_thread(
        get_empresas_listas_para_envio,
        min_score=min_score, limit=limite_empresas,
    )
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
        rubro      = empresa["rubro"]
        score      = empresa["score"]
        metricas["procesadas"] += 1

        logger.info(
            "--- Procesando | empresa='%s' | dominio=%s | score=%d | perfil=%s ---",
            nombre, dominio, score, perfil_cv,
        )

        # Doble check de cooldown (race condition safety)
        en_cd = await asyncio.to_thread(esta_en_cooldown, empresa_id)
        if en_cd:
            logger.info("En cooldown, omitiendo | empresa='%s'", nombre)
            metricas["omitidas"] += 1
            continue

        # Selección del contacto de máxima prioridad
        contactos = await asyncio.to_thread(get_contactos_by_empresa, empresa_id)
        contactos_email = [
            c for c in contactos
            if c["tipo"] in ("RRHH", "General") and "@" in c["email_o_link"]
        ]
        if not contactos_email:
            logger.info("Sin emails disponibles, omitiendo | empresa='%s'", nombre)
            metricas["omitidas"] += 1
            continue

        contacto_obj = sorted(contactos_email, key=lambda c: c["prioridad"])[0]
        if contacto_obj["prioridad"] > 3:
            logger.info("Prioridad demasiado baja, omitiendo | empresa='%s'", nombre)
            metricas["omitidas"] += 1
            continue

        destinatario = contacto_obj["email_o_link"]

        # Construir email con CV dinámico (compilación Typst en subproceso)
        try:
            msg, asunto_usado = await _construir_email(
                config, destinatario, nombre, perfil_cv, rubro,
            )
        except CVCompilationError as exc:
            logger.error(
                "Fallo de compilación Typst | empresa='%s' | %s",
                nombre, str(exc)[:300],
            )
            metricas["errores"] += 1
            continue
        except FileNotFoundError as exc:
            logger.error("Plantilla Typst no encontrada | %s", exc)
            metricas["errores"] += 1
            continue
        except Exception as exc:
            logger.error(
                "Error inesperado construyendo email | empresa='%s' | %s: %s",
                nombre, type(exc).__name__, str(exc)[:200],
            )
            metricas["errores"] += 1
            continue

        # Rate limiting — asyncio.sleep no bloquea el loop durante el jitter
        if not es_primer_envio and not dry_run:
            sleep_seg = random.randint(SMTP_JITTER_MIN_S, SMTP_JITTER_MAX_S)
            logger.info(
                "Rate limit: %d seg (~%.1f min)…", sleep_seg, sleep_seg / 60
            )
            await asyncio.sleep(sleep_seg)

        # Envío real o simulado
        if dry_run:
            logger.info(
                "[DRY-RUN] OK | to=%s | subject='%s' | perfil=%s",
                destinatario, asunto_usado[:60], perfil_cv,
            )
            metricas["enviadas"] += 1
            es_primer_envio = False
            continue

        # _enviar_via_smtp es sync (smtplib) → to_thread para no bloquear
        exito = await asyncio.to_thread(_enviar_via_smtp, config, msg)

        if exito:
            envio_id = await asyncio.to_thread(
                registrar_envio,
                empresa_id,
                f"CV_{config.sender_name}_{nombre}.pdf",
                asunto_usado,
                "enviado",
            )
            logger.info(
                "✓ Enviado | empresa='%s' | to=%s | envio_id=%d",
                nombre, destinatario, envio_id,
            )
            metricas["enviadas"] += 1
        else:
            await asyncio.to_thread(
                registrar_envio,
                empresa_id,
                f"CV_{config.sender_name}_{nombre}.pdf",
                asunto_usado,
                "rebotado",
            )
            logger.warning("✗ Fallo de envío | empresa='%s' | to=%s", nombre, destinatario)
            metricas["errores"] += 1

        es_primer_envio = False

    logger.info(
        "=== Campaña finalizada | procesadas=%d | enviadas=%d | "
        "omitidas=%d | errores=%d ===",
        metricas["procesadas"], metricas["enviadas"],
        metricas["omitidas"],   metricas["errores"],
    )
    return metricas


# ---------------------------------------------------------------------------
# Entrypoint CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="JobBot — Motor de envío de emails")
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--min-score", type=int, default=55)
    parser.add_argument("--limite",    type=int, default=20)
    args = parser.parse_args()

    metricas = asyncio.run(
        procesar_envios_pendientes(
            min_score=args.min_score,
            limite_empresas=args.limite,
            dry_run=args.dry_run,
        )
    )
    print(f"\nResultado: {metricas}")