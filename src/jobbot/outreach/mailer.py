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

from jobbot.config import SENDER_NAME, SMTP_JITTER_MIN_S, SMTP_JITTER_MAX_S
from jobbot.cv.builder import compilar_cv_dinamico, CVCompilationError
from jobbot.db.manager import (
    esta_en_cooldown,
    get_asuntos_usados_by_empresa,
    get_contactos_by_empresa,
    get_empresas_listas_para_envio,
    registrar_envio,
)
from jobbot.outreach.templates import ASUNTOS, CUERPOS, FIRMA_TEMPLATE

logger = logging.getLogger("jobbot.mailer")


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
    mailer.py conoce el perfil final (CV_Tech / CV_Admin_IT / CV_Hybrid) y el rubro
    detectado — con eso alcanza para personalizar el CV en forma útil.

    Args:
        perfil_cv: 'CV_Tech' | 'CV_Admin_IT' | 'CV_Hybrid'
        rubro:     Sector detectado durante el scraping (puede ser None).

    Returns:
        Lista de keywords ordenadas por relevancia para ese rubro.
    """
    base_tech = [
        "Go/Fiber", "PostgreSQL", "SvelteKit", "TypeScript",
        "Rust/Tauri", "Linux", "Pentesting", "APIs REST",
    ]
    base_admin = [
        "Facturación AFIP", "POS Morphi", "Control de stock",
        "Microsoft 365", "Excel avanzado", "Soporte IT",
        "Windows Server/GPO", "Gestión documental",
    ]
    base_hybrid = [
        "Operaciones IT", "Python automation", "Soporte técnico",
        "Redes TCP/IP", "Linux/Windows", "Hardening",
        "Documentación", "Mejora de procesos",
    ]

    if perfil_cv == "CV_Tech":
        keywords = base_tech
    elif perfil_cv == "CV_Hybrid":
        keywords = base_hybrid
    else:
        keywords = base_admin

    # Enriquecimiento por rubro — sobrescribe la base con keywords más específicas
    if rubro:
        r = rubro.lower()
        if any(t in r for t in ("software", "sistemas", "saas", "devops", "dev", "qa")):
            keywords = [
                "Go/Fiber", "PostgreSQL", "SvelteKit", "APIs REST",
                "Linux", "CI/CD", "Pentesting", "Cloudflare",
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
                "Operaciones IT", "Excel avanzado", "Soporte técnico",
                "Automatización Python", "Tracking de envíos", "Redes TCP/IP",
            ]
        elif any(t in r for t in ("manufactura", "industrial", "planta", "consultora")):
            keywords = [
                "Infraestructura IT", "Automatización de procesos",
                "Documentación técnica", "Soporte a usuarios",
                "Linux/Windows", "Stock e inventario", "Python",
            ]

    return keywords


def _seleccionar_indice_template(
    nombre_empresa: str,
    asuntos_usados: set[str],
) -> int:
    candidatos: list[int] = []
    for idx, asunto_template in enumerate(ASUNTOS):
        asunto_renderizado = _render_template(
            asunto_template,
            nombre_empresa=nombre_empresa,
        )
        if asunto_renderizado not in asuntos_usados:
            candidatos.append(idx)

    if not candidatos:
        candidatos = list(range(len(ASUNTOS)))
    return random.choice(candidatos)


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

    pdf_bytes = await compilar_cv_dinamico(nombre_empresa, keywords, perfil_cv=perfil_cv)

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
    asuntos_usados: set[str],
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

    template_idx = _seleccionar_indice_template(nombre_empresa, asuntos_usados)
    asunto = _render_template(
        ASUNTOS[template_idx],
        nombre_empresa=nombre_empresa,
    )
    cuerpo = _render_template(
        CUERPOS[template_idx % len(CUERPOS)],
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
        asuntos_usados = await asyncio.to_thread(
            get_asuntos_usados_by_empresa,
            empresa_id,
        )

        # Construir email con CV dinámico (compilación Typst en subproceso)
        try:
            msg, asunto_usado = await _construir_email(
                config, destinatario, nombre, perfil_cv, rubro, asuntos_usados,
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
        if not dry_run:
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
