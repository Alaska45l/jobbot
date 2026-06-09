"""
aticma/pipeline.py — JobBot ATICMA Pipeline
Pipeline end-to-end para postulación a empresas ATICMA de Mar del Plata.

Pasos:
  1. Importar empresas ATICMA a la DB (loader.py)
  2. Scrapear sitios web de las que tienen URL (scraper/engine.py)
  3. Analizar HTML y extraer keywords/servicios (scraper/extractor.py)
  4. Refinar routing de CV según datos scrapeados (router.py)
  5. Generar y enviar emails con CV personalizado (outreach/mailer.py)

Python: 3.11+
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

from jobbot.aticma.loader import import_aticma_to_db
from jobbot.aticma.router import route_company_to_cv, derive_puesto_objetivo
from jobbot.db.manager import (
    get_empresas_aticma_listas_para_envio,
    get_empresas_aticma_pendientes,
    get_empresas_aticma_stats,
    update_empresa_scraped_data,
    get_contactos_by_empresa,
    get_asuntos_usados_by_empresa,
    esta_en_cooldown,
    registrar_envio,
)
from jobbot.scraper.extractor import analyze_company_html
from jobbot.cv.builder import compilar_cv_dinamico, CVCompilationError

logger = logging.getLogger("jobbot.aticma")


# ---------------------------------------------------------------------------
# Fase 1: Importación de datos ATICMA
# ---------------------------------------------------------------------------

async def fase_import(
    file_path: Optional[str] = None,
    estado: object = None,
) -> dict[str, int]:
    """Importa empresas ATICMA a la DB. Idempotente (upsert)."""
    if estado:
        estado.fase_actual = "ATICMA: Importando empresas a DB…"

    kwargs = {}
    if file_path:
        kwargs["file_path"] = file_path

    stats = await asyncio.to_thread(import_aticma_to_db, **kwargs)

    logger.info("ATICMA importación completa: %s", stats)
    if estado:
        estado.fase_actual = (
            f"ATICMA: {stats['imported']} empresas importadas, "
            f"{stats['contacts']} contactos"
        )
    return stats


# ---------------------------------------------------------------------------
# Fase 2: Scraping de sitios web con datos reales
# ---------------------------------------------------------------------------

async def fase_scrape(
    concurrencia: int = 2,
    estado: object = None,
) -> dict[str, int]:
    """
    Scrapea los sitios web de las empresas ATICMA pendientes.
    Usa el motor de scraping existente para navegación stealth.
    """
    stats = {"scrapeadas": 0, "sin_sitio": 0, "errores": 0}

    pendientes = await asyncio.to_thread(get_empresas_aticma_pendientes, 200)
    if not pendientes:
        logger.info("ATICMA: No hay empresas pendientes de scraping.")
        if estado:
            estado.fase_actual = "ATICMA: Sin empresas pendientes de scraping"
        return stats

    total = len(pendientes)
    if estado:
        estado.fase_actual = f"ATICMA: Scrapeando {total} sitios…"

    # Import lazy para evitar cargar Playwright si no se necesita
    try:
        from jobbot.scraper.engine import scrape_dominio
    except ImportError:
        logger.error("No se pudo importar scraper.engine — ¿Playwright instalado?")
        return stats

    sem = asyncio.Semaphore(concurrencia)

    async def _scrape_one(empresa_row) -> None:
        empresa_id = empresa_row["id"]
        nombre = empresa_row["nombre"]
        dominio = empresa_row["dominio"]

        # Empresas sin sitio real (.local) — usar solo datos del JSON
        if dominio.endswith(".local"):
            desc_aticma = empresa_row["descripcion_aticma"] or ""
            perfil_cv = route_company_to_cv(desc_aticma, nombre)
            puesto = derive_puesto_objetivo(perfil_cv)

            await asyncio.to_thread(
                update_empresa_scraped_data,
                empresa_id,
                keywords_scraped="",
                descripcion_scraped=desc_aticma,
                tiene_vacantes=False,
                perfil_cv=perfil_cv,
            )
            stats["sin_sitio"] += 1
            logger.info(
                "ATICMA sin sitio | empresa=%s | perfil=%s | puesto=%s",
                nombre, perfil_cv, puesto,
            )
            return

        async with sem:
            if estado:
                estado.fase_actual = f"ATICMA: Scrapeando {nombre}…"

            try:
                html = await scrape_dominio(dominio)
                if not html:
                    logger.warning("ATICMA: HTML vacío | empresa=%s", nombre)
                    stats["errores"] += 1
                    return

                # Analizar el HTML con el extractor
                profile = analyze_company_html(html, nombre)

                # Re-routing con keywords scrapeadas
                desc_aticma = empresa_row["descripcion_aticma"] or ""
                perfil_cv = route_company_to_cv(
                    descripcion=desc_aticma,
                    nombre=nombre,
                    scraped_keywords=profile.technologies + profile.services,
                )

                keywords_json = json.dumps(
                    profile.technologies + profile.services,
                    ensure_ascii=False,
                )

                await asyncio.to_thread(
                    update_empresa_scraped_data,
                    empresa_id,
                    keywords_scraped=keywords_json,
                    descripcion_scraped=profile.description[:500],
                    tiene_vacantes=profile.has_openings,
                    perfil_cv=perfil_cv,
                    score=70 if profile.has_openings else 55,
                )
                stats["scrapeadas"] += 1

                logger.info(
                    "ATICMA scrapeada | empresa=%s | techs=%d | services=%d | "
                    "vacantes=%s | perfil=%s",
                    nombre, len(profile.technologies), len(profile.services),
                    profile.has_openings, perfil_cv,
                )

            except Exception as exc:
                logger.error(
                    "ATICMA scrape error | empresa=%s | %s: %s",
                    nombre, type(exc).__name__, str(exc)[:200],
                )
                stats["errores"] += 1

    # Ejecutar scraping en paralelo con límite de concurrencia
    tasks = [_scrape_one(emp) for emp in pendientes]
    await asyncio.gather(*tasks, return_exceptions=True)

    if estado:
        estado.fase_actual = (
            f"ATICMA scraping completo: {stats['scrapeadas']} OK, "
            f"{stats['sin_sitio']} sin sitio, {stats['errores']} errores"
        )
    logger.info("ATICMA scraping completo: %s", stats)
    return stats


# ---------------------------------------------------------------------------
# Fase 3: Envío de postulaciones
# ---------------------------------------------------------------------------

async def fase_mail(
    dry_run: bool = False,
    estado: object = None,
) -> dict[str, int]:
    """
    Envía emails de postulación a empresas ATICMA listas.
    Similar a procesar_envios_pendientes pero adaptado para ATICMA:
    - No requiere min_score (pre-calificadas)
    - Usa el router para puesto_objetivo
    - Personaliza el CV con datos scrapeados
    """
    import os
    import random
    import re
    import smtplib
    from email.message import EmailMessage
    from email.utils import formatdate, make_msgid

    from jobbot.config import SMTP_JITTER_MIN_S, SMTP_JITTER_MAX_S
    from jobbot.outreach.mailer import (
        ConfigSMTP,
        _construir_email,
        _enviar_via_smtp,
    )

    metricas = {"procesadas": 0, "enviadas": 0, "omitidas": 0, "errores": 0}

    config = ConfigSMTP.from_env()

    if dry_run:
        logger.warning("=== ATICMA MODO DRY-RUN: no se enviará ningún correo ===")

    if estado:
        label = "[DRY-RUN] " if dry_run else ""
        estado.fase_actual = f"{label}ATICMA: Preparando envíos…"

    empresas = await asyncio.to_thread(
        get_empresas_aticma_listas_para_envio, limit=100,
    )

    if not empresas:
        logger.info("ATICMA: No hay empresas listas para envío.")
        if estado:
            estado.fase_actual = "ATICMA: Sin empresas pendientes de envío"
        return metricas

    logger.info("ATICMA: %d empresas listas para envío", len(empresas))
    es_primer_envio = True

    for empresa in empresas:
        empresa_id = empresa["id"]
        nombre = empresa["nombre"]
        dominio = empresa["dominio"]
        perfil_cv = empresa["perfil_cv"] or "CV_IT_QA"
        rubro = empresa["rubro"]
        metricas["procesadas"] += 1

        if estado:
            estado.fase_actual = f"ATICMA: Procesando {nombre}…"

        logger.info(
            "ATICMA procesando | empresa='%s' | dominio=%s | perfil=%s",
            nombre, dominio, perfil_cv,
        )

        # Verificar cooldown
        en_cd = await asyncio.to_thread(esta_en_cooldown, empresa_id)
        if en_cd:
            logger.info("ATICMA en cooldown | empresa='%s'", nombre)
            metricas["omitidas"] += 1
            continue

        # Buscar contacto de email
        contactos = await asyncio.to_thread(get_contactos_by_empresa, empresa_id)
        contactos_email = [
            c for c in contactos
            if c["tipo"] in ("RRHH", "General") and "@" in c["email_o_link"]
        ]
        if not contactos_email:
            logger.info("ATICMA sin email | empresa='%s'", nombre)
            metricas["omitidas"] += 1
            continue

        # Priorizar RRHH sobre General
        contacto = sorted(contactos_email, key=lambda c: c["prioridad"])[0]
        destinatario = contacto["email_o_link"]

        asuntos_usados = await asyncio.to_thread(
            get_asuntos_usados_by_empresa, empresa_id,
        )

        # Construir email
        try:
            msg, asunto_usado = await _construir_email(
                config, destinatario, nombre, perfil_cv, rubro, asuntos_usados,
            )
        except CVCompilationError as exc:
            logger.error(
                "ATICMA Typst fail | empresa='%s' | %s",
                nombre, str(exc)[:300],
            )
            metricas["errores"] += 1
            continue
        except Exception as exc:
            logger.error(
                "ATICMA build error | empresa='%s' | %s: %s",
                nombre, type(exc).__name__, str(exc)[:200],
            )
            metricas["errores"] += 1
            continue

        # Rate limiting
        if not es_primer_envio and not dry_run:
            sleep_s = random.randint(SMTP_JITTER_MIN_S, SMTP_JITTER_MAX_S)
            logger.info("ATICMA rate limit: %d seg…", sleep_s)
            await asyncio.sleep(sleep_s)

        # Enviar o simular
        if dry_run:
            logger.info(
                "[DRY-RUN] ATICMA | to=%s | subject='%s' | perfil=%s",
                destinatario, asunto_usado[:60], perfil_cv,
            )
            metricas["enviadas"] += 1
            es_primer_envio = False
            continue

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
                "✓ ATICMA enviado | empresa='%s' | to=%s | envio_id=%d",
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
            logger.warning(
                "✗ ATICMA fallo | empresa='%s' | to=%s",
                nombre, destinatario,
            )
            metricas["errores"] += 1

        es_primer_envio = False

    if estado:
        estado.fase_actual = (
            f"ATICMA envíos completos: "
            f"Enviados={metricas['enviadas']} | "
            f"Errores={metricas['errores']} | "
            f"Omitidos={metricas['omitidas']}"
        )
    logger.info("ATICMA campaña finalizada: %s", metricas)
    return metricas


# ---------------------------------------------------------------------------
# Pipeline completo
# ---------------------------------------------------------------------------

async def pipeline_aticma(
    args,
    estado,
) -> None:
    """
    Pipeline end-to-end ATICMA:
      1. Import → 2. Scrape → 3. Mail
    """
    logger.info("=== Pipeline ATICMA iniciado ===")

    file_path = getattr(args, "aticma_file", None)
    dry_run = getattr(args, "dry_run", False)
    concurrencia = getattr(args, "concurrencia", 2)

    # Fase 1: Importar empresas
    import_stats = await fase_import(file_path=file_path, estado=estado)

    # Fase 2: Scrapear sitios web
    scrape_stats = await fase_scrape(
        concurrencia=concurrencia, estado=estado,
    )

    # Mostrar estadísticas pre-envío
    aticma_stats = await asyncio.to_thread(get_empresas_aticma_stats)
    logger.info("ATICMA estadísticas DB: %s", aticma_stats)

    # Fase 3: Enviar postulaciones
    mail_stats = await fase_mail(dry_run=dry_run, estado=estado)

    # Resumen final
    estado.fase_actual = (
        f"ATICMA completo | "
        f"Import: {import_stats['imported']} | "
        f"Scrape: {scrape_stats['scrapeadas']} OK | "
        f"Mail: {mail_stats['enviadas']} enviados"
    )
    logger.info("=== Pipeline ATICMA finalizado ===")
