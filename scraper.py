"""
scraper.py — JobBot Async Stealth Scraper
Módulo de scraping asíncrono con evasión antibot, navegación profunda
y bloqueo de media para eficiencia de red.

Python: 3.11+
Dependencias: playwright, asyncio, urllib (stdlib)
"""
from __future__ import annotations

import asyncio
import logging
import random
import re
import urllib.parse
import urllib.robotparser
from typing import Optional
from urllib.error import URLError

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError,
)

from scoring import analizar_empresa, ResultadoScoring
from db_manager import (
    init_db,
    upsert_empresa,
    insert_contacto,
    get_empresa_by_dominio,
    get_connection,
)
from utils.browser import CHROMIUM_ARGS, apply_stealth   # REFACTOR: centralizado

logger = logging.getLogger("jobbot.scraper")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
NAV_TIMEOUT_MS: int      = 25_000
WAIT_LOAD_MS: int        = 4_000
BETWEEN_PAGES_MIN: float = 1.8
BETWEEN_PAGES_MAX: float = 4.5
SCRAPING_COOLDOWN_DAYS: int = 7

PRIORITY_PATHS: tuple[str, ...] = (
    "/contacto", "/contactanos", "/contact",
    "/nosotros", "/about", "/sobre-nosotros", "/quienes-somos",
    "/equipo", "/team", "/staff",
    "/empresa", "/company",
    "/recursos-humanos", "/rrhh", "/trabaja-con-nosotros", "/empleos",
)

BLOCKED_RESOURCE_TYPES: frozenset[str] = frozenset({
    "image", "media", "font", "stylesheet",
    "websocket", "eventsource", "manifest", "other",
})

BLOCKED_DOMAINS: frozenset[str] = frozenset({
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "tiktok.com", "youtube.com", "google.com", "google.com.ar",
    "whatsapp.com", "mercadolibre.com.ar", "mercadopago.com",
    "wikipedia.org", "computrabajo.com.ar", "zonajobs.com.ar",
    "bumeran.com.ar", "linkedin.com", "randstad.com.ar",
})

USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
)

VIEWPORTS: tuple[dict[str, int], ...] = (
    {"width": 1920, "height": 1080},
    {"width": 1680, "height": 1050},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
)


# ---------------------------------------------------------------------------
# Cooldown de scraping
# ---------------------------------------------------------------------------

def _esta_en_cooldown_scraping(
    empresa_id: int,
    cooldown_days: int = SCRAPING_COOLDOWN_DAYS,
) -> bool:
    from datetime import datetime, timedelta, timezone
    cutoff = (
        datetime.now(tz=timezone.utc) - timedelta(days=cooldown_days)
    ).isoformat()

    sql = """
        SELECT 1 FROM empresas
        WHERE id = ?
          AND fecha_scraping >= ?
        LIMIT 1;
    """
    with get_connection() as conn:
        resultado = conn.execute(sql, (empresa_id, cutoff)).fetchone()

    en_cooldown = resultado is not None
    logger.debug(
        "Cooldown scraping | empresa_id=%d | en_cooldown=%s | ventana=%d días",
        empresa_id, en_cooldown, cooldown_days,
    )
    return en_cooldown


# ---------------------------------------------------------------------------
# Helpers de dominio
# ---------------------------------------------------------------------------

def _normalizar_dominio(url_o_dominio: str) -> str:
    url = url_o_dominio.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def _extraer_dominio_raiz(url: str) -> str:
    return urllib.parse.urlparse(url).netloc.lower().lstrip("www.")


def _es_dominio_bloqueado(url: str) -> bool:
    dominio = _extraer_dominio_raiz(url)
    return any(dominio.endswith(b) for b in BLOCKED_DOMAINS)


def _es_enlace_interno(url: str, dominio_base: str) -> bool:
    netloc = urllib.parse.urlparse(url).netloc.lower().lstrip("www.")
    return netloc == dominio_base or netloc == f"www.{dominio_base}"


# ---------------------------------------------------------------------------
# robots.txt
# ---------------------------------------------------------------------------

def _verificar_robots(url_base: str, user_agent: str = "*") -> bool:
    robots_url = urllib.parse.urljoin(url_base, "/robots.txt")
    parser = urllib.robotparser.RobotFileParser()
    parser.set_url(robots_url)
    try:
        parser.read()
        permitido = parser.can_fetch(user_agent, url_base)
        if not permitido:
            logger.warning("robots.txt bloquea el acceso | url=%s", url_base)
        return permitido
    except Exception as exc:
        logger.debug("robots.txt no disponible, asumiendo permiso | error=%s", exc)
        return True


# ---------------------------------------------------------------------------
# Contexto stealth
# ---------------------------------------------------------------------------

async def _crear_contexto_stealth(browser: Browser) -> BrowserContext:
    """
    Crea un contexto aislado a partir del browser compartido.
    REFACTOR v1.2: usa apply_stealth() de utils.browser en lugar de
    add_init_script() inline (eliminada la copia duplicada).
    """
    ua = random.choice(USER_AGENTS)
    vp = random.choice(VIEWPORTS)

    context = await browser.new_context(
        user_agent=ua,
        viewport=vp,
        locale="es-AR",
        timezone_id="America/Argentina/Buenos_Aires",
        permissions=[],
        java_script_enabled=True,
        accept_downloads=False,
        ignore_https_errors=False,
        extra_http_headers={
            "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT":             "1",
        },
    )

    await apply_stealth(context)   # REFACTOR: centralizado en utils.browser

    logger.debug(
        "Contexto stealth creado | ua=...%s | viewport=%dx%d",
        ua[-30:], vp["width"], vp["height"],
    )
    return context


# ---------------------------------------------------------------------------
# Interceptor de recursos
# ---------------------------------------------------------------------------

async def _bloquear_recursos_innecesarios(page: Page) -> None:
    async def _interceptor(route, request):
        if request.resource_type in BLOCKED_RESOURCE_TYPES:
            await route.abort()
        else:
            await route.continue_()
    await page.route("**/*", _interceptor)


# ---------------------------------------------------------------------------
# Navegación y extracción
# ---------------------------------------------------------------------------

async def _navegar_y_extraer(
    page: Page,
    url: str,
    dominio_base: str,
) -> tuple[str, list[str]]:
    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        if response is None:
            return "", []
        if response.status in (403, 429, 500, 503):
            logger.warning("HTTP %d recibido, saltando | url=%s", response.status, url)
            return "", []

        await asyncio.sleep(random.uniform(BETWEEN_PAGES_MIN, BETWEEN_PAGES_MAX))
        html = await page.content()

        hrefs: list[str] = await page.eval_on_selector_all(
            "a[href]",
            "elements => elements.map(el => el.href).filter(h => h && h.startsWith('http'))",
        )
        enlaces_internos = [
            h for h in hrefs
            if _es_enlace_interno(h, dominio_base) and not _es_dominio_bloqueado(h)
        ]

        logger.info(
            "Página scrapeada | url=%s | status=%d | html=%d chars | links=%d",
            url, response.status, len(html), len(enlaces_internos),
        )
        return html, enlaces_internos

    except PlaywrightTimeoutError:
        logger.error("Timeout | url=%s | %dms", url, NAV_TIMEOUT_MS)
        return "", []
    except PlaywrightError as exc:
        logger.error("Error Playwright | url=%s | %s", url, str(exc)[:200])
        return "", []


async def _navegar_rutas_prioritarias(
    page: Page,
    url_base: str,
    dominio_base: str,
    enlaces_home: list[str],
) -> list[tuple[str, str]]:
    resultados: list[tuple[str, str]] = []
    urls_visitadas: set[str] = {url_base}

    candidatos_directos = {urllib.parse.urljoin(url_base, p) for p in PRIORITY_PATHS}
    links_prioritarios = [
        u for u in enlaces_home
        if any(p in u.lower() for p in PRIORITY_PATHS)
    ]
    for u in candidatos_directos:
        if u not in links_prioritarios:
            links_prioritarios.append(u)

    for url in links_prioritarios:
        if url in urls_visitadas:
            continue
        urls_visitadas.add(url)
        html, _ = await _navegar_y_extraer(page, url, dominio_base)
        if html:
            resultados.append((url, html))

    logger.info("Navegación profunda | dominio=%s | páginas=%d", dominio_base, len(resultados))
    return resultados


# ---------------------------------------------------------------------------
# procesar_dominio
# ---------------------------------------------------------------------------

async def procesar_dominio(
    url_o_dominio: str,
    nombre_empresa: Optional[str] = None,
    rubro: Optional[str] = None,
    min_score_para_log: int = 0,
    browser: Optional[Browser] = None,
    forzar_rescraping: bool = False,
) -> Optional[ResultadoScoring]:
    """
    Pipeline completo para un dominio: robots check → stealth browser →
    scraping profundo → scoring → persistencia en DB.
    """
    url_base     = _normalizar_dominio(url_o_dominio)
    dominio_base = _extraer_dominio_raiz(url_base)
    nombre       = nombre_empresa or dominio_base

    logger.info("Iniciando procesamiento | dominio=%s", dominio_base)

    empresa_existente = await asyncio.to_thread(get_empresa_by_dominio, dominio_base)
    if empresa_existente and not forzar_rescraping:
        en_cooldown = await asyncio.to_thread(
            _esta_en_cooldown_scraping, empresa_existente["id"]
        )
        if en_cooldown:
            logger.info("Empresa en cooldown de scraping | dominio=%s", dominio_base)
            return None

    robots_ok = await asyncio.to_thread(_verificar_robots, url_base)
    if not robots_ok:
        logger.warning("robots.txt deniega acceso | dominio=%s", dominio_base)
        return None

    async def _scrape_con_browser(b: Browser) -> str:
        context = await _crear_contexto_stealth(b)
        try:
            page = await context.new_page()
            await _bloquear_recursos_innecesarios(page)

            html_home, enlaces_home = await _navegar_y_extraer(page, url_base, dominio_base)
            if not html_home:
                logger.error("No se pudo cargar la home | dominio=%s", dominio_base)
                return ""

            # FIX v1.2: list + join en lugar de +=
            # Con += cada iteración crea una nueva string en memoria.
            # Con páginas de 200–500 KB y 10 sub-páginas por dominio,
            # se generaban ~10 objetos intermedios de hasta 5 MB cada uno.
            html_parts = [html_home]
            for _, html_pagina in await _navegar_rutas_prioritarias(
                page, url_base, dominio_base, enlaces_home
            ):
                html_parts.append(html_pagina)

            return "\n".join(html_parts)
        finally:
            await context.close()

    html_total: str = ""
    try:
        if browser is not None:
            html_total = await _scrape_con_browser(browser)
        else:
            async with async_playwright() as pw:
                own_browser = await pw.chromium.launch(
                    headless=True, args=CHROMIUM_ARGS   # REFACTOR: desde utils.browser
                )
                try:
                    html_total = await _scrape_con_browser(own_browser)
                finally:
                    await own_browser.close()

    except PlaywrightError as exc:
        logger.exception("Error crítico Playwright | dominio=%s | %s", dominio_base, str(exc)[:300])
        return None
    except Exception as exc:
        logger.exception("Error inesperado | dominio=%s | %s", dominio_base, str(exc)[:300])
        return None

    if not html_total.strip():
        logger.warning("HTML total vacío | dominio=%s", dominio_base)
        return None

    logger.info("HTML acumulado | dominio=%s | chars=%d", dominio_base, len(html_total))

    resultado: ResultadoScoring = await asyncio.to_thread(
        analizar_empresa, html_total, dominio_base, True,
    )

    if resultado.score_total >= min_score_para_log:
        logger.info(
            "Scoring | dominio=%s | score=%d | perfil=%s | contactos=%d | apto=%s",
            dominio_base, resultado.score_total, resultado.perfil_cv,
            len(resultado.contactos), resultado.apto_envio_auto,
        )

    empresa_id: int = await asyncio.to_thread(
        upsert_empresa,
        nombre, dominio_base,
        rubro or resultado.rubro_detectado,
        resultado.perfil_cv,
        resultado.score_total,
    )

    for contacto in resultado.contactos:
        await asyncio.to_thread(
            insert_contacto,
            empresa_id, contacto.valor, contacto.tipo, contacto.prioridad,
        )

    logger.info(
        "Empresa persistida | dominio=%s | id=%d | contactos=%d",
        dominio_base, empresa_id, len(resultado.contactos),
    )
    return resultado


# ---------------------------------------------------------------------------
# procesar_lote — 1 browser, N contextos aislados
# ---------------------------------------------------------------------------

async def procesar_lote(
    dominios: list[str],
    concurrencia: int = 3,
    min_score_para_log: int = 30,
    forzar_rescraping: bool = False,
) -> dict[str, Optional[ResultadoScoring]]:
    """
    Procesa una lista de dominios con concurrencia controlada.
    Lanza UN solo proceso Chromium y crea un contexto aislado por dominio.
    """
    semaforo  = asyncio.Semaphore(concurrencia)
    resultados: dict[str, Optional[ResultadoScoring]] = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True, args=CHROMIUM_ARGS   # REFACTOR: desde utils.browser
        )
        logger.info(
            "Browser Chromium lanzado | dominios=%d | concurrencia=%d",
            len(dominios), concurrencia,
        )

        async def _tarea(dominio: str) -> None:
            async with semaforo:
                await asyncio.sleep(random.uniform(0.5, 2.5))
                resultado = await procesar_dominio(
                    dominio,
                    min_score_para_log=min_score_para_log,
                    browser=browser,
                    forzar_rescraping=forzar_rescraping,
                )
                resultados[dominio] = resultado

        await asyncio.gather(
            *[asyncio.create_task(_tarea(d)) for d in dominios],
            return_exceptions=True,
        )
        await browser.close()
        logger.info("Browser Chromium cerrado.")

    exitosos = sum(1 for v in resultados.values() if v is not None)
    aptos    = sum(1 for v in resultados.values() if v and v.apto_envio_auto)
    logger.info(
        "Lote completado | total=%d | exitosos=%d | omitidos=%d | aptos_envio=%d",
        len(dominios), exitosos, len(dominios) - exitosos, aptos,
    )
    return resultados


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    init_db()
    dominios_prueba = ["https://recursoshumanos.com.ar", "https://tecnomdp.com.ar"]
    res = asyncio.run(procesar_lote(dominios_prueba, concurrencia=2, min_score_para_log=20))
    for dom, r in res.items():
        if r:
            print(f"\n{'='*60}\n  Dominio: {dom}\n  Score: {r.score_total}\n  Apto: {r.apto_envio_auto}")
        else:
            print(f"\n  {dom}: omitido")