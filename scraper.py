"""
scraper.py — JobBot Async Stealth Scraper
Módulo de scraping asíncrono con evasión antibot, navegación profunda
y bloqueo de media para eficiencia de red.

Cambios v1.1:
  - Browser Chromium compartido en procesar_lote (1 lanzamiento, N contextos)
  - procesar_dominio acepta browser opcional; si no recibe uno, crea el suyo
  - Cooldown de scraping separado del cooldown de envío (SCRAPING_COOLDOWN_DAYS)
  - Import de DDGS movido al top del módulo con ImportError temprano

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
from typing import Optional, Callable
from urllib.error import URLError

try:
    from ddgs import DDGS
except ImportError as exc:
    raise ImportError(
        "duckduckgo-search no encontrado. Instalá con: pip install ddgs"
    ) from exc

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

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("jobbot.scraper")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
NAV_TIMEOUT_MS: int      = 25_000
WAIT_LOAD_MS: int        = 4_000
BETWEEN_PAGES_MIN: float = 1.8
BETWEEN_PAGES_MAX: float = 4.5

# Cooldown independiente para scraping (re-scrapear es barato, re-enviar no)
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

# Args de lanzamiento de Chromium — definidos una sola vez para reutilizar
_CHROMIUM_ARGS: list[str] = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-gpu",
    "--disable-setuid-sandbox",
    "--disable-extensions",
    "--disable-infobars",
    "--memory-pressure-off",
    "--disable-background-networking",
]


# ---------------------------------------------------------------------------
# Cooldown de scraping (separado del cooldown de envío de db_manager)
# ---------------------------------------------------------------------------

def _esta_en_cooldown_scraping(
    empresa_id: int,
    cooldown_days: int = SCRAPING_COOLDOWN_DAYS,
) -> bool:
    """
    Verifica si la empresa fue scrapeada recientemente.
    Usa fecha_scraping de la tabla empresas, no el historial de envíos.

    Separar este cooldown del de envío permite:
      - Re-scrapear una empresa para actualizar score/contactos sin enviar nada.
      - Controlar ambos períodos de forma independiente desde la CLI.

    Args:
        empresa_id:    ID de la empresa.
        cooldown_days: Días mínimos entre scrapings. Default: 7.

    Returns:
        True si fue scrapeada dentro del período de cooldown.
    """
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
# Helpers de dominio (sin cambios)
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
# robots.txt (sin cambios)
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
# Contexto stealth — ahora recibe el Browser ya lanzado
# ---------------------------------------------------------------------------

async def _crear_contexto_stealth(browser: Browser) -> BrowserContext:
    """
    Crea un contexto aislado (cookies, storage, fingerprint propios)
    a partir del browser compartido. Cada dominio recibe su propio contexto,
    pero todos comparten el mismo proceso Chromium.
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

    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins',   { get: () => [1, 2, 3] });
        Object.defineProperty(navigator, 'languages', { get: () => ['es-AR', 'es', 'en'] });
        if (window.chrome) { window.chrome.runtime = {}; }
    """)

    logger.debug("Contexto stealth creado | ua=...%s | viewport=%dx%d", ua[-30:], vp["width"], vp["height"])
    return context


# ---------------------------------------------------------------------------
# Interceptor de recursos (sin cambios)
# ---------------------------------------------------------------------------

async def _bloquear_recursos_innecesarios(page: Page) -> None:
    async def _interceptor(route, request):
        if request.resource_type in BLOCKED_RESOURCE_TYPES:
            await route.abort()
        else:
            await route.continue_()
    await page.route("**/*", _interceptor)


# ---------------------------------------------------------------------------
# Navegación y extracción (sin cambios)
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
# procesar_dominio — ahora acepta un browser externo opcional
# ---------------------------------------------------------------------------

async def procesar_dominio(
    url_o_dominio: str,
    nombre_empresa: Optional[str] = None,
    rubro: Optional[str] = None,
    min_score_para_log: int = 0,
    browser: Optional[Browser] = None,          # ← NUEVO
    forzar_rescraping: bool = False,             # ← NUEVO: ignora cooldown de scraping
) -> Optional[ResultadoScoring]:
    """
    Pipeline completo para un dominio: robots check → stealth browser →
    scraping profundo → scoring → persistencia en DB.

    Si se provee `browser`, lo usa directamente (modo batch eficiente).
    Si no se provee, lanza y destruye su propio proceso Chromium
    (modo standalone, útil para pruebas de un solo dominio).

    Args:
        url_o_dominio:      Dominio o URL semilla.
        nombre_empresa:     Nombre comercial (se infiere del dominio si no se provee).
        rubro:              Sector conocido de antemano (opcional).
        min_score_para_log: Score mínimo para loguear como relevante.
        browser:            Browser Playwright compartido. Si es None, crea uno propio.
        forzar_rescraping:  Si True, ignora el cooldown de scraping (no el de envío).
    """
    url_base     = _normalizar_dominio(url_o_dominio)
    dominio_base = _extraer_dominio_raiz(url_base)
    nombre       = nombre_empresa or dominio_base

    logger.info("Iniciando procesamiento | dominio=%s", dominio_base)

    # ------------------------------------------------------------------
    # 0. Cooldown de scraping (independiente del cooldown de envío)
    # ------------------------------------------------------------------
    empresa_existente = await asyncio.to_thread(get_empresa_by_dominio, dominio_base)
    if empresa_existente and not forzar_rescraping:
        en_cooldown = await asyncio.to_thread(
            _esta_en_cooldown_scraping, empresa_existente["id"]
        )
        if en_cooldown:
            logger.info("Empresa en cooldown de scraping, omitiendo | dominio=%s", dominio_base)
            return None

    # ------------------------------------------------------------------
    # 1. robots.txt
    # ------------------------------------------------------------------
    robots_ok = await asyncio.to_thread(_verificar_robots, url_base)
    if not robots_ok:
        logger.warning("robots.txt deniega acceso | dominio=%s", dominio_base)
        return None

    # ------------------------------------------------------------------
    # 2. Scraping — usa browser externo si se proveyó, o crea uno propio
    # ------------------------------------------------------------------
    html_total: str = ""

    async def _scrape_con_browser(b: Browser) -> str:
        """Lógica de scraping aislada; recibe un browser ya lanzado."""
        context = await _crear_contexto_stealth(b)
        try:
            page = await context.new_page()
            await _bloquear_recursos_innecesarios(page)

            html_home, enlaces_home = await _navegar_y_extraer(page, url_base, dominio_base)
            if not html_home:
                logger.error("No se pudo cargar la home | dominio=%s", dominio_base)
                return ""

            html_acumulado = html_home
            for _, html_pagina in await _navegar_rutas_prioritarias(
                page, url_base, dominio_base, enlaces_home
            ):
                html_acumulado += "\n" + html_pagina

            return html_acumulado
        finally:
            # El contexto siempre se cierra; el browser sigue vivo si es externo
            await context.close()

    try:
        if browser is not None:
            # Modo batch: reutilizar el browser compartido
            html_total = await _scrape_con_browser(browser)
        else:
            # Modo standalone: lanzar y destruir Chromium para este dominio
            async with async_playwright() as pw:
                own_browser = await pw.chromium.launch(
                    headless=True, args=_CHROMIUM_ARGS
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

    # ------------------------------------------------------------------
    # 3. Scoring
    # ------------------------------------------------------------------
    resultado: ResultadoScoring = await asyncio.to_thread(
        analizar_empresa, html_total, dominio_base, True,
    )

    if resultado.score_total >= min_score_para_log:
        logger.info(
            "Scoring | dominio=%s | score=%d | perfil=%s | contactos=%d | apto=%s",
            dominio_base, resultado.score_total, resultado.perfil_cv,
            len(resultado.contactos), resultado.apto_envio_auto,
        )

    # ------------------------------------------------------------------
    # 4. Persistencia
    # ------------------------------------------------------------------
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

    Lanza UN solo proceso Chromium para todo el lote y crea un contexto
    aislado por dominio. Comparado con la versión anterior (1 proceso por
    dominio), el ahorro en tiempo de startup es de ~2-4 segundos por dominio.

    Con 100 dominios y concurrencia=3:
      - Antes: 100 lanzamientos de Chromium en secuencia dentro del semáforo
      - Ahora: 1 lanzamiento, 100 contextos (3 concurrentes)

    Args:
        dominios:          Lista de dominios/URLs a procesar.
        concurrencia:      Máximo de dominios en paralelo. Default 3.
        min_score_para_log: Score mínimo para loguear como relevante.
        forzar_rescraping:  Si True, ignora cooldown de scraping en todos.

    Returns:
        Dict {dominio: ResultadoScoring | None}.
    """
    semaforo  = asyncio.Semaphore(concurrencia)
    resultados: dict[str, Optional[ResultadoScoring]] = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=_CHROMIUM_ARGS)
        logger.info(
            "Browser Chromium lanzado para lote | dominios=%d | concurrencia=%d",
            len(dominios), concurrencia,
        )

        async def _tarea(dominio: str) -> None:
            async with semaforo:
                await asyncio.sleep(random.uniform(0.5, 2.5))  # jitter
                resultado = await procesar_dominio(
                    dominio,
                    min_score_para_log=min_score_para_log,
                    browser=browser,                    # ← browser compartido
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


# ---------------------------------------------------------------------------
# Entrypoint de prueba rápida
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    init_db()

    dominios_prueba = [
        "https://recursoshumanos.com.ar",
        "https://tecnomdp.com.ar",
    ]

    resultados = asyncio.run(
        procesar_lote(dominios_prueba, concurrencia=2, min_score_para_log=20)
    )

    for dominio, res in resultados.items():
        if res:
            print(f"\n{'='*60}")
            print(f"  Dominio   : {dominio}")
            print(f"  Perfil CV : {res.perfil_cv}")
            print(f"  Score     : {res.score_total}")
            print(f"  Apto envío: {res.apto_envio_auto}")
            for c in res.contactos:
                print(f"    [{c.tipo:8}] P{c.prioridad} | +{c.puntos}pts | {c.valor}")
        else:
            print(f"\n  {dominio}: omitido")