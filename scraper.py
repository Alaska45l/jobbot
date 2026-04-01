"""
scraper.py — JobBot Async Stealth Scraper
Módulo de scraping asíncrono con evasión antibot, navegación profunda
y bloqueo de media para eficiencia de red.

Autor: JobBot Project
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
from urllib.request import urlopen
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
    esta_en_cooldown,
    get_empresa_by_dominio,
)

# ---------------------------------------------------------------------------
# Logging estructurado
# ---------------------------------------------------------------------------
logger = logging.getLogger("jobbot.scraper")

# ---------------------------------------------------------------------------
# Constantes de configuración
# ---------------------------------------------------------------------------

# Timeouts en milisegundos (Playwright usa ms)
NAV_TIMEOUT_MS: int     = 25_000
WAIT_LOAD_MS: int       = 4_000
BETWEEN_PAGES_MIN: float = 1.8   # segundos
BETWEEN_PAGES_MAX: float = 4.5

# Rutas internas de alta prioridad a buscar proactivamente
PRIORITY_PATHS: tuple[str, ...] = (
    "/contacto", "/contactanos", "/contact",
    "/nosotros", "/about", "/sobre-nosotros", "/quienes-somos",
    "/equipo", "/team", "/staff",
    "/empresa", "/company",
    "/recursos-humanos", "/rrhh", "/trabaja-con-nosotros", "/empleos",
)

# Tipos de recursos a bloquear para ahorrar ancho de banda
BLOCKED_RESOURCE_TYPES: frozenset[str] = frozenset({
    "image", "media", "font", "stylesheet",
    "websocket", "eventsource", "manifest", "other",
})

# Dominios gigantes a ignorar aunque aparezcan en los resultados
BLOCKED_DOMAINS: frozenset[str] = frozenset({
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "tiktok.com", "youtube.com", "google.com", "google.com.ar",
    "whatsapp.com", "mercadolibre.com.ar", "mercadopago.com",
    "wikipedia.org", "computrabajo.com.ar", "zonajobs.com.ar",
    "bumeran.com.ar", "linkedin.com", "randstad.com.ar",
})

# Pool de User-Agents realistas (Chrome/Firefox en Windows/macOS/Linux)
USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
)

# Viewports realistas para no parecer headless
VIEWPORTS: tuple[dict[str, int], ...] = (
    {"width": 1920, "height": 1080},
    {"width": 1680, "height": 1050},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
)

# ---------------------------------------------------------------------------
# Helpers de dominio
# ---------------------------------------------------------------------------

def _normalizar_dominio(url_o_dominio: str) -> str:
    """
    Asegura que la entrada tenga esquema https://.
    Acepta tanto 'empresa.com.ar' como 'https://empresa.com.ar/pagina'.

    Args:
        url_o_dominio: URL o dominio raw.

    Returns:
        URL con esquema garantizado.
    """
    url = url_o_dominio.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def _extraer_dominio_raiz(url: str) -> str:
    """
    Extrae el dominio raíz (netloc) de una URL completa.

    Args:
        url: URL completa.

    Returns:
        Dominio sin esquema ni path (ej: 'empresa.com.ar').
    """
    parsed = urllib.parse.urlparse(url)
    return parsed.netloc.lower().lstrip("www.")


def _es_dominio_bloqueado(url: str) -> bool:
    """Verifica si una URL pertenece a un dominio en la lista de bloqueo."""
    dominio = _extraer_dominio_raiz(url)
    return any(dominio.endswith(blocked) for blocked in BLOCKED_DOMAINS)


def _es_enlace_interno(url: str, dominio_base: str) -> bool:
    """
    Verifica que una URL pertenezca al mismo dominio que la empresa objetivo.

    Args:
        url:          URL a evaluar.
        dominio_base: Dominio raíz de la empresa (sin www).

    Returns:
        True si el enlace es interno al dominio.
    """
    parsed = urllib.parse.urlparse(url)
    netloc = parsed.netloc.lower().lstrip("www.")
    return netloc == dominio_base or netloc == f"www.{dominio_base}"


# ---------------------------------------------------------------------------
# Verificación de robots.txt
# ---------------------------------------------------------------------------

def _verificar_robots(url_base: str, user_agent: str = "*") -> bool:
    """
    Consulta el robots.txt del dominio y determina si el scraping está permitido.
    Si el archivo no existe o hay un error de red, asume permiso (fail-open).

    Args:
        url_base:   URL raíz de la empresa (ej: 'https://empresa.com.ar').
        user_agent: User-Agent a verificar. Default '*' (any bot).

    Returns:
        True si se permite el scraping, False si está explícitamente prohibido.
    """
    robots_url = urllib.parse.urljoin(url_base, "/robots.txt")
    parser = urllib.robotparser.RobotFileParser()
    parser.set_url(robots_url)

    try:
        parser.read()
        permitido = parser.can_fetch(user_agent, url_base)
        if not permitido:
            logger.warning("robots.txt bloquea el acceso | url=%s", url_base)
        return permitido
    except (URLError, OSError, Exception) as exc:
        # Sin robots.txt o timeout → asumir permitido
        logger.debug("robots.txt no disponible, asumiendo permiso | error=%s", exc)
        return True


# ---------------------------------------------------------------------------
# Configuración del contexto Playwright (stealth)
# ---------------------------------------------------------------------------

async def _crear_contexto_stealth(browser: Browser) -> BrowserContext:
    """
    Crea un contexto de navegador con configuración stealth:
    - User-Agent aleatorio del pool
    - Viewport aleatorio realista
    - Locale e idioma argentino
    - Desactivación de webdriver fingerprint vía JS injection

    Args:
        browser: Instancia de Browser de Playwright.

    Returns:
        BrowserContext configurado y listo para usar.
    """
    ua = random.choice(USER_AGENTS)
    vp = random.choice(VIEWPORTS)

    context = await browser.new_context(
        user_agent=ua,
        viewport=vp,
        locale="es-AR",
        timezone_id="America/Argentina/Buenos_Aires",
        # Deshabilitar WebRTC fingerprinting
        permissions=[],
        # No revelar que es automatización
        java_script_enabled=True,
        accept_downloads=False,
        ignore_https_errors=False,
        extra_http_headers={
            "Accept-Language":  "es-AR,es;q=0.9,en;q=0.8",
            "Accept":           "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding":  "gzip, deflate, br",
            "DNT":              "1",
        },
    )

    # Inyectar JS que elimina los marcadores de automatización de Chromium
    await context.add_init_script("""
        // Eliminar navigator.webdriver
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
        });

        // Simular plugins de un navegador real (Chrome tiene 3 por defecto)
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3],
        });

        // Simular idiomas reales
        Object.defineProperty(navigator, 'languages', {
            get: () => ['es-AR', 'es', 'en'],
        });

        // Remover chrome.runtime solo si existe (evita errores en Firefox)
        if (window.chrome) {
            window.chrome.runtime = {};
        }
    """)

    logger.debug("Contexto stealth creado | ua=%s | viewport=%dx%d", ua, vp["width"], vp["height"])
    return context


# ---------------------------------------------------------------------------
# Interceptor de recursos (bloqueo de media)
# ---------------------------------------------------------------------------

async def _bloquear_recursos_innecesarios(page: Page) -> None:
    """
    Registra un interceptor de rutas que aborta requests de tipos no útiles.
    Ahorra significativamente ancho de banda y tiempo de carga.

    Args:
        page: Página de Playwright sobre la que aplicar el interceptor.
    """
    async def _interceptor(route, request):
        if request.resource_type in BLOCKED_RESOURCE_TYPES:
            await route.abort()
        else:
            await route.continue_()

    await page.route("**/*", _interceptor)


# ---------------------------------------------------------------------------
# Navegación y extracción de HTML
# ---------------------------------------------------------------------------

async def _navegar_y_extraer(
    page: Page,
    url: str,
    dominio_base: str,
) -> tuple[str, list[str]]:
    """
    Navega a una URL, espera la carga, extrae el HTML y los enlaces internos.

    Args:
        page:         Página de Playwright.
        url:          URL a visitar.
        dominio_base: Dominio raíz para filtrar enlaces internos.

    Returns:
        Tupla (html_crudo, lista_de_urls_internas_encontradas).
        Retorna ('', []) ante cualquier error de navegación.
    """
    try:
        response = await page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=NAV_TIMEOUT_MS,
        )

        if response is None:
            logger.warning("Sin respuesta del servidor | url=%s", url)
            return "", []

        status = response.status
        if status in (403, 429, 500, 503):
            logger.warning("HTTP %d recibido, saltando | url=%s", status, url)
            return "", []

        # Pausa humana para evitar detección por velocidad de requests
        await asyncio.sleep(random.uniform(BETWEEN_PAGES_MIN, BETWEEN_PAGES_MAX))

        html = await page.content()

        # Extraer todos los href internos del DOM actual
        hrefs: list[str] = await page.eval_on_selector_all(
            "a[href]",
            """elements => elements
                .map(el => el.href)
                .filter(href => href && href.startsWith('http'))
            """,
        )

        enlaces_internos = [
            h for h in hrefs
            if _es_enlace_interno(h, dominio_base) and not _es_dominio_bloqueado(h)
        ]

        logger.info(
            "Página scrapeada | url=%s | status=%d | html=%d chars | links_internos=%d",
            url, status, len(html), len(enlaces_internos),
        )
        return html, enlaces_internos

    except PlaywrightTimeoutError:
        logger.error("Timeout al navegar | url=%s | timeout=%dms", url, NAV_TIMEOUT_MS)
        return "", []
    except PlaywrightError as exc:
        # Cubre DNS failures, ERR_CONNECTION_REFUSED, ERR_NAME_NOT_RESOLVED, etc.
        logger.error("Error de Playwright | url=%s | error=%s", url, str(exc)[:200])
        return "", []


async def _navegar_rutas_prioritarias(
    page: Page,
    url_base: str,
    dominio_base: str,
    enlaces_home: list[str],
) -> list[tuple[str, str]]:
    """
    Navega proactivamente a rutas de alta probabilidad de contacto.
    Primero intenta URLs encontradas en la home que matcheen paths prioritarios,
    luego construye las URLs directamente si no se encontraron.

    Args:
        page:           Página de Playwright.
        url_base:       URL raíz de la empresa.
        dominio_base:   Dominio raíz.
        enlaces_home:   Links internos encontrados en la home.

    Returns:
        Lista de tuplas (url_visitada, html_extraído) para las páginas exitosas.
    """
    resultados: list[tuple[str, str]] = []
    urls_visitadas: set[str] = {url_base}

    # Construir candidatos: primero los links reales de la home,
    # luego los paths prioritarios construidos directamente
    candidatos_directos = {
        urllib.parse.urljoin(url_base, path) for path in PRIORITY_PATHS
    }
    # Priorizar links reales encontrados en la home
    links_prioritarios = [
        url for url in enlaces_home
        if any(path in url.lower() for path in PRIORITY_PATHS)
    ]
    # Agregar los construidos directamente que no estén ya
    for url in candidatos_directos:
        if url not in links_prioritarios:
            links_prioritarios.append(url)

    for url in links_prioritarios:
        if url in urls_visitadas:
            continue
        urls_visitadas.add(url)

        html, _ = await _navegar_y_extraer(page, url, dominio_base)
        if html:
            resultados.append((url, html))

    logger.info(
        "Navegación profunda | dominio=%s | páginas_con_contenido=%d",
        dominio_base, len(resultados),
    )
    return resultados


# ---------------------------------------------------------------------------
# Función principal del módulo
# ---------------------------------------------------------------------------

async def procesar_dominio(
    url_o_dominio: str,
    nombre_empresa: Optional[str] = None,
    rubro: Optional[str] = None,
    min_score_para_log: int = 0,
) -> Optional[ResultadoScoring]:
    """
    Pipeline completo para un dominio: robots check → stealth browser →
    scraping profundo → scoring → persistencia en DB.

    Garantiza el cierre del browser en cualquier escenario de error mediante
    un bloque finally, previniendo procesos Chromium zombis en el SO.

    Args:
        url_o_dominio:      Dominio o URL semilla de la empresa.
        nombre_empresa:     Nombre comercial (opcional, se infiere del dominio si no se provee).
        rubro:              Sector de actividad conocido de antemano (opcional).
        min_score_para_log: Score mínimo para loguear la empresa como relevante.

    Returns:
        ResultadoScoring si el proceso fue exitoso, None si se abortó.
    """
    url_base = _normalizar_dominio(url_o_dominio)
    dominio_base = _extraer_dominio_raiz(url_base)
    nombre = nombre_empresa or dominio_base

    logger.info("Iniciando procesamiento | dominio=%s", dominio_base)

    # ------------------------------------------------------------------
    # 0. Check de cooldown: no procesar si ya fue contactada recientemente
    # ------------------------------------------------------------------
    empresa_existente = await asyncio.to_thread(get_empresa_by_dominio, dominio_base)
    if empresa_existente:
        en_cooldown = await asyncio.to_thread(esta_en_cooldown, empresa_existente["id"])
        if en_cooldown:
            logger.info("Empresa en cooldown, omitiendo | dominio=%s", dominio_base)
            return None

    # ------------------------------------------------------------------
    # 1. Verificar robots.txt (ética y stealth: si nos dicen que no, nos vamos)
    # ------------------------------------------------------------------
    robots_ok = await asyncio.to_thread(_verificar_robots, url_base)
    if not robots_ok:
        logger.warning("robots.txt deniega acceso, abortando | dominio=%s", dominio_base)
        return None

    # ------------------------------------------------------------------
    # 2. Scraping con Playwright
    # ------------------------------------------------------------------
    browser: Optional[Browser] = None
    html_total: str = ""

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-setuid-sandbox",
                    "--disable-extensions",
                    "--disable-infobars",
                    # Reducir fingerprint de memoria
                    "--memory-pressure-off",
                    "--disable-background-networking",
                ],
            )

            context = await _crear_contexto_stealth(browser)

            try:
                page = await context.new_page()
                await _bloquear_recursos_innecesarios(page)

                # --- Home ---
                html_home, enlaces_home = await _navegar_y_extraer(
                    page, url_base, dominio_base
                )

                if not html_home:
                    logger.error(
                        "No se pudo cargar la home, abortando | dominio=%s", dominio_base
                    )
                    return None

                html_total = html_home

                # --- Navegación profunda a páginas de contacto/equipo ---
                paginas_adicionales = await _navegar_rutas_prioritarias(
                    page, url_base, dominio_base, enlaces_home
                )
                for _, html_pagina in paginas_adicionales:
                    html_total += "\n" + html_pagina

            finally:
                # El contexto se cierra siempre, incluso si hay error en scraping
                await context.close()

    except PlaywrightError as exc:
        logger.exception(
            "Error crítico de Playwright | dominio=%s | error=%s",
            dominio_base, str(exc)[:300],
        )
        return None
    except Exception as exc:
        logger.exception(
            "Error inesperado en scraping | dominio=%s | error=%s",
            dominio_base, str(exc)[:300],
        )
        return None
    # El browser.close() es manejado automáticamente por async_playwright()
    # como context manager — no queda ningún proceso Chromium zombie.

    if not html_total.strip():
        logger.warning("HTML total vacío tras scraping | dominio=%s", dominio_base)
        return None

    logger.info(
        "HTML acumulado | dominio=%s | total_chars=%d",
        dominio_base, len(html_total),
    )

    # ------------------------------------------------------------------
    # 3. Scoring (sincrónico — se corre en thread para no bloquear el loop)
    # ------------------------------------------------------------------
    resultado: ResultadoScoring = await asyncio.to_thread(
        analizar_empresa,
        html_total,
        dominio_base,
        True,   # tiene_ssl (ya usamos https://)
    )

    if resultado.score_total >= min_score_para_log:
        logger.info(
            "Scoring | dominio=%s | score=%d | perfil=%s | contactos=%d | apto=%s",
            dominio_base,
            resultado.score_total,
            resultado.perfil_cv,
            len(resultado.contactos),
            resultado.apto_envio_auto,
        )

    # ------------------------------------------------------------------
    # 4. Persistencia en DB (sincrónico — thread para no bloquear el loop)
    # ------------------------------------------------------------------
    empresa_id: int = await asyncio.to_thread(
        upsert_empresa,
        nombre,
        dominio_base,
        rubro or resultado.rubro_detectado,
        resultado.perfil_cv,
        resultado.score_total,
    )

    for contacto in resultado.contactos:
        await asyncio.to_thread(
            insert_contacto,
            empresa_id,
            contacto.valor,
            contacto.tipo,
            contacto.prioridad,
        )

    logger.info(
        "Empresa persistida | dominio=%s | empresa_id=%d | contactos_guardados=%d",
        dominio_base, empresa_id, len(resultado.contactos),
    )

    return resultado


# ---------------------------------------------------------------------------
# Procesamiento en lote (batch)
# ---------------------------------------------------------------------------

async def procesar_lote(
    dominios: list[str],
    concurrencia: int = 3,
    min_score_para_log: int = 30,
) -> dict[str, Optional[ResultadoScoring]]:
    """
    Procesa una lista de dominios con concurrencia controlada usando un Semaphore.
    Evita saturar la red y disparar rate limits en sitios con WAF compartido.

    Args:
        dominios:           Lista de dominios/URLs a procesar.
        concurrencia:       Máximo de dominios procesados en paralelo. Default 3.
        min_score_para_log: Score mínimo para loguear como relevante.

    Returns:
        Dict {dominio: ResultadoScoring | None}.
    """
    semaforo = asyncio.Semaphore(concurrencia)
    resultados: dict[str, Optional[ResultadoScoring]] = {}

    async def _tarea(dominio: str) -> None:
        async with semaforo:
            # Jitter entre tareas para distribuir las requests en el tiempo
            await asyncio.sleep(random.uniform(0.5, 2.5))
            resultado = await procesar_dominio(
                dominio,
                min_score_para_log=min_score_para_log,
            )
            resultados[dominio] = resultado

    tareas = [asyncio.create_task(_tarea(d)) for d in dominios]
    await asyncio.gather(*tareas, return_exceptions=True)

    exitosos   = sum(1 for v in resultados.values() if v is not None)
    omitidos   = len(dominios) - exitosos
    aptos      = sum(1 for v in resultados.values() if v and v.apto_envio_auto)

    logger.info(
        "Lote completado | total=%d | exitosos=%d | omitidos=%d | aptos_envio=%d",
        len(dominios), exitosos, omitidos, aptos,
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

    # Inicializar la base de datos antes de correr el scraper
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
            print(f"  Contactos :")
            for c in res.contactos:
                print(f"    [{c.tipo:8}] P{c.prioridad} | +{c.puntos}pts | {c.valor}")
        else:
            print(f"\n  {dominio}: omitido (robots.txt, cooldown o error de red)")
