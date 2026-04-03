"""
── NOTAS DE USO ────────────────────────────────────────────────────────────
  · La primera ejecución abrirá una ventana de Chromium no-headless para
    escanear el QR de WhatsApp Web. La sesión queda guardada en ./wa_profile/.
  · Ejecuciones posteriores entran directamente sin QR.
  · headless=False es OBLIGATORIO en la primera ejecución y recomendado
    siempre, ya que WhatsApp Web detecta y bloquea navegadores headless puros.
  · El jitter entre mensajes (3–7 minutos) es el mínimo recomendado para
    evitar el shadowban temporal de Meta.
────────────────────────────────────────────────────────────────────────────

Python: 3.11+
Dependencias: playwright (async), asyncio, urllib (stdlib)
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from playwright.async_api import (
    async_playwright,
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError,
)

from db_manager import get_connection

COOLDOWN_DAYS = 7

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("jobbot.wa_sender")

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

WA_PROFILE_DIR: Path = Path(__file__).parent / "wa_profile"
WA_BASE_URL:    str  = "https://web.whatsapp.com"
WA_SEND_URL:    str  = "https://web.whatsapp.com/send?phone={phone}&text={text}"

# Tiempos de espera Playwright (ms)
TIMEOUT_QR_MS:       int = 120_000   # 2 min para escanear el QR
TIMEOUT_CHAT_MS:     int = 45_000    # Espera a que cargue la ventana de chat
TIMEOUT_SEND_BTN_MS: int = 20_000   # Espera al botón de enviar
TIMEOUT_POPUP_MS:    int = 5_000    # Chequeo rápido del popup de número inválido
TIMEOUT_NAV_MS:      int = 30_000   # Navegación general

# Jitter anti-ban (segundos) — NO bajar de 180 s en producción
JITTER_MIN_S: int = 180   # 3 minutos
JITTER_MAX_S: int = 450   # 7.5 minutos

# Envíos por sesión (Meta puede banear si se superan ~40–50/día)
LIMITE_DIARIO_WA: int = 30

# ─────────────────────────────────────────────────────────────────────────────
# Selectores de WhatsApp Web
# Ordenados de más específico a más genérico como fallback.
# ─────────────────────────────────────────────────────────────────────────────

# Pantalla de login / QR
_SEL_QR:         str = 'canvas[aria-label], [data-testid="qrcode"]'
# Pantalla principal cargada
_SEL_MAIN:       str = '#pane-side, [data-testid="chat-list"], [data-testid="chat-list-search"], header[data-testid="chatlist-header"]'
# Botón de enviar mensaje
_SEL_SEND:       tuple[str, ...] = (
    '[data-testid="send"]',
    'button[aria-label="Enviar"]',
    'button[aria-label="Send"]',
    'span[data-icon="send"]',
)
# Popup de número inválido
_SEL_POPUP:      str = '[data-testid="popup-contents"]'
# Texto que identifica un número inválido dentro del popup
_POPUP_INVALID_TEXTS: tuple[str, ...] = (
    "número de teléfono compartido",
    "phone number shared",
    "invalid phone",
    "no está registrado",
    "not registered",
)
# Botón OK del popup
_SEL_POPUP_OK: tuple[str, ...] = (
    '[data-testid="popup-contents"] button',
    'div[role="dialog"] button',
)

# ─────────────────────────────────────────────────────────────────────────────
# Regex — Números de WhatsApp con formato argentino
# (Para agregar en scoring.py / scraper.py)
# ─────────────────────────────────────────────────────────────────────────────

# ── Explicación de los grupos capturados ─────────────────────────────────────
#
#  Grupo 1 — Formato wa.me / wa.link (link directo de WhatsApp):
#    wa.me/5492231234567  |  wa.link/abc123 (no captura links cortos)
#
#  Grupo 2 — Formato E.164 internacional:
#    +54 9 223 123 4567  |  +549 2231234567  |  +54-9-223-123-4567
#
#  Grupo 3 — Formato nacional con 0:
#    0223 123-4567  |  (0223) 123 4567  |  0223-123-4567
#
#  Grupo 4 — Formato local (solo área 223 y adyacentes de MdP):
#    223 123 4567  |  223-1234567
#
#  El patrón es CONSERVADOR: exige al menos 7 dígitos después del código
#  de área para evitar falsos positivos (versiones de SW, fechas, etc.).
#
#  Para usar en scoring.py, agregar junto a _RE_EMAIL:
#
#    from wa_sender import _RE_WHATSAPP, normalizar_numero_ar
#
# ─────────────────────────────────────────────────────────────────────────────

_RE_WHATSAPP: re.Pattern[str] = re.compile(
    r"""
    (?:
        # Grupo 1: links wa.me con código de país 54
        wa\.me/(?:549?)(\d{10,11})
    |
        # Grupo 2: formato internacional +54 9, área y número SEPARADOS
        # para validar la suma de dígitos en el normalizador
        \+54\s*9?\s*
        \(?  # paréntesis de apertura opcional
        ((?:11|2(?:2[0-4679]|3[3-8]|4[013-9]|6[0124-8]|7[1-4]|9[1-469])|
            3(?:3[28]|4[0-9]|5[25-8]|6[1-3579]|7[0246-9]|8[2357-9])))
        \)?  # paréntesis de cierre opcional
        [\s\-]*
        (\d[\d\s\-]{5,8}\d)  # número local
    |
        # Grupo 3: formato nacional con 0 (ej: 0223 123-4567)
        \(?0(\d{2,4})\)?
        [\s\-]*
        (\d[\d\s\-]{5,8}\d)
    |
        # Grupo 4: formato local directo MdP y zona (ej: 223 123 4567)
        \b(2(?:2[0-4679]|3[3-8]|4[013-9]|6[0124-8]|7[1-4]|9[1-469])|
           3(?:3[28]|4[0-9]|5[25-8]|6[1-3579]|7[0246-9]|8[2357-9]))
        [\s\-]*
        (\d[\d\s\-]{5,7}\d)
        \b
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)


def normalizar_numero_ar(match: re.Match) -> Optional[str]:
    """
    Convierte cualquier match de _RE_WHATSAPP al formato E.164 argentino
    (+549XXXXXXXXXX). Actúa como "patovica" estricto: exige exactamente
    10 dígitos netos (área + número local). Cualquier suma distinta → None.

    Mapeo de grupos (alineado con _RE_WHATSAPP):
        grupos[0]           → Grupo 1: wa.me/549XXXXXXXXXX
        grupos[1], grupos[2] → Grupo 2: +54 9 (AREA) NÚMERO  ← área separada
        grupos[3], grupos[4] → Grupo 3: 0AREA NÚMERO
        grupos[5], grupos[6] → Grupo 4: local AREA NÚMERO

    Returns:
        Número en formato '+549XXXXXXXXXX' o None si no supera la validación.
    """
    grupos = match.groups()

    # Grupo 1: wa.me/549XXXXXXXXXX
    if grupos[0]:
        digitos = re.sub(r'\D', '', grupos[0])
        if len(digitos) == 10 and not digitos.startswith('0'):
            return f"+549{digitos}"
        return None

    # Grupo 2: +54 9 (AREA) NÚMERO — área y número capturados por separado
    if grupos[1] and grupos[2]:
        area   = re.sub(r'\D', '', grupos[1])
        numero = re.sub(r'\D', '', grupos[2])
        if numero.startswith('15'):
            numero = numero[2:]
        if len(area) + len(numero) == 10 and not area.startswith('0'):
            return f"+549{area}{numero}"
        return None

    # Grupo 3: 0AREA NÚMERO
    if grupos[3] and grupos[4]:
        area   = re.sub(r'\D', '', grupos[3])
        numero = re.sub(r'\D', '', grupos[4])
        if numero.startswith('15'):
            numero = numero[2:]
        if len(area) + len(numero) == 10 and not area.startswith('0'):
            return f"+549{area}{numero}"
        return None

    # Grupo 4: AREA NÚMERO (local, sin prefijo)
    if grupos[5] and grupos[6]:
        area   = re.sub(r'\D', '', grupos[5])
        numero = re.sub(r'\D', '', grupos[6])
        if numero.startswith('15'):
            numero = numero[2:]
        if len(area) + len(numero) == 10 and not area.startswith('0'):
            return f"+549{area}{numero}"
        return None

    return None


def extraer_numeros_whatsapp(html: str) -> list[str]:
    """
    Extrae todos los números de WhatsApp únicos de un HTML,
    devueltos en formato E.164 (+549XXXXXXXXXX).

    Conveniente para llamar desde analizar_empresa() en scoring.py.

    Args:
        html: HTML crudo de la página.

    Returns:
        Lista de números únicos en formato E.164, sin duplicados.
    """
    encontrados: set[str] = set()
    for match in _RE_WHATSAPP.finditer(html):
        numero = normalizar_numero_ar(match)
        if numero:
            encontrados.add(numero)
    return sorted(encontrados)


# ─────────────────────────────────────────────────────────────────────────────
# Plantillas de mensajes
# ─────────────────────────────────────────────────────────────────────────────

SENDER_NAME: str = os.getenv("SENDER_NAME", "Alaska")

_MENSAJES_WA: tuple[str, ...] = (
    (
        "Hola, buen día! Mi nombre es {nombre} y me comunico para dejar mi perfil "
        "ante la posibilidad de que estén buscando personal administrativo o de soporte IT. "
        "Soy de Mar del Plata, disponibilidad inmediata. Hay alguien de RRHH a quien pueda "
        "escribirle? Muchas gracias."
    ),
    (
        "Hola, cómo están? Soy {nombre}, de Mar del Plata. Les escribo porque me "
        "interesa sumarme al equipo de {empresa}. Tengo experiencia en administración con "
        "perfil IT. Tienen alguna posición disponible o puedo dejar mi CV?"
    ),
    (
        "Buen día, mi nombre es {nombre}. Encontré información de {empresa} y me pareció "
        "muy interesante el rubro. Cuento con experiencia en gestión administrativa y "
        "soporte técnico, busco nuevas oportunidades en MdP. Podrían indicarme a quién "
        "dirigirme? Gracias"
    ),
)


def _construir_mensaje(nombre_empresa: str) -> str:
    """Selecciona y renderiza aleatoriamente una plantilla de mensaje."""
    tpl = random.choice(_MENSAJES_WA)
    return tpl.format(nombre=SENDER_NAME, empresa=nombre_empresa)


# ─────────────────────────────────────────────────────────────────────────────
# Capa de base de datos — funciones específicas de WhatsApp
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ContactoWA:
    """Resultado aplanado para el pipeline de envío."""
    contacto_id:  int
    empresa_id:   int
    nombre_empresa: str
    numero:       str   # E.164 format: +549XXXXXXXXXX
    prioridad:    int


def get_contactos_whatsapp_pendientes(
    limit: int = 20,
    cooldown_days: int = COOLDOWN_DAYS,
) -> list[ContactoWA]:
    """
    Retorna contactos de tipo 'WhatsApp' que NO han recibido un mensaje
    dentro del período de cooldown.

    JOIN con campanas_envios para excluir empresas ya contactadas
    recientemente (mismo mecanismo que en mailer.py).

    Args:
        limit:        Máximo de contactos a procesar.
        cooldown_days: Días de cooldown entre mensajes a la misma empresa.

    Returns:
        Lista de ContactoWA listos para enviar.
    """
    cutoff: str = (
        datetime.now(tz=timezone.utc) - timedelta(days=cooldown_days)
    ).isoformat()

    sql = """
        SELECT
            c.id          AS contacto_id,
            c.empresa_id,
            e.nombre      AS nombre_empresa,
            c.email_o_link AS numero,
            c.prioridad
        FROM contactos c
        JOIN empresas e ON e.id = c.empresa_id
        LEFT JOIN campanas_envios ce
            ON ce.empresa_id = c.empresa_id
            AND ce.asunto_usado = c.email_o_link   -- número usado como clave
            AND ce.cv_enviado   = 'WhatsApp'
            AND ce.fecha_envio  >= :cutoff
            AND ce.estado IN ('enviado', 'pendiente')
        WHERE c.tipo = 'WhatsApp'
          AND ce.id IS NULL                        -- sin envío reciente
        ORDER BY c.prioridad ASC, e.score DESC
        LIMIT :limit;
    """
    with get_connection() as conn:
        rows = conn.execute(sql, {"cutoff": cutoff, "limit": limit}).fetchall()

    contactos = [
        ContactoWA(
            contacto_id=r["contacto_id"],
            empresa_id=r["empresa_id"],
            nombre_empresa=r["nombre_empresa"],
            numero=r["numero"],
            prioridad=r["prioridad"],
        )
        for r in rows
    ]
    logger.info(
        "Contactos WA pendientes: %d (cooldown=%d días)", len(contactos), cooldown_days
    )
    return contactos


def registrar_envio_wa(
    empresa_id: int,
    numero: str,
    estado: str = "enviado",
) -> int:
    """
    Registra un envío de WhatsApp en campanas_envios.

    Reutiliza la tabla existente con:
      cv_enviado   = 'WhatsApp'
      asunto_usado = número E.164 (actúa como clave de deduplicación)

    Args:
        empresa_id: ID de la empresa destinataria.
        numero:     Número en formato E.164.
        estado:     'enviado' | 'rebotado' | 'pendiente'.

    Returns:
        ID del registro creado.
    """
    estados_validos = {"pendiente", "enviado", "rebotado", "respondido"}
    if estado not in estados_validos:
        raise ValueError(f"Estado inválido: '{estado}'.")

    sql = """
        INSERT INTO campanas_envios (empresa_id, cv_enviado, asunto_usado, estado)
        VALUES (?, 'WhatsApp', ?, ?);
    """
    with get_connection() as conn:
        cursor = conn.execute(sql, (empresa_id, numero, estado))
        row_id: int = cursor.lastrowid  # type: ignore[assignment]

    logger.info(
        "Envío WA registrado | empresa_id=%d | numero=%s | estado=%s | id=%d",
        empresa_id, numero, estado, row_id,
    )
    return row_id


def count_envios_wa_hoy() -> int:
    """Retorna cuántos mensajes de WA fueron enviados hoy (UTC)."""
    sql = """
        SELECT COUNT(*) FROM campanas_envios
        WHERE cv_enviado  = 'WhatsApp'
          AND estado      = 'enviado'
          AND fecha_envio >= strftime('%Y-%m-%dT00:00:00Z', 'now');
    """
    with get_connection() as conn:
        resultado = conn.execute(sql).fetchone()
    return int(resultado[0]) if resultado else 0


# ─────────────────────────────────────────────────────────────────────────────
# Sesión persistente de WhatsApp Web
# ─────────────────────────────────────────────────────────────────────────────

async def _crear_contexto_persistente(headless: bool = False) -> BrowserContext:
    """
    Lanza (o reanuda) un contexto persistente de Chromium con la sesión
    de WhatsApp Web guardada en WA_PROFILE_DIR.

    headless=False es el default porque WhatsApp Web detecta y bloquea
    Chromium en modo headless a través de fingerprinting de Canvas/WebGL.
    Después de la primera autenticación con QR, puedes experimentar con
    headless=True (algunos entornos lo soportan si el perfil ya tiene sesión).

    Returns:
        BrowserContext con la sesión activa.
    """
    WA_PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    # Nota: launch_persistent_context retorna BrowserContext, no Browser.
    # No existe pw.chromium.launch() + context.launch_persistent() en la misma
    # instancia; se usa directamente launch_persistent_context.
    pw_instance = await async_playwright().__aenter__()  # guardado en el caller

    context = await pw_instance.chromium.launch_persistent_context(
        user_data_dir=str(WA_PROFILE_DIR),
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-extensions",
            "--disable-infobars",
            "--disable-background-networking",
            "--disable-gpu",
        ],
        ignore_https_errors=False,
        locale="es-AR",
        timezone_id="America/Argentina/Buenos_Aires",
        viewport={"width": 1280, "height": 800},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    )

    # Anti-detección: ocultar webdriver antes de cargar cualquier página
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins',   { get: () => [1, 2, 3] });
        Object.defineProperty(navigator, 'languages', { get: () => ['es-AR', 'es', 'en'] });
        if (window.chrome) { window.chrome.runtime = {}; }
    """)

    logger.info(
        "Contexto persistente lanzado | perfil=%s | headless=%s",
        WA_PROFILE_DIR, headless,
    )
    return context, pw_instance   # type: ignore[return-value]


async def _esperar_autenticacion(page: Page) -> bool:
    """
    Bloquea hasta que WhatsApp Web muestre la pantalla principal
    (sesión ya activa o QR escaneado por el usuario).

    Primero detecta si ya hay sesión activa. Si no, espera el QR
    y luego la pantalla principal.

    Returns:
        True si la autenticación fue exitosa, False si hubo timeout.
    """
    logger.info("Verificando estado de sesión WhatsApp Web…")

    try:
        # Chequeo rápido: ¿ya estamos logueados?
        await page.wait_for_selector(_SEL_MAIN, timeout=8_000)
        logger.info("Sesión activa detectada — sin QR necesario.")
        return True
    except PlaywrightTimeoutError:
        pass

    # No hay sesión: esperar QR
    logger.warning(
        "No se detectó sesión activa. "
        "Esperando QR en la ventana del navegador (%.0f seg)…",
        TIMEOUT_QR_MS / 1000,
    )
    try:
        await page.wait_for_selector(_SEL_QR, timeout=15_000)
        logger.info("QR visible — el usuario tiene %.0f seg para escanearlo.", TIMEOUT_QR_MS / 1000)
    except PlaywrightTimeoutError:
        logger.warning("QR no detectado, esperando igualmente…")

    # Esperar hasta que la pantalla principal aparezca post-escaneo
    try:
        await page.wait_for_selector(_SEL_MAIN, timeout=TIMEOUT_QR_MS)
        logger.info("✓ QR escaneado — sesión de WhatsApp Web activa.")
        return True
    except PlaywrightTimeoutError:
        logger.error("Timeout esperando autenticación (%.0f seg). Abortando.", TIMEOUT_QR_MS / 1000)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Envío individual de mensaje
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ResultadoEnvioWA:
    numero:        str
    empresa:       str
    empresa_id:    int
    exito:         bool
    estado:        str          # 'enviado' | 'rebotado' | 'error'
    detalle:       str = ""


async def _click_send_button(page: Page) -> bool:
    """
    Intenta hacer click en el botón de enviar con múltiples selectores
    de fallback. Retorna True si tuvo éxito.
    """
    for selector in _SEL_SEND:
        try:
            btn = await page.wait_for_selector(selector, timeout=TIMEOUT_SEND_BTN_MS // len(_SEL_SEND))
            if btn:
                await btn.click()
                logger.debug("Botón de enviar clickeado | selector=%s", selector)
                return True
        except (PlaywrightTimeoutError, PlaywrightError):
            continue
    return False


async def _verificar_popup_invalido(page: Page) -> bool:
    """
    Verifica si WhatsApp mostró el popup de número inválido/no registrado.

    Si lo detecta, intenta cerrar el popup para no dejar la página bloqueada.

    Returns:
        True si el número es inválido, False si la ventana de chat cargó bien.
    """
    try:
        popup = await page.wait_for_selector(_SEL_POPUP, timeout=TIMEOUT_POPUP_MS)
        if not popup:
            return False

        texto_popup = (await popup.inner_text()).lower()
        es_invalido = any(t in texto_popup for t in _POPUP_INVALID_TEXTS)

        if es_invalido:
            logger.warning("Popup de número inválido detectado | texto='%s'", texto_popup[:80])
            # Intentar cerrar el popup para no bloquear la sesión
            for sel_ok in _SEL_POPUP_OK:
                try:
                    ok_btn = page.locator(sel_ok).first
                    await ok_btn.click(timeout=3_000)
                    break
                except Exception:
                    continue
            return True

    except PlaywrightTimeoutError:
        # No apareció ningún popup → la ventana de chat cargó correctamente
        pass
    except PlaywrightError as exc:
        logger.debug("Error verificando popup: %s", str(exc)[:100])

    return False


async def enviar_mensaje_wa(
    page: Page,
    contacto: ContactoWA,
    dry_run: bool = False,
) -> ResultadoEnvioWA:
    """
    Envía un mensaje a un número de WhatsApp utilizando la URL directa.

    Flujo:
      1. Construir URL con el número y el mensaje pre-cargado.
      2. Navegar a la URL.
      3. Verificar si aparece el popup de número inválido.
      4. Si no hay popup, hacer click en el botón de enviar.
      5. Esperar confirmación visual (2 s es suficiente).

    Args:
        page:     Página de Playwright dentro del contexto persistente.
        contacto: Datos del contacto a contactar.
        dry_run:  Si True, no navega ni hace click — solo loguea.

    Returns:
        ResultadoEnvioWA con el resultado del intento.
    """
    mensaje  = _construir_mensaje(contacto.nombre_empresa)
    url_send = WA_SEND_URL.format(
        phone=urllib.parse.quote(contacto.numero),
        text=urllib.parse.quote(mensaje),
    )

    if dry_run:
        logger.info(
            "[DRY-RUN] WA | empresa='%s' | numero=%s | mensaje='%s…'",
            contacto.nombre_empresa, contacto.numero, mensaje[:60],
        )
        return ResultadoEnvioWA(
            numero=contacto.numero, empresa=contacto.nombre_empresa,
            empresa_id=contacto.empresa_id,
            exito=True, estado="enviado", detalle="dry-run",
        )

    logger.info(
        "Enviando WA | empresa='%s' | numero=%s",
        contacto.nombre_empresa, contacto.numero,
    )

    try:
        # Navegar a la URL de envío directo
        response = await page.goto(url_send, wait_until="domcontentloaded", timeout=TIMEOUT_NAV_MS)
        if response and response.status in (403, 429, 500, 503):
            return ResultadoEnvioWA(
                numero=contacto.numero, empresa=contacto.nombre_empresa,
                empresa_id=contacto.empresa_id,
                exito=False, estado="error",
                detalle=f"HTTP {response.status}",
            )

        # Dar tiempo a que cargue la UI del chat
        await asyncio.sleep(3.0)

        # ── Verificar popup de número inválido ANTES de intentar enviar ──
        if await _verificar_popup_invalido(page):
            return ResultadoEnvioWA(
                numero=contacto.numero, empresa=contacto.nombre_empresa,
                empresa_id=contacto.empresa_id,
                exito=False, estado="rebotado",
                detalle="Número no registrado en WhatsApp",
            )

        # ── Intentar click en el botón de enviar ──
        if not await _click_send_button(page):
            # Segundo chequeo de popup (puede aparecer tarde)
            if await _verificar_popup_invalido(page):
                return ResultadoEnvioWA(
                    numero=contacto.numero, empresa=contacto.nombre_empresa,
                    empresa_id=contacto.empresa_id,
                    exito=False, estado="rebotado",
                    detalle="Número no registrado (detectado post-click)",
                )
            return ResultadoEnvioWA(
                numero=contacto.numero, empresa=contacto.nombre_empresa,
                empresa_id=contacto.empresa_id,
                exito=False, estado="error",
                detalle="Botón de enviar no encontrado",
            )

        # Esperar a que el mensaje se registre (doble tick)
        await asyncio.sleep(2.5)

        logger.info(
            "✓ WA enviado | empresa='%s' | numero=%s",
            contacto.nombre_empresa, contacto.numero,
        )
        return ResultadoEnvioWA(
            numero=contacto.numero, empresa=contacto.nombre_empresa,
            empresa_id=contacto.empresa_id,
            exito=True, estado="enviado",
        )

    except PlaywrightTimeoutError as exc:
        return ResultadoEnvioWA(
            numero=contacto.numero, empresa=contacto.nombre_empresa,
            empresa_id=contacto.empresa_id,
            exito=False, estado="error",
            detalle=f"Timeout: {str(exc)[:80]}",
        )
    except PlaywrightError as exc:
        return ResultadoEnvioWA(
            numero=contacto.numero, empresa=contacto.nombre_empresa,
            empresa_id=contacto.empresa_id,
            exito=False, estado="error",
            detalle=f"Playwright error: {str(exc)[:80]}",
        )
    except Exception as exc:
        logger.exception("Error inesperado | numero=%s | %s", contacto.numero, exc)
        return ResultadoEnvioWA(
            numero=contacto.numero, empresa=contacto.nombre_empresa,
            empresa_id=contacto.empresa_id,
            exito=False, estado="error",
            detalle=f"Error inesperado: {str(exc)[:80]}",
        )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────────────────────

async def procesar_envios_wa(
    limite: int = 20,
    dry_run: bool = False,
    headless: bool = False,
) -> dict[str, int]:
    """
    Pipeline completo de envío por WhatsApp:
      1. Verificar cuota diaria.
      2. Obtener contactos pendientes de la DB.
      3. Lanzar sesión persistente de Chromium.
      4. Autenticar (QR o sesión existente).
      5. Para cada contacto: enviar, registrar y dormir (jitter).

    Args:
        limite:   Máximo de mensajes a enviar en esta ejecución.
        dry_run:  Si True, simula el envío sin abrir WhatsApp real.
        headless: Modo sin ventana. Peligroso en producción (ver docstring).

    Returns:
        {'procesados': N, 'enviados': N, 'rebotados': N, 'errores': N}
    """
    metricas = {"procesados": 0, "enviados": 0, "rebotados": 0, "errores": 0}

    if dry_run:
        logger.warning("=== MODO DRY-RUN WA: no se enviarán mensajes reales ===")

    # ── 1. Cuota diaria ───────────────────────────────────────────────────────
    enviados_hoy = await asyncio.to_thread(count_envios_wa_hoy)
    cuota_restante = max(0, LIMITE_DIARIO_WA - enviados_hoy)
    if cuota_restante == 0:
        logger.warning(
            "Cuota diaria de WA alcanzada (%d mensajes). "
            "Ejecutar mañana para continuar.", LIMITE_DIARIO_WA,
        )
        return metricas

    limite_efectivo = min(limite, cuota_restante)
    logger.info(
        "Cuota WA | enviados_hoy=%d | límite_diario=%d | cuota_restante=%d | "
        "procesar_ahora=%d",
        enviados_hoy, LIMITE_DIARIO_WA, cuota_restante, limite_efectivo,
    )

    # ── 2. Contactos pendientes ───────────────────────────────────────────────
    contactos = await asyncio.to_thread(
        get_contactos_whatsapp_pendientes, limite_efectivo
    )
    if not contactos:
        logger.info("No hay contactos WA pendientes.")
        return metricas

    # ── 3–4. Sesión Playwright ────────────────────────────────────────────────
    async with async_playwright() as pw:
        WA_PROFILE_DIR.mkdir(parents=True, exist_ok=True)

        context = await pw.chromium.launch_persistent_context(
            user_data_dir=str(WA_PROFILE_DIR),
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-extensions",
                "--disable-infobars",
                "--disable-background-networking",
                "--disable-gpu",
            ],
            ignore_https_errors=False,
            locale="es-AR",
            timezone_id="America/Argentina/Buenos_Aires",
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )

        # Anti-detección global para el contexto
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins',   { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['es-AR', 'es', 'en'] });
            if (window.chrome) { window.chrome.runtime = {}; }
        """)

        try:
            # Usar la primera página disponible (evita abrir tab adicional)
            pages = context.pages
            page  = pages[0] if pages else await context.new_page()

            # Navegar a WhatsApp Web si no estamos ya ahí
            if WA_BASE_URL not in page.url:
                await page.goto(WA_BASE_URL, wait_until="domcontentloaded", timeout=TIMEOUT_NAV_MS)

            # Autenticación / verificación de sesión
            if not dry_run:
                autenticado = await _esperar_autenticacion(page)
                if not autenticado:
                    logger.error("No se pudo autenticar en WhatsApp Web. Abortando.")
                    return metricas
            else:
                logger.info("[DRY-RUN] Saltando verificación de sesión WA.")

            # ── 5. Loop de envíos ─────────────────────────────────────────────
            es_primer_envio = True
            for contacto in contactos:
                metricas["procesados"] += 1

                logger.info(
                    "--- WA | empresa='%s' | numero=%s [%d/%d] ---",
                    contacto.nombre_empresa, contacto.numero,
                    metricas["procesados"], len(contactos),
                )

                # Jitter ANTES del envío (excepto el primero)
                if not es_primer_envio and not dry_run:
                    sleep_s = random.randint(JITTER_MIN_S, JITTER_MAX_S)
                    logger.info(
                        "Rate limit: esperando %d seg (~%.1f min)…",
                        sleep_s, sleep_s / 60,
                    )
                    await asyncio.sleep(sleep_s)

                resultado = await enviar_mensaje_wa(page, contacto, dry_run=dry_run)

                # Registrar en DB (incluso en dry_run para poder auditar)
                if not dry_run:
                    await asyncio.to_thread(
                        registrar_envio_wa,
                        contacto.empresa_id,
                        contacto.numero,
                        resultado.estado,
                    )

                # Actualizar métricas
                if resultado.exito:
                    metricas["enviados"] += 1
                elif resultado.estado == "rebotado":
                    metricas["rebotados"] += 1
                    logger.warning(
                        "✗ Número inválido | empresa='%s' | numero=%s | %s",
                        contacto.nombre_empresa, contacto.numero, resultado.detalle,
                    )
                else:
                    metricas["errores"] += 1
                    logger.error(
                        "✗ Error | empresa='%s' | numero=%s | %s",
                        contacto.nombre_empresa, contacto.numero, resultado.detalle,
                    )

                es_primer_envio = False

        finally:
            await context.close()
            logger.info("Contexto WA cerrado.")

    logger.info(
        "=== Campaña WA finalizada | procesados=%d | enviados=%d | "
        "rebotados=%d | errores=%d ===",
        metricas["procesados"], metricas["enviados"],
        metricas["rebotados"], metricas["errores"],
    )
    return metricas


# ─────────────────────────────────────────────────────────────────────────────
# CLI — Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="JobBot WA Sender — Motor de envío por WhatsApp Web",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  # Primera ejecución: abre ventana para escanear QR\n"
            "  python wa_sender.py --limite 5\n\n"
            "  # Auditoría sin abrir WhatsApp real\n"
            "  python wa_sender.py --dry-run --limite 20\n\n"
            "  # Producción: usa sesión guardada en ./wa_profile/\n"
            "  python wa_sender.py --limite 10\n"
        ),
    )
    parser.add_argument(
        "--limite", type=int, default=10,
        help="Máximo de mensajes a enviar en esta ejecución (default: 10)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", dest="dry_run",
        help="Simula el envío sin abrir WhatsApp real",
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Modo sin ventana (experimental, puede ser bloqueado por WA)",
    )
    parser.add_argument(
        "--test-regex", type=str, default=None, dest="test_regex",
        metavar="HTML_O_TEXTO",
        help="Testea el regex de extracción de números con el texto dado",
    )
    args = parser.parse_args()

    # ── Modo de prueba de regex ───────────────────────────────────────────────
    if args.test_regex:
        numeros = extraer_numeros_whatsapp(args.test_regex)
        if numeros:
            print(f"Números encontrados ({len(numeros)}):")
            for n in numeros:
                print(f"  {n}")
        else:
            print("No se encontraron números de WhatsApp en el texto.")
        raise SystemExit(0)

    # ── Pipeline principal ────────────────────────────────────────────────────
    try:
        metricas = asyncio.run(
            procesar_envios_wa(
                limite=args.limite,
                dry_run=args.dry_run,
                headless=args.headless,
            )
        )
        print(f"\nResultado: {metricas}")
    except KeyboardInterrupt:
        print("\n[Interrumpido por el usuario. DB consistente.]")
    except Exception as exc:
        logger.exception("Error fatal: %s", exc)
        raise SystemExit(1)