"""
wa_sender.py — JobBot WhatsApp Web Sender

Python: 3.11+
Dependencias: playwright (async), asyncio, urllib (stdlib)
"""
from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import time
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

from config import (
    SENDER_NAME,
    WA_JITTER_MIN_S,g
    WA_JITTER_MAX_S,
    WA_LIMITE_DIARIO,
    COOLDOWN_WA_DAYS,
)
from utils.phone import extraer_numeros_whatsapp   # re-export para compatibilidad con CLI
from utils.browser import CHROMIUM_ARGS, apply_stealth
from db_manager import get_connection

logger = logging.getLogger("jobbot.wa_sender")

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

WA_PROFILE_DIR: Path = Path(__file__).parent / "wa_profile"
WA_BASE_URL:    str  = "https://web.whatsapp.com"
WA_SEND_URL:    str  = "https://web.whatsapp.com/send?phone={phone}&text={text}"

TIMEOUT_QR_MS:       int = 120_000
TIMEOUT_CHAT_MS:     int = 45_000
TIMEOUT_SEND_BTN_MS: int = 20_000
TIMEOUT_POPUP_MS:    int = 5_000
TIMEOUT_NAV_MS:      int = 30_000

# ─────────────────────────────────────────────────────────────────────────────
# Selectores de WhatsApp Web — versionados
#
# Última verificación: 2026-Q1
# Si el login falla, probar con estos alternativos:
#   "qr":   'div[data-js-state="disconnected"] canvas'
#   "main": 'div[role="main"][tabindex="-1"]'
#   "send": 'button[data-testid="compose-btn-send"]'
#
# Centralizar aquí facilita actualizaciones cuando WA cambia su DOM
# sin tener que buscar strings dispersos por todo el módulo.
# ─────────────────────────────────────────────────────────────────────────────

_SEL: dict[str, str | tuple[str, ...]] = {
    "qr":   'canvas[aria-label], [data-testid="qrcode"]',
    "main": (
        '#pane-side, [data-testid="chat-list"], '
        '[data-testid="chat-list-search"], '
        'header[data-testid="chatlist-header"]'
    ),
    "send": (
        '[data-testid="send"]',
        'button[aria-label="Enviar"]',
        'button[aria-label="Send"]',
        'span[data-icon="send"]',
    ),
    "popup":    '[data-testid="popup-contents"]',
    "popup_ok": (
        '[data-testid="popup-contents"] button',
        'div[role="dialog"] button',
    ),
    "qr_data": 'div[data-ref]',
}

_POPUP_INVALID_TEXTS: tuple[str, ...] = (
    "número de teléfono compartido",
    "phone number shared",
    "invalid phone",
    "no está registrado",
    "not registered",
)

# ─────────────────────────────────────────────────────────────────────────────
# Plantillas de mensajes
# ─────────────────────────────────────────────────────────────────────────────

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
    import random
    tpl = random.choice(_MENSAJES_WA)
    return tpl.format(nombre=SENDER_NAME, empresa=nombre_empresa)


# ─────────────────────────────────────────────────────────────────────────────
# Base de datos — funciones específicas de WhatsApp
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ContactoWA:
    contacto_id:    int
    empresa_id:     int
    nombre_empresa: str
    numero:         str
    prioridad:      int


def get_contactos_whatsapp_pendientes(
    limit: int = 20,
    cooldown_days: int = COOLDOWN_WA_DAYS,   # REFACTOR: desde config
) -> list[ContactoWA]:
    """
    Retorna contactos de tipo 'WhatsApp' sin envío reciente (cooldown).
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
            AND ce.asunto_usado = c.email_o_link
            AND ce.cv_enviado   = 'WhatsApp'
            AND ce.fecha_envio  >= :cutoff
            AND ce.estado IN ('enviado', 'pendiente')
        WHERE c.tipo = 'WhatsApp'
          AND ce.id IS NULL
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


def registrar_envio_wa(empresa_id: int, numero: str, estado: str = "enviado") -> int:
    """
    Registra un envío de WhatsApp en campanas_envios.
    cv_enviado='WhatsApp', asunto_usado=número E.164 (clave de dedup).
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
# Autenticación WhatsApp Web
# ─────────────────────────────────────────────────────────────────────────────

import time as _time


async def _esperar_autenticacion(
    page: "Page",
    estado: Optional[object] = None,
) -> bool:
    """
    Authenticates a WhatsApp Web session with robust wall-clock timeout tracking.

    FIX #9: elapsed_ms was incremented by poll_interval regardless of how long
    wait_for_selector actually took, making the 2-minute ceiling and the 18-second
    recapture interval both inaccurate. This version uses time.monotonic() for
    all time comparisons.
    """
    import subprocess as _subprocess
    from pathlib import Path as _Path

    viewer_proc: Optional[_subprocess.Popen] = None  # type: ignore[type-arg]
    QR_IMAGE_PATH = _Path(__file__).parent / "wa_qr.png"

    deadline       = _time.monotonic() + (TIMEOUT_QR_MS / 1_000)
    last_recapture = _time.monotonic()
    RECAPTURE_S    = 18.0

    try:
        # Phase 1: check for an active cached session
        try:
            await page.wait_for_selector(_SEL["main"], timeout=8_000)
            logger.info("✓ Sesión activa detectada (sin QR requerido).")
            return True
        except PlaywrightTimeoutError:
            pass

        # Phase 2: wait for QR canvas
        try:
            qr_element = await page.wait_for_selector(_SEL["qr"], timeout=20_000)
        except PlaywrightTimeoutError:
            logger.error("No apareció el canvas del QR en 20 segundos.")
            return False

        if qr_element is None:
            return False

        # Phase 3: screenshot + open viewer
        await qr_element.screenshot(path=str(QR_IMAGE_PATH))
        logger.info("QR capturado en: %s", QR_IMAGE_PATH)

        try:
            viewer_proc = _subprocess.Popen(
                ["xdg-open", str(QR_IMAGE_PATH)],
                stdout=_subprocess.DEVNULL,
                stderr=_subprocess.DEVNULL,
            )
            logger.info(
                "Visor de imágenes abierto (PID %d). Escaneá el QR.",
                viewer_proc.pid,
            )
        except FileNotFoundError:
            logger.warning(
                "xdg-open no encontrado. Imagen en: %s — abrila manualmente.",
                QR_IMAGE_PATH,
            )
        
        if estado is not None:
            try:
                with estado._lock:  # type: ignore[attr-defined]
                    estado.fase_actual = "WA Auth: QR abierto — esperando escaneo…"  # type: ignore[attr-defined]
            except AttributeError:
                pass

        # Phase 4: poll loop with wall-clock tracking
        POLL_TIMEOUT_MS = 2_000

        while _time.monotonic() < deadline:
            try:
                await page.wait_for_selector(_SEL["main"], timeout=POLL_TIMEOUT_MS)
                logger.info("✓ QR escaneado. Sesión de WhatsApp Web activa.")
                return True
            except PlaywrightTimeoutError:
                pass

            # Recapture on wall-clock interval, not poll count
            now = _time.monotonic()
            if now - last_recapture >= RECAPTURE_S:
                last_recapture = now
                try:
                    qr_element = await page.query_selector(_SEL["qr"])
                    if qr_element:
                        await qr_element.screenshot(path=str(QR_IMAGE_PATH))
                        logger.info(
                            "QR rotado — imagen actualizada (%.0fs transcurridos).",
                            now - (deadline - TIMEOUT_QR_MS / 1_000),
                        )
                    else:
                        logger.info("Canvas del QR ausente — posiblemente en proceso de login.")
                except PlaywrightError:
                    pass

        logger.error("Timeout: QR no escaneado en %.0f segundos.", TIMEOUT_QR_MS / 1_000)
        return False

    finally:
        if viewer_proc is not None:
            try:
                viewer_proc.terminate()
                viewer_proc.wait(timeout=2)
            except _subprocess.TimeoutExpired:
                viewer_proc.kill()
            except ProcessLookupError:
                pass

        if QR_IMAGE_PATH.exists():
            try:
                QR_IMAGE_PATH.unlink()
            except OSError as exc:
                logger.warning("No se pudo eliminar %s: %s", QR_IMAGE_PATH, exc)


# ─────────────────────────────────────────────────────────────────────────────
# Envío individual
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ResultadoEnvioWA:
    numero:     str
    empresa:    str
    empresa_id: int
    exito:      bool
    estado:     str
    detalle:    str = ""


async def _click_send_button(page: Page) -> bool:
    """Intenta click en enviar con múltiples selectores de fallback."""
    send_selectors: tuple[str, ...] = _SEL["send"]  # type: ignore[assignment]
    timeout_por_sel = TIMEOUT_SEND_BTN_MS // len(send_selectors)

    for selector in send_selectors:
        try:
            btn = await page.wait_for_selector(selector, timeout=timeout_por_sel)
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
    Si lo detecta, intenta cerrar el popup.
    """
    try:
        popup = await page.wait_for_selector(_SEL["popup"], timeout=TIMEOUT_POPUP_MS)
        if not popup:
            return False

        texto_popup = (await popup.inner_text()).lower()
        es_invalido = any(t in texto_popup for t in _POPUP_INVALID_TEXTS)

        if es_invalido:
            logger.warning("Popup de número inválido | texto='%s'", texto_popup[:80])
            popup_ok_selectors: tuple[str, ...] = _SEL["popup_ok"]  # type: ignore[assignment]
            for sel_ok in popup_ok_selectors:
                try:
                    ok_btn = page.locator(sel_ok).first
                    await ok_btn.click(timeout=3_000)
                    break
                except Exception:
                    continue
            return True

    except PlaywrightTimeoutError:
        pass
    except PlaywrightError as exc:
        logger.debug("Error verificando popup: %s", str(exc)[:100])

    return False


async def enviar_mensaje_wa(
    page: Page,
    contacto: ContactoWA,
    dry_run: bool = False,
) -> ResultadoEnvioWA:
    """Envía un mensaje a un número de WhatsApp via URL directa."""
    import random as _random
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

    logger.info("Enviando WA | empresa='%s' | numero=%s", contacto.nombre_empresa, contacto.numero)

    try:
        response = await page.goto(url_send, wait_until="domcontentloaded", timeout=TIMEOUT_NAV_MS)
        if response and response.status in (403, 429, 500, 503):
            return ResultadoEnvioWA(
                numero=contacto.numero, empresa=contacto.nombre_empresa,
                empresa_id=contacto.empresa_id,
                exito=False, estado="error", detalle=f"HTTP {response.status}",
            )

        await asyncio.sleep(3.0)

        if await _verificar_popup_invalido(page):
            return ResultadoEnvioWA(
                numero=contacto.numero, empresa=contacto.nombre_empresa,
                empresa_id=contacto.empresa_id,
                exito=False, estado="rebotado", detalle="Número no registrado en WhatsApp",
            )

        if not await _click_send_button(page):
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
                exito=False, estado="error", detalle="Botón de enviar no encontrado",
            )

        await asyncio.sleep(2.5)
        logger.info("✓ WA enviado | empresa='%s' | numero=%s", contacto.nombre_empresa, contacto.numero)
        return ResultadoEnvioWA(
            numero=contacto.numero, empresa=contacto.nombre_empresa,
            empresa_id=contacto.empresa_id, exito=True, estado="enviado",
        )

    except PlaywrightTimeoutError as exc:
        return ResultadoEnvioWA(
            numero=contacto.numero, empresa=contacto.nombre_empresa,
            empresa_id=contacto.empresa_id,
            exito=False, estado="error", detalle=f"Timeout: {str(exc)[:80]}",
        )
    except PlaywrightError as exc:
        return ResultadoEnvioWA(
            numero=contacto.numero, empresa=contacto.nombre_empresa,
            empresa_id=contacto.empresa_id,
            exito=False, estado="error", detalle=f"Playwright error: {str(exc)[:80]}",
        )
    except Exception as exc:
        logger.exception("Error inesperado | numero=%s | %s", contacto.numero, exc)
        return ResultadoEnvioWA(
            numero=contacto.numero, empresa=contacto.nombre_empresa,
            empresa_id=contacto.empresa_id,
            exito=False, estado="error", detalle=f"Error inesperado: {str(exc)[:80]}",
        )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────────────────────

async def procesar_envios_wa(
    limite: int = 20,
    dry_run: bool = False,
    headless: bool = False,
    estado: Optional["EstadoBot"] = None,
) -> dict[str, int]:
    """
    Pipeline completo: cuota diaria → contactos pendientes →
    sesión Playwright → autenticación → loop de envíos con jitter.
    """
    import random as _random
    metricas = {"procesados": 0, "enviados": 0, "rebotados": 0, "errores": 0}

    if dry_run:
        logger.warning("=== MODO DRY-RUN WA: no se enviarán mensajes reales ===")

    enviados_hoy   = await asyncio.to_thread(count_envios_wa_hoy)
    cuota_restante = max(0, WA_LIMITE_DIARIO - enviados_hoy)
    if cuota_restante == 0:
        logger.warning(
            "Cuota diaria WA alcanzada (%d mensajes). Ejecutar mañana.", WA_LIMITE_DIARIO
        )
        return metricas

    limite_efectivo = min(limite, cuota_restante)
    logger.info(
        "Cuota WA | hoy=%d | límite=%d | restante=%d | procesar=%d",
        enviados_hoy, WA_LIMITE_DIARIO, cuota_restante, limite_efectivo,
    )

    contactos = await asyncio.to_thread(get_contactos_whatsapp_pendientes, limite_efectivo)
    if not contactos:
        logger.info("No hay contactos WA pendientes.")
        return metricas

    async with async_playwright() as pw:
        WA_PROFILE_DIR.mkdir(parents=True, exist_ok=True)

        context = await pw.chromium.launch_persistent_context(
            user_data_dir=str(WA_PROFILE_DIR),
            headless=headless,
            args=CHROMIUM_ARGS,   # REFACTOR: desde utils.browser
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

        await apply_stealth(context)   # REFACTOR: desde utils.browser

        try:
            pages = context.pages
            page  = pages[0] if pages else await context.new_page()

            if WA_BASE_URL not in page.url:
                await page.goto(WA_BASE_URL, wait_until="domcontentloaded", timeout=TIMEOUT_NAV_MS)

            if not dry_run:
                autenticado = await _esperar_autenticacion(page, estado)
                if not autenticado:
                    logger.error("No se pudo autenticar en WhatsApp Web. Abortando.")
                    return metricas
            else:
                logger.info("[DRY-RUN] Saltando verificación de sesión WA.")

            es_primer_envio = True
            for contacto in contactos:
                metricas["procesados"] += 1
                logger.info(
                    "--- WA | empresa='%s' | numero=%s [%d/%d] ---",
                    contacto.nombre_empresa, contacto.numero,
                    metricas["procesados"], len(contactos),
                )

                if not es_primer_envio and not dry_run:
                    sleep_s = _random.randint(WA_JITTER_MIN_S, WA_JITTER_MAX_S)
                    logger.info("Rate limit: %d seg (~%.1f min)…", sleep_s, sleep_s / 60)
                    await asyncio.sleep(sleep_s)

                resultado = await enviar_mensaje_wa(page, contacto, dry_run=dry_run)

                if not dry_run:
                    await asyncio.to_thread(
                        registrar_envio_wa,
                        contacto.empresa_id, contacto.numero, resultado.estado,
                    )

                if resultado.exito:
                    metricas["enviados"] += 1
                elif resultado.estado == "rebotado":
                    metricas["rebotados"] += 1
                    logger.warning(
                        "✗ Número inválido | empresa='%s' | %s",
                        contacto.nombre_empresa, resultado.detalle,
                    )
                else:
                    metricas["errores"] += 1
                    logger.error(
                        "✗ Error | empresa='%s' | %s",
                        contacto.nombre_empresa, resultado.detalle,
                    )

                es_primer_envio = False

        finally:
            await context.close()
            logger.info("Contexto WA cerrado.")

    logger.info(
        "=== Campaña WA finalizada | procesados=%d | enviados=%d | rebotados=%d | errores=%d ===",
        metricas["procesados"], metricas["enviados"], metricas["rebotados"], metricas["errores"],
    )
    return metricas


# ─────────────────────────────────────────────────────────────────────────────
# CLI
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
            "  python wa_sender.py --limite 5\n"
            "  python wa_sender.py --dry-run --limite 20\n"
            "  python wa_sender.py --test-regex '223 555-1234'\n"
        ),
    )
    parser.add_argument("--limite",     type=int,  default=10)
    parser.add_argument("--dry-run",    action="store_true", dest="dry_run")
    parser.add_argument("--headless",   action="store_true")
    parser.add_argument("--test-regex", type=str,  default=None, dest="test_regex", metavar="TEXTO")
    args = parser.parse_args()

    if args.test_regex:
        numeros = extraer_numeros_whatsapp(args.test_regex)
        if numeros:
            print(f"Números encontrados ({len(numeros)}):")
            for n in numeros:
                print(f"  {n}")
        else:
            print("No se encontraron números de WhatsApp en el texto.")
        raise SystemExit(0)

    try:
        metricas = asyncio.run(
            procesar_envios_wa(
                limite=args.limite, dry_run=args.dry_run, headless=args.headless,
            )
        )
        print(f"\nResultado: {metricas}")
    except KeyboardInterrupt:
        print("\n[Interrumpido por el usuario. DB consistente.]")
    except Exception as exc:
        logger.exception("Error fatal: %s", exc)
        raise SystemExit(1)