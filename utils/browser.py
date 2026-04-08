"""
utils/browser.py — JobBot Browser Utilities

Python: 3.11+
Dependencias: playwright
"""
from __future__ import annotations

from playwright.async_api import BrowserContext

# ---------------------------------------------------------------------------
# Argumentos de lanzamiento de Chromium
# Aplican tanto en modo headless (scraper) como en modo visible (wa_sender).
# ---------------------------------------------------------------------------
CHROMIUM_ARGS: list[str] = [
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
# Script de anti-detección de webdriver
# Se inyecta vía add_init_script() antes de que cualquier página cargue.
# ---------------------------------------------------------------------------
ANTI_DETECTION_SCRIPT: str = """
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'plugins',   { get: () => [1, 2, 3] });
    Object.defineProperty(navigator, 'languages', { get: () => ['es-AR', 'es', 'en'] });
    if (window.chrome) { window.chrome.runtime = {}; }
"""


async def apply_stealth(context: BrowserContext) -> None:
    """
    Aplica el script de anti-detección a un BrowserContext de Playwright.

    Llamar después de crear el contexto y antes de abrir cualquier página,
    para que el script se inyecte en todos los frames subsiguientes.

    Args:
        context: Contexto de Playwright activo (stealth o persistente).
    """
    await context.add_init_script(ANTI_DETECTION_SCRIPT)