"""
utils/browser.py — JobBot Browser Utilities

Python: 3.11+
Dependencias: playwright
"""
from __future__ import annotations

import aiofiles
from pathlib import Path
from playwright.async_api import BrowserContext, Page

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

async def apply_stealth(context_or_page: BrowserContext | Page) -> None:
    """Inyecta el payload stealth nativo para evadir detección."""
    stealth_path = Path(__file__).parent / "stealth.min.js"
    
    async with aiofiles.open(stealth_path, mode='r') as f:
        stealth_js = await f.read()

    if hasattr(context_or_page, 'add_init_script'):
        await context_or_page.add_init_script(stealth_js)