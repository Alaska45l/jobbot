import asyncio
import urllib.request
from playwright.async_api import async_playwright

async def sacar_foto_stealth():
    print("Descargando payload stealth...")
    stealth_url = "https://raw.githubusercontent.com/requireCool/stealth.min.js/main/stealth.min.js"
    req = urllib.request.Request(stealth_url, headers={'User-Agent': 'Mozilla/5.0'})
    stealth_js = urllib.request.urlopen(req).read().decode('utf-8')

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        await page.add_init_script(stealth_js)
        
        print("Navegando al auditor de bots (Sannysoft)...")
        await page.goto("https://bot.sannysoft.com/", wait_until="networkidle")
        
        await asyncio.sleep(3)
        
        await page.screenshot(path="stealth_aprobado.png", full_page=True)
        print("¡Foto guardada como stealth_aprobado.png!")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(sacar_foto_stealth())