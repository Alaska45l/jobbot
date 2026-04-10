"""
jobbot_tui.py — JobBot TUI Presentation Layer
Teenage Engineering / Dieter Rams aesthetic: Minimalist Retro-Futurism.
"Less, but better."

Python: 3.11+
Dependencias: rich, qrcode, shutil (stdlib)
"""
from __future__ import annotations

import shutil
import time
from enum import Enum, auto
from functools import lru_cache
from typing import Optional

import qrcode

from rich import box
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.style import Style
from rich.table import Table
from rich.text import Text

# ─────────────────────────────────────────────────────────────────────────────
# TE PALETTE — "Machined aluminium + one accent at a time"
# ─────────────────────────────────────────────────────────────────────────────

class P:
    """Teenage Engineering × Dieter Rams color tokens."""
    CHROME   = "#D4D4D8"
    MATTE    = "#A1A1AA"
    CHASSIS  = "#3F3F46"
    VOID     = "#18181B"

    ORANGE   = "#FF6B35"
    YELLOW   = "#FFCC00"
    BLUE     = "#007AFF"
    RED      = "#FF3B30"

    s_chrome  = Style(color=CHROME)
    s_matte   = Style(color=MATTE)
    s_chassis = Style(color=CHASSIS)
    s_orange  = Style(color=ORANGE, bold=True)
    s_yellow  = Style(color=YELLOW)
    s_blue    = Style(color=BLUE,   bold=True)
    s_red     = Style(color=RED,    bold=True)
    s_dim     = Style(color=CHASSIS, italic=True)


# ─────────────────────────────────────────────────────────────────────────────
# BOT STATES
# ─────────────────────────────────────────────────────────────────────────────

class BotState(Enum):
    IDLE      = auto()
    DORKING   = auto()
    SCRAPING  = auto()
    MAILING   = auto()
    WA        = auto()
    SUCCESS   = auto()


# ─────────────────────────────────────────────────────────────────────────────
# JOB MASCOT — 8-bit LCD pet
# ─────────────────────────────────────────────────────────────────────────────

class JobMascot:

    CAT_BODY  = P.s_chrome
    EYE_SLEEP = P.s_chassis
    EYE_ALERT = P.s_orange
    EYE_HAPPY = P.s_blue
    EYE_WARN  = P.s_yellow
    EYE_ERR   = P.s_red

    def _draw_sleep(self) -> Text:
        return Text.assemble(
            ("              ████          ████    \n", self.CAT_BODY),
            ("              ██  ██      ██  ██    \n", self.CAT_BODY),
            ("              ██    ██████    ██    \n", self.CAT_BODY),
            ("      ████  ██                  ██  \n", self.CAT_BODY),
            ("    ██  ██  ██    ", self.CAT_BODY),
            ("──", self.EYE_SLEEP), ("      ", self.CAT_BODY), ("──", self.EYE_SLEEP),
            ("    ██  \n", self.CAT_BODY),
            ("    ██  ██  ██                  ██  \n", self.CAT_BODY),
            ("    ██    ██  ██              ██    \n", self.CAT_BODY),
            ("      ██    ██████          ██      \n", self.CAT_BODY),
            ("        ██                  ██      \n", self.CAT_BODY),
            ("        ██████████████████████      \n", self.CAT_BODY),
            ("                                    \n", self.CAT_BODY),
            ("                                    ", self.CAT_BODY),
        )

    def _draw_alert(self) -> Text:
        return Text.assemble(
            ("              ████          ████    \n", self.CAT_BODY),
            ("              ██  ██      ██  ██    \n", self.CAT_BODY),
            ("              ██    ██████    ██    \n", self.CAT_BODY),
            ("      ████  ██                  ██  \n", self.CAT_BODY),
            ("    ██  ██  ██    ", self.CAT_BODY),
            ("██", self.EYE_ALERT), ("      ", self.CAT_BODY), ("██", self.EYE_ALERT),
            ("    ██  \n", self.CAT_BODY),
            ("    ██  ██  ██                  ██  \n", self.CAT_BODY),
            ("    ██    ██  ██              ██    \n", self.CAT_BODY),
            ("      ██    ██████          ██      \n", self.CAT_BODY),
            ("        ██                  ██      \n", self.CAT_BODY),
            ("          ██                ██      \n", self.CAT_BODY),
            ("          ██  ██  ████  ██  ██      \n", self.CAT_BODY),
            ("                                      ", self.CAT_BODY),
        )

    def _draw_warn(self) -> Text:
        return Text.assemble(
            ("              ████          ████    \n", self.CAT_BODY),
            ("              ██  ██      ██  ██    \n", self.CAT_BODY),
            ("              ██    ██████    ██    \n", self.CAT_BODY),
            ("      ████  ██                  ██  \n", self.CAT_BODY),
            ("    ██  ██  ██    ", self.CAT_BODY),
            ("▀▀", self.EYE_WARN), ("      ", self.CAT_BODY), ("▀▀", self.EYE_WARN),
            ("    ██  \n", self.CAT_BODY),
            ("    ██  ██  ██                  ██  \n", self.CAT_BODY),
            ("    ██    ██  ██              ██    \n", self.CAT_BODY),
            ("      ██    ██████          ██      \n", self.CAT_BODY),
            ("        ██                  ██      \n", self.CAT_BODY),
            ("          ██                ██      \n", self.CAT_BODY),
            ("          ██  ██  ████  ██  ██      \n", self.CAT_BODY),
            ("                                      ", self.CAT_BODY),
        )

    def _draw_happy(self) -> Text:
        return Text.assemble(
            ("              ████          ████    \n", self.CAT_BODY),
            ("              ██  ██      ██  ██    \n", self.CAT_BODY),
            ("              ██    ██████    ██    \n", self.CAT_BODY),
            ("      ████  ██                  ██  \n", self.CAT_BODY),
            ("    ██  ██  ██    ", self.CAT_BODY),
            ("^^", self.EYE_HAPPY), ("      ", self.CAT_BODY), ("^^", self.EYE_HAPPY),
            ("    ██  \n", self.CAT_BODY),
            ("    ██  ██  ██                  ██  \n", self.CAT_BODY),
            ("    ██    ██  ██              ██    \n", self.CAT_BODY),
            ("      ██    ██████          ██      \n", self.CAT_BODY),
            ("        ██                  ██      \n", self.CAT_BODY),
            ("          ██                ██      \n", self.CAT_BODY),
            ("          ██  ██  ████  ██  ██      \n", self.CAT_BODY),
            ("                                      ", self.CAT_BODY),
        )

    def _draw_alert_blink(self) -> Text:
        return Text.assemble(
            ("              ████          ████    \n", self.CAT_BODY),
            ("              ██  ██      ██  ██    \n", self.CAT_BODY),
            ("              ██    ██████    ██    \n", self.CAT_BODY),
            ("      ████  ██                  ██  \n", self.CAT_BODY),
            ("    ██  ██  ██    ", self.CAT_BODY),
            ("──", self.EYE_ALERT), ("      ", self.CAT_BODY), ("──", self.EYE_ALERT),
            ("    ██  \n", self.CAT_BODY),
            ("    ██  ██  ██                  ██  \n", self.CAT_BODY),
            ("    ██    ██  ██              ██    \n", self.CAT_BODY),
            ("      ██    ██████          ██      \n", self.CAT_BODY),
            ("        ██                  ██      \n", self.CAT_BODY),
            ("          ██                ██      \n", self.CAT_BODY),
            ("          ██  ██  ████  ██  ██      \n", self.CAT_BODY),
            ("                                      ", self.CAT_BODY),
        )

    def get_frame(self, state: BotState, tick: int = 0) -> Text:
        match state:
            case BotState.IDLE:
                return self._draw_sleep()
            case BotState.DORKING | BotState.SCRAPING:
                return self._draw_alert_blink() if (tick % 15 == 14) else self._draw_alert()
            case BotState.MAILING | BotState.WA:
                return self._draw_warn()
            case BotState.SUCCESS:
                return self._draw_happy()
            case _:
                return self._draw_sleep()

    def get_status_line(self, state: BotState) -> Text:
        labels = {
            BotState.IDLE:     ("■ STANDBY",      P.s_chassis),
            BotState.DORKING:  ("▶ OSINT / DORK", P.s_orange),
            BotState.SCRAPING: ("▶ SCRAPING",      P.s_orange),
            BotState.MAILING:  ("▷ SMTP DISPATCH", P.s_yellow),
            BotState.WA:       ("▷ WA DISPATCH",   P.s_yellow),
            BotState.SUCCESS:  ("● CONFIRMED",     P.s_blue),
        }
        label, style = labels.get(state, ("■ STANDBY", P.s_chassis))
        t = Text(justify="center")
        t.append(f"  {label}  ", style=style)
        return t


# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

_mascot = JobMascot()


def _header(elapsed: str, phase: str) -> Panel:
    row = Text(justify="left")
    row.append("  JOB-BOT // ORCHESTRATOR", style=Style(color=P.CHROME, bold=True))
    row.append("  ·  ", style=P.s_chassis)
    row.append(phase, style=P.s_orange)

    clock = Text(f"  ⏱  {elapsed}  ", style=P.s_matte, justify="right")

    tbl = Table.grid(expand=True)
    tbl.add_column(ratio=3)
    tbl.add_column(ratio=1)
    tbl.add_row(row, clock)

    return Panel(tbl, box=box.SQUARE, border_style=P.CHASSIS, padding=(0, 0))


def _mascot_panel(state: BotState, tick: int) -> Panel:
    art    = _mascot.get_frame(state, tick)
    status = _mascot.get_status_line(state)
    body   = Text.assemble(art, "\n", status)

    return Panel(
        body,
        title=Text("  SYS // PET  ", style=P.s_matte),
        title_align="left",
        box=box.HEAVY,
        border_style=P.CHASSIS,
        padding=(1, 1),
    )

_STYLE_DARK  = "#000000 on #000000" 
_STYLE_LIGHT = "#ffffff on #ffffff" 
_STYLE_TOP   = "#000000 on #ffffff" 
_STYLE_BOT   = "#000000 on #ffffff" 


@lru_cache(maxsize=1)
def _qr_panel(qr_data: str) -> Panel:
    """
    Renderiza el QR forzando el Modo Compacto (Half-Blocks) y nivel L
    para garantizar que entre verticalmente en pantallas de notebooks.
    Conserva los colores absolutos para ser inmune a los temas.
    """
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=1,
        border=1, 
    )
    qr.add_data(qr_data)
    qr.make(fit=True)
    matrix = qr.modules

    matrix_height = len(matrix)
    matrix_width = len(matrix[0]) if matrix_height > 0 else 0

    texto_qr = Text(justify="center")

    for r in range(0, matrix_height, 2):
        for c in range(matrix_width):
            top = matrix[r][c]
            bot = matrix[r + 1][c] if r + 1 < matrix_height else False

            if top and bot:
                # Arriba negro, abajo negro
                texto_qr.append("█", style="#000000 on #000000")
            elif not top and not bot:
                # Arriba blanco, abajo blanco
                texto_qr.append("█", style="#FFFFFF on #FFFFFF")
            elif top and not bot:
                # Arriba negro, abajo blanco
                texto_qr.append("▀", style="#000000 on #FFFFFF")
            else: 
                # Arriba blanco, abajo negro
                texto_qr.append("▄", style="#000000 on #FFFFFF")
        texto_qr.append("\n")

    if texto_qr.plain.endswith("\n"):
        texto_qr.right_crop(1)

    return Panel(
        texto_qr,
        title="[bold yellow] 🔐 AUTH REQUERIDA [/]",
        subtitle="[dim]ESCANEA CON WHATSAPP[/]",
        border_style="bright_blue",
        box=box.DOUBLE_EDGE,
        padding=(0, 2),
        expand=False,
    )


def _metric(label: str, value: str | int, style: Style = P.s_chrome) -> tuple:
    return (
        Text(f"  {label}", style=P.s_matte),
        Text(str(value),   style=style),
    )


def _telemetry_panel(metrics: dict) -> Panel:
    tbl = Table(
        box=None,
        show_header=False,
        show_edge=False,
        expand=True,
        padding=(0, 2),
    )
    tbl.add_column("key",   style=P.s_matte,  ratio=3, no_wrap=True)
    tbl.add_column("value", style=P.s_chrome, ratio=2, justify="right")

    def section(name: str) -> None:
        tbl.add_row(
            Text(f" {name}", style=Style(color=P.CHASSIS, bold=True)),
            Text(""),
        )

    def row(label: str, val: str | int, style: Style = P.s_chrome) -> None:
        tbl.add_row(*_metric(label, val, style))

    def blank() -> None:
        tbl.add_row(Text(""), Text(""))

    section("[ OSINT ]")
    row("SEEDS FOUND",   metrics.get("seeds_found",    "—"))
    row("DOMAINS TOTAL", metrics.get("scraping_total", "—"))
    row("PROCESSED",     metrics.get("scraping_done",  "—"))
    row("ACTIVE",        metrics.get("scraping_active","—"), P.s_orange)
    row("SCORED OK",     metrics.get("scored_ok",      "—"), P.s_blue)
    blank()

    section("[ EMAIL_ENGINE ]")
    row("QUEUED",  metrics.get("mail_queued",  "—"))
    row("SENT",    metrics.get("mail_sent",    "—"), P.s_blue)
    row("BOUNCED", metrics.get("mail_bounced", "—"), P.s_red)
    row("SKIPPED", metrics.get("mail_skipped", "—"), P.s_yellow)
    row("ERRORS",  metrics.get("mail_errors",  "—"), P.s_red)
    blank()

    section("[ WA_ENGINE ]")
    row("QUEUED",  metrics.get("wa_queued",  "—"))
    row("SENT",    metrics.get("wa_sent",    "—"), P.s_blue)
    row("BOUNCED", metrics.get("wa_bounced", "—"), P.s_red)
    row("ERRORS",  metrics.get("wa_errors",  "—"), P.s_red)
    daily_cap = metrics.get("wa_daily_cap", 30)
    used      = metrics.get("wa_sent", 0)
    if not isinstance(used, int):
        used = 0
    row(
        "DAILY CAP", f"{used} / {daily_cap}",
        P.s_orange if used >= daily_cap * 0.8 else P.s_chrome,
    )

    return Panel(
        tbl,
        title=Text("  TELEMETRY ARRAY  ", style=P.s_matte),
        title_align="left",
        box=box.HEAVY,
        border_style=P.CHASSIS,
        padding=(1, 0),
    )


def _syslog_panel(logs: list[str]) -> Panel:
    tape = Text(overflow="fold")
    for line in logs:
        upper = line.upper()
        if "ERROR" in upper or "CRITICAL" in upper:
            tape.append(line + "\n", style=P.s_red)
        elif "WARNING" in upper or "WARN" in upper:
            tape.append(line + "\n", style=P.s_yellow)
        else:
            tape.append(line + "\n", style=P.s_dim)

    if not logs:
        tape = Text("  no events recorded.", style=P.s_dim)

    return Panel(
        tape,
        title=Text("  SYS // LOG  ", style=P.s_chassis),
        title_align="left",
        box=box.SQUARE,
        border_style=P.CHASSIS,
        padding=(0, 1),
    )


def generate_dashboard(
    state:      BotState,
    metrics:    dict,
    logs:       list[str],
    elapsed:    str = "00:00:00",
    phase:      str = "STANDBY",
    tick:       int = 0,
    wa_qr_data: str = "",
) -> Layout:
    """
    Pure function — toma estado, devuelve un Layout completamente renderizado.
    Llamar una vez por ciclo de refresh dentro del loop Live().

    Args:
        state:      BotState enum actual.
        metrics:    Dict plano de telemetría (ver claves en _telemetry_panel).
        logs:       Lista de strings de log recientes (el más nuevo al final).
        elapsed:    Tiempo transcurrido "HH:MM:SS".
        phase:      Descripción corta de la fase actual del pipeline.
        tick:       Entero monotónicamente creciente para animaciones.
        wa_qr_data: Payload string del QR de WhatsApp para renderizar en TUI.
    """
    root = Layout()

    root.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=8),
    )

    root["body"].split_row(
        Layout(name="mascot", minimum_size=88),
        Layout(name="telemetry"),
    )

    root["header"].update(_header(elapsed, phase))

    if wa_qr_data:
        # Leer tamaño de terminal en este instante para que lru_cache invalide
        # correctamente si el usuario redimensiona la ventana.
        t_cols, t_rows = shutil.get_terminal_size(fallback=(80, 24))
        root["body"]["mascot"].update(_qr_panel(wa_qr_data, t_cols, t_rows))
    else:
        root["body"]["mascot"].update(_mascot_panel(state, tick))

    root["body"]["telemetry"].update(_telemetry_panel(metrics))
    root["footer"].update(_syslog_panel(logs[-14:]))

    return root


# ─────────────────────────────────────────────────────────────────────────────
# INTEGRATION SHIM
# ─────────────────────────────────────────────────────────────────────────────

def bot_state_from_phase(phase_str: str) -> BotState:
    p = phase_str.lower()
    if any(k in p for k in ("dork", "duckg", "osint", "semilla")):
        return BotState.DORKING
    if any(k in p for k in ("scrape", "scrap", "playwright", "extray")):
        return BotState.SCRAPING
    if any(k in p for k in ("smtp", "mail", "email", "correo", "dispatch")):
        return BotState.MAILING
    if any(k in p for k in ("whatsapp", "wa ", "mensaj")):
        return BotState.WA
    if any(k in p for k in ("enviado", "confirmad", "exitoso", "success")):
        return BotState.SUCCESS
    return BotState.IDLE


def metrics_from_estado(snap: dict) -> dict:
    return {
        "seeds_found":      snap.get("scraping_total", "—"),
        "scraping_total":   snap.get("scraping_total", "—"),
        "scraping_done":    snap.get("scraping_procesados", "—"),
        "scraping_active":  len(snap.get("activos", [])),
        "scored_ok":        sum(
            1 for r in snap.get("terminados", []) if r.get("estado") == "OK"
        ),
        "mail_queued":      snap.get("mail_procesadas", "—"),
        "mail_sent":        snap.get("mail_enviadas",   "—"),
        "mail_bounced":     "—",
        "mail_skipped":     snap.get("mail_omitidas",   "—"),
        "mail_errors":      snap.get("mail_errores",    "—"),
        "wa_queued":        "—",
        "wa_sent":          "—",
        "wa_bounced":       "—",
        "wa_errors":        "—",
        "wa_daily_cap":     30,
        "wa_qr_data":       snap.get("wa_qr_data", ""),
    }


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE PREVIEW — python jobbot_tui.py
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import asyncio

    DEMO_LOGS = [
        "08:12:01 I [jobbot.dork] Dorking [1/4] | rubro=software house",
        "08:12:04 I [jobbot.dork] Semilla | techmdp.com.ar | software house",
        "08:12:07 I [jobbot.dork] Semilla | nextware.com.ar | software house",
        "08:12:11 W [jobbot.dork] DDGS rate limit | intento 1/3 | backoff=32s",
        "08:13:44 I [jobbot.scraper] OK | techmdp.com.ar | score=87 | apto=True",
        "08:13:51 I [jobbot.mailer] ✓ Enviado | empresa='TechMDP SRL' | envio_id=4",
        "08:14:02 E [jobbot.mailer] ✗ Fallo de envío | empresa='XYZ SRL'",
    ]

    DEMO_METRICS = {
        "seeds_found": 48, "scraping_total": 48, "scraping_done": 31,
        "scraping_active": 3, "scored_ok": 19,
        "mail_queued": 12, "mail_sent": 7, "mail_bounced": 1,
        "mail_skipped": 3, "mail_errors": 1,
        "wa_queued": 5, "wa_sent": 2, "wa_bounced": 0,
        "wa_errors": 0, "wa_daily_cap": 30,
    }

    STATES = [
        BotState.IDLE, BotState.DORKING, BotState.SCRAPING,
        BotState.MAILING, BotState.WA, BotState.SUCCESS,
    ]

    async def _preview() -> None:
        start = time.monotonic()
        tick  = 0
        with Live(auto_refresh=False, screen=False) as live:
            for state in STATES:
                for _ in range(20):
                    elapsed_s = int(time.monotonic() - start)
                    h, rem = divmod(elapsed_s, 3600)
                    m, s   = divmod(rem, 60)
                    layout  = generate_dashboard(
                        state   = state,
                        metrics = DEMO_METRICS,
                        logs    = DEMO_LOGS,
                        elapsed = f"{h:02d}:{m:02d}:{s:02d}",
                        phase   = state.name,
                        tick    = tick,
                    )
                    live.update(layout, refresh=True)
                    tick += 1
                    await asyncio.sleep(0.05)

    asyncio.run(_preview())