"""
jobbot_tui.py — JobBot TUI Presentation Layer
Teenage Engineering / Dieter Rams aesthetic: Minimalist Retro-Futurism.
"Less, but better."

Drop-in replacement for the render_dashboard() in main.py.
NO async logic is touched. Only the presentation layer.

Python: 3.11+
Dependencias: rich
"""
from __future__ import annotations

import io
import qrcode
from functools import lru_cache
import time
from enum import Enum, auto
from typing import Optional

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
    # Neutrals (raw aluminium)
    CHROME   = "#D4D4D8"   # foreground / body text
    MATTE    = "#A1A1AA"   # secondary / dim text
    CHASSIS  = "#3F3F46"   # borders, idle elements, sleep eyes
    VOID     = "#18181B"   # deep background reference (not directly settable in Rich)

    # Accent primaries — one at a time, never mixed
    ORANGE   = "#FF6B35"   # active / alert / dorking  (TE Signal Orange)
    YELLOW   = "#FFCC00"   # warning / rate-limit       (TE Studio Yellow)
    BLUE     = "#007AFF"   # success / happy            (TE Sys Blue)
    RED      = "#FF3B30"   # error / rebotado           (TE Alarm Red)

    # Pre-built Rich Style shortcuts
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
    IDLE      = auto()   # waiting / rate-limit jitter
    DORKING   = auto()   # DuckDuckGo OSINT phase
    SCRAPING  = auto()   # Playwright stealth phase
    MAILING   = auto()   # SMTP dispatch
    WA        = auto()   # WhatsApp Web phase
    SUCCESS   = auto()   # confirmed send / high-score find


# ─────────────────────────────────────────────────────────────────────────────
# JOB MASCOT — 8-bit LCD pet on a hardware screen
# ─────────────────────────────────────────────────────────────────────────────

class JobMascot:
    """
    Pixel-art mascot rendered as a Rich Text object.
    Strict 36x13 character grid to prevent any layout shifting.
    Based on Teenage Engineering / Dieter Rams solid hardware aesthetics.
    """

    # TE palette wired to mascot regions
    CAT_BODY  = P.s_chrome    # aluminium hull
    EYE_SLEEP = P.s_chassis   # matte dark grey  — IDLE
    EYE_ALERT = P.s_orange    # signal orange    — DORKING / SCRAPING
    EYE_HAPPY = P.s_blue      # sys blue         — SUCCESS
    EYE_WARN  = P.s_yellow    # studio yellow    — MAILING / WA
    EYE_ERR   = P.s_red       # alarm red        — error state

    def _draw_sleep(self) -> Text:
        """IDLE state — sleeping, eyes as flat lines."""
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
            ("        ██████████████  ██  ██      \n", self.CAT_BODY),
            ("                                    \n", self.CAT_BODY),
            ("                                    ", self.CAT_BODY),
        )

    def _draw_alert(self) -> Text:
        """DORKING / SCRAPING — eyes wide open, signal orange."""
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
        """MAILING / WA — studio yellow eyes, squinting/scanning."""
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
        """SUCCESS — sys blue eyes, happy expression."""
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

    # ── Blinking animation helper (2-frame cycle on ALERT) ──────────────────

    def _draw_alert_blink(self) -> Text:
        """Alternate frame for DORKING — flat blink effect."""
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

    # ── Public API ───────────────────────────────────────────────────────────

    def get_frame(self, state: BotState, tick: int = 0) -> Text:
        """
        Returns the correct animation frame for the current bot state.
        tick is an integer that increments each refresh — drives blink cycles.
        """
        match state:
            case BotState.IDLE:
                return self._draw_sleep()
            case BotState.DORKING | BotState.SCRAPING:
                # 2-frame blink: 14 normal, 1 blink, repeat
                return self._draw_alert_blink() if (tick % 15 == 14) else self._draw_alert()
            case BotState.MAILING | BotState.WA:
                return self._draw_warn()
            case BotState.SUCCESS:
                return self._draw_happy()
            case _:
                return self._draw_sleep()

    def get_status_line(self, state: BotState) -> Text:
        """One-liner status tag shown beneath the mascot panel."""
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
# DASHBOARD — generate_dashboard()
# The ONLY function that should be called from main.py's Live loop.
# ─────────────────────────────────────────────────────────────────────────────

_mascot = JobMascot()


def _header(elapsed: str, phase: str) -> Panel:
    """
    Top strip — hardware label + phase + clock.
    SQUARE box = industrial corners, zero decoration.
    """
    row = Text(justify="left")
    row.append("  JOB-BOT // ORCHESTRATOR", style=Style(color=P.CHROME, bold=True))
    row.append("  ·  ", style=P.s_chassis)
    row.append(phase, style=P.s_orange)

    clock = Text(f"  ⏱  {elapsed}  ", style=P.s_matte, justify="right")

    # Combine into a single-row table so elapsed floats right
    tbl = Table.grid(expand=True)
    tbl.add_column(ratio=3)
    tbl.add_column(ratio=1)
    tbl.add_row(row, clock)

    return Panel(tbl, box=box.SQUARE, border_style=P.CHASSIS, padding=(0, 0))


def _mascot_panel(state: BotState, tick: int) -> Panel:
    """
    Left column — the "LCD screen" housing the mascot.
    Fixed width feels like a physical display cutout.
    """
    art    = _mascot.get_frame(state, tick)
    status = _mascot.get_status_line(state)

    body = Text.assemble(art, "\n", status)

    return Panel(
        body,
        title=Text("  SYS // PET  ", style=P.s_matte),
        title_align="left",
        box=box.HEAVY,
        border_style=P.CHASSIS,
        padding=(1, 1),
    )


@lru_cache(maxsize=1)
def _qr_panel(qr_data: str) -> Panel:
    """
    Renderiza el QR de WhatsApp Web en terminal.

    Historial de cambios:
      v1 (original): version=1, border=1 → DataOverflowError en payloads
                     largos de WA Auth; quiet zone insuficiente.
      v2 (parche 1): version=None, border=2 → auto-sizing correcto.
                     no_wrap=True + overflow="crop" → aún truncaba si
                     minimum_size del Layout era menor que el ancho del QR.
      v3 (este):     Elimina no_wrap/overflow del Text. El Panel usa
                     expand=False para no estirar el QR, y el Layout
                     garantiza minimum_size=88 para contenerlo.
                     Estilo aplicado al Text completo (O(1), no por carácter).
    """
    qr = qrcode.QRCode(
        version=None,                               # auto-size según payload
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=1,
        border=2,                                   # quiet zone: 2 módulos c/lado
    )
    qr.add_data(qr_data)
    qr.make(fit=True)
    matrix = qr.modules

    lines: list[str] = []
    for r in range(0, len(matrix), 2):
        row_str = ""
        for c in range(len(matrix[0])):
            top = matrix[r][c]
            bot = matrix[r + 1][c] if r + 1 < len(matrix) else False
            if not top and not bot: row_str += "█"
            elif not top and bot:   row_str += "▀"
            elif top and not bot:   row_str += "▄"
            else:                   row_str += " "
        lines.append(row_str)

    # Estilo en el objeto Text, no por carácter.
    # "white on black" fuerza el contraste correcto independientemente
    # del tema de terminal (Arch/KDE Plasma con paleta oscura transparente).
    qr_content = Text(
        "\n".join(lines),
        style="white on black",
        justify="center",
        # Sin no_wrap ni overflow: Rich renderiza el ancho natural del QR.
        # El Layout con minimum_size=88 garantiza que haya espacio.
    )

    return Panel(
        qr_content,
        title="[bold yellow] 🔐 AUTH REQUERIDA [/]",
        subtitle="[dim]ESCANEA CON WHATSAPP[/]",
        border_style="bright_blue",
        box=box.DOUBLE_EDGE,
        padding=(1, 2),
        expand=False,   # El panel no se estira: el QR mantiene su geometría.
    )


def _metric(label: str, value: str | int, style: Style = P.s_chrome) -> tuple:
    return (
        Text(f"  {label}", style=P.s_matte),
        Text(str(value),   style=style),
    )


def _telemetry_panel(metrics: dict) -> Panel:
    """
    Right column — three metric groups in a single Table.
    Groups: [OSINT]  [EMAIL_ENGINE]  [WA_ENGINE]
    No headers repeated — section labels act as dividers.
    """
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

    # ── [OSINT] ─────────────────────────────────────────────────────────────
    section("[ OSINT ]")
    row("SEEDS FOUND",   metrics.get("seeds_found",   "—"))
    row("DOMAINS TOTAL", metrics.get("scraping_total", "—"))
    row("PROCESSED",     metrics.get("scraping_done",  "—"))
    row("ACTIVE",        metrics.get("scraping_active","—"), P.s_orange)
    row("SCORED OK",     metrics.get("scored_ok",      "—"), P.s_blue)
    blank()

    # ── [EMAIL_ENGINE] ───────────────────────────────────────────────────────
    section("[ EMAIL_ENGINE ]")
    row("QUEUED",     metrics.get("mail_queued",    "—"))
    row("SENT",       metrics.get("mail_sent",      "—"), P.s_blue)
    row("BOUNCED",    metrics.get("mail_bounced",   "—"), P.s_red)
    row("SKIPPED",    metrics.get("mail_skipped",   "—"), P.s_yellow)
    row("ERRORS",     metrics.get("mail_errors",    "—"), P.s_red)
    blank()

    # ── [WA_ENGINE] ──────────────────────────────────────────────────────────
    section("[ WA_ENGINE ]")
    row("QUEUED",     metrics.get("wa_queued",      "—"))
    row("SENT",       metrics.get("wa_sent",        "—"), P.s_blue)
    row("BOUNCED",    metrics.get("wa_bounced",     "—"), P.s_red)
    row("ERRORS",     metrics.get("wa_errors",      "—"), P.s_red)
    daily_cap = metrics.get("wa_daily_cap", 30)
    used      = metrics.get("wa_sent", 0)
    row("DAILY CAP",  f"{used} / {daily_cap}",
        P.s_orange if used >= daily_cap * 0.8 else P.s_chrome)

    return Panel(
        tbl,
        title=Text("  TELEMETRY ARRAY  ", style=P.s_matte),
        title_align="left",
        box=box.HEAVY,
        border_style=P.CHASSIS,
        padding=(1, 0),
    )


def _syslog_panel(logs: list[str]) -> Panel:
    """
    Footer — raw event tape. Last N lines, no decoration.
    Color coded: ERROR=red, WARNING=yellow, INFO=chassis dim.
    """
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
    state:   BotState,
    metrics: dict,
    logs:    list[str],
    elapsed: str = "00:00:00",
    phase:   str = "STANDBY",
    tick:    int = 0,
    wa_qr_data: str = "",
) -> Layout:
    """
    Pure function — takes state, returns a fully rendered Layout.
    Call this once per refresh cycle inside your Live() loop.

    Args:
        state:   Current BotState enum value.
        metrics: Flat dict of telemetry values (see _telemetry_panel keys).
        logs:    List of recent log strings (newest last).
        elapsed: Human-readable elapsed time string "HH:MM:SS".
        phase:   Short description of the current pipeline phase.
        tick:    Monotonically increasing integer for animation frames.
        wa_qr_data: String containing the QR payload to render.

    Returns:
        rich.layout.Layout ready to be passed to live.update().
    """
    root = Layout()

    root.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=8),
    )

    root["body"].split_row(
        Layout(name="mascot", minimum_size=88),  # v12 QR → 79 chars, v14 → 87 chars
        Layout(name="telemetry"),
    )

    root["header"].update(_header(elapsed, phase))
    
    if wa_qr_data:
        root["body"]["mascot"].update(_qr_panel(wa_qr_data))
    else:
        root["body"]["mascot"].update(_mascot_panel(state, tick))
        
    root["body"]["telemetry"].update(_telemetry_panel(metrics))
    root["footer"].update(_syslog_panel(logs[-14:]))   # last 14 lines max

    return root


# ─────────────────────────────────────────────────────────────────────────────
# INTEGRATION SHIM — drop into main.py's _async_main()
# ─────────────────────────────────────────────────────────────────────────────

def bot_state_from_phase(phase_str: str) -> BotState:
    """
    Maps the free-text EstadoBot.fase_actual to a BotState enum.
    Keeps main.py's EstadoBot intact — no changes to your data model.
    """
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
    """
    Adapts your existing EstadoBot.snapshot() dict to the flat metrics
    dict expected by generate_dashboard(). No EstadoBot changes required.
    """
    return {
        # OSINT
        "seeds_found":      snap.get("scraping_total", "—"),
        "scraping_total":   snap.get("scraping_total", "—"),
        "scraping_done":    snap.get("scraping_procesados", "—"),
        "scraping_active":  len(snap.get("activos", [])),
        "scored_ok":        sum(
            1 for r in snap.get("terminados", []) if r.get("estado") == "OK"
        ),
        # EMAIL
        "mail_queued":      snap.get("mail_procesadas", "—"),
        "mail_sent":        snap.get("mail_enviadas",   "—"),
        "mail_bounced":     "—",   # add to EstadoBot if needed
        "mail_skipped":     snap.get("mail_omitidas",   "—"),
        "mail_errors":      snap.get("mail_errores",    "—"),
        # WA
        "wa_queued":        "—",
        "wa_sent":          "—",
        "wa_bounced":       "—",
        "wa_errors":        "—",
        "wa_daily_cap":     30,
        "wa_qr_data":       snap.get("wa_qr_data", ""),
    }


# ─────────────────────────────────────────────────────────────────────────────
# HOW TO WIRE INTO main.py — replace _async_main()'s Live block with this:
# ─────────────────────────────────────────────────────────────────────────────
#
#   from jobbot_tui import generate_dashboard, bot_state_from_phase, metrics_from_estado
#
#   async def _async_main(args):
#       ...
#       tick = 0
#
#       async def _refresh_loop(live: Live) -> None:
#           nonlocal tick
#           while not stop_event.is_set():
#               snap    = estado.snapshot()
#               state   = bot_state_from_phase(snap["fase_actual"])
#               metrics = metrics_from_estado(snap)
#               layout  = generate_dashboard(
#                   state   = state,
#                   metrics = metrics,
#                   logs    = snap["log_lines"],
#                   elapsed = snap["elapsed"],
#                   phase   = snap["fase_actual"].upper(),
#                   tick    = tick,
#               )
#               try:
#                   live.update(layout, refresh=True)
#               except Exception:
#                   pass
#               tick += 1
#               await asyncio.sleep(DASHBOARD_REFRESH_S)
#
#       with Live(
#           generate_dashboard(BotState.IDLE, {}, [], tick=0),
#           auto_refresh=False,
#           screen=False,
#           redirect_stderr=False,
#       ) as live:
#           refresh_task = asyncio.create_task(_refresh_loop(live))
#           try:
#               if   args.dork:   await pipeline_dork(args, estado)
#               elif args.scrape: await pipeline_scrape(args, estado)
#               elif args.mail:   await pipeline_mail(args, estado)
#               elif args.wa:     await pipeline_wa(args, estado)
#               elif args.auto:   await pipeline_auto(args, estado)
#           finally:
#               stop_event.set()
#               refresh_task.cancel()
#               try:   await refresh_task
#               except asyncio.CancelledError: pass
#               live.update(generate_dashboard(state, metrics, snap["log_lines"]))
#
# ─────────────────────────────────────────────────────────────────────────────


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

    STATES = [BotState.IDLE, BotState.DORKING, BotState.SCRAPING,
              BotState.MAILING, BotState.WA, BotState.SUCCESS]

    async def _preview():
        start = time.monotonic()
        tick  = 0
        with Live(auto_refresh=False, screen=False) as live:
            for state in STATES:
                for _ in range(20):   # show each state for ~1 second
                    elapsed_s = int(time.monotonic() - start)
                    h, rem = divmod(elapsed_s, 3600)
                    m, s   = divmod(rem, 60)
                    elapsed = f"{h:02d}:{m:02d}:{s:02d}"
                    layout  = generate_dashboard(
                        state   = state,
                        metrics = DEMO_METRICS,
                        logs    = DEMO_LOGS,
                        elapsed = elapsed,
                        phase   = state.name,
                        tick    = tick,
                    )
                    live.update(layout, refresh=True)
                    tick += 1
                    await asyncio.sleep(0.05)

    asyncio.run(_preview())