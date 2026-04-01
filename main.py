"""
main.py — JobBot Orchestrator v1.0
Pipeline completo: OSINT Dorking → Async Scraping → Lead Scoring → Cold Email

Arquitectura:
  ┌─ asyncio event loop ─────────────────────────────────────────────────┐
  │  ┌─ Rich Live (screen=True) ──────────────────────────────────────┐  │
  │  │   async refresh_task  →  live.update(render_dashboard(estado)) │  │
  │  │   pipeline coroutines →  procesar_dominio() / to_thread(mail)  │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  └──────────────────────────────────────────────────────────────────────┘

Decisiones de diseño:
  - Rich Live con refresh_per_second=0: el refresh es controlado por una
    coroutine asíncrona, no por el thread interno de Rich, lo que permite
    que Playwright (que usa su propio event loop interno) coexista sin
    conflictos de threading.
  - threading.Lock en EstadoBot: el thread del render de Rich y el event
    loop de asyncio comparten estado; el Lock previene race conditions en
    secciones de escritura/lectura de listas y contadores.
  - Logging redirigido a deque interno: ningún handler escribe a stderr
    durante la sesión Live, eliminando la corrupción visual del TUI.
  - procesar_envios_pendientes en asyncio.to_thread: la función tiene
    time.sleep() bloqueante (rate limiting SMTP); enviarla al thread pool
    libera el event loop para el refresh del dashboard.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import re
import urllib.parse
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock as ThreadLock
from typing import Optional

from rich import box
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from db_manager import (
    get_connection,
    get_empresas_ordenadas_por_score,
    init_db,
    upsert_empresa,
)
from mailer import procesar_envios_pendientes
from scraper import procesar_dominio, procesar_lote


# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

# Portales de empleo y dominios basura excluidos del dorking
PORTALES_EXCLUIDOS: frozenset[str] = frozenset({
    "bumeran.com.ar", "zonajobs.com.ar", "computrabajo.com.ar",
    "indeed.com", "indeed.com.ar", "linkedin.com", "reempleos.com.ar",
    "arg.trabajando.com", "empleos.clarin.com", "jobbol.com",
    "aptitud.com.ar", "gruporia.com", "randstad.com.ar",
    "adecco.com.ar", "manpower.com.ar", "infojobs.net",
    "ziprecruiter.com", "glassdoor.com", "monster.com",
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "google.com", "google.com.ar", "wikipedia.org",
    "mercadolibre.com.ar", "mercadopago.com", "tiktok.com",
    "whatsapp.com", "paginas-amarillas.com.ar", "ar.computrabajo.com", 
    "jobted.com.ar", "jobted.com", "paginasamarillas.com.ar", "infoisinfo-ar.com",
    "infoisinfo.com.ar", "adecco.com", "inta.gob.ar",
     "conicet.gov.ar", "uba.ar", "unlp.edu.ar",
    "buscojobs.com", "buscojobs.com.ar",
    "bacap.com.ar", "revistacentral.com.ar",
    "domain.com", "example.com", "abc.xyz",
    "edu.ar", "mdp.edu.ar", "ufasta.edu.ar", "caece.edu.ar", "atlantida.edu.ar",
})

RUBROS_DEFAULT: list[str] = [
    "software house",
    "soporte técnico pc",
    "clínica",
    "centro médico",
    "estudio contable",
    "QA testing",
    "servicios informáticos",
    "laboratorio médico",
    "desarrollo sistemas"
]

# Máximo de filas visibles en la tabla de scraping del dashboard
MAX_SCRAPING_ROWS: int = 18

# Máximo de líneas en el panel de log del dashboard
MAX_LOG_LINES: int = 14

# Intervalo de refresco del dashboard (segundos). 4 FPS = equilibrio CPU/fluidez.
DASHBOARD_REFRESH_S: float = 0.25

# Intervalo de polling a la DB durante el pipeline de mail (segundos)
MAIL_POLL_INTERVAL_S: float = 3.0


# ─────────────────────────────────────────────────────────────────────────────
# Logging: handler que escribe al buffer interno del TUI (no a stderr)
# ─────────────────────────────────────────────────────────────────────────────

class _TUILogHandler(logging.Handler):
    """
    Captura todos los registros de logging en un deque thread-safe.
    El dashboard renderiza este buffer en el panel de log en vivo.
    """

    def __init__(self, buffer: deque[str], lock: ThreadLock) -> None:
        super().__init__()
        self._buf  = buffer
        self._lock = lock

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
        except Exception:
            line = record.getMessage()
        with self._lock:
            self._buf.append(line)


def _configurar_logging(buffer: deque[str], lock: ThreadLock) -> None:
    """
    Reemplaza todos los handlers del logger raíz por el handler de buffer TUI.
    Silencia librerías externas verbosas para no contaminar el log del dashboard.
    """
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Eliminar handlers previos (stdout, stderr, fichero, etc.)
    for h in root.handlers[:]:
        root.removeHandler(h)
        h.close()

    handler = _TUILogHandler(buffer, lock)
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s %(levelname).1s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(handler)

    # Reducir ruido de librerías de terceros
    for noisy_logger in ("playwright", "asyncio", "urllib3", "httpx", "httpcore"):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)


# ─────────────────────────────────────────────────────────────────────────────
# Estado compartido del dashboard (thread-safe)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EstadoBot:
    """
    Estado mutable compartido entre el event loop de asyncio y el hilo de
    refresh de Rich. Toda escritura en campos mutables (listas, contadores)
    debe adquirir _lock para garantizar consistencia.
    """

    fase_actual: str      = "Iniciando…"
    inicio:      datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # ── Panel izquierdo: progreso de scraping ─────────────────────────────
    scraping_total:      int  = 0
    scraping_procesados: int  = 0
    scraping_rows:       list = field(default_factory=list)
    # Cada fila: {"dominio": str, "score": int, "perfil_cv": str, "estado": str}

    # ── Panel derecho: estadísticas de campaña email ──────────────────────
    mail_procesadas: int = 0
    mail_enviadas:   int = 0
    mail_errores:    int = 0
    mail_omitidas:   int = 0

    # ── Buffer de log (renderizado en el sub-panel inferior derecho) ───────
    log_buffer: deque = field(default_factory=lambda: deque(maxlen=MAX_LOG_LINES))

    # Lock: protege secciones críticas accedidas desde múltiples threads
    _lock: ThreadLock = field(default_factory=ThreadLock)

    # ──────────────────────────────────────────────────────────────────────

    def upsert_scraping_row(
        self,
        dominio:  str,
        score:    int,
        perfil_cv: str,
        estado:   str,
    ) -> None:
        """Inserta o actualiza una fila en la tabla de scraping. Thread-safe."""
        with self._lock:
            for row in self.scraping_rows:
                if row["dominio"] == dominio:
                    row["score"]     = score
                    row["perfil_cv"] = perfil_cv
                    row["estado"]    = estado
                    return
            self.scraping_rows.append({
                "dominio":  dominio,
                "score":    score,
                "perfil_cv": perfil_cv,
                "estado":   estado,
            })
            # Ventana deslizante: mantener solo las últimas N filas
            if len(self.scraping_rows) > MAX_SCRAPING_ROWS:
                del self.scraping_rows[0]

    def elapsed(self) -> str:
        """Retorna el tiempo transcurrido desde el inicio en formato HH:MM:SS."""
        delta  = datetime.now(timezone.utc) - self.inicio
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m, s   = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard: funciones de renderizado
# ─────────────────────────────────────────────────────────────────────────────

def _render_header(estado: EstadoBot) -> Panel:
    """Panel superior: ASCII banner, fase actual y tiempo transcurrido."""
    t = Text(justify="center")
    t.append(
        "  ╔══════════════════════════════════════════════════════════╗\n",
        style="bold bright_cyan",
    )
    t.append("  ║  ", style="bold bright_cyan")
    t.append("◈  J O B B O T  ", style="bold bright_white")
    t.append("v1.0", style="bold bright_yellow")
    t.append(
        "   ·   OSINT  ·  Scraping  ·  Cold Email  ·  MdP  ◈",
        style="bold bright_white",
    )
    t.append("  ║\n", style="bold bright_cyan")
    t.append(
        "  ╚══════════════════════════════════════════════════════════╝\n",
        style="bold bright_cyan",
    )
    t.append("  Fase: ", style="dim")
    t.append(estado.fase_actual, style="bold green")
    t.append(f"    ⏱  {estado.elapsed()}", style="dim")

    return Panel(t, border_style="bright_blue", padding=(0, 1))


_ESTADO_ESTILOS: dict[str, str] = {
    "OK":        "bold green",
    "Error":     "bold red",
    "Cooldown":  "bold yellow",
    "Scrapeando": "bold bright_yellow",
    "Semilla":   "dim cyan",
    "Omitido":   "dim white",
}


def _render_scraping_panel(estado: EstadoBot) -> Panel:
    """Panel izquierdo: tabla de progreso de scraping con score y perfil de CV."""
    tbl = Table(
        box=box.SIMPLE_HEAD,
        header_style="bold cyan",
        show_lines=False,
        expand=True,
        padding=(0, 1),
    )
    tbl.add_column("Dominio",   no_wrap=True, max_width=30)
    tbl.add_column("Score",     justify="right",  width=7)
    tbl.add_column("Perfil CV", justify="center", width=14)
    tbl.add_column("Estado",    justify="center", width=12)

    with estado._lock:
        rows_snapshot = list(estado.scraping_rows)

    for row in rows_snapshot:
        score    = row["score"]
        perfil   = row.get("perfil_cv") or "–"
        est_str  = row.get("estado",    "–")

        score_style  = "bold green"   if score >= 55 else (
                       "bold yellow"  if score >= 20 else "dim red"
        )
        perfil_style = "bright_cyan"    if perfil == "CV_Tech"    else (
                       "bright_magenta" if perfil == "CV_Admin_IT" else "dim"
        )
        est_style    = _ESTADO_ESTILOS.get(est_str, "white")

        tbl.add_row(
            Text(row["dominio"], overflow="ellipsis"),
            Text(str(score),     style=score_style),
            Text(perfil,         style=perfil_style),
            Text(est_str,        style=est_style),
        )

    total      = estado.scraping_total
    procesados = estado.scraping_procesados
    pct_str    = f"{(procesados / total * 100):.0f}%" if total > 0 else "–"

    return Panel(
        tbl,
        title="[bold cyan]🔍 Scraping Progress[/bold cyan]",
        subtitle=f"[dim]{procesados} / {total}  ({pct_str})[/dim]",
        border_style="cyan",
    )


def _render_mail_panel(estado: EstadoBot) -> Panel:
    """Sub-panel derecho superior: métricas de la campaña de emails."""
    tbl = Table(
        box=box.SIMPLE,
        show_header=False,
        expand=True,
        padding=(0, 2),
    )
    tbl.add_column("Métrica", style="dim",    ratio=2)
    tbl.add_column("Valor",   justify="right", ratio=1)

    tbl.add_row("📤  Procesadas", Text(str(estado.mail_procesadas), style="bold white"))
    tbl.add_row("✅  Enviadas",    Text(str(estado.mail_enviadas),   style="bold green"))
    tbl.add_row("❌  Errores",     Text(str(estado.mail_errores),    style="bold red"))
    tbl.add_row("⏭   Omitidas",   Text(str(estado.mail_omitidas),   style="bold yellow"))

    return Panel(
        tbl,
        title="[bold magenta]📧 Campaña Email[/bold magenta]",
        border_style="magenta",
    )


def _render_log_panel(estado: EstadoBot) -> Panel:
    """Sub-panel derecho inferior: cola de logs con coloreado por nivel."""
    with estado._lock:
        lines = list(estado.log_buffer)

    t = Text(overflow="fold")
    for line in lines:
        if "CRITICAL" in line or "ERROR" in line:
            t.append(line + "\n", style="red")
        elif "WARNING" in line:
            t.append(line + "\n", style="yellow")
        else:
            t.append(line + "\n", style="dim white")

    if not lines:
        t = Text("Sin actividad registrada…", style="dim italic")

    return Panel(
        t,
        title="[dim]📋 Log en vivo[/dim]",
        border_style="dim",
    )


def render_dashboard(estado: EstadoBot) -> Layout:
    """
    Construye y retorna el Layout completo del dashboard.

    Estructura:
      ┌──────────── header (size=6) ─────────────┐
      ├────── left (ratio=3) ─┬─ right (ratio=2) ┤
      │  Scraping Table       │  Mail Stats (8)   │
      │                       ├───────────────────┤
      │                       │  Log Panel        │
      └───────────────────────┴───────────────────┘
    """
    root = Layout()
    root.split_column(
        Layout(name="header", size=6),
        Layout(name="body"),
    )
    root["body"].split_row(
        Layout(name="left",  ratio=3),
        Layout(name="right", ratio=2),
    )
    root["body"]["right"].split_column(
        Layout(name="mail_stats", size=8),
        Layout(name="log"),
    )

    root["header"].update(_render_header(estado))
    root["body"]["left"].update(_render_scraping_panel(estado))
    root["body"]["right"]["mail_stats"].update(_render_mail_panel(estado))
    root["body"]["right"]["log"].update(_render_log_panel(estado))

    return root


# ─────────────────────────────────────────────────────────────────────────────
# Helpers: dominio, DDGS, DB stats
# ─────────────────────────────────────────────────────────────────────────────

def _extraer_dominio_limpio(url: str) -> Optional[str]:
    """
    Extrae el dominio raíz (sin www) de una URL arbitraria.
    Rechaza IPs, dominios sin TLD y cadenas malformadas.
    """
    try:
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        netloc = urllib.parse.urlparse(url).netloc.lower().lstrip("www.")
        if not netloc or "." not in netloc:
            return None
        # Rechazar direcciones IP
        if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", netloc):
            return None
        return netloc
    except Exception:
        return None


def _es_portal_excluido(dominio: str) -> bool:
    """True si el dominio coincide exactamente o es subdominio de un portal excluido."""
    return any(
        dominio == portal or dominio.endswith("." + portal)
        for portal in PORTALES_EXCLUIDOS
    )


def _ddgs_text_sync(query: str, max_results: int) -> list[dict]:
    from ddgs import DDGS
    return list(DDGS().text(query, max_results=max_results))

def _query_mail_stats_db() -> dict[str, int]:
    """
    Consulta métricas de envíos del día actual directamente en la DB.
    Función síncrona, ejecutar con asyncio.to_thread durante el pipeline mail.
    """
    sql = """
        SELECT
            COUNT(*)                                                AS total,
            SUM(CASE WHEN estado = 'enviado'   THEN 1 ELSE 0 END)  AS enviadas,
            SUM(CASE WHEN estado = 'rebotado'  THEN 1 ELSE 0 END)  AS errores,
            SUM(CASE WHEN estado = 'pendiente' THEN 1 ELSE 0 END)  AS pendientes
        FROM campanas_envios
        WHERE fecha_envio >= strftime('%Y-%m-%dT00:00:00Z', 'now');
    """
    try:
        with get_connection() as conn:
            row = conn.execute(sql).fetchone()
            return {
                "total":      int(row["total"]      or 0),
                "enviadas":   int(row["enviadas"]   or 0),
                "errores":    int(row["errores"]    or 0),
                "pendientes": int(row["pendientes"] or 0),
            }
    except Exception:
        return {"total": 0, "enviadas": 0, "errores": 0, "pendientes": 0}


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Dorking (recolección de URLs semilla)
# ─────────────────────────────────────────────────────────────────────────────

async def recolectar_urls_semilla(
    rubros:  list[str],
    zona:    str = "Mar del Plata",
    limite:  int = 30,
    estado:  Optional[EstadoBot] = None,
) -> int:
    """
    Busca empresas locales via DuckDuckGo Dorking y siembra la DB.

    Estrategia de dork:
      - Una query por rubro: 'empresa {rubro} "{zona}" -site:portal1 ...'
      - Excluye hasta 12 portales de empleo mediante operadores -site:
      - Extrae el dominio raíz de cada resultado y descarta portales adicionales
      - Persiste con upsert_empresa(score=0) — sin scoring hasta el scraping

    Args:
        rubros:  Lista de sectores a buscar.
        zona:    Zona geográfica a incluir en la query.
        limite:  Máximo de resultados DDG por rubro.
        estado:  Objeto de estado del dashboard para actualización en tiempo real.

    Returns:
        Total de dominios únicos insertados o actualizados en la DB.

    Raises:
        ImportError: Si duckduckgo-search no está instalado en el entorno.
    """
    logger_fn = logging.getLogger("jobbot.dork")


    dominios_insertados = 0

    for idx, rubro in enumerate(rubros, start=1):
        query = f'site:ar "{zona}" {rubro} (contacto OR rrhh OR empleos)'
        logger_fn.info(
            "Dorking [%d/%d] | rubro=%s", idx, len(rubros), rubro
        )

        if estado:
            estado.fase_actual = f"🔎 Dorking [{idx}/{len(rubros)}]: {rubro}…"

        try:
            resultados: list[dict] = await asyncio.to_thread(
                _ddgs_text_sync, query, limite
            )
        except ImportError:
            logger_fn.error(
                "duckduckgo_search no encontrado. "
                "Instalá con: pip install ddgs"
            )
            raise
        except Exception as exc:
            logger_fn.warning(
                "DDGS error | rubro=%s | error=%s", rubro, str(exc)[:120]
            )
            # Back-off ante posible rate limit de DuckDuckGo
            await asyncio.sleep(2.0)
            continue

        for resultado in resultados:
            url = resultado.get("href", "")
            if not url:
                continue

            dominio = _extraer_dominio_limpio(url)
            if not dominio or _es_portal_excluido(dominio):
                continue

            titulo = (
                (resultado.get("title") or dominio)
                .split(" - ")[0]
                .strip()[:100]
            )

            try:
                await asyncio.to_thread(
                    upsert_empresa,
                    nombre=titulo,
                    dominio=dominio,
                    rubro=rubro,
                    score=0,
                )
                dominios_insertados += 1
                logger_fn.info("Semilla | dominio=%s | rubro=%s", dominio, rubro)

                if estado:
                    estado.upsert_scraping_row(dominio, 0, "–", "Semilla")

            except Exception as exc:
                logger_fn.error(
                    "Fallo al persistir semilla | dominio=%s | error=%s",
                    dominio, str(exc)[:80],
                )

        # Pausa cortés entre queries para no saturar la API de DDG
        await asyncio.sleep(1.5)

    logger_fn.info("Dorking finalizado | semillas_insertadas=%d", dominios_insertados)
    return dominios_insertados


async def pipeline_dork(args: argparse.Namespace, estado: EstadoBot) -> None:
    estado.fase_actual = "🔎 Iniciando DuckDuckGo Dorking…"

    insertados = await recolectar_urls_semilla(
        rubros=args.rubros,
        zona="Mar del Plata",
        limite=args.limite_dork,
        estado=estado,
    )
    estado.fase_actual = f"✅ Dorking completo — {insertados} dominios semilla en DB"


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Scraping
# ─────────────────────────────────────────────────────────────────────────────

async def pipeline_scrape(args: argparse.Namespace, estado: EstadoBot) -> None:
    """
    Lee todos los dominios de la DB y ejecuta el scraper asíncrono con
    concurrencia controlada por Semaphore.
    """
    logger_fn = logging.getLogger("jobbot.main")
    estado.fase_actual = "🔍 Cargando dominios desde DB…"

    empresas = await asyncio.to_thread(
        get_empresas_ordenadas_por_score, 0, 1000
    )
    dominios: list[str] = [str(e["dominio"]) for e in empresas]

    if not dominios:
        estado.fase_actual = "⚠  Sin dominios en DB. Ejecutá --dork primero."
        logger_fn.warning("No hay dominios para scrapear (DB vacía).")
        return

    estado.scraping_total      = len(dominios)
    estado.scraping_procesados = 0
    estado.fase_actual         = f"🔍 Scrapeando {len(dominios)} dominios…"

    resultados = await procesar_lote(
        dominios,
        concurrencia=args.concurrencia,
        forzar_rescraping=getattr(args, "forzar_rescraping", False),
    )

    with estado._lock:
        for dominio, resultado in resultados.items():
            estado.scraping_procesados += 1
            if resultado:
                estado.upsert_scraping_row(
                    dominio,
                    resultado.score_total,
                    resultado.perfil_cv,
                    "OK",
                )
            else:
                estado.upsert_scraping_row(dominio, 0, "–", "Omitido/Error")

    exitosos = sum(1 for r in estado.scraping_rows if r.get("estado") == "OK")
    estado.fase_actual = (
        f"✅ Scraping completo — {exitosos} / {len(dominios)} exitosos"
    )
    logger_fn.info(
        "Lote de scraping finalizado | total=%d | exitosos=%d",
        len(dominios), exitosos,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Mail
# ─────────────────────────────────────────────────────────────────────────────

async def pipeline_mail(args: argparse.Namespace, estado: EstadoBot) -> None:
    """
    Ejecuta el motor de envíos de mailer.py en un thread separado para no
    bloquear el event loop (el mailer tiene time.sleep() de 3-8 min entre
    envíos para respetar el rate limiting SMTP).

    Mientras el thread trabaja, hace polling a la DB cada MAIL_POLL_INTERVAL_S
    segundos para mostrar métricas actualizadas en el dashboard.
    """
    logger_fn = logging.getLogger("jobbot.main")
    dry_run   = getattr(args, "dry_run",   False)
    min_score = getattr(args, "min_score", 55)

    estado.fase_actual = (
        "📧 [DRY-RUN] Campaña email en progreso…"
        if dry_run else
        "📧 Campaña email en progreso…"
    )

    # Lanzar el mailer en un thread para no bloquear el event loop
    mail_task: asyncio.Task = asyncio.create_task(
        asyncio.to_thread(
            procesar_envios_pendientes,
            min_score=min_score,
            limite_empresas=50,
            dry_run=dry_run,
        )
    )

    # Polling activo a la DB para estadísticas en tiempo real
    while not mail_task.done():
        stats = await asyncio.to_thread(_query_mail_stats_db)
        with estado._lock:
            estado.mail_procesadas = stats["total"]
            estado.mail_enviadas   = stats["enviadas"]
            estado.mail_errores    = stats["errores"]
        await asyncio.sleep(MAIL_POLL_INTERVAL_S)

    # Obtener el resultado final (o la excepción si algo falló)
    try:
        metricas: dict[str, int] = await mail_task
    except Exception as exc:
        logger_fn.error("Error crítico en pipeline mail | %s", str(exc)[:150])
        metricas = {"procesadas": 0, "enviadas": 0, "omitidas": 0, "errores": 1}

    # Sincronizar el estado del dashboard con el resultado definitivo
    with estado._lock:
        estado.mail_procesadas = metricas.get("procesadas", 0)
        estado.mail_enviadas   = metricas.get("enviadas",   0)
        estado.mail_errores    = metricas.get("errores",    0)
        estado.mail_omitidas   = metricas.get("omitidas",   0)

    estado.fase_actual = (
        f"✅ Campaña finalizada — "
        f"Enviados: {estado.mail_enviadas} | "
        f"Errores: {estado.mail_errores} | "
        f"Omitidos: {estado.mail_omitidas}"
    )
    logger_fn.info("Campaña email finalizada | métricas=%s", metricas)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Auto (completo)
# ─────────────────────────────────────────────────────────────────────────────

async def pipeline_auto(args: argparse.Namespace, estado: EstadoBot) -> None:
    """Ejecuta el pipeline completo: dork → scrape → mail en secuencia."""
    logger_fn = logging.getLogger("jobbot.main")
    logger_fn.info("=== Pipeline AUTO iniciado ===")

    await pipeline_dork(args, estado)
    await pipeline_scrape(args, estado)
    await pipeline_mail(args, estado)

    estado.fase_actual = "🏁 Pipeline completo finalizado"
    logger_fn.info("=== Pipeline AUTO completado ===")


# ─────────────────────────────────────────────────────────────────────────────
# CLI: definición de argumentos
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python main.py",
        description=(
            "JobBot v1.0 — Herramienta de OSINT, scraping y cold email\n"
            "para búsqueda laboral automatizada en Mar del Plata, Buenos Aires."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos de uso:\n"
            "  python main.py --dork\n"
            "  python main.py --dork --rubros logística inmobiliaria --limite-dork 20\n"
            "  python main.py --scrape --concurrencia 5\n"
            "  python main.py --mail --min-score 60 --dry-run\n"
            "  python main.py --auto\n"
            "\nVariables de entorno requeridas para --mail / --auto:\n"
            "  SMTP_HOST, SMTP_USER, SMTP_PASS\n"
            "  SMTP_PORT (default: 587), SENDER_NAME, GITHUB_USER, LINKEDIN_USER\n"
        ),
    )

    # ── Modos de ejecución (mutuamente excluyentes, uno requerido) ────────
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--dork",
        action="store_true",
        help="Recolectar URLs semilla via DuckDuckGo Dorking",
    )
    mode_group.add_argument(
        "--scrape",
        action="store_true",
        help="Scrapear y puntuar todos los dominios en la DB",
    )
    mode_group.add_argument(
        "--mail",
        action="store_true",
        help="Ejecutar campaña de cold emails",
    )
    mode_group.add_argument(
        "--auto",
        action="store_true",
        help="Pipeline completo: dork → scrape → mail",
    )

    # ── Parámetros del dorking ────────────────────────────────────────────
    parser.add_argument(
        "--rubros",
        nargs="+",
        default=RUBROS_DEFAULT,
        metavar="RUBRO",
        help=(
            f"Sectores a buscar en DDG (default: {len(RUBROS_DEFAULT)} rubros). "
            "Ej: --rubros 'logística' 'estudio contable'"
        ),
    )

    parser.add_argument(
        "--forzar-rescraping",
        action="store_true",
        dest="forzar_rescraping",
        help="Ignorar cooldown de scraping y re-procesar todos los dominios",
    )
    
    parser.add_argument(
        "--limite-dork",
        type=int,
        default=30,
        dest="limite_dork",
        metavar="N",
        help="Máximo de resultados DDG por rubro (default: 30)",
    )

    # ── Parámetros del scraping ───────────────────────────────────────────
    parser.add_argument(
        "--concurrencia",
        type=int,
        default=3,
        metavar="N",
        help="Dominios scrapeados en paralelo (default: 3, recomendado ≤ 5)",
    )

    # ── Parámetros del mailer ─────────────────────────────────────────────
    parser.add_argument(
        "--min-score",
        type=int,
        default=55,
        dest="min_score",
        metavar="N",
        help="Score mínimo de empresa para envío automático (default: 55)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Construir emails sin enviarlos — auditoría de plantillas y adjuntos",
    )

    return parser


# ─────────────────────────────────────────────────────────────────────────────
# Entry point asíncrono
# ─────────────────────────────────────────────────────────────────────────────

async def _async_main(args: argparse.Namespace) -> None:
    """
    Coroutine principal. Inicializa la DB, configura el TUI y delega al
    pipeline correspondiente según el modo de ejecución.
    """
    # Inicializar el esquema SQLite (idempotente)
    await asyncio.to_thread(init_db)

    estado = EstadoBot()

    # Redirigir logging al buffer interno del dashboard ANTES de cualquier log
    _configurar_logging(estado.log_buffer, estado._lock)

    logger_fn = logging.getLogger("jobbot.main")
    modo_activo = next(
        m for m in ("dork", "scrape", "mail", "auto") if getattr(args, m)
    )
    logger_fn.info(
        "JobBot v1.0 iniciado | modo=%s | dry_run=%s",
        modo_activo, getattr(args, "dry_run", False),
    )

    stop_event = asyncio.Event()

    async def _refresh_loop(live: Live) -> None:
        """
        Coroutine que actualiza el dashboard a DASHBOARD_REFRESH_HZ FPS.
        Corre como tarea concurrente al pipeline principal en el mismo
        event loop — ningún hilo adicional de Python involucrado.
        """
        while not stop_event.is_set():
            try:
                live.update(render_dashboard(estado), refresh=True)
            except Exception:
                pass  # Error de render no debe interrumpir el pipeline
            await asyncio.sleep(DASHBOARD_REFRESH_S)

    # refresh_per_second=0: deshabilita el auto-refresh interno de Rich.
    # Nosotros controlamos el ciclo de render desde la coroutine _refresh_loop.
    # screen=True: modo pantalla alternativa (no contamina el scroll histórico).
    # redirect_stderr=False: el logging ya está redirigido al buffer interno.
    with Live(
        render_dashboard(estado),
        auto_refresh=False,
        screen=False,
        redirect_stderr=False,
    ) as live:
        refresh_task = asyncio.create_task(_refresh_loop(live))

        try:
            if args.dork:
                await pipeline_dork(args, estado)
            elif args.scrape:
                await pipeline_scrape(args, estado)
            elif args.mail:
                await pipeline_mail(args, estado)
            elif args.auto:
                await pipeline_auto(args, estado)

        finally:
            # Garantizar que el refresh_task se detiene limpiamente
            # independientemente de cómo se termina el pipeline
            stop_event.set()
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass

            # Render final con el estado definitivo antes de salir de Live
            try:
                live.update(render_dashboard(estado))
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# Entry point sincrónico
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = _build_parser()
    args   = parser.parse_args()

    # Validaciones de argparse que no puede expresar el modelo de grupos
    if args.dry_run and not (args.mail or args.auto):
        parser.error("--dry-run solo tiene efecto con --mail o --auto")

    if args.concurrencia < 1 or args.concurrencia > 10:
        parser.error("--concurrencia debe estar entre 1 y 10")

    # Consola de error fuera del contexto Live (no disponible aún)
    err_console = Console(stderr=True)

    try:
        asyncio.run(_async_main(args))

    except KeyboardInterrupt:
        err_console.print(
            "\n[bold yellow]⚠  Ejecución interrumpida por el usuario "
            "(Ctrl+C). La DB queda consistente.[/bold yellow]"
        )

    except EnvironmentError as exc:
        # ConfigSMTP.from_env() lanza esto cuando faltan vars de entorno SMTP
        err_console.print(
            f"\n[bold red]✗ Error de configuración:[/bold red] {exc}\n"
            "[dim]Revisá: SMTP_HOST, SMTP_USER, SMTP_PASS en tu entorno.[/dim]"
        )
        raise SystemExit(1)

    except ImportError as exc:
        err_console.print(
            f"\n[bold red]✗ Dependencia faltante:[/bold red] {exc}\n"
            "[dim]Instalá con: pip install duckduckgo-search playwright rich[/dim]"
        )
        raise SystemExit(1)

    except Exception as exc:
        err_console.print(
            f"\n[bold red]✗ Error fatal inesperado:[/bold red] {exc}"
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
