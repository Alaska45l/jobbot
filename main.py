"""
main.py — JobBot Orchestrator v2.3
Pipeline completo: OSINT Dorking → Async Scraping → Lead Scoring → Cold Email

Python: 3.11+
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import random
import re
import urllib.parse
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock as ThreadLock
from typing import Optional, TypeAlias   # FIX: TypeAlias en lugar de `type X =` (3.12+)

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
# scraper y playwright se importan lazy dentro de pipeline_scrape/pipeline_mail
# para no bloquear --help ni modos que no los necesitan.

# ─────────────────────────────────────────────────────────────────────────────
# Paleta de colores
# ─────────────────────────────────────────────────────────────────────────────
C_ACCENT    = "cyan"
C_OK        = "green"
C_WARN      = "yellow"
C_ERR       = "red"
C_DIM       = "bright_black"
C_ACTIVE    = "cyan"
C_HEADER    = "bold white"

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────
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
    "jobted.com.ar", "jobted.com", "paginasamarillas.com.ar",
    "infoisinfo-ar.com", "infoisinfo.com.ar", "adecco.com", "inta.gob.ar",
    "conicet.gov.ar", "uba.ar", "unlp.edu.ar",
    "buscojobs.com", "buscojobs.com.ar", "bacap.com.ar",
    "revistacentral.com.ar", "domain.com", "example.com", "abc.xyz",
    "edu.ar", "mdp.edu.ar", "ufasta.edu.ar", "caece.edu.ar", "atlantida.edu.ar",
})

RUBROS_DEFAULT: list[str] = [
    "software house", "soporte técnico pc", "clínica", "centro médico",
    "estudio contable", "QA testing", "servicios informáticos",
    "laboratorio médico", "desarrollo sistemas",
]

MAX_SCRAPING_ROWS: int  = 18
MAX_ACTIVOS_ROWS:  int  = 6
MAX_LOG_LINES:     int  = 14
DASHBOARD_REFRESH_S: float = 0.25
MAIL_POLL_INTERVAL_S: float = 3.0


# ─────────────────────────────────────────────────────────────────────────────
# Logging → buffer interno del TUI
# ─────────────────────────────────────────────────────────────────────────────

class _TUILogHandler(logging.Handler):
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
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in root.handlers[:]:
        root.removeHandler(h)
        h.close()
    handler = _TUILogHandler(buffer, lock)
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s %(levelname).1s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(handler)
    for noisy in ("playwright", "asyncio", "urllib3", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ─────────────────────────────────────────────────────────────────────────────
# Estado compartido — SLIDING WINDOW
# ─────────────────────────────────────────────────────────────────────────────

# FIX: TypeAlias compatible con Python 3.11+
# Antes: `type ScrapingRow = dict` → SyntaxError en 3.11 (sintaxis de 3.12+)
ScrapingRow: TypeAlias = dict

_ESTADOS_ACTIVOS: frozenset[str] = frozenset({"Scrapeando", "Semilla"})


@dataclass
class EstadoBot:
    """
    Estado mutable compartido entre el event loop de asyncio y el hilo
    de refresh de Rich. La sliding window separa activos de terminados.
    """
    fase_actual: str      = "Iniciando…"
    inicio:      datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    scraping_total:      int = 0
    scraping_procesados: int = 0

    scraping_activos:    deque = field(
        default_factory=lambda: deque(maxlen=MAX_ACTIVOS_ROWS)
    )
    scraping_terminados: deque = field(
        default_factory=lambda: deque(maxlen=MAX_SCRAPING_ROWS - MAX_ACTIVOS_ROWS)
    )

    mail_procesadas: int = 0
    mail_enviadas:   int = 0
    mail_errores:    int = 0
    mail_omitidas:   int = 0

    log_buffer: deque = field(default_factory=lambda: deque(maxlen=MAX_LOG_LINES))

    _lock: ThreadLock = field(default_factory=ThreadLock)

    def upsert_scraping_row(self, dominio: str, score: int, perfil_cv: str, estado: str) -> None:
        row: ScrapingRow = {
            "dominio": dominio, "score": score, "perfil_cv": perfil_cv, "estado": estado,
        }
        es_activo = estado in _ESTADOS_ACTIVOS

        with self._lock:
            if es_activo:
                for r in self.scraping_activos:
                    if r["dominio"] == dominio:
                        r.update(row)
                        return
                self.scraping_activos.append(row)
            else:
                activos_filtrados = [r for r in self.scraping_activos if r["dominio"] != dominio]
                self.scraping_activos.clear()
                self.scraping_activos.extend(activos_filtrados)
                for r in self.scraping_terminados:
                    if r["dominio"] == dominio:
                        r.update(row)
                        return
                self.scraping_terminados.appendleft(row)

    def snapshot(self) -> dict:
        """Copia atómica del estado. El render usa esta copia sin adquirir el lock."""
        with self._lock:
            return {
                "fase_actual":         self.fase_actual,
                "elapsed":             self.elapsed(),
                "scraping_total":      self.scraping_total,
                "scraping_procesados": self.scraping_procesados,
                "activos":             list(self.scraping_activos),
                "terminados":          list(self.scraping_terminados),
                "mail_procesadas":     self.mail_procesadas,
                "mail_enviadas":       self.mail_enviadas,
                "mail_errores":        self.mail_errores,
                "mail_omitidas":       self.mail_omitidas,
                "log_lines":           list(self.log_buffer),
            }

    def elapsed(self) -> str:
        delta  = datetime.now(timezone.utc) - self.inicio
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m, s   = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard — renderizado a partir de snapshot (sin locks en render)
# ─────────────────────────────────────────────────────────────────────────────

def _render_header(snap: dict) -> Panel:
    t = Text(justify="center")
    t.append("  ◈  J O B B O T  ", style="bold white")
    t.append("v2.3", style=C_ACCENT)
    t.append("   ·   OSINT · Scraping · Cold Email · MdP  ◈\n", style="white")
    t.append("  Fase: ", style=C_DIM)
    t.append(snap["fase_actual"], style=f"bold {C_OK}")
    t.append(f"    ⏱  {snap['elapsed']}", style=C_DIM)
    return Panel(t, box=box.HEAVY_EDGE, border_style=C_ACCENT, padding=(0, 2))


def _row_style(estado: str, score: int) -> tuple[str, str, str]:
    if estado == "Scrapeando":
        return C_ACTIVE, C_ACTIVE, f"bold {C_ACTIVE}"
    if estado == "Semilla":
        return C_DIM, C_DIM, C_DIM
    if estado == "OK":
        sc = C_OK if score >= 55 else (C_WARN if score >= 20 else C_ERR)
        return sc, C_ACCENT, f"bold {C_OK}"
    if estado == "Error":
        return C_ERR, C_DIM, f"bold {C_ERR}"
    return C_DIM, C_DIM, C_DIM


def _render_scraping_panel(snap: dict) -> Panel:
    tbl = Table(box=box.SIMPLE, header_style=f"bold {C_ACCENT}",
                show_lines=False, expand=True, padding=(0, 1))
    tbl.add_column("Dominio",  no_wrap=True, max_width=32)
    tbl.add_column("Score",    justify="right",  width=6)
    tbl.add_column("Perfil",   justify="center", width=12)
    tbl.add_column("Estado",   justify="center", width=11)

    activos    = snap["activos"]
    terminados = snap["terminados"]

    for row in activos:
        sc_s, pf_s, est_s = _row_style(row["estado"], row["score"])
        tbl.add_row(
            Text(row["dominio"],            style=f"bold {C_ACTIVE}", overflow="ellipsis"),
            Text("–",                       style=C_DIM),
            Text(row.get("perfil_cv", "–"), style=pf_s),
            Text(row["estado"],             style=est_s),
        )

    if activos and terminados:
        tbl.add_row(Text("─" * 32, style=C_DIM), Text(""), Text(""), Text(""))

    for row in terminados:
        sc_s, pf_s, est_s = _row_style(row["estado"], row["score"])
        tbl.add_row(
            Text(row["dominio"],                   overflow="ellipsis"),
            Text(str(row["score"]),                style=sc_s),
            Text(row.get("perfil_cv") or "–",      style=pf_s),
            Text(row["estado"],                    style=est_s),
        )

    total = snap["scraping_total"]
    proc  = snap["scraping_procesados"]
    pct   = f"{proc/total*100:.0f}%" if total > 0 else "–"

    return Panel(
        tbl,
        title=f"[bold {C_ACCENT}]  Scraping Progress  [/bold {C_ACCENT}]",
        subtitle=f"[{C_DIM}]{proc} / {total}  ({pct})  ·  activos: {len(activos)}[/{C_DIM}]",
        box=box.HEAVY_EDGE, border_style=C_ACCENT,
    )


def _render_mail_panel(snap: dict) -> Panel:
    tbl = Table(box=box.SIMPLE, show_header=False, expand=True, padding=(0, 2))
    tbl.add_column("k", style=C_DIM,     ratio=3)
    tbl.add_column("v", justify="right", ratio=1)
    tbl.add_row("Procesadas", Text(str(snap["mail_procesadas"]), style="bold white"))
    tbl.add_row("Enviadas",   Text(str(snap["mail_enviadas"]),   style=f"bold {C_OK}"))
    tbl.add_row("Errores",    Text(str(snap["mail_errores"]),    style=f"bold {C_ERR}"))
    tbl.add_row("Omitidas",   Text(str(snap["mail_omitidas"]),   style=f"bold {C_WARN}"))
    return Panel(tbl, title="[bold white]  Campaña Email  [/bold white]",
                 box=box.HEAVY_EDGE, border_style="magenta")


def _render_log_panel(snap: dict) -> Panel:
    lines = snap["log_lines"]
    t = Text(overflow="fold")
    for line in lines:
        if "ERROR" in line or "CRITICAL" in line:
            t.append(line + "\n", style=C_ERR)
        elif "WARNING" in line:
            t.append(line + "\n", style=C_WARN)
        else:
            t.append(line + "\n", style=C_DIM)
    if not lines:
        t = Text("Sin actividad registrada…", style=f"italic {C_DIM}")
    return Panel(t, title=f"[{C_DIM}]  Log en vivo  [/{C_DIM}]",
                 box=box.HEAVY_EDGE, border_style=C_DIM)


def render_dashboard(estado: EstadoBot) -> Layout:
    """Punto de entrada del render. Toma el snapshot UNA sola vez."""
    snap = estado.snapshot()

    root = Layout()
    root.split_column(Layout(name="header", size=5), Layout(name="body"))
    root["body"].split_row(Layout(name="left", ratio=3), Layout(name="right", ratio=2))
    root["body"]["right"].split_column(
        Layout(name="mail_stats", size=7), Layout(name="log")
    )

    root["header"].update(_render_header(snap))
    root["body"]["left"].update(_render_scraping_panel(snap))
    root["body"]["right"]["mail_stats"].update(_render_mail_panel(snap))
    root["body"]["right"]["log"].update(_render_log_panel(snap))

    return root


# ─────────────────────────────────────────────────────────────────────────────
# Helpers: dominio, DDGS con retry, DB stats
# ─────────────────────────────────────────────────────────────────────────────

def _extraer_dominio_limpio(url: str) -> Optional[str]:
    try:
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        netloc = urllib.parse.urlparse(url).netloc.lower().lstrip("www.")
        if not netloc or "." not in netloc:
            return None
        if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", netloc):
            return None
        return netloc
    except Exception:
        return None


def _es_portal_excluido(dominio: str) -> bool:
    return any(
        dominio == p or dominio.endswith("." + p)
        for p in PORTALES_EXCLUIDOS
    )


def _ddgs_text_sync(query: str, max_results: int) -> list[dict]:
    from ddgs import DDGS
    return list(DDGS().text(query, max_results=max_results))


async def _ddgs_con_retry(
    query: str,
    max_results: int,
    max_intentos: int = 3,
) -> list[dict]:
    """
    Wrapper de _ddgs_text_sync con backoff exponencial para rate limits.

    DuckDuckGo rate-limita activamente bots. Un RatelimitException
    requiere 30–120 segundos de espera mínima, no 2. Sin este retry,
    un solo bloqueo silencia todo el dorking de la sesión.

    Backoff: intento 0 → ~30s, intento 1 → ~60s, intento 2 → falla.

    Args:
        query:        Consulta de búsqueda.
        max_results:  Máximo de resultados a pedir.
        max_intentos: Número de intentos antes de rendirse (default 3).

    Returns:
        Lista de resultados o [] si todos los intentos fallaron.

    Raises:
        ImportError: Si el paquete ddgs no está instalado (no tiene sentido reintentar).
    """
    logger_fn = logging.getLogger("jobbot.dork")

    for intento in range(max_intentos):
        try:
            return await asyncio.to_thread(_ddgs_text_sync, query, max_results)
        except ImportError:
            raise   # módulo no instalado: no reintentar
        except Exception as exc:
            es_ratelimit = (
                "ratelimit" in type(exc).__name__.lower()
                or "202" in str(exc)
                or "429" in str(exc)
            )
            if intento == max_intentos - 1:
                logger_fn.error(
                    "DDGS falló definitivamente | intentos=%d | query='%s' | %s: %s",
                    max_intentos, query[:60], type(exc).__name__, str(exc)[:100],
                )
                return []

            espera = (30 * (2 ** intento)) + random.uniform(0, 10)
            logger_fn.warning(
                "DDGS %s | intento %d/%d | backoff=%.0fs | query='%s'",
                "rate limit" if es_ratelimit else "error",
                intento + 1, max_intentos, espera, query[:40],
            )
            await asyncio.sleep(espera)

    return []


def _query_mail_stats_db() -> dict[str, int]:
    sql = """
        SELECT
            COUNT(*)                                               AS total,
            SUM(CASE WHEN estado='enviado'   THEN 1 ELSE 0 END)   AS enviadas,
            SUM(CASE WHEN estado='rebotado'  THEN 1 ELSE 0 END)   AS errores,
            SUM(CASE WHEN estado='pendiente' THEN 1 ELSE 0 END)   AS pendientes
        FROM campanas_envios
        WHERE fecha_envio >= strftime('%Y-%m-%dT00:00:00Z','now');
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
# Pipeline: Dorking
# ─────────────────────────────────────────────────────────────────────────────

async def recolectar_urls_semilla(
    rubros: list[str],
    zona:   str = "Mar del Plata",
    limite: int = 30,
    estado: Optional[EstadoBot] = None,
) -> int:
    logger_fn = logging.getLogger("jobbot.dork")
    insertados = 0

    for idx, rubro in enumerate(rubros, start=1):
        query = f'site:ar "{zona}" {rubro} (contacto OR rrhh OR empleos)'
        logger_fn.info("Dorking [%d/%d] | rubro=%s", idx, len(rubros), rubro)

        if estado:
            estado.fase_actual = f"Dorking [{idx}/{len(rubros)}]: {rubro}…"

        # FIX: _ddgs_con_retry reemplaza el try/except con sleep de 2s.
        # Un rate limit de DDGS requiere 30–120s; 2s garantizaba fallos en cascada.
        resultados = await _ddgs_con_retry(query, limite)
        if not resultados:
            await asyncio.sleep(random.uniform(3.5, 7.5))
            continue

        for r in resultados:
            url = r.get("href", "")
            if not url:
                continue
            dominio = _extraer_dominio_limpio(url)
            if not dominio or _es_portal_excluido(dominio):
                continue
            titulo = ((r.get("title") or dominio).split(" - ")[0].strip()[:100])
            try:
                await asyncio.to_thread(upsert_empresa, nombre=titulo, dominio=dominio, rubro=rubro, score=0)
                insertados += 1
                logger_fn.info("Semilla | %s | %s", dominio, rubro)
                if estado:
                    estado.upsert_scraping_row(dominio, 0, "–", "Semilla")
            except Exception as exc:
                logger_fn.error("Fallo semilla | %s | %s", dominio, str(exc)[:80])

        pausa = random.uniform(3.5, 7.5)
        logger_fn.debug("Anti-ban: pausa %.1fs antes del próximo rubro", pausa)
        await asyncio.sleep(pausa)

    logger_fn.info("Dorking finalizado | semillas=%d", insertados)
    return insertados


async def pipeline_dork(args: argparse.Namespace, estado: EstadoBot) -> None:
    estado.fase_actual = "Iniciando DuckDuckGo Dorking…"
    logger_fn = logging.getLogger("jobbot.dork")

    rubros_finales = list(args.rubros)
    rubros_file = getattr(args, "rubros_file", None)
    if rubros_file:
        try:
            with open(rubros_file, "r", encoding="utf-8") as f:
                rubros_archivo = [
                    line.strip() for line in f
                    if line.strip() and not line.startswith("#")
                ]
                rubros_finales.extend(rubros_archivo)
                rubros_finales = list(dict.fromkeys(rubros_finales))
            logger_fn.info("Cargados %d rubros desde %s", len(rubros_archivo), rubros_file)
        except Exception as e:
            logger_fn.error("Error leyendo %s: %s", rubros_file, e)

    n = await recolectar_urls_semilla(
        rubros=rubros_finales, zona="Mar del Plata",
        limite=args.limite_dork, estado=estado,
    )
    estado.fase_actual = f"Dorking completo — {n} dominios semilla en DB"


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Scraping
# ─────────────────────────────────────────────────────────────────────────────

async def pipeline_scrape(args: argparse.Namespace, estado: EstadoBot) -> None:
    from scraper import procesar_dominio   # lazy: playwright solo se necesita aquí
    logger_fn = logging.getLogger("jobbot.main")
    estado.fase_actual = "Cargando dominios desde DB…"

    empresas = await asyncio.to_thread(get_empresas_ordenadas_por_score, 0, 1000)
    dominios: list[str] = [str(e["dominio"]) for e in empresas]

    if not dominios:
        estado.fase_actual = "Sin dominios en DB. Ejecutá --dork primero."
        logger_fn.warning("DB vacía, nada para scrapear.")
        return

    with estado._lock:
        estado.scraping_total      = len(dominios)
        estado.scraping_procesados = 0
    estado.fase_actual = f"Scrapeando {len(dominios)} dominios…"

    semaforo = asyncio.Semaphore(args.concurrencia)

    async def _tarea_con_ui(dominio: str) -> None:
        async with semaforo:
            estado.upsert_scraping_row(dominio, 0, "–", "Scrapeando")
            try:
                resultado = await procesar_dominio(
                    dominio,
                    min_score_para_log=0,
                    forzar_rescraping=getattr(args, "forzar_rescraping", False),
                )
                if resultado:
                    estado.upsert_scraping_row(
                        dominio, resultado.score_total, resultado.perfil_cv, "OK"
                    )
                    logger_fn.info(
                        "OK | %s | score=%d | perfil=%s | apto=%s",
                        dominio, resultado.score_total,
                        resultado.perfil_cv, resultado.apto_envio_auto,
                    )
                else:
                    estado.upsert_scraping_row(dominio, 0, "–", "Omitido")
            except Exception as exc:
                logger_fn.error("Error | dominio=%s | %s", dominio, str(exc)[:100])
                estado.upsert_scraping_row(dominio, 0, "–", "Error")
            finally:
                with estado._lock:
                    estado.scraping_procesados += 1

    # FIX: inspeccionar resultados de gather en lugar de descartarlos.
    # Con return_exceptions=True sin inspección, PlaywrightError y
    # sqlite3.OperationalError desaparecían silenciosamente.
    resultados_gather = await asyncio.gather(
        *[asyncio.create_task(_tarea_con_ui(d)) for d in dominios],
        return_exceptions=True,
    )
    for r in resultados_gather:
        if isinstance(r, BaseException):
            logger_fn.error(
                "Tarea de scraping sin capturar | %s: %s",
                type(r).__name__, str(r)[:200],
            )

    with estado._lock:
        exitosos = sum(1 for r in estado.scraping_terminados if r.get("estado") == "OK")
    estado.fase_actual = f"Scraping completo — {exitosos} / {len(dominios)} exitosos"
    logger_fn.info("Lote finalizado | total=%d | exitosos=%d", len(dominios), exitosos)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Mail
# ─────────────────────────────────────────────────────────────────────────────

async def pipeline_mail(args: argparse.Namespace, estado: EstadoBot) -> None:
    logger_fn = logging.getLogger("jobbot.main")
    dry_run   = getattr(args, "dry_run",   False)
    min_score = getattr(args, "min_score", 55)
    estado.fase_actual = "[DRY-RUN] Campaña email…" if dry_run else "Campaña email en progreso…"

    mail_task: asyncio.Task = asyncio.create_task(
        asyncio.to_thread(
            procesar_envios_pendientes,
            min_score=min_score, limite_empresas=50, dry_run=dry_run,
        )
    )

    while not mail_task.done():
        stats = await asyncio.to_thread(_query_mail_stats_db)
        with estado._lock:
            estado.mail_procesadas = stats["total"]
            estado.mail_enviadas   = stats["enviadas"]
            estado.mail_errores    = stats["errores"]
        await asyncio.sleep(MAIL_POLL_INTERVAL_S)

    try:
        metricas: dict[str, int] = await mail_task
    except Exception as exc:
        logger_fn.error("Error crítico pipeline mail | %s", str(exc)[:150])
        metricas = {"procesadas": 0, "enviadas": 0, "omitidas": 0, "errores": 1}

    with estado._lock:
        estado.mail_procesadas = metricas.get("procesadas", 0)
        estado.mail_enviadas   = metricas.get("enviadas",   0)
        estado.mail_errores    = metricas.get("errores",    0)
        estado.mail_omitidas   = metricas.get("omitidas",   0)

    estado.fase_actual = (
        f"Campaña finalizada — "
        f"Enviados: {estado.mail_enviadas} | "
        f"Errores: {estado.mail_errores} | "
        f"Omitidos: {estado.mail_omitidas}"
    )
    logger_fn.info("Campaña email finalizada | %s", metricas)


async def pipeline_wa(args: argparse.Namespace, estado: EstadoBot) -> None:
    from wa_sender import procesar_envios_wa
    estado.fase_actual = "Campaña WhatsApp en progreso…"
    metricas = await procesar_envios_wa(
        limite=getattr(args, "limite", 10),
        dry_run=getattr(args, "dry_run", False),
        headless=getattr(args, "headless", False),
    )
    estado.fase_actual = (
        f"Campaña WA finalizada — "
        f"Enviados: {metricas['enviados']} | Rebotados: {metricas['rebotados']}"
    )


async def pipeline_auto(args: argparse.Namespace, estado: EstadoBot) -> None:
    logging.getLogger("jobbot.main").info("=== Pipeline AUTO iniciado ===")
    await pipeline_dork(args, estado)
    await pipeline_scrape(args, estado)
    await pipeline_mail(args, estado)
    estado.fase_actual = "Pipeline completo finalizado"


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python main.py",
        description="JobBot v2.3 — OSINT, scraping y cold email para MdP.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  python main.py --dork\n"
            "  python main.py --scrape --concurrencia 5 --forzar-rescraping\n"
            "  python main.py --mail --min-score 60 --dry-run\n"
            "  python main.py --auto\n"
        ),
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dork",   action="store_true")
    mode.add_argument("--scrape", action="store_true")
    mode.add_argument("--mail",   action="store_true")
    mode.add_argument("--auto",   action="store_true")
    mode.add_argument("--wa",     action="store_true")

    parser.add_argument("--rubros",       nargs="+", default=RUBROS_DEFAULT, metavar="RUBRO")
    parser.add_argument("--rubros-file",  type=str,  default=None, dest="rubros_file", metavar="FILE")
    parser.add_argument("--limite-dork",  type=int,  default=30, dest="limite_dork")
    parser.add_argument("--concurrencia", type=int,  default=3)
    parser.add_argument("--min-score",    type=int,  default=55, dest="min_score")
    parser.add_argument("--dry-run",      action="store_true", dest="dry_run")
    parser.add_argument("--limite",       type=int,  default=10)
    parser.add_argument("--headless",     action="store_true")
    parser.add_argument("--forzar-rescraping", action="store_true", dest="forzar_rescraping")
    return parser


# ─────────────────────────────────────────────────────────────────────────────
# Entry point asíncrono
# ─────────────────────────────────────────────────────────────────────────────

async def _async_main(args: argparse.Namespace) -> None:
    await asyncio.to_thread(init_db)
    estado = EstadoBot()
    _configurar_logging(estado.log_buffer, estado._lock)

    logger_fn = logging.getLogger("jobbot.main")
    modo = next(m for m in ("dork", "scrape", "mail", "wa", "auto") if getattr(args, m))
    logger_fn.info("JobBot v2.3 | modo=%s | dry_run=%s", modo, getattr(args, "dry_run", False))

    stop_event = asyncio.Event()

    async def _refresh_loop(live: Live) -> None:
        while not stop_event.is_set():
            try:
                live.update(render_dashboard(estado), refresh=True)
            except Exception:
                pass   # FIX: era `pass_async_main` → NameError que mataba el loop
            await asyncio.sleep(DASHBOARD_REFRESH_S)

    with Live(render_dashboard(estado), auto_refresh=False, screen=False, redirect_stderr=False) as live:
        refresh_task = asyncio.create_task(_refresh_loop(live))
        try:
            if   args.dork:   await pipeline_dork(args, estado)
            elif args.scrape: await pipeline_scrape(args, estado)
            elif args.mail:   await pipeline_mail(args, estado)
            elif args.wa:     await pipeline_wa(args, estado)
            elif args.auto:   await pipeline_auto(args, estado)
        finally:
            stop_event.set()
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass
            try:
                live.update(render_dashboard(estado))
            except Exception:
                pass


def main() -> None:
    parser = _build_parser()
    args   = parser.parse_args()

    if args.dry_run and not (args.mail or args.auto):
        parser.error("--dry-run solo tiene efecto con --mail, --wa o --auto")
    if not (1 <= args.concurrencia <= 10):
        parser.error("--concurrencia debe estar entre 1 y 10")

    err = Console(stderr=True)
    try:
        asyncio.run(_async_main(args))
    except KeyboardInterrupt:
        err.print("\n[bold yellow]Interrumpido por el usuario. DB consistente.[/bold yellow]")
    except EnvironmentError as exc:
        err.print(f"\n[bold red]Error de configuración:[/bold red] {exc}")
        raise SystemExit(1)
    except ImportError as exc:
        err.print(f"\n[bold red]Dependencia faltante:[/bold red] {exc}")
        raise SystemExit(1)
    except Exception as exc:
        err.print(f"\n[bold red]Error fatal:[/bold red] {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()