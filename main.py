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
import signal
import urllib.parse
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock as ThreadLock
from typing import Optional, TypeAlias

from dotenv import load_dotenv
load_dotenv()

from rich.console import Console
from rich.live import Live

from jobbot_tui import BotState, bot_state_from_phase, generate_dashboard

from db_manager import (
    get_connection,
    get_empresas_ordenadas_por_score,
    init_db,
    upsert_empresa,
)
from mailer import procesar_envios_pendientes

# ─────────────────────────────────────────────────────────────────────────────
# Constants
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

MAX_SCRAPING_ROWS:    int   = 18
MAX_ACTIVOS_ROWS:     int   = 6
MAX_LOG_LINES:        int   = 14
DASHBOARD_REFRESH_S:  float = 0.25
MAIL_POLL_INTERVAL_S: float = 3.0

# Maximum wall-clock time for a single complete dork→scrape→mail cycle.
_CYCLE_TIMEOUT_S: float = 4 * 3600.0
# Exponential backoff on consecutive cycle failures.
_BACKOFF_BASE_S:  float = 60.0
_BACKOFF_CAP_S:   float = 3600.0   # 1 hour ceiling
# After this many consecutive failures, stop retrying and escalate.
_MAX_CONSECUTIVE_FAILURES: int = 8


# ─────────────────────────────────────────────────────────────────────────────
# Logging → internal TUI buffer
# ─────────────────────────────────────────────────────────────────────────────

class _TUILogHandler(logging.Handler):
    """
    Thread-safe log handler that appends formatted records to a shared deque.

    FIX #6: Uses its OWN dedicated lock (_buf_lock) separate from the
    EstadoBot state lock. The original implementation shared estado._lock
    between emit() and snapshot(), creating a deadlock if any code path
    emitted a log record while holding estado._lock.

    The buffer deque is shared by reference with EstadoBot.log_buffer.
    Reads from snapshot() use EstadoBot._lock; writes here use _buf_lock.
    The two locks are intentionally disjoint to prevent lock-ordering deadlocks.
    """

    def __init__(self, buffer: deque[str]) -> None:
        super().__init__()
        self._buf      = buffer
        self._buf_lock = ThreadLock()   # dedicated lock — never shared

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
        except Exception:
            line = record.getMessage()
        # Only the buffer lock is held here; EstadoBot._lock is never touched.
        with self._buf_lock:
            self._buf.append(line)


def _configurar_logging(buffer: deque[str]) -> None:
    """
    Configures root logger to funnel into the TUI buffer.

    FIX: Handler now owns its own lock. The estado._lock argument
    is removed — callers no longer need to pass it.
    """
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in root.handlers[:]:
        root.removeHandler(h)
        h.close()

    handler = _TUILogHandler(buffer)
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s %(levelname).1s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(handler)
    for noisy in ("playwright", "asyncio", "urllib3", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ─────────────────────────────────────────────────────────────────────────────
# Shared state — sliding window + telemetry counters
# ─────────────────────────────────────────────────────────────────────────────

ScrapingRow: TypeAlias = dict
_ESTADOS_ACTIVOS: frozenset[str] = frozenset({"Scrapeando", "Semilla"})


@dataclass
class EstadoBot:
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

    # ── Telemetry counters exposed to generate_dashboard() ──────────────────
    emails_ok:   int = 0    # confirmed SMTP sends
    emails_fail: int = 0    # bounced / SMTP errors
    wa_ok:       int = 0    # confirmed WhatsApp sends
    wa_fail:     int = 0    # bounced + WA errors combined
    target:      str = "—"  # human-readable label of what is being processed
    wa_qr_data:  str = ""   # native QR code data for terminal rendering

    log_buffer: deque = field(default_factory=lambda: deque(maxlen=MAX_LOG_LINES))

    _lock: ThreadLock = field(default_factory=ThreadLock)

    # ── Sliding window helpers ───────────────────────────────────────────────

    def upsert_scraping_row(
        self, dominio: str, score: int, perfil_cv: str, estado: str
    ) -> None:
        row: ScrapingRow = {
            "dominio": dominio, "score": score,
            "perfil_cv": perfil_cv, "estado": estado,
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
                activos_filtrados = [
                    r for r in self.scraping_activos if r["dominio"] != dominio
                ]
                self.scraping_activos.clear()
                self.scraping_activos.extend(activos_filtrados)
                for r in self.scraping_terminados:
                    if r["dominio"] == dominio:
                        r.update(row)
                        return
                self.scraping_terminados.appendleft(row)

    def reset_cycle_metrics(self) -> None:
        """Resets all per-cycle counters on EstadoBot while preserving daemon-level state."""
        with self._lock:
            self.scraping_total      = 0
            self.scraping_procesados = 0
            self.scraping_activos.clear()
            self.scraping_terminados.clear()
            self.mail_procesadas = 0
            self.mail_enviadas   = 0
            self.mail_errores    = 0
            self.mail_omitidas   = 0
            self.emails_ok       = 0
            self.emails_fail     = 0
            self.wa_ok           = 0
            self.wa_fail         = 0
            self.target          = "—"

    def snapshot(self) -> dict:
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
                # ── new telemetry ──
                "emails_ok":           self.emails_ok,
                "emails_fail":         self.emails_fail,
                "wa_ok":               self.wa_ok,
                "wa_fail":             self.wa_fail,
                "target":              self.target,
                "wa_qr_data":          self.wa_qr_data,
                # ── log tape ──
                "log_lines":           list(self.log_buffer),
            }

    def elapsed(self) -> str:
        delta  = datetime.now(timezone.utc) - self.inicio
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m, s   = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"


# ─────────────────────────────────────────────────────────────────────────────
# UI Metrics adapter
# Translates EstadoBot.snapshot() → the flat dict generate_dashboard() expects.
# ─────────────────────────────────────────────────────────────────────────────

def _build_ui_metrics(snap: dict) -> dict:
    terminados = snap.get("terminados", [])
    scored_ok  = sum(1 for r in terminados if r.get("estado") == "OK")

    return {
        # ── [OSINT] ──────────────────────────────────────────────────────────
        "seeds_found":    snap.get("scraping_total",      0),
        "scraping_total": snap.get("scraping_total",      0),
        "scraping_done":  snap.get("scraping_procesados", 0),
        "scraping_active": len(snap.get("activos",        [])),
        "scored_ok":       scored_ok,
        # ── [EMAIL_ENGINE] ───────────────────────────────────────────────────
        "mail_queued":    snap.get("mail_procesadas", 0),
        "mail_sent":      snap.get("emails_ok",       0),
        "mail_bounced":   "—",
        "mail_skipped":   snap.get("mail_omitidas",   0),
        "mail_errors":    snap.get("emails_fail",     0),
        # ── [WA_ENGINE] ──────────────────────────────────────────────────────
        "wa_queued":      "—",
        "wa_sent":        snap.get("wa_ok",           0),
        "wa_bounced":     snap.get("wa_fail",         0),
        "wa_errors":      "—",
        "wa_daily_cap":   30,
        # ── current target label (used by future TUI panels) ─────────────────
        "target":         snap.get("target",          "—"),
        "wa_qr_data":     snap.get("wa_qr_data",      ""),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helpers: domain normalization, DDGS with retry, DB stats
# ─────────────────────────────────────────────────────────────────────────────

def cargar_rubros(ruta_archivo: str = "rubros.txt") -> list[str]:
    logger = logging.getLogger("jobbot.main")
    ruta = Path(ruta_archivo)
    
    if not ruta.exists():
        logger.critical("No se encontró el archivo %s. Crealo con tu lista de rubros.", ruta_archivo)
        # Fallback de emergencia mínimo para que no crashee en la cara
        return ["software house", "soporte técnico pc"]
        
    with open(ruta, "r", encoding="utf-8") as f:
        rubros = [
            line.strip() 
            for line in f 
            if line.strip() and not line.startswith("#")
        ]
        
    logger.info("Se cargaron %d rubros desde %s", len(rubros), ruta_archivo)
    return rubros

def _construir_query_dork(rubro: str, zona: str = "") -> str:
    """Builds a DuckDuckGo OSINT query string with no double-space artifacts."""
    parts: list[str] = ["site:.ar"]
    if zona.strip():
        parts.append(f'"{zona.strip()}"')
    parts.append(f'"{rubro}"')
    parts.append('(contacto OR rrhh OR empleos OR "trabaja con nosotros" OR cv)')
    return " ".join(parts)

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
    logger_fn = logging.getLogger("jobbot.dork")

    for intento in range(max_intentos):
        try:
            return await asyncio.to_thread(_ddgs_text_sync, query, max_results)
        except ImportError:
            raise   # package not installed — retrying is pointless
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
        query = _construir_query_dork(rubro, zona)
        logger_fn.info(
            "Dorking [%d/%d] | rubro=%s | zona=%s | query='%s'",
            idx, len(rubros), rubro, zona or "nacional", query,
        )

        if estado:
            estado.fase_actual = f"Dorking [{idx}/{len(rubros)}]: {rubro}…"
            with estado._lock:
                # ── metrics: broadcast current OSINT target to the TUI ───────
                estado.target = rubro

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
                await asyncio.to_thread(
                    upsert_empresa,
                    nombre=titulo, dominio=dominio, rubro=rubro, score=0, es_seed=True,
                )
                insertados += 1
                logger_fn.info("Semilla | %s | %s", dominio, rubro)
                if estado:
                    estado.upsert_scraping_row(dominio, 0, "–", "Semilla")
                    with estado._lock:
                        # ── metrics: each new seed increments the total ───────
                        estado.scraping_total += 1
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

    # 1. Cargamos la lista gigante que creamos
    ruta = getattr(args, "rubros_file", None) or "rubros.txt"
    mis_rubros = cargar_rubros(ruta)

    # 2. Se la pasamos a la función de recolección en tu pipeline_dork
    n = await recolectar_urls_semilla(
        rubros=mis_rubros, 
        zona="",  # Nacional
        limite=args.limite_dork, 
        estado=estado,
    )

    estado.fase_actual = f"Dorking completo — {n} dominios semilla en DB"
    with estado._lock:
        estado.target = "—"   # ── metrics: clear target on completion


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Scraping
# ─────────────────────────────────────────────────────────────────────────────

def _make_progress_hook(estado: EstadoBot, logger_fn: logging.Logger):
    """
    Retorna un callable compatible con la firma on_progress de procesar_lote.
    Mantiene el acoplamiento cero entre scraper.py y EstadoBot.
    """
    def _hook(dominio: str, resultado) -> None:
        if resultado is not None:
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

        with estado._lock:
            estado.scraping_procesados += 1
            estado.target = dominio

    return _hook


async def pipeline_scrape(args: argparse.Namespace, estado: EstadoBot) -> None:
    from scraper import procesar_lote   # lazy: playwright solo se necesita aquí
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

    resultados = await procesar_lote(
        dominios=dominios,
        concurrencia=args.concurrencia,
        min_score_para_log=0,
        forzar_rescraping=getattr(args, "forzar_rescraping", False),
        on_progress=_make_progress_hook(estado, logger_fn),
    )

    with estado._lock:
        estado.scraping_procesados = len(resultados)
        exitosos = sum(1 for v in resultados.values() if v is not None)
        estado.target = "—"   # ── metrics: clear target on completion
        
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
        procesar_envios_pendientes(
            min_score=min_score, limite_empresas=50, dry_run=dry_run,
        )
    )

    # Poll the DB every MAIL_POLL_INTERVAL_S for live counter updates
    while not mail_task.done():
        stats = await asyncio.to_thread(_query_mail_stats_db)
        with estado._lock:
            estado.mail_procesadas = stats["total"]
            estado.mail_enviadas   = stats["enviadas"]
            estado.mail_errores    = stats["errores"]
            # ── metrics: sync fine-grained counters for the TE telemetry panel
            estado.emails_ok   = stats["enviadas"]
            estado.emails_fail = stats["errores"]
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
        # ── metrics: overwrite with authoritative final counts from the engine
        estado.emails_ok   = metricas.get("enviadas", 0)
        estado.emails_fail = metricas.get("errores",  0)
        estado.target      = "—"   # ── metrics: clear target on completion

    estado.fase_actual = (
        f"Campaña finalizada — "
        f"Enviados: {estado.mail_enviadas} | "
        f"Errores: {estado.mail_errores} | "
        f"Omitidos: {estado.mail_omitidas}"
    )
    logger_fn.info("Campaña email finalizada | %s", metricas)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: WhatsApp
# ─────────────────────────────────────────────────────────────────────────────

async def pipeline_wa(args: argparse.Namespace, estado: EstadoBot) -> None:
    from wa_sender import procesar_envios_wa   # lazy: playwright only needed here
    estado.fase_actual = "Campaña WhatsApp en progreso…"
    with estado._lock:
        # ── metrics: label the LCD panel while WA Web authenticates ──────────
        estado.target = "WA Web — esperando sesión…"

    metricas = await procesar_envios_wa(
        limite=getattr(args, "limite",   10),
        dry_run=getattr(args, "dry_run", False),
        headless=True,
        estado=estado,
    )

    with estado._lock:
        # ── metrics: persist final WA counters into EstadoBot ─────────────────
        estado.wa_ok   = metricas.get("enviados",  0)
        estado.wa_fail = (
            metricas.get("rebotados", 0) + metricas.get("errores", 0)
        )
        estado.target  = "—"   # ── metrics: clear target on completion

    estado.fase_actual = (
        f"Campaña WA finalizada — "
        f"Enviados: {metricas['enviados']} | "
        f"Rebotados: {metricas['rebotados']}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Auto (sequential full run)
# ─────────────────────────────────────────────────────────────────────────────

async def pipeline_auto(args: argparse.Namespace, estado: EstadoBot) -> None:
    logger_fn = logging.getLogger("jobbot.main")
    logger_fn.info("=== Pipeline AUTO (DAEMON MODE) iniciado ===")

    ciclo                = 1
    consecutive_failures = 0

    while True:
        estado.reset_cycle_metrics()
        estado.fase_actual = f"Iniciando Ciclo #{ciclo}…"
        with estado._lock:
            estado.target = f"Ciclo {ciclo}"

        try:
            async with asyncio.timeout(_CYCLE_TIMEOUT_S):
                await pipeline_dork(args, estado)
                await pipeline_scrape(args, estado)
                await pipeline_mail(args, estado)

            consecutive_failures = 0
            logger_fn.info("Ciclo %d completado exitosamente.", ciclo)

        except asyncio.CancelledError:
            logger_fn.info(
                "Señal de apagado recibida durante ciclo %d. "
                "Deteniendo daemon de forma segura.",
                ciclo,
            )
            raise

        except TimeoutError:
            consecutive_failures += 1
            logger_fn.error(
                "Ciclo %d excedió el timeout de %.0f segundos (fallo #%d).",
                ciclo, _CYCLE_TIMEOUT_S, consecutive_failures,
            )
            estado.fase_actual = (
                f"Ciclo #{ciclo} → TIMEOUT. "
                f"Backoff #{consecutive_failures}…"
            )

        except Exception as exc:
            consecutive_failures += 1
            logger_fn.error(
                "Error crítico en ciclo %d | %s: %s (fallo #%d)",
                ciclo, type(exc).__name__, str(exc)[:200], consecutive_failures,
                exc_info=True,
            )
            estado.fase_actual = (
                f"Error en Ciclo #{ciclo} ({type(exc).__name__}). "
                f"Backoff #{consecutive_failures}…"
            )

        else:
            pausa_s = random.uniform(1_500, 2_700)  # 25–45 minutes
            estado.fase_actual = (
                f"Ciclo #{ciclo} completo. "
                f"Durmiendo {pausa_s / 60:.1f} min…"
            )
            logger_fn.info(
                "Ciclo %d completado. Pausa anti-ban de %.1f minutos.",
                ciclo, pausa_s / 60,
            )
            try:
                await asyncio.sleep(pausa_s)
            except asyncio.CancelledError:
                logger_fn.info("Interrumpido durante el sleep. Apagando daemon…")
                raise
            ciclo += 1
            continue

        if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
            msg = (
                f"Daemon abortado: {consecutive_failures} fallos consecutivos "
                f"sin recuperación. Revisá los logs y reiniciá manualmente."
            )
            logger_fn.critical(msg)
            estado.fase_actual = f"⛔ DAEMON ABORTADO — {consecutive_failures} fallos"
            raise RuntimeError(msg)

        deterministic = _BACKOFF_BASE_S * (2 ** (consecutive_failures - 1))
        jitter        = random.uniform(0, _BACKOFF_BASE_S)
        backoff_s     = min(deterministic + jitter, _BACKOFF_CAP_S)

        logger_fn.warning(
            "Backoff #%d: durmiendo %.0f segundos antes de reintentar ciclo %d.",
            consecutive_failures, backoff_s, ciclo,
        )
        estado.fase_actual = (
            f"Backoff #{consecutive_failures}: "
            f"{backoff_s:.0f}s antes de reintentar…"
        )
        try:
            await asyncio.sleep(backoff_s)
        except asyncio.CancelledError:
            logger_fn.info("Interrumpido durante backoff. Apagando daemon…")
            raise

        ciclo += 1


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

    parser.add_argument("--rubros",            nargs="+", default=RUBROS_DEFAULT, metavar="RUBRO")
    parser.add_argument("--rubros-file",       type=str,  default=None, dest="rubros_file", metavar="FILE")
    parser.add_argument("--limite-dork",       type=int,  default=30,   dest="limite_dork")
    parser.add_argument("--concurrencia",      type=int,  default=3)
    parser.add_argument("--min-score",         type=int,  default=55,   dest="min_score")
    parser.add_argument("--dry-run",           action="store_true",     dest="dry_run")
    parser.add_argument("--limite",            type=int,  default=10)
    parser.add_argument("--headless",          action="store_true")
    parser.add_argument("--forzar-rescraping", action="store_true",     dest="forzar_rescraping")
    return parser


# ─────────────────────────────────────────────────────────────────────────────
# Async entry point
# ─────────────────────────────────────────────────────────────────────────────

async def _async_main(args: argparse.Namespace) -> None:
    await asyncio.to_thread(init_db)

    estado = EstadoBot()
    _configurar_logging(estado.log_buffer)

    _main_task = asyncio.current_task()

    def _on_sigterm() -> None:
        logging.getLogger("jobbot.main").warning(
            "SIGTERM received — cancelling main task for graceful shutdown."
        )
        if _main_task and not _main_task.done():
            _main_task.cancel()

    try:
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, _on_sigterm)
    except NotImplementedError:
        pass

    logger_fn = logging.getLogger("jobbot.main")
    modo = next(m for m in ("dork", "scrape", "mail", "wa", "auto") if getattr(args, m))
    logger_fn.info(
        "JobBot v2.3 | modo=%s | dry_run=%s", modo, getattr(args, "dry_run", False)
    )

    # tick drives the mascot's 2-frame blink animation in jobbot_tui
    tick       = 0
    stop_event = asyncio.Event()

    # ── Refresh coroutine — owns the Live handle, touches nothing else ───────

    async def _refresh_loop(live: Live) -> None:
        nonlocal tick
        while not stop_event.is_set():
            snap = estado.snapshot()
            try:
                live.update(
                    generate_dashboard(
                        state   = bot_state_from_phase(snap["fase_actual"]),
                        metrics = _build_ui_metrics(snap),
                        logs    = snap["log_lines"],
                        elapsed = snap["elapsed"],
                        # Truncate to keep the header strip clean
                        phase   = snap["fase_actual"].upper()[:48],
                        tick    = tick,
                        wa_qr_data = snap["wa_qr_data"],
                    ),
                    refresh=True,
                )
            except Exception:
                pass   # never let a render crash kill the backend
            tick += 1
            await asyncio.sleep(DASHBOARD_REFRESH_S)

    # ── Bootstrap render — shown immediately before the first pipeline tick ──

    initial_layout = generate_dashboard(
        state   = BotState.IDLE,
        metrics = {},
        logs    = [],
        elapsed = "00:00:00",
        phase   = "INICIANDO…",
        tick    = 0,
        wa_qr_data = "",
    )

    with Live(
        initial_layout,
        auto_refresh=False,
        screen=False,
        redirect_stderr=False,
    ) as live:
        refresh_task = asyncio.create_task(_refresh_loop(live))

        try:
            if   args.dork:   await pipeline_dork(args, estado)
            elif args.scrape: await pipeline_scrape(args, estado)
            elif args.mail:   await pipeline_mail(args, estado)
            elif args.wa:     await pipeline_wa(args, estado)
            elif args.auto:   await pipeline_auto(args, estado)
        finally:
            # Stop the refresh loop cleanly before Live closes its context
            stop_event.set()
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass

            # ── Final static render — freeze dashboard at its terminal state ─
            snap = estado.snapshot()
            try:
                live.update(
                    generate_dashboard(
                        state   = bot_state_from_phase(snap["fase_actual"]),
                        metrics = _build_ui_metrics(snap),
                        logs    = snap["log_lines"],
                        elapsed = snap["elapsed"],
                        phase   = snap["fase_actual"].upper()[:48],
                        tick    = tick,
                        wa_qr_data = snap["wa_qr_data"],
                    )
                )
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# Synchronous entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = _build_parser()
    args   = parser.parse_args()

    if args.dry_run and not (args.mail or args.auto or args.wa):
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