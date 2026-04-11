"""
main.py — JobBot Orchestrator v2.6
Modelo Productor-Consumidor con asyncio.Queue.

Correcciones v2.6 vs v2.5:
  - DEADLOCK FIX: asyncio.Event "consumer_ready" coordina el arranque.
    El Productor no encola nada hasta que el Consumidor confirme que
    Chromium está vivo y leyendo la cola.
  - TIMEOUT en pw.chromium.launch() — el hang silencioso ya no es posible.
  - t.exception() sobre tarea cancelada lanzaba CancelledError → corregido
    con chequeo previo de t.cancelled().
  - "tareas" nunca se reasigna: asyncio.wait() retorna nuevos sets pero
    nosotros siempre mutamos el set original. El bug de callbacks
    referenciando el set viejo queda eliminado.
  - pipeline_dork_scrape atrapa BaseException (no solo CancelledError) y
    garantiza que ambas tareas sean canceladas antes de re-lanzar, evitando
    que el productor quede bloqueado en put() en background.
  - exc_info=True en todos los logs de error crítico.

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
# Tunables
# ─────────────────────────────────────────────────────────────────────────────

# Máximo de contextos Playwright en paralelo.
# Cada contexto Chromium ~300-500 MB. Con 8 GB activos, 2 es seguro.
MAX_PLAYWRIGHT: int = 2

# Back-pressure: cuántas semillas puede acumular la cola antes de bloquear
# al Productor. Ahora que el Consumidor está listo ANTES de que el Productor
# llene la cola, 50 es un valor cómodo.
QUEUE_MAXSIZE: int = 50

# Timeout para pw.chromium.launch(). Si Chromium no levanta en este tiempo
# en tu Arch, hay un problema de entorno (sandbox, libs, display).
BROWSER_LAUNCH_TIMEOUT_S: float = 60.0

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

MAX_SCRAPING_ROWS:    int   = 18
MAX_ACTIVOS_ROWS:     int   = 6
MAX_LOG_LINES:        int   = 14
DASHBOARD_REFRESH_S:  float = 0.25
MAIL_POLL_INTERVAL_S: float = 3.0
_CYCLE_TIMEOUT_S:     float = 86_400.0
_BACKOFF_BASE_S:      float = 60.0
_BACKOFF_CAP_S:       float = 3_600.0
_MAX_CONSECUTIVE_FAILURES: int = 8

# Sentinel de fin de stream para la cola.
_QUEUE_SENTINEL = object()

# Cool-off del Productor ante rate-limits consecutivos de DDGS.
_PRODUCER_429_THRESHOLD: int   = 3
_PRODUCER_COOLOFF_S:     float = 600.0


# ─────────────────────────────────────────────────────────────────────────────
# Logging → TUI buffer
# ─────────────────────────────────────────────────────────────────────────────

class _TUILogHandler(logging.Handler):
    def __init__(self, buffer: deque[str]) -> None:
        super().__init__()
        self._buf      = buffer
        self._buf_lock = ThreadLock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
        except Exception:
            line = record.getMessage()
        with self._buf_lock:
            self._buf.append(line)


def _configurar_logging(buffer: deque[str]) -> None:
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
# Estado compartido (TUI)
# ─────────────────────────────────────────────────────────────────────────────

ScrapingRow: TypeAlias = dict
_ESTADOS_ACTIVOS: frozenset[str] = frozenset({"Scrapeando", "Semilla"})


@dataclass
class EstadoBot:
    fase_actual: str      = "Iniciando…"
    inicio:      datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
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
    emails_ok:   int = 0
    emails_fail: int = 0
    wa_ok:       int = 0
    wa_fail:     int = 0
    target:      str = "—"
    wa_qr_data:  str = ""
    log_buffer: deque = field(
        default_factory=lambda: deque(maxlen=MAX_LOG_LINES)
    )
    _lock: ThreadLock = field(default_factory=ThreadLock)

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
                "emails_ok":           self.emails_ok,
                "emails_fail":         self.emails_fail,
                "wa_ok":               self.wa_ok,
                "wa_fail":             self.wa_fail,
                "target":              self.target,
                "wa_qr_data":          self.wa_qr_data,
                "log_lines":           list(self.log_buffer),
            }

    def elapsed(self) -> str:
        delta  = datetime.now(timezone.utc) - self.inicio
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m, s   = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"


# ─────────────────────────────────────────────────────────────────────────────
# UI Metrics adapter
# ─────────────────────────────────────────────────────────────────────────────

def _build_ui_metrics(snap: dict) -> dict:
    terminados = snap.get("terminados", [])
    scored_ok  = sum(1 for r in terminados if r.get("estado") == "OK")
    return {
        "seeds_found":     snap.get("scraping_total",      0),
        "scraping_total":  snap.get("scraping_total",      0),
        "scraping_done":   snap.get("scraping_procesados", 0),
        "scraping_active": len(snap.get("activos",         [])),
        "scored_ok":       scored_ok,
        "mail_queued":     snap.get("mail_procesadas", 0),
        "mail_sent":       snap.get("emails_ok",       0),
        "mail_bounced":    "—",
        "mail_skipped":    snap.get("mail_omitidas",   0),
        "mail_errors":     snap.get("emails_fail",     0),
        "wa_queued":       "—",
        "wa_sent":         snap.get("wa_ok",           0),
        "wa_bounced":      snap.get("wa_fail",         0),
        "wa_errors":       "—",
        "wa_daily_cap":    30,
        "target":          snap.get("target",          "—"),
        "wa_qr_data":      snap.get("wa_qr_data",      ""),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helpers de dominio y búsqueda
# ─────────────────────────────────────────────────────────────────────────────

def cargar_rubros(ruta_archivo: str = "rubros.txt") -> list[str]:
    logger = logging.getLogger("jobbot.main")
    ruta   = Path(ruta_archivo)
    if not ruta.exists():
        logger.critical("No se encontró %s. Usando rubros por defecto.", ruta_archivo)
        return RUBROS_DEFAULT[:]
    with open(ruta, "r", encoding="utf-8") as f:
        rubros = [
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#")
        ]
    logger.info("Cargados %d rubros desde %s", len(rubros), ruta_archivo)
    return rubros


def _construir_query_dork(rubro: str, zona: str = "") -> str:
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
            raise
        except Exception as exc:
            es_ratelimit = (
                "ratelimit" in type(exc).__name__.lower()
                or "202" in str(exc)
                or "429" in str(exc)
            )
            if intento == max_intentos - 1:
                raise
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
# Helpers para gestión de tareas en vuelo
# ─────────────────────────────────────────────────────────────────────────────

def _recolectar_terminadas(tareas: set[asyncio.Task], logger_fn: logging.Logger) -> None:
    """
    Descarta las tareas completadas del set y loguea sus excepciones.
    Muta `tareas` en lugar de retornar un nuevo set, para que todos los
    callers siempre vean el mismo objeto de referencia.

    IMPORTANTE: nunca llamar t.exception() sin verificar t.cancelled() antes.
    Las tareas canceladas no tienen excepción — invocar .exception() sobre
    ellas lanza CancelledError.
    """
    terminadas = {t for t in tareas if t.done()}
    for t in terminadas:
        tareas.discard(t)
        if t.cancelled():
            continue
        exc = t.exception()
        if exc is not None:
            logger_fn.error(
                "Tarea de scraping fallida: %s: %s",
                type(exc).__name__, exc,
                exc_info=False,   # stack ya fue logueado dentro de la tarea
            )


async def _esperar_una_terminada(
    tareas: set[asyncio.Task],
    logger_fn: logging.Logger,
) -> None:
    """
    Espera a que al menos una tarea del set termine y descarta las completadas.
    No reasigna `tareas` — siempre muta el set original.
    """
    if not tareas:
        return
    # asyncio.wait retorna (done_set, pending_set) — ambos son NUEVOS sets.
    # Nunca hacemos "tareas = pending" porque rompe las referencias externas.
    done_set, _ = await asyncio.wait(tareas, return_when=asyncio.FIRST_COMPLETED)
    for t in done_set:
        tareas.discard(t)
        if t.cancelled():
            continue
        exc = t.exception()
        if exc is not None:
            logger_fn.error(
                "Tarea de scraping fallida: %s: %s",
                type(exc).__name__, exc,
                exc_info=False,
            )


# ─────────────────────────────────────────────────────────────────────────────
# PRODUCTOR — Dorking OSINT con cool-off 429
# ─────────────────────────────────────────────────────────────────────────────

async def _productor_dork(
    cola:          asyncio.Queue,
    rubros:        list[str],
    limite_dork:   int,
    estado:        EstadoBot,
    consumer_ready: asyncio.Event,
) -> None:
    """
    Espera a que el Consumidor señalice que Chromium está levantado y listo
    antes de comenzar a encolar semillas. Esto elimina la condición de carrera
    que causaba el deadlock: el Productor ya no puede llenar la cola antes de
    que el Consumidor esté escuchando.
    """
    logger_fn            = logging.getLogger("jobbot.dork")
    errores_429_seguidos = 0
    insertados_total     = 0

    # ── Punto de sincronización: esperar al Consumidor ────────────────────────
    estado.fase_actual = "Esperando inicialización del browser Chromium…"
    logger_fn.info("Productor: esperando señal 'consumer_ready'…")
    await consumer_ready.wait()

    # Si el Consumidor falló durante el launch, el event igual se setea (para
    # desbloquear a este productor) pero la tarea consumidor ya estará en error.
    # El gather en pipeline_dork_scrape lo detectará y cancelará al productor.
    if consumer_ready.is_set():
        logger_fn.info("Productor: Consumidor listo. Iniciando dorking.")

    for idx, rubro in enumerate(rubros, start=1):
        query = _construir_query_dork(rubro, zona="")
        logger_fn.info(
            "Dorking [%d/%d] | rubro=%s",
            idx, len(rubros), rubro,
        )
        estado.fase_actual = f"Dorking [{idx}/{len(rubros)}]: {rubro}…"
        with estado._lock:
            estado.target = rubro

        try:
            resultados = await _ddgs_con_retry(query, limite_dork)
            errores_429_seguidos = 0
        except asyncio.CancelledError:
            logger_fn.info("Productor cancelado durante dorking.")
            raise
        except Exception as exc:
            errores_429_seguidos += 1
            logger_fn.warning(
                "DDGS fallo definitivo | rubro='%s' | %s: %s | 429_consec=%d",
                rubro, type(exc).__name__, str(exc)[:120], errores_429_seguidos,
            )
            if errores_429_seguidos >= _PRODUCER_429_THRESHOLD:
                logger_fn.warning(
                    "=== COOL-OFF PRODUCTOR: %d errores 429. "
                    "Durmiendo %.0f min. El Scraper sigue activo. ===",
                    errores_429_seguidos, _PRODUCER_COOLOFF_S / 60,
                )
                estado.fase_actual = (
                    f"Productor cool-off {_PRODUCER_COOLOFF_S / 60:.0f} min "
                    f"(429 × {errores_429_seguidos}) — Scraper activo."
                )
                await asyncio.sleep(_PRODUCER_COOLOFF_S)
                errores_429_seguidos = 0
                logger_fn.info("Productor retomando tras cool-off.")
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
                    nombre=titulo, dominio=dominio,
                    rubro=rubro, score=0, es_seed=True,
                )
                insertados_total += 1
                logger_fn.info("Semilla | %s | %s", dominio, rubro)
                estado.upsert_scraping_row(dominio, 0, "–", "Semilla")
                with estado._lock:
                    estado.scraping_total += 1

                await cola.put(dominio)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger_fn.error(
                    "Fallo semilla | %s | %s: %s",
                    dominio, type(exc).__name__, str(exc)[:80],
                )

        await asyncio.sleep(random.uniform(3.5, 7.5))

    logger_fn.info(
        "Productor finalizado | semillas=%d | cola_size=%d",
        insertados_total, cola.qsize(),
    )
    await cola.put(_QUEUE_SENTINEL)


# ─────────────────────────────────────────────────────────────────────────────
# CONSUMIDOR — Scraping con semáforo de RAM
# ─────────────────────────────────────────────────────────────────────────────

async def _consumidor_scrape(
    cola:           asyncio.Queue,
    concurrencia:   int,
    estado:         EstadoBot,
    forzar:         bool = False,
    consumer_ready: Optional[asyncio.Event] = None,
) -> dict:
    """
    Levanta Chromium con timeout explícito, señaliza al Productor cuando está
    listo, y procesa items de la cola con concurrencia limitada por semáforo.

    Gestión de tareas en vuelo:
      - `tareas` es un set que se muta en lugar de reasignarse, para que los
        callbacks y las referencias externas siempre apunten al mismo objeto.
      - La recolección de tareas completadas usa _recolectar_terminadas() y
        _esperar_una_terminada(), que verifican t.cancelled() antes de llamar
        t.exception(), evitando el CancelledError fantasma.
    """
    from scraper import procesar_dominio, CHROMIUM_ARGS
    from playwright.async_api import async_playwright

    logger_fn      = logging.getLogger("jobbot.scraper")
    concur_efectiva = min(concurrencia, MAX_PLAYWRIGHT)
    semaforo       = asyncio.Semaphore(concur_efectiva)
    # Cuántas tareas podemos tener en vuelo antes de aplicar back-pressure.
    # 3× la concurrencia efectiva es un buffer razonable.
    MAX_IN_FLIGHT  = concur_efectiva * 3
    resultados:    dict[str, object] = {}
    # Este set NUNCA se reasigna: siempre se muta con add/discard.
    tareas:        set[asyncio.Task] = set()

    async with async_playwright() as pw:

        # ── Lanzamiento de Chromium con timeout ──────────────────────────────
        estado.fase_actual = "Inicializando Chromium…"
        try:
            browser = await asyncio.wait_for(
                pw.chromium.launch(
                    headless=True,
                    args=CHROMIUM_ARGS,
                    ignore_default_args=["--enable-automation"],
                ),
                timeout=BROWSER_LAUNCH_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            msg = (
                f"pw.chromium.launch() no respondió en {BROWSER_LAUNCH_TIMEOUT_S:.0f}s. "
                "Verificá: chromium instalado, --no-sandbox, sin Xvfb colgado."
            )
            logger_fn.critical(msg)
            if consumer_ready:
                # Desbloquear al Productor aunque sea para fallar limpiamente:
                # el gather verá la excepción y cancelará al Productor.
                consumer_ready.set()
            raise RuntimeError(msg)
        except Exception as exc:
            logger_fn.critical(
                "pw.chromium.launch() falló: %s: %s",
                type(exc).__name__, exc,
                exc_info=True,
            )
            if consumer_ready:
                consumer_ready.set()
            raise

        logger_fn.info(
            "Consumidor: Chromium lanzado | semáforo=%d", concur_efectiva
        )
        # Señalizar al Productor que el browser está operativo.
        if consumer_ready:
            consumer_ready.set()

        # ── Loop principal ────────────────────────────────────────────────────
        try:
            while True:
                # Obtener el siguiente item de la cola con timeout para no
                # bloquear para siempre si el Productor tarda.
                try:
                    dominio = await asyncio.wait_for(cola.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    # Cola vacía momentáneamente (Productor en pausa/cooloff).
                    # Aprovechar para recolectar tareas terminadas.
                    _recolectar_terminadas(tareas, logger_fn)
                    continue

                # ── Fin de stream ─────────────────────────────────────────────
                if dominio is _QUEUE_SENTINEL:
                    logger_fn.info(
                        "Consumidor: EOF recibido. "
                        "Esperando %d tareas en vuelo…",
                        len(tareas),
                    )
                    if tareas:
                        # gather con return_exceptions=True para no perder
                        # los resultados aunque alguna tarea falle.
                        gather_results = await asyncio.gather(
                            *tareas, return_exceptions=True
                        )
                        for gr in gather_results:
                            if isinstance(gr, Exception) and not isinstance(
                                gr, asyncio.CancelledError
                            ):
                                logger_fn.error(
                                    "Tarea final fallida: %s: %s",
                                    type(gr).__name__, gr,
                                )
                    break

                # ── Dispatching ───────────────────────────────────────────────

                # Capturar `dom` en el scope de la función interna para evitar
                # el bug clásico de closures en loops.
                async def _scrape_uno(dom: str) -> None:
                    async with semaforo:
                        await asyncio.sleep(random.uniform(0.5, 2.5))
                        estado.upsert_scraping_row(dom, 0, "–", "Scrapeando")
                        with estado._lock:
                            estado.target = dom
                        try:
                            resultado = await procesar_dominio(
                                dom,
                                min_score_para_log=0,
                                browser=browser,
                                forzar_rescraping=forzar,
                            )
                        except Exception as exc:
                            logger_fn.error(
                                "Error scrapeando %s | %s: %s",
                                dom, type(exc).__name__, exc,
                                exc_info=True,
                            )
                            resultado = None

                        resultados[dom] = resultado
                        if resultado is not None:
                            estado.upsert_scraping_row(
                                dom,
                                resultado.score_total,
                                resultado.perfil_cv,
                                "OK",
                            )
                            logger_fn.info(
                                "OK | %s | score=%d | apto=%s",
                                dom,
                                resultado.score_total,
                                resultado.apto_envio_auto,
                            )
                        else:
                            estado.upsert_scraping_row(dom, 0, "–", "Omitido")

                        with estado._lock:
                            estado.scraping_procesados += 1

                tarea = asyncio.create_task(_scrape_uno(dominio), name=f"scrape-{dominio}")
                tareas.add(tarea)
                # Callback que limpia el set cuando la tarea termina
                # naturalmente (no en cancelación ni en reasignación).
                tarea.add_done_callback(tareas.discard)

                # ── Back-pressure ─────────────────────────────────────────────
                # Si tenemos demasiadas tareas en vuelo, esperar a que alguna
                # termine antes de seguir creando más.
                if len(tareas) >= MAX_IN_FLIGHT:
                    await _esperar_una_terminada(tareas, logger_fn)

        except asyncio.CancelledError:
            logger_fn.info(
                "Consumidor cancelado. Cancelando %d tareas en vuelo…",
                len(tareas),
            )
            for t in list(tareas):
                t.cancel()
            await asyncio.gather(*tareas, return_exceptions=True)
            raise
        finally:
            await browser.close()
            logger_fn.info("Consumidor: browser Chromium cerrado.")

    exitosos = sum(1 for v in resultados.values() if v is not None)
    aptos    = sum(
        1 for v in resultados.values()
        if v is not None and getattr(v, "apto_envio_auto", False)
    )
    logger_fn.info(
        "Consumidor finalizado | total=%d | exitosos=%d | aptos=%d",
        len(resultados), exitosos, aptos,
    )
    return {"total": len(resultados), "exitosos": exitosos, "aptos": aptos}


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Dork+Scrape en paralelo (Productor-Consumidor)
# ─────────────────────────────────────────────────────────────────────────────

async def pipeline_dork_scrape(args: argparse.Namespace, estado: EstadoBot) -> None:
    """
    Lanza el Productor y el Consumidor como tareas concurrentes.

    Protocolo de arranque:
      1. Se crea consumer_ready (Event) compartido.
      2. El Consumidor levanta Chromium y hace consumer_ready.set().
      3. El Productor hace await consumer_ready.wait() antes de encolar nada.
      4. Solo cuando el browser está vivo empieza el flujo de semillas.

    Manejo de fallos:
      Si cualquiera de las dos tareas falla (BaseException), se cancelan
      ambas, se espera su limpieza y se re-lanza la excepción original.
      Esto garantiza que el productor no quede bloqueado en cola.put()
      en background cuando el pipeline_auto atrape la excepción.
    """
    logger_fn     = logging.getLogger("jobbot.main")
    ruta          = getattr(args, "rubros_file", None) or "rubros.txt"
    rubros        = cargar_rubros(ruta)
    cola:         asyncio.Queue[str] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    consumer_ready                   = asyncio.Event()

    estado.fase_actual = "Iniciando Productor+Consumidor en paralelo…"
    logger_fn.info(
        "Pipeline P-C | rubros=%d | concurrencia=%d | "
        "max_playwright=%d | queue_maxsize=%d",
        len(rubros), args.concurrencia, MAX_PLAYWRIGHT, QUEUE_MAXSIZE,
    )

    productor  = asyncio.create_task(
        _productor_dork(cola, rubros, args.limite_dork, estado, consumer_ready),
        name="productor-dork",
    )
    consumidor = asyncio.create_task(
        _consumidor_scrape(
            cola, args.concurrencia, estado,
            forzar=getattr(args, "forzar_rescraping", False),
            consumer_ready=consumer_ready,
        ),
        name="consumidor-scrape",
    )

    try:
        # return_exceptions=False: si cualquiera lanza, gather cancela la
        # otra tarea y re-lanza la excepción aquí.
        resultados = await asyncio.gather(
            productor, consumidor, return_exceptions=False
        )
        metricas_scrape = resultados[1]
        estado.fase_actual = (
            f"Dork+Scrape completo — "
            f"procesados: {metricas_scrape['total']} | "
            f"aptos: {metricas_scrape['aptos']}"
        )
        logger_fn.info("Pipeline P-C finalizado | %s", metricas_scrape)

    except BaseException as exc:
        # Atrapar BaseException (incluye CancelledError, RuntimeError,
        # PlaywrightError, etc.) para garantizar la limpieza completa.
        is_cancel = isinstance(exc, asyncio.CancelledError)
        if not is_cancel:
            logger_fn.error(
                "Pipeline P-C falló: %s: %s",
                type(exc).__name__, exc,
                exc_info=True,
            )
        else:
            logger_fn.info("Pipeline P-C cancelado — limpiando tareas.")

        # Cancelar ambas tareas y esperar su limpieza completa.
        # Esto es crítico: si el Productor estaba bloqueado en cola.put()
        # (cola llena), la cancelación le entregará CancelledError en ese
        # await y saldrá limpiamente.
        pending = [t for t in (productor, consumidor) if not t.done()]
        for t in pending:
            t.cancel()

        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        raise


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Solo Dork (sin scraping inmediato)
# ─────────────────────────────────────────────────────────────────────────────

async def pipeline_dork(args: argparse.Namespace, estado: EstadoBot) -> None:
    """Ejecuta sólo el Productor. Drena la cola con un consumidor nulo."""
    logger_fn = logging.getLogger("jobbot.dork")
    ruta      = getattr(args, "rubros_file", None) or "rubros.txt"
    rubros    = cargar_rubros(ruta)

    estado.fase_actual = "Iniciando DuckDuckGo Dorking…"

    # La cola no necesita maxsize aquí porque el drain es instantáneo.
    cola:           asyncio.Queue[str] = asyncio.Queue()
    consumer_ready                     = asyncio.Event()
    # En modo solo-dork no hay browser: señalizar ready de inmediato.
    consumer_ready.set()

    async def _drain() -> None:
        while True:
            item = await cola.get()
            cola.task_done()
            if item is _QUEUE_SENTINEL:
                break

    await asyncio.gather(
        _productor_dork(cola, rubros, args.limite_dork, estado, consumer_ready),
        _drain(),
    )
    estado.fase_actual = f"Dorking completo — {estado.scraping_total} dominios en DB"
    logger_fn.info("Dorking completo | dominios=%d", estado.scraping_total)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Solo Scrape (toma dominios de la DB)
# ─────────────────────────────────────────────────────────────────────────────

async def pipeline_scrape(args: argparse.Namespace, estado: EstadoBot) -> None:
    """Carga dominios de la DB en una cola y los procesa con el Consumidor."""
    logger_fn = logging.getLogger("jobbot.main")
    estado.fase_actual = "Cargando dominios desde DB…"

    empresas = await asyncio.to_thread(get_empresas_ordenadas_por_score, 0, 5_000)
    dominios: list[str] = [str(e["dominio"]) for e in empresas]

    if not dominios:
        estado.fase_actual = "Sin dominios en DB. Ejecutá --dork primero."
        logger_fn.warning("DB vacía, nada para scrapear.")
        return

    with estado._lock:
        estado.scraping_total      = len(dominios)
        estado.scraping_procesados = 0

    estado.fase_actual = f"Scrapeando {len(dominios)} dominios…"

    cola:           asyncio.Queue[str] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    consumer_ready                     = asyncio.Event()

    async def _productor_db() -> None:
        # Esperar a que el Consumidor esté listo, igual que en dork_scrape.
        await consumer_ready.wait()
        for dom in dominios:
            await cola.put(dom)
        await cola.put(_QUEUE_SENTINEL)

    await asyncio.gather(
        _productor_db(),
        _consumidor_scrape(
            cola, args.concurrencia, estado,
            forzar=getattr(args, "forzar_rescraping", False),
            consumer_ready=consumer_ready,
        ),
    )
    estado.fase_actual = (
        f"Scraping completo — {estado.scraping_procesados} / {len(dominios)}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Mail
# ─────────────────────────────────────────────────────────────────────────────

async def pipeline_mail(args: argparse.Namespace, estado: EstadoBot) -> None:
    logger_fn = logging.getLogger("jobbot.main")
    dry_run   = getattr(args, "dry_run",   False)
    min_score = getattr(args, "min_score", 55)
    estado.fase_actual = (
        "[DRY-RUN] Campaña email…" if dry_run else "Campaña email en progreso…"
    )

    mail_task: asyncio.Task = asyncio.create_task(
        procesar_envios_pendientes(
            min_score=min_score, limite_empresas=50, dry_run=dry_run,
        )
    )

    while not mail_task.done():
        stats = await asyncio.to_thread(_query_mail_stats_db)
        with estado._lock:
            estado.mail_procesadas = stats["total"]
            estado.mail_enviadas   = stats["enviadas"]
            estado.mail_errores    = stats["errores"]
            estado.emails_ok       = stats["enviadas"]
            estado.emails_fail     = stats["errores"]
        await asyncio.sleep(MAIL_POLL_INTERVAL_S)

    try:
        metricas: dict[str, int] = await mail_task
    except Exception as exc:
        logger_fn.error(
            "Error crítico pipeline mail | %s: %s",
            type(exc).__name__, exc,
            exc_info=True,
        )
        metricas = {"procesadas": 0, "enviadas": 0, "omitidas": 0, "errores": 1}

    with estado._lock:
        estado.mail_procesadas = metricas.get("procesadas", 0)
        estado.mail_enviadas   = metricas.get("enviadas",   0)
        estado.mail_errores    = metricas.get("errores",    0)
        estado.mail_omitidas   = metricas.get("omitidas",   0)
        estado.emails_ok       = metricas.get("enviadas",   0)
        estado.emails_fail     = metricas.get("errores",    0)
        estado.target          = "—"

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
    from wa_sender import procesar_envios_wa
    estado.fase_actual = "Campaña WhatsApp en progreso…"
    with estado._lock:
        estado.target = "WA Web — esperando sesión…"

    metricas = await procesar_envios_wa(
        limite=getattr(args, "limite",   10),
        dry_run=getattr(args, "dry_run", False),
        headless=True,
        estado=estado,
    )

    with estado._lock:
        estado.wa_ok   = metricas.get("enviados",  0)
        estado.wa_fail = (
            metricas.get("rebotados", 0) + metricas.get("errores", 0)
        )
        estado.target  = "—"

    estado.fase_actual = (
        f"Campaña WA finalizada — "
        f"Enviados: {metricas['enviados']} | "
        f"Rebotados: {metricas['rebotados']}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline: Auto (daemon loop)
# ─────────────────────────────────────────────────────────────────────────────

async def pipeline_auto(args: argparse.Namespace, estado: EstadoBot) -> None:
    logger_fn = logging.getLogger("jobbot.main")
    logger_fn.info("=== Pipeline AUTO (DAEMON v2.6 P-C) iniciado ===")

    ciclo                = 1
    consecutive_failures = 0

    while True:
        estado.reset_cycle_metrics()
        estado.fase_actual = f"Iniciando Ciclo #{ciclo}…"
        with estado._lock:
            estado.target = f"Ciclo {ciclo}"

        ciclo_ok = False
        try:
            async with asyncio.timeout(_CYCLE_TIMEOUT_S):
                await pipeline_dork_scrape(args, estado)
                await pipeline_mail(args, estado)

            ciclo_ok             = True
            consecutive_failures = 0
            logger_fn.info("Ciclo %d completado.", ciclo)

        except asyncio.CancelledError:
            logger_fn.info(
                "Señal de apagado recibida durante ciclo %d.", ciclo
            )
            raise

        except TimeoutError:
            consecutive_failures += 1
            logger_fn.error(
                "Ciclo %d timeout (%.0f h) | fallo #%d.",
                ciclo, _CYCLE_TIMEOUT_S / 3_600, consecutive_failures,
                exc_info=True,
            )
            estado.fase_actual = (
                f"Ciclo #{ciclo} → TIMEOUT. Backoff #{consecutive_failures}…"
            )

        except Exception as exc:
            consecutive_failures += 1
            logger_fn.error(
                "Error en ciclo %d | %s: %s | fallo #%d",
                ciclo, type(exc).__name__, str(exc)[:200], consecutive_failures,
                exc_info=True,
            )
            estado.fase_actual = (
                f"Error en Ciclo #{ciclo} ({type(exc).__name__}). "
                f"Backoff #{consecutive_failures}…"
            )

        if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
            msg = (
                f"Daemon abortado: {consecutive_failures} fallos consecutivos. "
                "Revisá los logs."
            )
            logger_fn.critical(msg)
            estado.fase_actual = f"⛔ DAEMON ABORTADO — {consecutive_failures} fallos"
            raise RuntimeError(msg)

        if ciclo_ok:
            pausa_s = random.uniform(1_500, 2_700)
            estado.fase_actual = (
                f"Ciclo #{ciclo} completo. "
                f"Durmiendo {pausa_s / 60:.1f} min…"
            )
            logger_fn.info(
                "Ciclo %d completo. Pausa anti-ban %.1f min.",
                ciclo, pausa_s / 60,
            )
            try:
                await asyncio.sleep(pausa_s)
            except asyncio.CancelledError:
                logger_fn.info("Interrumpido durante pausa. Apagando daemon…")
                raise
        else:
            deterministic = _BACKOFF_BASE_S * (2 ** (consecutive_failures - 1))
            jitter        = random.uniform(0, _BACKOFF_BASE_S)
            backoff_s     = min(deterministic + jitter, _BACKOFF_CAP_S)
            logger_fn.warning(
                "Backoff #%d: %.0f s antes de reintentar ciclo %d.",
                consecutive_failures, backoff_s, ciclo + 1,
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
        description=(
            "JobBot v2.6 — OSINT, scraping Productor-Consumidor y cold email."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  python main.py --dork\n"
            "  python main.py --scrape --concurrencia 2\n"
            "  python main.py --dork-scrape --concurrencia 2\n"
            "  python main.py --mail --min-score 60 --dry-run\n"
            "  python main.py --auto\n"
        ),
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dork",        action="store_true",
                      help="Solo dorking (semillas a DB, sin scraping)")
    mode.add_argument("--scrape",      action="store_true",
                      help="Solo scraping (dominios desde DB)")
    mode.add_argument("--dork-scrape", action="store_true", dest="dork_scrape",
                      help="Dork+Scrape en paralelo (Productor-Consumidor)")
    mode.add_argument("--mail",        action="store_true")
    mode.add_argument("--auto",        action="store_true")
    mode.add_argument("--wa",          action="store_true")

    parser.add_argument("--rubros-file",  type=str, default=None,
                        dest="rubros_file", metavar="FILE")
    parser.add_argument("--limite-dork",  type=int, default=30, dest="limite_dork")
    parser.add_argument("--concurrencia", type=int, default=2,
                        help=f"Instancias Playwright (máx {MAX_PLAYWRIGHT} por RAM)")
    parser.add_argument("--min-score",    type=int, default=55, dest="min_score")
    parser.add_argument("--dry-run",      action="store_true",  dest="dry_run")
    parser.add_argument("--limite",       type=int, default=10)
    parser.add_argument("--headless",     action="store_true")
    parser.add_argument("--forzar-rescraping", action="store_true",
                        dest="forzar_rescraping")
    return parser


# ─────────────────────────────────────────────────────────────────────────────
# Async entry point
# ─────────────────────────────────────────────────────────────────────────────

async def _async_main(args: argparse.Namespace) -> None:
    await asyncio.to_thread(init_db)

    estado    = EstadoBot()
    _configurar_logging(estado.log_buffer)

    _main_task = asyncio.current_task()
    logger_fn  = logging.getLogger("jobbot.main")

    def _on_signal(signame: str) -> None:
        logger_fn.warning(
            "%s recibido — cancelando tarea principal.", signame
        )
        if _main_task and not _main_task.done():
            _main_task.cancel()

    loop = asyncio.get_running_loop()
    for sig, name in ((signal.SIGTERM, "SIGTERM"), (signal.SIGINT, "SIGINT")):
        try:
            loop.add_signal_handler(sig, _on_signal, name)
        except NotImplementedError:
            pass

    modo = next(
        m for m in ("dork", "scrape", "dork_scrape", "mail", "wa", "auto")
        if getattr(args, m, False)
    )
    logger_fn.info(
        "JobBot v2.6 | modo=%s | dry_run=%s | max_playwright=%d | "
        "browser_launch_timeout=%.0fs",
        modo, getattr(args, "dry_run", False),
        MAX_PLAYWRIGHT, BROWSER_LAUNCH_TIMEOUT_S,
    )

    tick       = 0
    stop_event = asyncio.Event()

    async def _refresh_loop(live: Live) -> None:
        nonlocal tick
        while not stop_event.is_set():
            snap = estado.snapshot()
            try:
                live.update(
                    generate_dashboard(
                        state      = bot_state_from_phase(snap["fase_actual"]),
                        metrics    = _build_ui_metrics(snap),
                        logs       = snap["log_lines"],
                        elapsed    = snap["elapsed"],
                        phase      = snap["fase_actual"].upper()[:48],
                        tick       = tick,
                        wa_qr_data = snap["wa_qr_data"],
                    ),
                    refresh=True,
                )
            except Exception:
                pass
            tick += 1
            await asyncio.sleep(DASHBOARD_REFRESH_S)

    initial_layout = generate_dashboard(
        state=BotState.IDLE, metrics={}, logs=[],
        elapsed="00:00:00", phase="INICIANDO…", tick=0, wa_qr_data="",
    )

    with Live(
        initial_layout,
        auto_refresh=False,
        screen=False,
        redirect_stderr=False,
    ) as live:
        refresh_task = asyncio.create_task(_refresh_loop(live))

        try:
            if   args.dork:        await pipeline_dork(args, estado)
            elif args.scrape:      await pipeline_scrape(args, estado)
            elif args.dork_scrape: await pipeline_dork_scrape(args, estado)
            elif args.mail:        await pipeline_mail(args, estado)
            elif args.wa:          await pipeline_wa(args, estado)
            elif args.auto:        await pipeline_auto(args, estado)
        finally:
            stop_event.set()
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass

            snap = estado.snapshot()
            try:
                live.update(
                    generate_dashboard(
                        state      = bot_state_from_phase(snap["fase_actual"]),
                        metrics    = _build_ui_metrics(snap),
                        logs       = snap["log_lines"],
                        elapsed    = snap["elapsed"],
                        phase      = snap["fase_actual"].upper()[:48],
                        tick       = tick,
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
        err.print(
            "\n[bold yellow]Interrumpido. DB consistente (WAL). "
            "No quedan procesos Chromium huérfanos.[/bold yellow]"
        )
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