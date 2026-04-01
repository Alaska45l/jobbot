"""
db_manager.py — JobBot Database Engine
Motor de base de datos SQLite para el sistema JobBot.

Autor: JobBot Project
Python: 3.11+
Dependencias: stdlib únicamente (sqlite3, logging, contextlib, typing)
"""

import sqlite3
import logging
import contextlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Configuración de logging estructurado
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("jobbot.db_manager")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).parent / "jobbot.db"
COOLDOWN_DAYS: int = 90

# ---------------------------------------------------------------------------
# DDL — Definición del esquema relacional
# ---------------------------------------------------------------------------
_DDL_STATEMENTS: tuple[str, ...] = (
    # Habilitar claves foráneas (SQLite las ignora por defecto)
    "PRAGMA foreign_keys = ON;",

    """
    CREATE TABLE IF NOT EXISTS empresas (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre         TEXT    NOT NULL,
        dominio        TEXT    NOT NULL UNIQUE,
        rubro          TEXT,
        perfil_cv      TEXT    CHECK(perfil_cv IN ('CV_Tech', 'CV_Admin_IT')),
        score          INTEGER NOT NULL DEFAULT 0,
        fecha_scraping TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    ) STRICT;
    """,

    """
    CREATE TABLE IF NOT EXISTS contactos (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id    INTEGER NOT NULL REFERENCES empresas(id) ON DELETE CASCADE,
        email_o_link  TEXT    NOT NULL UNIQUE,
        tipo          TEXT    NOT NULL CHECK(tipo IN ('RRHH', 'General', 'LinkedIn')),
        prioridad     INTEGER NOT NULL CHECK(prioridad BETWEEN 0 AND 3)
    ) STRICT;
    """,

    """
    CREATE TABLE IF NOT EXISTS campanas_envios (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id   INTEGER NOT NULL REFERENCES empresas(id) ON DELETE CASCADE,
        fecha_envio  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
        cv_enviado   TEXT    NOT NULL,
        asunto_usado TEXT    NOT NULL,
        estado       TEXT    NOT NULL DEFAULT 'pendiente'
                             CHECK(estado IN ('pendiente', 'enviado', 'rebotado', 'respondido'))
    ) STRICT;
    """,

    # Índices para acelerar las búsquedas más frecuentes
    "CREATE INDEX IF NOT EXISTS idx_empresas_score   ON empresas(score DESC);",
    "CREATE INDEX IF NOT EXISTS idx_contactos_emp    ON contactos(empresa_id);",
    "CREATE INDEX IF NOT EXISTS idx_envios_emp_fecha ON campanas_envios(empresa_id, fecha_envio DESC);",
)


# ---------------------------------------------------------------------------
# Context manager: conexión a la base de datos
# ---------------------------------------------------------------------------
@contextlib.contextmanager
def get_connection(db_path: Path = DB_PATH):
    """
    Abre y cierra una conexión SQLite de forma segura.
    - Activa WAL mode para mejor concurrencia lectora.
    - Siempre activa foreign_keys (se resetea por conexión en SQLite).
    - Hace rollback automático ante cualquier excepción.

    Yields:
        sqlite3.Connection con row_factory = sqlite3.Row.
    """
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(
            database=str(db_path),
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            timeout=10,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        yield conn
        conn.commit()
    except sqlite3.Error as exc:
        if conn:
            conn.rollback()
        logger.exception("Error de base de datos: %s", exc)
        raise
    finally:
        if conn:
            conn.close()


# ---------------------------------------------------------------------------
# Inicialización del esquema
# ---------------------------------------------------------------------------
def init_db(db_path: Path = DB_PATH) -> None:
    """
    Crea las tablas e índices si no existen.
    Idempotente: seguro de llamar múltiples veces.

    Args:
        db_path: Ruta al archivo SQLite.
    """
    with get_connection(db_path) as conn:
        for statement in _DDL_STATEMENTS:
            conn.execute(statement)
    logger.info("Base de datos inicializada en: %s", db_path)


# ---------------------------------------------------------------------------
# CRUD — Empresas
# ---------------------------------------------------------------------------
def upsert_empresa(
    nombre: str,
    dominio: str,
    rubro: Optional[str] = None,
    perfil_cv: Optional[str] = None,
    score: int = 0,
) -> int:
    if not dominio or not dominio.strip():
        raise ValueError("El dominio no puede estar vacío.")

    dominio = dominio.strip().lower()

    # Agregamos 'RETURNING id' al final para que SQLite nos de el ID real
    sql = """
        INSERT INTO empresas (nombre, dominio, rubro, perfil_cv, score, fecha_scraping)
        VALUES (:nombre, :dominio, :rubro, :perfil_cv, :score, :fecha)
        ON CONFLICT(dominio) DO UPDATE SET
            nombre         = excluded.nombre,
            rubro          = excluded.rubro,
            perfil_cv      = excluded.perfil_cv,
            score          = excluded.score,
            fecha_scraping = excluded.fecha_scraping
        RETURNING id;
    """
    params = {
        "nombre":    nombre.strip(),
        "dominio":   dominio,
        "rubro":     rubro,
        "perfil_cv": perfil_cv,
        "score":     score,
        "fecha":     datetime.now(tz=timezone.utc).isoformat(),
    }

    with get_connection() as conn:
        cursor = conn.execute(sql, params)
        # Obtenemos el ID del resultado del RETURNING
        row = cursor.fetchone()
        row_id: int = row[0] if row else 0

    logger.info("Empresa upserted | dominio=%s | id=%d | score=%d", dominio, row_id, score)
    return row_id

def get_empresa_by_dominio(dominio: str) -> Optional[sqlite3.Row]:
    """
    Recupera una empresa por su dominio.

    Args:
        dominio: Dominio a buscar.

    Returns:
        sqlite3.Row si existe, None si no se encontró.
    """
    sql = "SELECT * FROM empresas WHERE dominio = ? LIMIT 1;"
    with get_connection() as conn:
        return conn.execute(sql, (dominio.strip().lower(),)).fetchone()


def get_empresas_ordenadas_por_score(min_score: int = 0, limit: int = 200) -> list[sqlite3.Row]:
    """
    Retorna empresas ordenadas de mayor a menor score para priorizar envíos.

    Args:
        min_score: Score mínimo para filtrar leads de baja calidad.
        limit:     Máximo de filas a devolver.

    Returns:
        Lista de sqlite3.Row ordenada por score DESC.
    """
    sql = """
        SELECT * FROM empresas
        WHERE score >= ?
        ORDER BY score DESC
        LIMIT ?;
    """
    with get_connection() as conn:
        return conn.execute(sql, (min_score, limit)).fetchall()


def update_score(empresa_id: int, nuevo_score: int) -> None:
    """
    Actualiza el score de una empresa por ID.

    Args:
        empresa_id:  ID de la empresa.
        nuevo_score: Nuevo valor calculado.
    """
    sql = "UPDATE empresas SET score = ? WHERE id = ?;"
    with get_connection() as conn:
        conn.execute(sql, (nuevo_score, empresa_id))
    logger.debug("Score actualizado | empresa_id=%d | score=%d", empresa_id, nuevo_score)


# ---------------------------------------------------------------------------
# CRUD — Contactos
# ---------------------------------------------------------------------------
def insert_contacto(
    empresa_id: int,
    email_o_link: str,
    tipo: str,
    prioridad: int,
) -> Optional[int]:
    """
    Inserta un contacto vinculado a una empresa.
    Silencia conflictos de unicidad (IGNORE) para ser idempotente.

    Args:
        empresa_id:   ID de la empresa propietaria del contacto.
        email_o_link: Email o URL de LinkedIn.
        tipo:         'RRHH' | 'General' | 'LinkedIn'.
        prioridad:    0 (máxima) a 3 (mínima).

    Returns:
        ID del nuevo registro, o None si ya existía (conflicto ignorado).

    Raises:
        ValueError: Si los parámetros son inválidos.
    """
    if tipo not in ("RRHH", "General", "LinkedIn"):
        raise ValueError(f"Tipo de contacto inválido: '{tipo}'.")
    if prioridad not in range(4):
        raise ValueError(f"Prioridad fuera de rango: {prioridad}.")
    if not email_o_link or not email_o_link.strip():
        raise ValueError("email_o_link no puede estar vacío.")

    sql = """
        INSERT OR IGNORE INTO contactos (empresa_id, email_o_link, tipo, prioridad)
        VALUES (?, ?, ?, ?);
    """
    with get_connection() as conn:
        cursor = conn.execute(sql, (empresa_id, email_o_link.strip(), tipo, prioridad))
        row_id = cursor.lastrowid if cursor.rowcount > 0 else None

    if row_id:
        logger.info(
            "Contacto insertado | empresa_id=%d | tipo=%s | prioridad=%d",
            empresa_id, tipo, prioridad,
        )
    else:
        logger.debug("Contacto duplicado ignorado | email_o_link=%s", email_o_link)

    return row_id


def get_contactos_by_empresa(empresa_id: int) -> list[sqlite3.Row]:
    """
    Retorna todos los contactos de una empresa ordenados por prioridad.

    Args:
        empresa_id: ID de la empresa.

    Returns:
        Lista de sqlite3.Row.
    """
    sql = """
        SELECT * FROM contactos
        WHERE empresa_id = ?
        ORDER BY prioridad ASC;
    """
    with get_connection() as conn:
        return conn.execute(sql, (empresa_id,)).fetchall()


# ---------------------------------------------------------------------------
# CRUD — Campañas / Envíos
# ---------------------------------------------------------------------------
def registrar_envio(
    empresa_id: int,
    cv_enviado: str,
    asunto_usado: str,
    estado: str = "enviado",
) -> int:
    """
    Registra un envío de campaña en el historial.

    Args:
        empresa_id:  ID de la empresa destinataria.
        cv_enviado:  Nombre del archivo de CV usado (ej: 'CV_Tech.pdf').
        asunto_usado: Asunto exacto del correo enviado.
        estado:      Estado inicial del envío.

    Returns:
        ID del registro creado.
    """
    estados_validos = {"pendiente", "enviado", "rebotado", "respondido"}
    if estado not in estados_validos:
        raise ValueError(f"Estado inválido: '{estado}'. Válidos: {estados_validos}")

    sql = """
        INSERT INTO campanas_envios (empresa_id, cv_enviado, asunto_usado, estado)
        VALUES (?, ?, ?, ?);
    """
    with get_connection() as conn:
        cursor = conn.execute(sql, (empresa_id, cv_enviado, asunto_usado, estado))
        row_id: int = cursor.lastrowid  # type: ignore[assignment]

    logger.info(
        "Envío registrado | empresa_id=%d | cv=%s | estado=%s | id=%d",
        empresa_id, cv_enviado, estado, row_id,
    )
    return row_id


def actualizar_estado_envio(envio_id: int, nuevo_estado: str) -> None:
    """
    Actualiza el estado de un envío existente (ej: de 'enviado' a 'respondido').

    Args:
        envio_id:     ID del registro en campanas_envios.
        nuevo_estado: Nuevo estado válido.
    """
    estados_validos = {"pendiente", "enviado", "rebotado", "respondido"}
    if nuevo_estado not in estados_validos:
        raise ValueError(f"Estado inválido: '{nuevo_estado}'.")

    sql = "UPDATE campanas_envios SET estado = ? WHERE id = ?;"
    with get_connection() as conn:
        conn.execute(sql, (nuevo_estado, envio_id))
    logger.info("Estado actualizado | envio_id=%d | nuevo_estado=%s", envio_id, nuevo_estado)


# ---------------------------------------------------------------------------
# Lógica de negocio — Cooldown Anti-Spam
# ---------------------------------------------------------------------------
def esta_en_cooldown(empresa_id: int, cooldown_days: int = COOLDOWN_DAYS) -> bool:
    """
    Verifica si una empresa recibió un envío dentro del período de cooldown.
    Previene spam y posibles blacklistings.

    El chequeo se hace a nivel de base de datos con una sola query,
    sin traer filas al intérprete Python (más eficiente).

    Args:
        empresa_id:    ID de la empresa a verificar.
        cooldown_days: Días mínimos entre envíos. Default: 90.

    Returns:
        True si está en cooldown (NO se debe enviar), False si está libre.
    """
    cutoff: str = (
        datetime.now(tz=timezone.utc) - timedelta(days=cooldown_days)
    ).isoformat()

    sql = """
        SELECT 1 FROM campanas_envios
        WHERE empresa_id = ?
          AND fecha_envio >= ?
          AND estado IN ('enviado', 'pendiente')
        LIMIT 1;
    """
    with get_connection() as conn:
        resultado = conn.execute(sql, (empresa_id, cutoff)).fetchone()

    en_cooldown = resultado is not None
    logger.debug(
        "Cooldown check | empresa_id=%d | en_cooldown=%s | ventana=%d días",
        empresa_id, en_cooldown, cooldown_days,
    )
    return en_cooldown


def get_empresas_listas_para_envio(
    min_score: int = 55,
    cooldown_days: int = COOLDOWN_DAYS,
    limit: int = 100,
) -> list[sqlite3.Row]:
    """
    Retorna empresas con score suficiente que NO están en cooldown.
    Usa una sola query con LEFT JOIN para mayor eficiencia.

    Args:
        min_score:     Score mínimo (inclusive).
        cooldown_days: Período de cooldown en días.
        limit:         Máximo de resultados.

    Returns:
        Lista de sqlite3.Row ordenada por score DESC.
    """
    cutoff: str = (
        datetime.now(tz=timezone.utc) - timedelta(days=cooldown_days)
    ).isoformat()

    sql = """
        SELECT e.*
        FROM empresas e
        LEFT JOIN campanas_envios ce
            ON ce.empresa_id = e.id
            AND ce.fecha_envio >= :cutoff
            AND ce.estado IN ('enviado', 'pendiente')
        WHERE e.score >= :min_score
          AND ce.id IS NULL
        ORDER BY e.score DESC
        LIMIT :limit;
    """
    with get_connection() as conn:
        rows = conn.execute(sql, {
            "cutoff":    cutoff,
            "min_score": min_score,
            "limit":     limit,
        }).fetchall()

    logger.info(
        "Empresas listas para envío: %d (min_score=%d, cooldown=%d días)",
        len(rows), min_score, cooldown_days,
    )
    return rows


# ---------------------------------------------------------------------------
# Entrypoint de prueba rápida
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    init_db()

    emp_id = upsert_empresa(
        nombre="TechMDP SRL",
        dominio="techmdp.com.ar",
        rubro="software",
        perfil_cv="CV_Tech",
        score=90,
    )
    insert_contacto(emp_id, "rrhh@techmdp.com.ar", tipo="RRHH", prioridad=1)
    insert_contacto(emp_id, "linkedin.com/company/techmdp", tipo="LinkedIn", prioridad=2)

    print(f"Empresa ID: {emp_id}")
    print(f"En cooldown: {esta_en_cooldown(emp_id)}")

    listas = get_empresas_listas_para_envio(min_score=50)
    print(f"Empresas listas para envío: {len(listas)}")
