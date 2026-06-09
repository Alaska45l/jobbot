"""
aticma/loader.py — JobBot ATICMA Data Loader
Parseo del archivo empresas_ATICMA.md e importación masiva a la base de datos.

Python: 3.11+
Dependencias: stdlib únicamente (json, re, logging, pathlib)
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from pathlib import Path

from jobbot.db.manager import insert_contacto, upsert_empresa_aticma
from jobbot.utils.domain import extract_domain as _extract_domain_util

logger = logging.getLogger("jobbot.aticma")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# Ruta por defecto al archivo de empresas ATICMA
_DEFAULT_ATICMA_PATH: str = str(
    Path(__file__).resolve().parents[5]
    / "Documents"
    / "Curriculums"
    / "empresas_ATICMA.md"
)

# Regex para localizar el bloque ```json ... ``` dentro del Markdown
_JSON_BLOCK_RE = re.compile(
    r"```json\s*\n(.*?)\n\s*```",
    re.DOTALL,
)


# ---------------------------------------------------------------------------
# Funciones públicas
# ---------------------------------------------------------------------------

def parse_aticma_json(file_path: str = _DEFAULT_ATICMA_PATH) -> list[dict]:
    """
    Abre el archivo Markdown de ATICMA, localiza el bloque ```json```,
    lo parsea y retorna la lista de diccionarios de empresas.

    Args:
        file_path: Ruta al archivo empresas_ATICMA.md.

    Returns:
        Lista de dicts con claves: empresa, descripcion, direccion,
        ubicacion, telefono, email, sitio_web.

    Raises:
        FileNotFoundError: Si el archivo no existe.
        ValueError: Si no se encuentra el bloque JSON o el parseo falla.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Archivo ATICMA no encontrado: {path}")

    contenido = path.read_text(encoding="utf-8")

    match = _JSON_BLOCK_RE.search(contenido)
    if not match:
        raise ValueError(
            f"No se encontró bloque ```json``` en: {path}\n"
            "El archivo debe contener un array JSON dentro de un code fence."
        )

    raw_json = match.group(1)

    try:
        empresas: list[dict] = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Error parseando JSON de ATICMA en {path}: {exc}"
        ) from exc

    if not isinstance(empresas, list):
        raise ValueError(
            f"Se esperaba un array JSON, se obtuvo: {type(empresas).__name__}"
        )

    logger.info(
        "ATICMA JSON parseado | archivo=%s | empresas=%d",
        path.name, len(empresas),
    )
    return empresas


def extract_domain(sitio_web: str | None) -> str | None:
    """
    Extrae un dominio limpio desde una URL.
    Delega al utilitario compartido en jobbot.utils.domain.

    Maneja URLs parciales (sin esquema), con www., trailing slashes, etc.
    Retorna None si la entrada es None, vacía o un email (no URL).

    Args:
        sitio_web: URL cruda del campo sitio_web de ATICMA, o None.

    Returns:
        Dominio limpio (ej: 'empresa.com.ar') o None si no es parseable.
    """
    if not sitio_web or not sitio_web.strip():
        return None

    url = sitio_web.strip()

    # Filtrar valores que son emails, no URLs
    # (ej: "julieta.lillo@hpc.org.ar" en el campo sitio_web)
    if "@" in url and "//" not in url:
        logger.debug("sitio_web parece un email, ignorando: %s", url)
        return None

    # Filtrar URLs de redes sociales (no son dominios propios)
    if "instagram.com" in url.lower() or "facebook.com" in url.lower():
        logger.debug("sitio_web es red social, ignorando: %s", url)
        return None

    return _extract_domain_util(url)


def _synthesize_domain(nombre_empresa: str) -> str:
    """
    Genera un dominio sintético a partir del nombre de la empresa.
    Usado cuando sitio_web es null o inválido.

    Ejemplo: "N y S Consultoria" → "nysconsultoria.local"

    El dominio usa .local para indicar que no es un sitio real.
    Se normalizan acentos y caracteres especiales.
    """
    # Normalizar: quitar acentos, pasar a minúsculas
    nombre = unicodedata.normalize("NFKD", nombre_empresa)
    nombre = nombre.encode("ascii", "ignore").decode("ascii")
    nombre = nombre.lower()

    # Quedarse solo con alfanuméricos
    nombre = re.sub(r"[^a-z0-9]", "", nombre)

    if not nombre:
        nombre = "empresa"

    return f"{nombre}.local"


def import_aticma_to_db(
    file_path: str = _DEFAULT_ATICMA_PATH,
) -> dict[str, int]:
    """
    Importa todas las empresas del archivo ATICMA a la base de datos.

    Para cada empresa:
      1. Extrae el dominio del sitio_web (o genera uno sintético).
      2. Llama a upsert_empresa() con es_seed=False (datos curados).
      3. Inserta el email como contacto tipo='General', prioridad=2.

    Args:
        file_path: Ruta al archivo empresas_ATICMA.md.

    Returns:
        Estadísticas: {"imported": N, "contacts": N, "skipped": N}
    """
    from jobbot.aticma.router import route_company_to_cv

    empresas = parse_aticma_json(file_path)

    stats: dict[str, int] = {"imported": 0, "contacts": 0, "skipped": 0}

    for emp in empresas:
        nombre = emp.get("empresa", "").strip()
        if not nombre:
            logger.warning("Empresa sin nombre, saltando: %s", emp)
            stats["skipped"] += 1
            continue

        descripcion = emp.get("descripcion", "") or ""
        email = emp.get("email", "") or ""
        sitio_web = emp.get("sitio_web")
        rubro = descripcion[:100] if descripcion else None

        # --- Dominio ---
        dominio = extract_domain(sitio_web)
        if not dominio:
            dominio = _synthesize_domain(nombre)
            logger.debug(
                "Dominio sintético generado | empresa='%s' | dominio='%s'",
                nombre, dominio,
            )

        # --- Routing: determinar perfil de CV adecuado ---
        perfil_cv = route_company_to_cv(descripcion, nombre)

        direccion = emp.get("direccion", "") or ""
        ubicacion = emp.get("ubicacion", "") or ""
        telefono = emp.get("telefono", "") or ""

        # --- Upsert empresa (con datos ATICMA extendidos) ---
        try:
            empresa_id = upsert_empresa_aticma(
                nombre=nombre,
                dominio=dominio,
                descripcion=descripcion,
                direccion=direccion,
                ubicacion=ubicacion,
                telefono=telefono,
                perfil_cv=perfil_cv,
                score=55,  # Pre-calificadas: aptas para envío
            )
            stats["imported"] += 1
        except Exception:
            logger.exception("Error importando empresa '%s'", nombre)
            stats["skipped"] += 1
            continue

        # --- Insertar contacto (email) ---
        email = email.strip()
        if email:
            try:
                result = insert_contacto(
                    empresa_id=empresa_id,
                    email_o_link=email,
                    tipo="General",
                    prioridad=2,
                )
                if result is not None:
                    stats["contacts"] += 1
            except Exception:
                logger.exception(
                    "Error insertando contacto para '%s' | email='%s'",
                    nombre, email,
                )

    logger.info(
        "Importación ATICMA completada | imported=%d | contacts=%d | skipped=%d",
        stats["imported"], stats["contacts"], stats["skipped"],
    )
    return stats


# ---------------------------------------------------------------------------
# Ejecución directa (para testing manual)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from jobbot.db.manager import init_db

    init_db()
    resultado = import_aticma_to_db()
    print(f"Resultado: {resultado}")
