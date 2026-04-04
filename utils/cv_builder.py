"""
utils/cv_builder.py — JobBot Dynamic CV Builder
Motor asíncrono para inyección de variables y compilación de CVs con Typst.

Flujo:
  1. Verifica que el binario `typst` esté disponible en PATH.
  2. Lee cvs/template.typ desde disco.
  3. Reemplaza {{ EMPRESA }} y {{ KEYWORDS }} con los valores recibidos.
  4. Escribe la plantilla renderizada en un TemporaryDirectory (autodestruct).
  5. Ejecuta `typst compile input.typ output.pdf` vía asyncio.create_subprocess_exec
     → NO bloquea el event loop. El proceso Typst corre en paralelo.
  6. Lee los bytes del PDF resultante y limpia el directorio temporal.
  7. Retorna bytes o lanza CVCompilationError con mensaje diagnóstico claro.

Python: 3.11+
Dependencias externas: typst CLI en PATH (no es un paquete Python)
Dependencias Python: stdlib únicamente (asyncio, tempfile, shutil, pathlib, logging)
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Final

logger = logging.getLogger("jobbot.cv_builder")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

TEMPLATE_PATH: Final[Path] = Path(__file__).parent.parent / "cvs" / "template.typ"

# Marcadores textuales que deben existir en la plantilla Typst.
# Se reemplazan con str.replace() — simple y determinista.
_MARKER_EMPRESA:  Final[str] = "{{ EMPRESA }}"
_MARKER_KEYWORDS: Final[str] = "{{ KEYWORDS }}"

# Timeout máximo para que typst termine la compilación (segundos).
_COMPILE_TIMEOUT_S: Final[float] = 30.0

# Keywords de fallback si el caller no provee ninguna.
# Cubren el perfil admin/IT más genérico para no dejar la sección vacía.
KEYWORDS_FALLBACK: Final[list[str]] = [
    "Soporte IT",
    "Administración",
    "Redes TCP/IP",
    "Microsoft 365",
    "Gestión documental",
    "Atención al cliente",
    "Python",
    "Linux",
]


# ---------------------------------------------------------------------------
# Excepción propia
# ---------------------------------------------------------------------------

class CVCompilationError(Exception):
    """
    Lanzada cuando la compilación del CV falla por cualquier motivo:
    binario ausente, plantilla inexistente, error de sintaxis Typst, timeout.

    El mensaje siempre incluye suficiente contexto para diagnosticar sin
    tener que revisar los logs — incluye empresa, stderr de typst y código.
    """


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

async def _verificar_typst() -> None:
    """
    Verifica que `typst` esté en PATH antes de intentar compilar.

    Usa shutil.which en un thread (I/O de filesystem) para no bloquear
    el event loop. Falla temprano con un mensaje de instalación claro.

    Raises:
        CVCompilationError: Si el binario no está disponible.
    """
    disponible = await asyncio.to_thread(shutil.which, "typst")
    if disponible is None:
        raise CVCompilationError(
            "El binario 'typst' no está disponible en PATH.\n"
            "Instalalo con:\n"
            "  cargo install typst-cli\n"
            "O descargá el binario desde:\n"
            "  https://github.com/typst/typst/releases\n"
            "Verificá con: typst --version"
        )
    logger.debug("typst encontrado en: %s", disponible)


def _formatear_keywords_typst(keywords: list[str]) -> str:
    """
    Convierte una lista Python al formato de array Typst inline.

    Ejemplo:
        ["Python", "Redes", "Soporte IT"]
        → '"Python", "Redes", "Soporte IT"'

    El resultado se inyecta en la plantilla así:
        #let kw_list = ({{ KEYWORDS }})
        → #let kw_list = ("Python", "Redes", "Soporte IT")

    Las comillas internas de cada keyword se escapan para evitar
    que rompan la sintaxis Typst.

    Args:
        keywords: Lista de strings a formatear.

    Returns:
        String listo para reemplazar {{ KEYWORDS }} en la plantilla.
    """
    escapadas = [kw.replace('"', r'\"') for kw in keywords]
    return ", ".join(f'"{kw}"' for kw in escapadas)


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

async def compilar_cv_dinamico(
    nombre_empresa: str,
    keywords: list[str],
) -> bytes:

    await _verificar_typst()

    # Verificación temprana de la plantilla — falla rápido con mensaje claro
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(
            f"Plantilla Typst no encontrada: {TEMPLATE_PATH}\n"
            "Asegurate de que 'cvs/template.typ' existe en la raíz del proyecto."
        )

    # Selección de keywords efectivas
    kw_efectivas: list[str] = keywords if keywords else KEYWORDS_FALLBACK
    kw_typst: str = _formatear_keywords_typst(kw_efectivas)

    logger.debug(
        "Iniciando compilación CV | empresa='%s' | keywords=%d [%s]",
        nombre_empresa,
        len(kw_efectivas),
        ", ".join(kw_efectivas[:3]) + ("…" if len(kw_efectivas) > 3 else ""),
    )

    # Inyección de variables en la plantilla
    # Usamos str.replace() — determinista, sin regex, sin librerías de templating.
    template_raw: str = await asyncio.to_thread(
        TEMPLATE_PATH.read_text, "utf-8"
    )
    template_renderizado: str = (
        template_raw
        .replace(_MARKER_EMPRESA,  nombre_empresa)
        .replace(_MARKER_KEYWORDS, kw_typst)
    )

    # Validación superficial: advertir si quedó algún marcador sin reemplazar
    for marker in (_MARKER_EMPRESA, _MARKER_KEYWORDS):
        if marker in template_renderizado:
            logger.warning(
                "Marcador '%s' no fue reemplazado en la plantilla | empresa='%s'",
                marker, nombre_empresa,
            )

    # Compilación en directorio temporal (autodestruct al salir del bloque)
    with tempfile.TemporaryDirectory(prefix="jobbot_cv_") as tmp_dir:
        tmp      = Path(tmp_dir)
        typ_in   = tmp / "cv.typ"
        pdf_out  = tmp / "cv.pdf"

        # --- NUEVO: Copiar la imagen de perfil al directorio temporal ---
        img_src = TEMPLATE_PATH.parent / "perfil.jpg"
        if img_src.exists():
            shutil.copy(img_src, tmp / "perfil.jpg")
        # ----------------------------------------------------------------

        # Escribir plantilla renderizada en el temp dir
        await asyncio.to_thread(
            typ_in.write_text, template_renderizado, "utf-8"
        )

        # Lanzar typst compile como subproceso asíncrono.
        # create_subprocess_exec (no shell=True) → sin inyección de comandos.
        proc = await asyncio.create_subprocess_exec(
            "typst", "compile",
            str(typ_in),
            str(pdf_out),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=_COMPILE_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()   # drenar pipes para evitar deadlock
            raise CVCompilationError(
                f"Timeout: typst compile superó {_COMPILE_TIMEOUT_S}s | "
                f"empresa='{nombre_empresa}'"
            )

        # Verificar código de salida
        if proc.returncode != 0:
            stderr_text = stderr.decode("utf-8", errors="replace").strip()
            raise CVCompilationError(
                f"typst compile falló (código {proc.returncode}) | "
                f"empresa='{nombre_empresa}'\n"
                f"stderr: {stderr_text[:600]}"
            )

        # Guardia extra: typst puede terminar con código 0 sin generar el PDF
        # si hay warnings fatales en versiones antiguas
        if not pdf_out.exists():
            raise CVCompilationError(
                f"typst compile terminó sin error pero no generó el PDF | "
                f"empresa='{nombre_empresa}' | esperado: {pdf_out}"
            )

        # Leer bytes dentro del bloque with (antes de que el tempdir se destruya)
        pdf_bytes: bytes = await asyncio.to_thread(pdf_out.read_bytes)

    logger.info(
        "CV compilado exitosamente | empresa='%s' | size=%d bytes | keywords=[%s]",
        nombre_empresa,
        len(pdf_bytes),
        ", ".join(kw_efectivas),
    )
    return pdf_bytes