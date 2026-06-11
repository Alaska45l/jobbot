"""Email templates for ATICMA direct job application in Argentine Spanish."""

from __future__ import annotations

from typing import Final


_PROFILE_ALIASES: Final[dict[str, str]] = {
    "CV_Tech": "CV_IT_QA",
    "CV_Admin_IT": "CV_BackOffice",
    "CV_Hybrid": "CV_Ciencia",
}


ASUNTOS_POR_PERFIL: Final[dict[str, tuple[str, ...]]] = {
    "CV_IT_QA": (
        "Postulación espontánea - QA / Soporte IT / Desarrollo Jr - Mar del Plata",
        "Perfil técnico local - QA, soporte IT y automatización",
        "QA / Soporte IT / Desarrollo Jr - Candidatura espontánea",
        "Perfil híbrido soporte + QA + desarrollo - Mar del Plata",
        "Postulación técnica local para {nombre_empresa}",
        "{nombre_empresa} - Perfil QA / Soporte IT / Desarrollo Jr",
    ),
    "CV_BackOffice": (
        "Postulación espontánea - Back Office técnico / Operaciones digitales",
        "Perfil local - operaciones digitales, documentación y soporte de gestión",
        "Back Office / Soporte administrativo técnico - Mar del Plata",
        "Operaciones digitales y soporte de gestión - Candidatura espontánea",
        "Postulación técnica-operativa para {nombre_empresa}",
        "{nombre_empresa} - Perfil de operaciones digitales y documentación",
    ),
    "CV_Ciencia": (
        "Postulación espontánea - Control de calidad / Laboratorio - Mar del Plata",
        "Perfil local - control de calidad, documentación técnica y laboratorio",
        "Control de calidad / Auxiliar de laboratorio - Candidatura espontánea",
        "Perfil técnico-científico local - documentación y procesos",
        "Postulación para Control de calidad o documentación técnica",
        "{nombre_empresa} - Perfil de control de calidad y documentación",
    ),
}

# Backward-compatible flattened tuple for older callers/tests that import ASUNTOS.
ASUNTOS: tuple[str, ...] = tuple(
    dict.fromkeys(
        asunto
        for asuntos_perfil in ASUNTOS_POR_PERFIL.values()
        for asunto in asuntos_perfil
    )
)


LINEAS_ENCAJE: Final[dict[str, str]] = {
    "CV_IT_QA": (
        "Me postulo de forma espontánea para oportunidades actuales o futuras "
        "en áreas de QA, soporte técnico, operaciones IT, documentación o "
        "desarrollo junior."
    ),
    "CV_BackOffice": (
        "Me postulo de forma espontánea para oportunidades actuales o futuras "
        "en back office técnico, operaciones digitales, documentación, "
        "soporte de gestión o mejora de procesos."
    ),
    "CV_Ciencia": (
        "Me postulo de forma espontánea para oportunidades actuales o futuras "
        "en control de calidad, laboratorio, documentación técnica, análisis "
        "de datos o soporte a procesos técnicos."
    ),
}


DETALLES_PERFIL: Final[dict[str, str]] = {
    "CV_IT_QA": (
        "Cuento con experiencia práctica en troubleshooting, soporte a "
        "usuarios, desarrollo web, revisión funcional y documentación de "
        "incidencias."
    ),
    "CV_BackOffice": (
        "Cuento con experiencia práctica en sistemas POS, carga y control de "
        "datos, facturación, inventario, reportes y soporte interno a procesos."
    ),
    "CV_Ciencia": (
        "Cuento con formación en ciencias y desarrollo, junto con experiencia "
        "operativa en control, registro, soporte de sistemas y revisión de "
        "inconsistencias."
    ),
}


FORTALEZAS_PERFIL: Final[dict[str, str]] = {
    "CV_IT_QA": (
        "Mi fortaleza principal es el análisis: identifico causas de errores, "
        "reproduzco problemas, documento con claridad y propongo mejoras."
    ),
    "CV_BackOffice": (
        "Mi fortaleza principal es ordenar información: detecto inconsistencias, "
        "documento procesos y traduzco problemas operativos en acciones claras."
    ),
    "CV_Ciencia": (
        "Mi fortaleza principal es el registro preciso: observo desvíos, "
        "documento hallazgos y sigo procedimientos con criterio analítico."
    ),
}


CONTEXTOS_FALLBACK: Final[dict[str, str]] = {
    "CV_IT_QA": (
        "Me interesa especialmente sumarme a equipos donde pueda aportar entre "
        "usuarios, soporte, QA, operaciones y desarrollo."
    ),
    "CV_BackOffice": (
        "Me interesa especialmente aportar en equipos que necesiten orden "
        "operativo, documentación clara y mejora de procesos internos."
    ),
    "CV_Ciencia": (
        "Me interesa especialmente aportar en equipos que valoren el control, "
        "la trazabilidad, la documentación y el análisis de procesos."
    ),
}


CUERPOS: tuple[str, ...] = (
    """\
Estimado equipo de {nombre_empresa}:

{linea_encaje}

Soy {nombre_remitente}, vivo en Mar del Plata. {detalle_perfil} \
{fortaleza_perfil}

{contexto_empresa}

Adjunto mi CV y quedo a disposición si consideran que mi perfil puede \
encajar en el equipo.

{firma}""",

    """\
Buenos días,

Soy {nombre_remitente}, de Mar del Plata, y les acerco mi candidatura \
espontánea para {nombre_empresa}.

{linea_encaje}

{detalle_perfil} {fortaleza_perfil}

Adjunto mi CV para que puedan evaluarlo. Quedo atenta a cualquier consulta.

{firma}""",

    """\
Hola equipo de {nombre_empresa},

Me presento: soy {nombre_remitente}. {linea_encaje}

{detalle_perfil}

{fortaleza_perfil} {contexto_empresa}

Les adjunto mi CV en PDF. Muchas gracias por su tiempo.

{firma}""",

    """\
Buenas tardes,

Soy {nombre_remitente}. Me interesa presentar mi perfil para búsquedas \
actuales o futuras en {nombre_empresa}.

{linea_encaje}

{detalle_perfil} Además, me destaco por comunicar problemas técnicos u \
operativos de forma clara y accionable.

Adjunto mi CV. Quedo a disposición para ampliar información.

{firma}""",

    """\
Estimados/as,

Mi nombre es {nombre_remitente} y resido en Mar del Plata.

{linea_encaje}

{fortaleza_perfil} {detalle_perfil}

Si el perfil resulta relevante para {nombre_empresa}, con gusto podemos \
coordinar una entrevista.

{firma}""",

    """\
Hola,

Soy {nombre_remitente}. Les escribo para dejar mi postulación espontánea en \
{nombre_empresa}.

{linea_encaje}

{contexto_empresa} {detalle_perfil}

Adjunto mi CV para evaluación y quedo atenta.

{firma}""",
)


FIRMA_TEMPLATE: str = """\
{nombre_remitente}
Mar del Plata, Buenos Aires
Email: {email_remitente}
Web: {github_user}.github.io/"""


def normalizar_perfil_cv(perfil_cv: str | None) -> str:
    """Normalize legacy profile codes to the current ATICMA CV names."""
    if not perfil_cv:
        return "CV_IT_QA"
    return _PROFILE_ALIASES.get(perfil_cv, perfil_cv)


def asuntos_para_perfil(perfil_cv: str | None) -> tuple[str, ...]:
    """Return subject templates tuned to the selected CV profile."""
    perfil = normalizar_perfil_cv(perfil_cv)
    return ASUNTOS_POR_PERFIL.get(perfil, ASUNTOS_POR_PERFIL["CV_IT_QA"])


def _limpiar_rubro(rubro: str | None) -> str:
    if not rubro:
        return ""
    limpio = " ".join(rubro.split())
    if len(limpio) <= 90:
        return limpio
    return limpio[:87].rsplit(" ", 1)[0].rstrip(".,;:") + "..."


def contexto_empresa_para_perfil(perfil_cv: str | None, rubro: str | None) -> str:
    """Build a cautious fit line from the profile and optional company sector."""
    perfil = normalizar_perfil_cv(perfil_cv)
    rubro_limpio = _limpiar_rubro(rubro)
    if not rubro_limpio:
        return CONTEXTOS_FALLBACK.get(perfil, CONTEXTOS_FALLBACK["CV_IT_QA"])

    if perfil == "CV_BackOffice":
        return (
            "Por el tipo de actividad vinculada a "
            f"{rubro_limpio}, creo que puedo aportar orden operativo, "
            "seguimiento y documentación clara."
        )
    if perfil == "CV_Ciencia":
        return (
            "Por el tipo de actividad vinculada a "
            f"{rubro_limpio}, creo que puedo aportar criterio analítico, "
            "control y documentación técnica."
        )
    return (
        "Por el tipo de actividad vinculada a "
        f"{rubro_limpio}, creo que puedo aportar desde soporte, QA, "
        "documentación y desarrollo junior."
    )


def variables_email_para_perfil(
    perfil_cv: str | None,
    rubro: str | None,
) -> dict[str, str]:
    """Return profile-aware text fragments used by the email templates."""
    perfil = normalizar_perfil_cv(perfil_cv)
    return {
        "linea_encaje": LINEAS_ENCAJE.get(perfil, LINEAS_ENCAJE["CV_IT_QA"]),
        "detalle_perfil": DETALLES_PERFIL.get(perfil, DETALLES_PERFIL["CV_IT_QA"]),
        "fortaleza_perfil": FORTALEZAS_PERFIL.get(
            perfil, FORTALEZAS_PERFIL["CV_IT_QA"]
        ),
        "contexto_empresa": contexto_empresa_para_perfil(perfil, rubro),
    }
