"""
scoring.py — JobBot Lead Scoring Engine
Algoritmo de puntuación de prospectos y detección de perfil de CV.

Python: 3.11+
Dependencias: stdlib únicamente (re, logging, typing, dataclasses)
"""
from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import Final

from jobbot.utils.phone import extraer_numeros_whatsapp

logger = logging.getLogger("jobbot.scoring")

# ---------------------------------------------------------------------------
# Constantes y pesos
# ---------------------------------------------------------------------------

CONTACT_WEIGHTS: Final[dict[str, int]] = {
    "email_rrhh":       40,
    "email_general":    15,
    "form_only":       -15,
    "no_ssl":          -10,
}

RUBRO_WEIGHTS: Final[dict[str, dict[str, int | str | list]]] = {
    "tech": {
        "keywords": [
            "software", "desarrollo", "ciberseguridad", "it", "sistemas",
            "saas", "devops", "cloud", "startup", "programación", "api",
            "backend", "frontend", "datos", "inteligencia artificial", "ia",
            "qa", "testing", "redes", "linux",
            # ATICMA-specific
            "integraciones", "seguridad", "iot", "videojuegos",
            "infraestructura", "robótica", "aplicaciones", "automatización",
        ],
        "score_bonus": 10,
        "cv":          "CV_IT_QA",
    },
    "admin_it": {
        "keywords": [
            "inmobiliaria", "estudio", "clínica", "administración",
            "contable", "jurídico", "salud", "comercio", "agencia",
            "consultorio", "laboratorio", "sanatorio", "legal", "escribanía",
            "propiedades", "real estate", "facturación", "secretaría",
            # ATICMA-specific
            "e-commerce", "crm", "logística", "stock", "páginas web",
            "sistemas de gestión", "consultoría", "marketing",
            "comunicación", "diseño", "seo", "publicidad",
        ],
        "score_bonus": 20,
        "cv":          "CV_BackOffice",
    },
    "hybrid": {
        "keywords": [
            "logística", "distribuidora", "parque industrial", "constructor",
            "transporte", "manufactura", "importadora", "exportadora",
            "consultora", "operaciones", "infraestructura", "mesa de ayuda",
            "soporte técnico", "departamento de sistemas", "sistemas internos",
            "automatización", "procesos", "inventario", "stock", "planta",
            "sucursal", "sucursales", "erp", "wms", "crm",
            # ATICMA-specific
            "biotecnología", "green tech", "industria 4.0", "producción",
            "calidad", "datos científicos", "agro", "laboratorio",
            "alimentos", "veterinaria", "medio ambiente",
        ],
        "score_bonus": 18,
        "cv":          "CV_Ciencia",
    },
}

HYBRID_TECH_THRESHOLD: Final[int] = 2
HYBRID_ADMIN_THRESHOLD: Final[int] = 2
HYBRID_DIRECT_THRESHOLD: Final[int] = 2

# Señales negativas que indican que el dominio NO es un prospecto B2B válido.
# Diseño de pesos:
#   - Peso ≤ -100 → exclusión automática sin importar los positivos.
#   - Peso entre -30 y -70 → penalización fuerte pero compensable si hay
#     contactos de RRHH directos (que suman 40-90 puntos).
#   - Señales débiles (-10 a -20) → solo empujan hacia abajo en casos borderline.
#
# RATIONALE de cada decisión:
#   "últimas noticias" / "cobertura" → portal de noticias inequívoco.
#   "escribir un comentario" → patrón canónico de blog (WP, Blogger, Ghost).
#   "agregar al carrito" → e-commerce B2C; más específico que "carrito".
#   "publicado por" + "hace X días" → loop de artículos de blog.
#   "redacción" → newsroom (no empresa).
#   "categorías" como señal aislada es débil (cualquier sitio las tiene);
#       se usa solo en combinación via _contar_ngrams.
#   "wordpress" removido: demasiados falsos positivos (PyMEs de MdP).
#   "portfolio" removido: software houses y agencias TIENEN portfolio y
#       son exactamente el perfil CV_IT_QA que queremos contactar.

NEGATIVE_SIGNALS: Final[dict[str, int]] = {
    "últimas noticias":      -90,
    "redacción":             -80,
    "cobertura periodística":-80,
    "enviado por el editor": -80,
    "sala de prensa":        -70,
    "nota de prensa":        -60,

    "escribir un comentario": -70,
    "publicado por":          -40,
    "deja una respuesta":     -60,
    "suscribirse al blog":    -60,
    "leer el artículo":       -30,

    # E-commerce B2C puro
    "agregar al carrito":     -60,
    "añadir al carrito":      -60,
    "mi carrito de compras":  -50,
    "checkout":               -40,

    # Sitios personales / CV online
    "este es mi portfolio":   -80,
    "currículum vitae personal": -60,
}

# Regex para extraer texto de <meta name="description"> y <title>.
# _strip_html elimina el contenido de los atributos HTML; estas etiquetas
# son las más informativas para detectar portales de noticias.
_RE_META_TAG: Final[re.Pattern[str]] = re.compile(
    r'<meta\b[^>]*/?>',
    re.IGNORECASE | re.DOTALL,
)
_RE_META_NAME_ATTR: Final[re.Pattern[str]] = re.compile(
    r'\bname\s*=\s*["\']description["\']',
    re.IGNORECASE,
)
_RE_META_CONTENT_ATTR: Final[re.Pattern[str]] = re.compile(
    r'\bcontent\s*=\s*(?:"([^"]*?)"|\'([^\']*?)\')',
    re.IGNORECASE | re.DOTALL,
)
_RE_TITLE: Final[re.Pattern[str]] = re.compile(
    r'<title[^>]*>(.*?)</title>',
    re.IGNORECASE | re.DOTALL,
)

_RRHH_PREFIXES: Final[frozenset[str]] = frozenset({
    "rrhh", "cv", "talento", "empleos", "trabajo", "personas",
    "recruiting", "recruitment", "hr", "humanresources", "seleccion",
})

_GENERAL_PREFIXES: Final[frozenset[str]] = frozenset({
    "info", "contacto", "hola", "administracion", "admin",
    "gerencia", "oficina", "ventas", "atencion",
})

_RE_EMAIL: Final[re.Pattern[str]] = re.compile(
    r"""
    \b
    ([A-Za-z0-9._%+\-]+)
    @
    (?!                            
        [^\s@,<>"']+              
        (?:\.png|\.jpg|\.jpeg|\.gif|\.webp|\.svg|\.pdf
          |\.mp4|\.woff|\.woff2|\.min\.js|\.min\.css)
        \b
    )
    ([A-Za-z0-9.\-]+\.[A-Za-z]{2,})
    \b
    """,
    re.VERBOSE | re.IGNORECASE,
)
_RE_WP_FORM: Final[re.Pattern[str]]          = re.compile(r'wpcf7|contact-form-7|wpforms|cf7', re.IGNORECASE)
_RE_HTML_TAGS: Final[re.Pattern[str]]        = re.compile(r'<[^>]+>')

# ---------------------------------------------------------------------------
# Estructuras de datos
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ContactoDetectado:
    valor:     str
    tipo:      str
    prioridad: int
    puntos:    int


@dataclass(slots=True)
class ResultadoScoring:
    """
    Resultado completo del análisis de una página empresa.

    v1.3 — añadido penalty_matches para trazabilidad de exclusiones.
    El score_total refleja el valor real (puede ser negativo fuerte);
    apto_envio_auto se computa sobre ese valor sin flooring, para que
    la exclusión por penalización sea siempre correcta.
    """
    perfil_cv:       str
    score_total:     int
    contactos:       list[ContactoDetectado] = field(default_factory=list)
    rubro_detectado: str                     = "desconocido"
    keyword_matches: dict[str, int]          = field(default_factory=dict)
    penalty_matches: dict[str, int]          = field(default_factory=dict)  # NUEVO
    tiene_form_solo: bool                    = False
    umbral_auto:     int                     = 55
    apto_envio_auto: bool                    = field(init=False)

    def __post_init__(self) -> None:
        self.apto_envio_auto = self.score_total >= self.umbral_auto

    @property
    def score_display(self) -> int:
        """Score flooreado en -20 para mostrar en la TUI sin valores extremos."""
        return max(-20, self.score_total)


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _strip_html(html: str) -> str:
    return _RE_HTML_TAGS.sub(" ", html)


def _clasificar_email(prefix: str) -> tuple[str, int, int]:
    if prefix in _RRHH_PREFIXES:
        return "RRHH", 1, CONTACT_WEIGHTS["email_rrhh"]
    if prefix in _GENERAL_PREFIXES:
        return "General", 2, CONTACT_WEIGHTS["email_general"]
    return "General", 3, CONTACT_WEIGHTS["email_general"] // 2


def _contar_keywords(texto: str, keywords: list[str]) -> int:
    texto_lower = texto.lower()
    return sum(
        1 for kw in keywords
        if re.search(r'\b' + re.escape(kw) + r'\b', texto_lower)
    )


def _seleccionar_perfil(keyword_matches: dict[str, int]) -> str:
    tech_matches = keyword_matches.get("tech", 0)
    admin_matches = keyword_matches.get("admin_it", 0)
    hybrid_matches = keyword_matches.get("hybrid", 0)

    if (
        tech_matches >= HYBRID_TECH_THRESHOLD
        and admin_matches >= HYBRID_ADMIN_THRESHOLD
    ):
        return "hybrid"

    if hybrid_matches >= HYBRID_DIRECT_THRESHOLD and (tech_matches or admin_matches):
        return "hybrid"

    if hybrid_matches > tech_matches and hybrid_matches > admin_matches:
        return "hybrid"

    if all(v == 0 for v in keyword_matches.values()):
        return "admin_it"

    return max(keyword_matches, key=lambda k: keyword_matches[k])


def _extraer_meta_description(html: str) -> str:
    for tag_match in _RE_META_TAG.finditer(html):
        tag_src = tag_match.group(0)
        if not _RE_META_NAME_ATTR.search(tag_src):
            continue
        content_m = _RE_META_CONTENT_ATTR.search(tag_src)
        if content_m:
            return content_m.group(1) or content_m.group(2) or ""
    return ""


def _extraer_texto_semantico(html: str) -> str:
    partes: list[str] = []
    m_title = _RE_TITLE.search(html)
    if m_title:
        partes.append(m_title.group(1))
    desc = _extraer_meta_description(html)
    if desc:
        partes.append(desc)
    return " ".join(partes).lower()


def _evaluar_penalizaciones(texto_plano: str, texto_semantico: str) -> tuple[int, dict[str, int]]:
    """
    Aplica NEGATIVE_SIGNALS sobre el contenido del sitio.

    Busca en la unión de texto_plano (body visible) + texto_semantico
    (title + meta description) para maximizar la cobertura sin duplicar
    el texto de análisis para las señales positivas.

    Returns:
        (penalizacion_total, dict con los términos encontrados y sus pesos)
    """
    texto_combinado = (texto_plano + " " + texto_semantico).lower()
    encontrados: dict[str, int] = {}
    total = 0

    for termino, peso in NEGATIVE_SIGNALS.items():
        if termino in texto_combinado:
            encontrados[termino] = peso
            total += peso
            logger.debug("Penalización | '%s' | %+d pts", termino, peso)

    if encontrados:
        logger.info(
            "Penalizaciones aplicadas | términos=%d | total=%+d pts | %s",
            len(encontrados), total,
            ", ".join(f"{t}({p})" for t, p in list(encontrados.items())[:3]),
        )
    return total, encontrados


# ---------------------------------------------------------------------------
# Motor de scoring
# ---------------------------------------------------------------------------

def analizar_empresa(
    html: str,
    dominio: str = "",
    tiene_ssl: bool = True,
    umbral_auto: int = 55,
) -> ResultadoScoring:
    """
    Analiza el HTML de una empresa y produce un ResultadoScoring.

    v1.3 — Fase 0: penalizaciones léxicas (NEGATIVE_SIGNALS) aplicadas
    antes que las señales positivas. Un sitio de noticias queda excluido
    aunque tenga un email de RRHH en el pie de página.
    """
    if not html or not html.strip():
        logger.warning("HTML vacío | dominio=%s", dominio)
        return ResultadoScoring(perfil_cv="CV_BackOffice", score_total=0, umbral_auto=umbral_auto)

    texto_plano    = _strip_html(html)
    texto_semantico = _extraer_texto_semantico(html)   # NUEVO: title + meta desc
    contactos:      list[ContactoDetectado] = []
    emails_vistos:  set[str] = set()

    # ── FASE 0: Penalizaciones (NUEVO) ──────────────────────────────────────
    penalizacion, penalty_matches = _evaluar_penalizaciones(texto_plano, texto_semantico)
    score: int = penalizacion

    # Cortocircuito: si las penalizaciones ya garantizan exclusión y no hay
    # contactos directos posibles, saltamos el análisis de contactos para
    # ahorrar CPU. El umbral de corte contempla email RRHH + WhatsApp.
    CORTE_TEMPRANO = -(umbral_auto + 75)
    if score <= CORTE_TEMPRANO:
        logger.info(
            "Exclusión temprana | dominio=%s | penalización=%+d (≤%d)",
            dominio, score, CORTE_TEMPRANO,
        )
        return ResultadoScoring(
            perfil_cv="CV_BackOffice",
            score_total=score,
            penalty_matches=penalty_matches,
            umbral_auto=umbral_auto,
        )

    # 1. Emails
    for match in _RE_EMAIL.finditer(html):
        prefix         = match.group(1).lower()
        email_completo = match.group(0).lower()

        if email_completo in emails_vistos:
            continue
        emails_vistos.add(email_completo)

        if any(ext in email_completo for ext in (
            ".png", ".jpg", ".jpeg", ".gif", ".webp",
            ".svg", ".pdf", ".mp4", ".woff", ".min",
        )):
            continue

        tipo, prioridad, puntos = _clasificar_email(prefix)
        contactos.append(ContactoDetectado(
            valor=email_completo, tipo=tipo, prioridad=prioridad, puntos=puntos,
        ))
        score += puntos
        logger.debug("Email | %s | tipo=%s | +%d pts", email_completo, tipo, puntos)

    # 2. WhatsApp
    numeros_wa = extraer_numeros_whatsapp(html)
    if numeros_wa:
        score += 35
        logger.debug("WhatsApp | %d números | +35 pts flat", len(numeros_wa))
    for numero in numeros_wa[:3]:
        contactos.append(ContactoDetectado(
            valor=numero, tipo="WhatsApp", prioridad=1, puntos=0,
        ))

    # 3. Penalizaciones
    tiene_form_solo = False
    if _RE_WP_FORM.search(html) and not emails_vistos:
        score += CONTACT_WEIGHTS["form_only"]
        tiene_form_solo = True
        logger.debug("Penalización: solo formulario | %d pts", CONTACT_WEIGHTS["form_only"])
    if not tiene_ssl:
        score += CONTACT_WEIGHTS["no_ssl"]
        logger.debug("Penalización: sin SSL | %d pts", CONTACT_WEIGHTS["no_ssl"])

    # 4. Detección de perfil CV
    keyword_matches: dict[str, int] = {
        k: _contar_keywords(texto_plano, v["keywords"])  # type: ignore[arg-type]
        for k, v in RUBRO_WEIGHTS.items()
    }
    perfil_key = _seleccionar_perfil(keyword_matches)
    perfil_data = RUBRO_WEIGHTS[perfil_key]
    perfil_cv: str = str(perfil_data["cv"])
    score += int(perfil_data["score_bonus"])  # type: ignore[arg-type]

    logger.info(
        "Scoring | dominio=%s | perfil=%s | score=%d | contactos=%d | apto=%s",
        dominio, perfil_cv, score, len(contactos), score >= umbral_auto,
    )

    return ResultadoScoring(
        perfil_cv=perfil_cv,
        score_total=score,
        contactos=sorted(contactos, key=lambda c: c.prioridad),
        rubro_detectado=perfil_key,
        keyword_matches=keyword_matches,
        penalty_matches=penalty_matches,
        tiene_form_solo=tiene_form_solo,
        umbral_auto=umbral_auto,
    )


# ---------------------------------------------------------------------------
# Serialización
# ---------------------------------------------------------------------------

def scoring_to_dict(resultado: ResultadoScoring) -> dict:
    return {
        "perfil_cv":       resultado.perfil_cv,
        "score_total":     resultado.score_total,
        "score_display":   resultado.score_display,
        "umbral_auto":     resultado.umbral_auto,
        "rubro_detectado": resultado.rubro_detectado,
        "keyword_matches": resultado.keyword_matches,
        "penalty_matches": resultado.penalty_matches,
        "tiene_form_solo": resultado.tiene_form_solo,
        "apto_envio_auto": resultado.apto_envio_auto,
        "contactos": [
            {"valor": c.valor, "tipo": c.tipo, "prioridad": c.prioridad, "puntos": c.puntos}
            for c in resultado.contactos
        ],
    }


if __name__ == "__main__":
    import json
    html_demo = """
    <html><body>
      <p>Somos una empresa de software en Mar del Plata.</p>
      <a href="mailto:rrhh@techmdp.com.ar">rrhh@techmdp.com.ar</a>
    </body></html>
    """
    resultado = analizar_empresa(html_demo, dominio="techmdp.com.ar", umbral_auto=60)
    print(json.dumps(scoring_to_dict(resultado), indent=2, ensure_ascii=False))
    assert resultado.apto_envio_auto == (resultado.score_total >= resultado.umbral_auto)
    print("\n✓ Consistencia umbral_auto verificada.")
