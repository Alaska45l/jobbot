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

from utils.phone import extraer_numeros_whatsapp   # FIX: ya no importa wa_sender

logger = logging.getLogger("jobbot.scoring")

# ---------------------------------------------------------------------------
# Constantes y pesos
# ---------------------------------------------------------------------------

CONTACT_WEIGHTS: Final[dict[str, int]] = {
    "linkedin_person":  50,
    "email_rrhh":       40,
    "email_general":    15,
    "linkedin_company": 10,
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
        ],
        "score_bonus": 10,
        "cv":          "CV_Tech",
    },
    "admin_it": {
        "keywords": [
            "inmobiliaria", "logística", "estudio", "clínica", "distribuidora",
            "administración", "contable", "jurídico", "parque industrial",
            "constructor", "transporte", "salud", "comercio", "manufactura",
            "importadora", "exportadora", "agencia", "consultora",
        ],
        "score_bonus": 20,
        "cv":          "CV_Admin_IT",
    },
}

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
#       son exactamente el perfil CV_Tech que queremos contactar.

NEGATIVE_SIGNALS: Final[dict[str, int]] = {
    # Portales de noticias / medios (exclusión casi segura)
    "últimas noticias":      -90,
    "redacción":             -80,
    "cobertura periodística":-80,
    "enviado por el editor": -80,
    "sala de prensa":        -70,
    "nota de prensa":        -60,

    # Patrones de blog (señales de contenido, no de empresa)
    "escribir un comentario": -70,   # footer de comentarios WP/Ghost
    "publicado por":          -40,   # byline de autor
    "deja una respuesta":     -60,   # WP comment form label
    "suscribirse al blog":    -60,
    "leer el artículo":       -30,

    # E-commerce B2C puro
    "agregar al carrito":     -60,
    "añadir al carrito":      -60,
    "mi carrito de compras":  -50,
    "checkout":               -40,   # puede coexistir con B2B, peso moderado

    # Sitios personales / CV online
    "este es mi portfolio":   -80,
    "currículum vitae personal": -60,
}

# Regex para extraer texto de <meta name="description"> y <title>.
# _strip_html elimina el contenido de los atributos HTML; estas etiquetas
# son las más informativas para detectar portales de noticias.
_RE_META_DESC: Final[re.Pattern[str]] = re.compile(
    r'<meta\s[^>]*name=["\']description["\']\s[^>]*content=["\'](.*?)["\']',
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
    r'\b([A-Za-z0-9._%+\-]+)@(?!.*(?:\.png|\.jpg|\.jpeg|\.gif|\.webp|\.svg|\.pdf|\.mp4))'
    r'([A-Za-z0-9.\-]+\.[A-Za-z]{2,})\b',
    re.IGNORECASE,
)
_RE_LINKEDIN_PERSON: Final[re.Pattern[str]]  = re.compile(r'linkedin\.com/in/[\w\-]+',      re.IGNORECASE)
_RE_LINKEDIN_COMPANY: Final[re.Pattern[str]] = re.compile(r'linkedin\.com/company/[\w\-]+', re.IGNORECASE)
_RE_WP_FORM: Final[re.Pattern[str]]          = re.compile(r'wpcf7|contact-form-7|wpforms|cf7', re.IGNORECASE)
_RE_HTML_TAGS: Final[re.Pattern[str]]        = re.compile(r'<[^>]+>')

_RRHH_ROLES: Final[frozenset[str]] = frozenset({
    "rrhh", "recursos humanos", "talent", "people", "recruiting",
    "reclutamiento", "selección", "hr manager", "people operations",
})

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
        # El check usa score_total sin floor: un sitio con -100 debe quedar excluido.
        # El floor (-20) solo existe para la TUI; se aplica en scoring_to_dict().
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


def _detectar_linkedin_persona_con_rol(html: str, url: str) -> bool:
    idx = html.lower().find(url.lower())
    if idx == -1:
        return False
    contexto = html[max(0, idx - 300): idx + 300].lower()
    return any(rol in contexto for rol in _RRHH_ROLES)


def _contar_keywords(texto: str, keywords: list[str]) -> int:
    texto_lower = texto.lower()
    return sum(
        1 for kw in keywords
        if re.search(r'\b' + re.escape(kw) + r'\b', texto_lower)
    )


def _extraer_texto_semantico(html: str) -> str:
    """
    Extrae el texto que _strip_html descarta: contenido de atributos HTML
    relevantes para el scoring (title, meta description).

    _strip_html() hace re.sub(r'<[^>]+>', ' ', html), eliminando no solo
    las etiquetas sino también los valores de sus atributos. Para el QR de
    noticias el campo más diagnóstico es <meta name="description">, que
    nunca aparece en el texto plano resultante.

    Returns:
        String con title + meta description del documento, en minúsculas.
    """
    partes: list[str] = []
    m_title = _RE_TITLE.search(html)
    if m_title:
        partes.append(m_title.group(1))
    m_desc = _RE_META_DESC.search(html)
    if m_desc:
        partes.append(m_desc.group(1))
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
        return ResultadoScoring(perfil_cv="CV_Admin_IT", score_total=0, umbral_auto=umbral_auto)

    texto_plano    = _strip_html(html)
    texto_semantico = _extraer_texto_semantico(html)   # NUEVO: title + meta desc
    contactos:      list[ContactoDetectado] = []
    emails_vistos:  set[str] = set()

    # ── FASE 0: Penalizaciones (NUEVO) ──────────────────────────────────────
    penalizacion, penalty_matches = _evaluar_penalizaciones(texto_plano, texto_semantico)
    score: int = penalizacion

    # Cortocircuito: si las penalizaciones ya garantizan exclusión y no hay
    # contactos de RRHH posibles, saltamos el análisis de contactos para
    # ahorrar CPU. El umbral de corte es -(umbral_auto + max_posible_contacto).
    # max_posible_contacto ≈ 90 (LinkedIn person con rol + email RRHH).
    CORTE_TEMPRANO = -(umbral_auto + 90)
    if score <= CORTE_TEMPRANO:
        logger.info(
            "Exclusión temprana | dominio=%s | penalización=%+d (≤%d)",
            dominio, score, CORTE_TEMPRANO,
        )
        return ResultadoScoring(
            perfil_cv="CV_Admin_IT",
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

    # 3. LinkedIn — Personas
    linkedin_personas: set[str] = set()
    for match in _RE_LINKEDIN_PERSON.finditer(html):
        url = match.group(0).lower()
        if url in linkedin_personas:
            continue
        linkedin_personas.add(url)
        tiene_rol = _detectar_linkedin_persona_con_rol(html, url)
        puntos    = CONTACT_WEIGHTS["linkedin_person"] if tiene_rol else CONTACT_WEIGHTS["linkedin_company"]
        prioridad = 0 if tiene_rol else 2
        contactos.append(ContactoDetectado(
            valor=f"https://www.{url}", tipo="LinkedIn", prioridad=prioridad, puntos=puntos,
        ))
        score += puntos
        logger.debug("LinkedIn persona | %s | rrhh=%s | +%d pts", url, tiene_rol, puntos)

    # 4. LinkedIn — Empresa
    linkedin_companies: set[str] = set()
    for match in _RE_LINKEDIN_COMPANY.finditer(html):
        url = match.group(0).lower()
        if url in linkedin_companies or url.replace("company/", "in/") in linkedin_personas:
            continue
        linkedin_companies.add(url)
        puntos = CONTACT_WEIGHTS["linkedin_company"]
        contactos.append(ContactoDetectado(
            valor=f"https://www.{url}", tipo="LinkedIn", prioridad=2, puntos=puntos,
        ))
        score += puntos
        logger.debug("LinkedIn company | %s | +%d pts", url, puntos)

    # 5. Penalizaciones
    tiene_form_solo = False
    if _RE_WP_FORM.search(html) and not emails_vistos:
        score += CONTACT_WEIGHTS["form_only"]
        tiene_form_solo = True
        logger.debug("Penalización: solo formulario | %d pts", CONTACT_WEIGHTS["form_only"])
    if not tiene_ssl:
        score += CONTACT_WEIGHTS["no_ssl"]
        logger.debug("Penalización: sin SSL | %d pts", CONTACT_WEIGHTS["no_ssl"])

    # 6. Detección de perfil CV
    keyword_matches: dict[str, int] = {
        k: _contar_keywords(texto_plano, v["keywords"])  # type: ignore[arg-type]
        for k, v in RUBRO_WEIGHTS.items()
    }
    perfil_key = (
        "admin_it"
        if all(v == 0 for v in keyword_matches.values())
        else max(keyword_matches, key=lambda k: keyword_matches[k])
    )
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
        "score_display":   resultado.score_display,      # NUEVO: flooreado para TUI
        "umbral_auto":     resultado.umbral_auto,
        "rubro_detectado": resultado.rubro_detectado,
        "keyword_matches": resultado.keyword_matches,
        "penalty_matches": resultado.penalty_matches,    # NUEVO: trazabilidad
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
      <a href="https://www.linkedin.com/company/techmdp">LinkedIn</a>
      <a href="https://www.linkedin.com/in/ana-garcia-hr">Ana García - Talent Acquisition</a>
    </body></html>
    """
    resultado = analizar_empresa(html_demo, dominio="techmdp.com.ar", umbral_auto=60)
    print(json.dumps(scoring_to_dict(resultado), indent=2, ensure_ascii=False))
    assert resultado.apto_envio_auto == (resultado.score_total >= resultado.umbral_auto)
    print("\n✓ Consistencia umbral_auto verificada.")