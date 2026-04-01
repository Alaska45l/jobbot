"""
scoring.py — JobBot Lead Scoring Engine
Algoritmo de puntuación de prospectos y detección de perfil de CV.

Autor: JobBot Project
Python: 3.11+
Dependencias: stdlib únicamente (re, logging, typing, dataclasses)
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import Final

logger = logging.getLogger("jobbot.scoring")

# ---------------------------------------------------------------------------
# Constantes y pesos del sistema de scoring
# ---------------------------------------------------------------------------

# Pesos de contactos detectados
CONTACT_WEIGHTS: Final[dict[str, int]] = {
    "linkedin_person":   50,   # Perfil de persona con rol RRHH/Talent/People
    "email_rrhh":        40,   # rrhh@, cv@, talento@, empleos@, trabajo@, personas@
    "email_general":     15,   # info@, contacto@, hola@, administracion@
    "linkedin_company":  10,   # Página de empresa en LinkedIn (sin perfil de persona)
    "form_only":        -15,   # Solo formulario de contacto, sin email real
    "no_ssl":           -10,   # Dominio sin HTTPS (proxy detectado en scraper)
}

# Pesos de keywords por categoría para determinar rubro y bonus de score
RUBRO_WEIGHTS: Final[dict[str, dict[str, int]]] = {
    "tech": {
        "keywords": [
            "software", "desarrollo", "ciberseguridad", "it", "sistemas",
            "saas", "devops", "cloud", "startup", "programación", "api",
            "backend", "frontend", "datos", "inteligencia artificial", "ia",
            "qa", "testing", "redes", "linux",
        ],
        "score_bonus": 10,
        "cv":         "CV_Tech",
    },
    "admin_it": {
        "keywords": [
            "inmobiliaria", "logística", "estudio", "clínica", "distribuidora",
            "administración", "contable", "jurídico", "parque industrial",
            "constructor", "transporte", "salud", "comercio", "manufactura",
            "importadora", "exportadora", "agencia", "consultora",
        ],
        "score_bonus": 20,  # Bonus más alto: perfil más escaso en el mercado
        "cv":         "CV_Admin_IT",
    },
}

# Prefijos de mails que implican contacto de RRHH (Prioridad alta)
_RRHH_PREFIXES: Final[frozenset[str]] = frozenset({
    "rrhh", "cv", "talento", "empleos", "trabajo", "personas",
    "recruiting", "recruitment", "hr", "humanresources", "seleccion",
})

# Prefijos de mails generales (Prioridad media)
_GENERAL_PREFIXES: Final[frozenset[str]] = frozenset({
    "info", "contacto", "hola", "administracion", "admin",
    "gerencia", "oficina", "ventas", "atencion",
})

# Regex compilados — compilar una sola vez a nivel módulo es más eficiente
_RE_EMAIL: Final[re.Pattern[str]] = re.compile(
    r'\b([A-Za-z0-9._%+\-]+)@(?!.*(?:\.png|\.jpg|\.jpeg|\.gif|\.webp|\.svg|\.pdf|\.mp4))([A-Za-z0-9.\-]+\.[A-Za-z]{2,})\b',
    re.IGNORECASE,
)
_RE_LINKEDIN_PERSON: Final[re.Pattern[str]] = re.compile(
    r'linkedin\.com/in/[\w\-]+',
    re.IGNORECASE,
)
_RE_LINKEDIN_COMPANY: Final[re.Pattern[str]] = re.compile(
    r'linkedin\.com/company/[\w\-]+',
    re.IGNORECASE,
)
_RE_WP_FORM: Final[re.Pattern[str]] = re.compile(
    r'wpcf7|contact-form-7|wpforms|cf7',
    re.IGNORECASE,
)
_RE_HTML_TAGS: Final[re.Pattern[str]] = re.compile(r'<[^>]+>')

# Roles de RRHH para detectar en texto de LinkedIn embebido
_RRHH_ROLES: Final[frozenset[str]] = frozenset({
    "rrhh", "recursos humanos", "talent", "people", "recruiting",
    "reclutamiento", "selección", "hr manager", "people operations",
})

# ---------------------------------------------------------------------------
# Estructuras de datos de salida
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ContactoDetectado:
    """Representa un contacto encontrado durante el análisis HTML."""
    valor:     str   # Email o URL de LinkedIn
    tipo:      str   # 'RRHH' | 'General' | 'LinkedIn'
    prioridad: int   # 0 = más alto, 3 = más bajo
    puntos:    int   # Aporte individual al score total


@dataclass(slots=True)
class ResultadoScoring:
    """Resultado completo del análisis de una página empresa."""
    perfil_cv:           str                     # 'CV_Tech' | 'CV_Admin_IT'
    score_total:         int                     # Score final acumulado
    contactos:           list[ContactoDetectado] = field(default_factory=list)
    rubro_detectado:     str                     = "desconocido"
    keyword_matches:     dict[str, int]          = field(default_factory=dict)
    tiene_form_solo:     bool                    = False
    apto_envio_auto:     bool                    = False   # True si score >= umbral

    # Umbral de envío automático
    UMBRAL_AUTO: int = 55

    def __post_init__(self) -> None:
        self.apto_envio_auto = self.score_total >= self.UMBRAL_AUTO


# ---------------------------------------------------------------------------
# Funciones de detección internas
# ---------------------------------------------------------------------------

def _strip_html(html: str) -> str:
    """Elimina todas las etiquetas HTML y retorna texto plano."""
    return _RE_HTML_TAGS.sub(" ", html)


def _clasificar_email(prefix: str) -> tuple[str, int, int]:
    """
    Clasifica un email según su prefijo.

    Args:
        prefix: Parte local del email (antes del @), en minúsculas.

    Returns:
        Tupla (tipo, prioridad, puntos).
    """
    if prefix in _RRHH_PREFIXES:
        return "RRHH", 1, CONTACT_WEIGHTS["email_rrhh"]
    if prefix in _GENERAL_PREFIXES:
        return "General", 2, CONTACT_WEIGHTS["email_general"]
    # Email con prefijo desconocido: lo tratamos como general con menor peso
    return "General", 3, CONTACT_WEIGHTS["email_general"] // 2


def _detectar_linkedin_persona_con_rol(html: str, url: str) -> bool:
    """
    Heurística: detecta si una URL de LinkedIn (/in/...) aparece cerca
    de palabras clave de rol de RRHH en el HTML circundante.

    Args:
        html: HTML completo de la página.
        url:  URL de LinkedIn encontrada.

    Returns:
        True si hay señales de que la persona tiene rol de RRHH.
    """
    # Buscar posición de la URL en el HTML y extraer contexto de ±300 chars
    idx = html.lower().find(url.lower())
    if idx == -1:
        return False

    contexto = html[max(0, idx - 300): idx + 300].lower()
    return any(rol in contexto for rol in _RRHH_ROLES)


def _contar_keywords(texto: str, keywords: list[str]) -> int:
    """
    Cuenta cuántas keywords únicas aparecen en el texto (case-insensitive).
    Usa word-boundary para evitar falsos positivos parciales.

    Args:
        texto:    Texto plano donde buscar.
        keywords: Lista de palabras clave.

    Returns:
        Número de keywords únicas encontradas.
    """
    texto_lower = texto.lower()
    return sum(
        1 for kw in keywords
        if re.search(r'\b' + re.escape(kw) + r'\b', texto_lower)
    )


# ---------------------------------------------------------------------------
# Motor de scoring principal
# ---------------------------------------------------------------------------

def analizar_empresa(
    html: str,
    dominio: str = "",
    tiene_ssl: bool = True,
    umbral_auto: int = 55,
) -> ResultadoScoring:
    """
    Analiza el HTML de una empresa y produce un ResultadoScoring completo.

    Proceso:
      1. Extrae emails y URLs de LinkedIn del HTML.
      2. Clasifica cada contacto y acumula puntos.
      3. Detecta el perfil de CV correcto por conteo de keywords.
      4. Aplica penalizaciones (formularios, sin SSL).
      5. Retorna el resultado estructurado, listo para persistir en la DB.

    Args:
        html:        HTML crudo de la página (puede incluir JS/CSS inline).
        dominio:     Dominio de la empresa (para logging y contexto).
        tiene_ssl:   False si el dominio no usa HTTPS.
        umbral_auto: Score mínimo para marcar como apto para envío automático.

    Returns:
        ResultadoScoring con todos los datos calculados.

    Example:
        >>> resultado = analizar_empresa(html_raw, dominio="empresa.com.ar")
        >>> print(resultado.perfil_cv, resultado.score_total)
        'CV_Admin_IT' 75
    """
    if not html or not html.strip():
        logger.warning("HTML vacío recibido | dominio=%s", dominio)
        return ResultadoScoring(perfil_cv="CV_Admin_IT", score_total=0)

    texto_plano = _strip_html(html)
    contactos: list[ContactoDetectado] = []
    score: int = 0
    emails_vistos: set[str] = set()

    # ------------------------------------------------------------------
    # 1. Detección de emails
    # ------------------------------------------------------------------
    for match in _RE_EMAIL.finditer(html):
        prefix = match.group(1).lower()
        email_completo = match.group(0).lower()

        if email_completo in emails_vistos:
            continue
        emails_vistos.add(email_completo)

        # Filtrar emails de imágenes, fuentes web y librerías (falsos positivos)
        if any(ext in email_completo for ext in [".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".pdf", ".mp4", ".woff", ".min"]):
            continue

        tipo, prioridad, puntos = _clasificar_email(prefix)
        contactos.append(ContactoDetectado(
            valor=email_completo,
            tipo=tipo,
            prioridad=prioridad,
            puntos=puntos,
        ))
        score += puntos
        logger.debug("Email detectado | %s | tipo=%s | +%d pts", email_completo, tipo, puntos)

    # ------------------------------------------------------------------
    # 2. Detección de LinkedIn — Personas (mayor valor)
    # ------------------------------------------------------------------
    linkedin_personas_vistos: set[str] = set()
    for match in _RE_LINKEDIN_PERSON.finditer(html):
        url = match.group(0).lower()
        if url in linkedin_personas_vistos:
            continue
        linkedin_personas_vistos.add(url)

        tiene_rol_rrhh = _detectar_linkedin_persona_con_rol(html, url)
        puntos = CONTACT_WEIGHTS["linkedin_person"] if tiene_rol_rrhh else CONTACT_WEIGHTS["linkedin_company"]
        prioridad = 0 if tiene_rol_rrhh else 2
        tipo = "LinkedIn"

        contactos.append(ContactoDetectado(
            valor=f"https://www.{url}",
            tipo=tipo,
            prioridad=prioridad,
            puntos=puntos,
        ))
        score += puntos
        logger.debug(
            "LinkedIn persona | %s | rrhh_rol=%s | +%d pts",
            url, tiene_rol_rrhh, puntos,
        )

    # ------------------------------------------------------------------
    # 3. Detección de LinkedIn — Empresa
    # ------------------------------------------------------------------
    linkedin_companies_vistos: set[str] = set()
    for match in _RE_LINKEDIN_COMPANY.finditer(html):
        url = match.group(0).lower()
        if url in linkedin_companies_vistos:
            continue
        linkedin_companies_vistos.add(url)

        # Evitar contar si ya capturamos el mismo dominio como perfil de persona
        if url.replace("company/", "in/") in linkedin_personas_vistos:
            continue

        puntos = CONTACT_WEIGHTS["linkedin_company"]
        contactos.append(ContactoDetectado(
            valor=f"https://www.{url}",
            tipo="LinkedIn",
            prioridad=2,
            puntos=puntos,
        ))
        score += puntos
        logger.debug("LinkedIn company | %s | +%d pts", url, puntos)

    # ------------------------------------------------------------------
    # 4. Penalizaciones
    # ------------------------------------------------------------------
    tiene_form_solo = False

    if _RE_WP_FORM.search(html) and not emails_vistos:
        score += CONTACT_WEIGHTS["form_only"]
        tiene_form_solo = True
        logger.debug("Penalización: solo formulario de contacto | %d pts", CONTACT_WEIGHTS["form_only"])

    if not tiene_ssl:
        score += CONTACT_WEIGHTS["no_ssl"]
        logger.debug("Penalización: sin SSL | %d pts", CONTACT_WEIGHTS["no_ssl"])

    # ------------------------------------------------------------------
    # 5. Detección de perfil de CV por conteo de keywords en texto plano
    # ------------------------------------------------------------------
    keyword_matches: dict[str, int] = {}
    for perfil_key, perfil_data in RUBRO_WEIGHTS.items():
        count = _contar_keywords(texto_plano, perfil_data["keywords"])
        keyword_matches[perfil_key] = count

    # El perfil ganador es el que tiene más keywords en el HTML
    perfil_ganador_key = max(keyword_matches, key=lambda k: keyword_matches[k])
    perfil_data_ganador = RUBRO_WEIGHTS[perfil_ganador_key]

    # Desempate: si ambos tienen 0 matches, el default es admin_it
    # (más amplio y seguro para el mercado marplatense)
    if all(v == 0 for v in keyword_matches.values()):
        perfil_ganador_key = "admin_it"
        perfil_data_ganador = RUBRO_WEIGHTS["admin_it"]

    perfil_cv: str = perfil_data_ganador["cv"]
    rubro: str = perfil_ganador_key
    score += perfil_data_ganador["score_bonus"]

    logger.info(
        "Scoring completado | dominio=%s | perfil=%s | score=%d | contactos=%d | apto=%s",
        dominio,
        perfil_cv,
        score,
        len(contactos),
        score >= umbral_auto,
    )

    resultado = ResultadoScoring(
        perfil_cv=perfil_cv,
        score_total=max(score, 0),   # El score nunca es negativo
        contactos=sorted(contactos, key=lambda c: c.prioridad),
        rubro_detectado=rubro,
        keyword_matches=keyword_matches,
        tiene_form_solo=tiene_form_solo,
    )
    resultado.UMBRAL_AUTO = umbral_auto
    resultado.apto_envio_auto = resultado.score_total >= umbral_auto

    return resultado


# ---------------------------------------------------------------------------
# Helper: convertir ResultadoScoring a dict para debug / serialización
# ---------------------------------------------------------------------------

def scoring_to_dict(resultado: ResultadoScoring) -> dict:
    """
    Serializa un ResultadoScoring a dict plano (JSON-compatible).

    Args:
        resultado: Output de analizar_empresa().

    Returns:
        Dict con todos los campos, incluyendo la lista de contactos.
    """
    return {
        "perfil_cv":       resultado.perfil_cv,
        "score_total":     resultado.score_total,
        "rubro_detectado": resultado.rubro_detectado,
        "keyword_matches": resultado.keyword_matches,
        "tiene_form_solo": resultado.tiene_form_solo,
        "apto_envio_auto": resultado.apto_envio_auto,
        "contactos": [
            {
                "valor":     c.valor,
                "tipo":      c.tipo,
                "prioridad": c.prioridad,
                "puntos":    c.puntos,
            }
            for c in resultado.contactos
        ],
    }


# ---------------------------------------------------------------------------
# Entrypoint de prueba rápida
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json

    html_demo = """
    <html>
    <head><title>TechMDP - Desarrollo de Software</title></head>
    <body>
      <p>Somos una empresa de software y desarrollo web en Mar del Plata.</p>
      <p>Contacto: <a href="mailto:rrhh@techmdp.com.ar">rrhh@techmdp.com.ar</a></p>
      <p>Seguinos en
        <a href="https://www.linkedin.com/company/techmdp">LinkedIn empresa</a>
      </p>
      <p>Hablá con nuestra HR Manager:
        <a href="https://www.linkedin.com/in/ana-garcia-hr">Ana García - Talent Acquisition</a>
      </p>
    </body>
    </html>
    """

    resultado = analizar_empresa(html_demo, dominio="techmdp.com.ar", tiene_ssl=True)
    print(json.dumps(scoring_to_dict(resultado), indent=2, ensure_ascii=False))
