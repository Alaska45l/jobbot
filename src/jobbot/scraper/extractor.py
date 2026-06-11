"""
scraper/extractor.py — JobBot Company Profile Extractor
Extrae información estructurada del HTML scrapeado de empresas ATICMA:
tecnologías, servicios, vacantes, descripción.

Python: 3.11+
Dependencias: stdlib únicamente (re, logging)
"""
from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import Final

logger = logging.getLogger("jobbot.extractor")

# ---------------------------------------------------------------------------
# Keywords de tecnologías conocidas
# ---------------------------------------------------------------------------

TECH_KEYWORDS: Final[tuple[str, ...]] = (
    # Lenguajes
    "python", "javascript", "typescript", "java", "c#", "c++", "php",
    "ruby", "go", "golang", "rust", "swift", "kotlin", "scala", "r",
    "dart", "elixir", "lua",
    # Frameworks frontend
    "react", "angular", "vue", "svelte", "next.js", "nuxt", "gatsby",
    # Frameworks backend
    "django", "flask", "fastapi", "express", "spring", "laravel",
    "rails", "asp.net", ".net", "node.js", "fiber",
    # Bases de datos
    "postgresql", "mysql", "mongodb", "redis", "sqlite", "oracle",
    "sql server", "elasticsearch", "dynamodb", "firebase",
    # Cloud / DevOps
    "aws", "azure", "gcp", "google cloud", "docker", "kubernetes",
    "terraform", "ansible", "jenkins", "github actions", "ci/cd",
    "cloudflare", "heroku", "digitalocean", "vercel",
    # Mobile
    "android", "ios", "react native", "flutter", "xamarin",
    # Data / IA
    "machine learning", "deep learning", "tensorflow", "pytorch",
    "pandas", "numpy", "scikit-learn", "inteligencia artificial",
    "big data", "data science", "power bi", "tableau",
    # IoT / Hardware
    "iot", "arduino", "raspberry pi", "embedded", "scada", "plc",
    "industria 4.0", "automatización industrial",
    # QA / Testing
    "selenium", "cypress", "jest", "junit", "testing", "qa",
    "quality assurance", "test automation",
    # ERP / Gestión
    "sap", "odoo", "salesforce", "hubspot", "crm", "erp",
    "tango gestión", "tiendanube", "woocommerce", "shopify",
    "mercado libre", "magento", "prestashop",
    # Seguridad
    "ciberseguridad", "pentesting", "owasp", "siem", "firewall",
    "vpn", "ssl", "nist",
    # Diseño / UX
    "ux", "ui", "figma", "adobe", "diseño web",
    # Otros
    "agile", "scrum", "kanban", "devops", "microservicios",
    "api rest", "graphql", "websocket", "git", "linux",
)

# Servicios comunes en empresas tech de MdP
SERVICE_PATTERNS: Final[tuple[str, ...]] = (
    "desarrollo de software", "desarrollo web", "desarrollo mobile",
    "aplicaciones móviles", "aplicaciones web", "consultoría",
    "soporte técnico", "infraestructura", "cloud computing",
    "transformación digital", "analítica de datos", "business intelligence",
    "e-commerce", "comercio electrónico", "tienda online",
    "marketing digital", "seo", "sem", "redes sociales",
    "diseño gráfico", "diseño web", "diseño ux/ui",
    "integraciones", "automatización", "robótica",
    "videojuegos", "realidad virtual", "realidad aumentada",
    "ciberseguridad", "seguridad informática",
    "outsourcing", "staff augmentation", "nearshoring",
    "capacitación", "formación", "educación",
    "biotecnología", "agro tech", "green tech",
    "sistemas de gestión", "erp", "crm",
    "producción audiovisual", "comunicación digital",
    "monitoreo", "alarmas", "seguimiento vehicular",
    "internet de las cosas", "domótica", "smart home",
    "calidad de software", "testing", "qa",
    "impresión 3d", "fabricación digital", "prototipado",
    "laboratorio", "control de calidad", "trazabilidad",
)

# Patrones que indican vacantes
JOB_INDICATORS: Final[tuple[str, ...]] = (
    "buscamos", "sumate", "únete", "postulate", "postulá",
    "estamos buscando", "oportunidad laboral", "vacante",
    "búsqueda activa", "estamos contratando", "we are hiring",
    "join us", "join our team", "work with us",
    "trabaja con nosotros", "envianos tu cv", "enviá tu cv",
    "oportunidades de empleo", "bolsa de trabajo",
    "rrhh@", "cv@", "empleo@", "talento@", "recruiting@",
)


# ---------------------------------------------------------------------------
# Estructura de datos
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class CompanyProfile:
    """Perfil extraído del scraping de una empresa ATICMA."""
    technologies: list[str] = field(default_factory=list)
    services: list[str] = field(default_factory=list)
    description: str = ""
    has_openings: bool = False
    job_keywords: list[str] = field(default_factory=list)
    raw_about: str = ""


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

_RE_HTML_TAGS: Final[re.Pattern[str]] = re.compile(r'<[^>]+>')
_RE_WHITESPACE: Final[re.Pattern[str]] = re.compile(r'\s+')

_RE_ABOUT_SECTIONS: Final[re.Pattern[str]] = re.compile(
    r'(?:sobre\s+nosotros|quiénes?\s+somos|about\s+us|nuestra?\s+empresa'
    r'|qué\s+hacemos|what\s+we\s+do|la\s+empresa)',
    re.IGNORECASE,
)


def _strip_html(html: str) -> str:
    """Elimina tags HTML y normaliza whitespace."""
    text = _RE_HTML_TAGS.sub(' ', html)
    return _RE_WHITESPACE.sub(' ', text).strip()


def _extract_about_section(html: str) -> str:
    """
    Intenta extraer la sección 'Sobre nosotros' / 'About us' del HTML.
    Busca headings comunes y toma el texto que sigue.
    """
    # Buscar en <meta name="description">
    meta_match = re.search(
        r'<meta\s+[^>]*name=["\']description["\'][^>]*content=["\'](.*?)["\']',
        html, re.IGNORECASE | re.DOTALL,
    )
    meta_desc = meta_match.group(1).strip() if meta_match else ""

    # Buscar sección "sobre nosotros" en el body
    about_text = ""
    for match in _RE_ABOUT_SECTIONS.finditer(html):
        start = match.end()
        # Tomar los próximos ~1000 chars después del heading
        chunk = html[start:start + 1500]
        # Limpiar HTML
        clean = _strip_html(chunk)
        if len(clean) > 30:  # Evitar fragmentos vacíos
            about_text = clean[:500]
            break

    # Combinar meta description + about text
    parts = []
    if meta_desc:
        parts.append(meta_desc)
    if about_text and about_text not in meta_desc:
        parts.append(about_text)

    return " ".join(parts)[:600] if parts else ""


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

def extract_tech_keywords(html: str) -> list[str]:
    """
    Detecta tecnologías mencionadas en el HTML.
    Retorna lista de tecnologías únicas encontradas, ordenadas por aparición.
    """
    text_lower = _strip_html(html).lower()
    found: list[str] = []
    seen: set[str] = set()

    for tech in TECH_KEYWORDS:
        if tech in seen:
            continue
        # Buscar como palabra completa (con tolerancia para puntos y #)
        pattern = r'(?<![a-záéíóúñ])' + re.escape(tech) + r'(?![a-záéíóúñ])'
        if re.search(pattern, text_lower):
            found.append(tech)
            seen.add(tech)

    logger.debug("Tecnologías detectadas: %d | %s", len(found), ", ".join(found[:5]))
    return found


def extract_services(html: str) -> list[str]:
    """
    Detecta servicios ofrecidos por la empresa en el HTML.
    Retorna lista de servicios únicos encontrados.
    """
    text_lower = _strip_html(html).lower()
    found: list[str] = []
    seen: set[str] = set()

    for service in SERVICE_PATTERNS:
        if service in seen:
            continue
        if service in text_lower:
            found.append(service)
            seen.add(service)

    logger.debug("Servicios detectados: %d | %s", len(found), ", ".join(found[:5]))
    return found


def extract_job_openings(html: str) -> tuple[bool, list[str]]:
    """
    Detecta si la empresa tiene vacantes o busca personal.

    Returns:
        (has_openings, job_keywords) donde job_keywords son las señales encontradas.
    """
    text_lower = _strip_html(html).lower()
    signals: list[str] = []

    for indicator in JOB_INDICATORS:
        if indicator in text_lower:
            signals.append(indicator)

    has_openings = len(signals) >= 1
    if has_openings:
        logger.info("Señales de vacantes detectadas: %s", ", ".join(signals[:3]))

    return has_openings, signals


def extract_company_description(html: str) -> str:
    """
    Extrae la descripción/misión de la empresa del HTML.
    Combina meta description con sección 'Sobre nosotros'.
    """
    return _extract_about_section(html)


def analyze_company_html(html: str, nombre_empresa: str = "") -> CompanyProfile:
    """
    Análisis completo del HTML de una empresa. Función de conveniencia
    que ejecuta todas las extracciones y devuelve un CompanyProfile.

    Args:
        html: HTML acumulado de todas las páginas scrapeadas de la empresa.
        nombre_empresa: Nombre de la empresa (para logging).

    Returns:
        CompanyProfile con toda la información extraída.
    """
    if not html or not html.strip():
        logger.warning("HTML vacío para análisis | empresa=%s", nombre_empresa)
        return CompanyProfile()

    technologies = extract_tech_keywords(html)
    services = extract_services(html)
    has_openings, job_keywords = extract_job_openings(html)
    description = extract_company_description(html)

    profile = CompanyProfile(
        technologies=technologies,
        services=services,
        description=description,
        has_openings=has_openings,
        job_keywords=job_keywords,
        raw_about=description,
    )

    logger.info(
        "CompanyProfile | empresa=%s | techs=%d | services=%d | "
        "openings=%s | desc_len=%d",
        nombre_empresa, len(technologies), len(services),
        has_openings, len(description),
    )
    return profile
