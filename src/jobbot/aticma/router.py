"""
aticma/router.py — JobBot ATICMA CV Router
Asigna el perfil de CV adecuado a cada empresa según su descripción,
nombre y keywords scrapeadas, usando reglas de coincidencia por palabra clave.

Python: 3.11+
Dependencias: stdlib únicamente (re, logging)

Mapeo de perfiles de CV:
  - cv_1.md → CV_IT_QA      (desarrollo, QA, cloud, seguridad, IA, etc.)
  - cv_2.md → CV_BackOffice (e-commerce, CRM, marketing, gestión, etc.)
  - cv_3.md → CV_Ciencia    (biotech, green tech, industria 4.0, agro, etc.)
"""

from __future__ import annotations

import logging
import re
from typing import Final

logger = logging.getLogger("jobbot.aticma")

# ---------------------------------------------------------------------------
# Reglas de routing
# ---------------------------------------------------------------------------
# Cada clave es un perfil de CV (como se almacena en la DB).
# Los valores son keywords que, si aparecen en la descripción/nombre de la
# empresa, indican que ese perfil es el adecuado.
#
# Las keywords se evalúan en orden de especificidad:
#   1. CV_Ciencia    (ciencia/industria — más específico)
#   2. CV_BackOffice (backoffice/marketing)
#   3. CV_IT_QA      (fallback técnico genérico)

ROUTING_RULES: Final[dict[str, list[str]]] = {
    "CV_IT_QA": [
        "software",
        "qa",
        "quality assurance",
        "integraciones",
        "cloud",
        "seguridad",
        "ciberseguridad",
        "iot",
        "internet of things",
        "videojuegos",
        "gaming",
        "infraestructura",
        "desarrollo",
        "aplicaciones",
        "sistemas",
        "robótica",
        "robotica",
        "ia",
        "inteligencia artificial",
        "machine learning",
        "datos",
        "data",
        "devops",
        "programación",
        "programacion",
        "código",
        "codigo",
        "web app",
        "api",
        "backend",
        "frontend",
        "full stack",
        "fullstack",
        "mobile",
        "plataforma",
        "tech",
        "tecnología",
        "tecnologia",
        "digital",
        "informática",
        "informatica",
        "electrónica",
        "electronica",
        "hardware",
        "redes",
        "networking",
        "criptomonedas",
        "blockchain",
        "fintech",
        "genexus",
        "microsoft",
        "trading",
    ],
    "CV_BackOffice": [
        "e-commerce",
        "ecommerce",
        "crm",
        "logística",
        "logistica",
        "stock",
        "páginas web",
        "paginas web",
        "página web",
        "pagina web",
        "sistemas de gestión",
        "sistemas de gestion",
        "gestión empresarial",
        "gestion empresarial",
        "consultoría",
        "consultoria",
        "marketing",
        "comunicación",
        "comunicacion",
        "diseño",
        "diseno",
        "seo",
        "publicidad",
        "branding",
        "audiovisual",
        "contenidos",
        "agencia",
        "facturación",
        "facturacion",
        "contable",
        "administración",
        "administracion",
        "rrhh",
        "recursos humanos",
        "capital humano",
        "educación",
        "educacion",
        "capacitación",
        "capacitacion",
        "inmobiliario",
        "propiedad horizontal",
        "sueldos",
        "liquidación",
        "liquidacion",
        "lealtad",
        "retail",
    ],
    "CV_Ciencia": [
        "biotecnología",
        "biotecnologia",
        "biotech",
        "green tech",
        "greentech",
        "industria 4.0",
        "producción",
        "produccion",
        "calidad",
        "datos científicos",
        "datos cientificos",
        "agro",
        "agropecuaria",
        "agropecuario",
        "laboratorio",
        "alimentos",
        "veterinaria",
        "medio ambiente",
        "ambiental",
        "vegetal",
        "riego",
        "bioestrategia",
        "industrial",
    ],
}

# Peso extra que reciben las scraped_keywords en el scoring.
_SCRAPED_WEIGHT: Final[int] = 3

# Mapeo de perfil de CV a puesto objetivo sugerido.
_PUESTO_OBJETIVO: Final[dict[str, str]] = {
    "CV_IT_QA": "QA Tester / Soporte IT / Desarrolladora Jr",
    "CV_BackOffice": "Back Office / Operaciones E-commerce / Soporte de Gestión",
    "CV_Ciencia": "Control de Calidad / Auxiliar de Laboratorio / Documentación Técnica",
}


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _normalize(text: str) -> str:
    """Normaliza texto para matching: minúsculas y espacios compactados."""
    return re.sub(r"\s+", " ", text.strip().lower())


def _count_keyword_matches(
    text: str,
    keywords: list[str],
    weight: int = 1,
) -> int:
    """
    Cuenta cuántas keywords aparecen en el texto normalizado.
    Cada match suma `weight` puntos.

    Keywords cortas (≤3 caracteres) usan word boundaries para evitar
    falsos positivos (ej: 'iot' dentro de 'Diagnósticos').
    """
    text_norm = _normalize(text)
    score = 0
    for kw in keywords:
        kw_lower = kw.lower()
        if len(kw_lower) <= 3:
            # Word boundary para keywords cortas — evita matches parciales
            if re.search(rf"\b{re.escape(kw_lower)}\b", text_norm):
                score += weight
        else:
            if kw_lower in text_norm:
                score += weight
    return score


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

def route_company_to_cv(
    descripcion: str,
    nombre: str,
    scraped_keywords: list[str] | None = None,
) -> str:
    """
    Determina el perfil de CV más adecuado para una empresa.

    Analiza la descripción, nombre y keywords scrapeadas buscando
    coincidencias con las reglas de routing definidas.

    Args:
        descripcion: Descripción de la empresa (del campo ATICMA o scraping).
        nombre: Nombre de la empresa.
        scraped_keywords: Keywords extraídas del sitio web (peso extra).

    Returns:
        Código de perfil: 'CV_IT_QA', 'CV_BackOffice' o 'CV_Ciencia'.
        Fallback: 'CV_IT_QA' (perfil técnico genérico más amplio).
    """
    # Texto base para búsqueda (nombre + descripción)
    texto_base = f"{nombre} {descripcion}"

    # Calcular score por perfil
    scores: dict[str, int] = {}

    for perfil, keywords in ROUTING_RULES.items():
        score = _count_keyword_matches(texto_base, keywords)

        # Keywords scrapeadas tienen peso extra
        if scraped_keywords:
            scraped_text = " ".join(scraped_keywords)
            score += _count_keyword_matches(
                scraped_text, keywords, weight=_SCRAPED_WEIGHT,
            )

        scores[perfil] = score

    # Evaluar en orden de especificidad para desempate:
    # CV_Hybrid (más nicho) > CV_Admin_IT > CV_Tech (fallback)
    priority_order = ["CV_Ciencia", "CV_BackOffice", "CV_IT_QA"]

    best_perfil = "CV_IT_QA"  # fallback
    best_score = 0

    for perfil in priority_order:
        perfil_score = scores.get(perfil, 0)
        # Desempate: perfiles más específicos ganan con score igual
        if perfil_score > best_score:
            best_score = perfil_score
            best_perfil = perfil

    logger.debug(
        "Routing | empresa='%s' | scores=%s | resultado=%s",
        nombre, scores, best_perfil,
    )
    return best_perfil


def derive_puesto_objetivo(perfil_cv: str) -> str:
    """
    Retorna un título de puesto objetivo adecuado según el perfil de CV.

    Args:
        perfil_cv: Código de perfil ('CV_Tech', 'CV_Admin_IT', 'CV_Hybrid').

    Returns:
        String con el puesto objetivo sugerido.
    """
    return _PUESTO_OBJETIVO.get(perfil_cv, _PUESTO_OBJETIVO["CV_IT_QA"])


# ---------------------------------------------------------------------------
# Ejecución directa (para testing manual)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Ejemplos rápidos de routing
    ejemplos = [
        ("Desarrollo de Software y robótica avanzada", "AOKI Tech"),
        ("Somos eCommerce", "ROLLPIX S.A."),
        ("Productos y Servicios en Biotecnología Vegetal", "Diagnósticos vegetales S.A."),
        ("Join to grow", "Attic"),
        ("Agencia de Marketing Digital especializada en SEO", "Seotronix"),
        ("Monitoreo de riego para el agro", "Ponce"),
    ]

    for desc, nombre in ejemplos:
        perfil = route_company_to_cv(desc, nombre)
        puesto = derive_puesto_objetivo(perfil)
        print(f"  {nombre:40s} → {perfil:15s} | {puesto}")
