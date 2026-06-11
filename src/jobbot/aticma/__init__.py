"""
aticma — JobBot ATICMA Integration
Carga, importación y routing de empresas del directorio ATICMA.
"""

from __future__ import annotations

from jobbot.aticma.loader import import_aticma_to_db, parse_aticma_json
from jobbot.aticma.router import derive_puesto_objetivo, route_company_to_cv

__all__ = [
    "parse_aticma_json",
    "import_aticma_to_db",
    "route_company_to_cv",
    "derive_puesto_objetivo",
]
