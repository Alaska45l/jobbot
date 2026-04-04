"""
utils/phone.py — JobBot Phone Utilities
Regex y funciones de normalización de números de teléfono argentinos
para detección de contactos de WhatsApp en HTML scrapeado.

Python: 3.11+
Dependencias: stdlib únicamente (re, typing)
"""
from __future__ import annotations

import re
from typing import Optional

# ---------------------------------------------------------------------------
# Regex — Números de WhatsApp con formato argentino
#
#  Grupo 1 — Formato wa.me:         wa.me/5492231234567
#  Grupo 2 — E.164 con área separada: +54 9 223 123 4567
#  Grupo 3 — Nacional con 0:        0223 123-4567
#  Grupo 4 — Local directo MdP:     223 123 4567
#
#  El patrón exige al menos 7 dígitos después del código de área
#  para evitar falsos positivos (versiones de SW, fechas, etc.).
# ---------------------------------------------------------------------------
_RE_WHATSAPP: re.Pattern[str] = re.compile(
    r"""
    (?:
        # Grupo 1: links wa.me con código de país 54
        wa\.me/(?:549?)(\d{10,11})
    |
        # Grupo 2: formato internacional +54 9, área y número SEPARADOS
        \+54\s*9?\s*
        \(?
        ((?:11|2(?:2[0-4679]|3[3-8]|4[013-9]|6[0124-8]|7[1-4]|9[1-469])|
            3(?:3[28]|4[0-9]|5[25-8]|6[1-3579]|7[0246-9]|8[2357-9])))
        \)?
        [\s\-]*
        (\d[\d\s\-]{5,8}\d)
    |
        # Grupo 3: formato nacional con 0 (ej: 0223 123-4567)
        \(?0(\d{2,4})\)?
        [\s\-]*
        (\d[\d\s\-]{5,8}\d)
    |
        # Grupo 4: formato local directo MdP y zona (ej: 223 123 4567)
        \b(2(?:2[0-4679]|3[3-8]|4[013-9]|6[0124-8]|7[1-4]|9[1-469])|
           3(?:3[28]|4[0-9]|5[25-8]|6[1-3579]|7[0246-9]|8[2357-9]))
        [\s\-]*
        (\d[\d\s\-]{5,7}\d)
        \b
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)


def normalizar_numero_ar(match: re.Match) -> Optional[str]:
    """
    Convierte cualquier match de _RE_WHATSAPP al formato E.164 argentino
    (+549XXXXXXXXXX). Exige exactamente 10 dígitos netos (área + número).
    Cualquier suma distinta → None.

    Mapeo de grupos:
        grupos[0]            → Grupo 1: wa.me/549XXXXXXXXXX
        grupos[1], grupos[2] → Grupo 2: +54 9 (AREA) NÚMERO
        grupos[3], grupos[4] → Grupo 3: 0AREA NÚMERO
        grupos[5], grupos[6] → Grupo 4: local AREA NÚMERO

    Returns:
        Número en formato '+549XXXXXXXXXX' o None si no pasa validación.
    """
    grupos = match.groups()

    # Grupo 1: wa.me/549XXXXXXXXXX
    if grupos[0]:
        digitos = re.sub(r'\D', '', grupos[0])
        if len(digitos) == 10 and not digitos.startswith('0'):
            return f"+549{digitos}"
        return None

    # Grupo 2: +54 9 (AREA) NÚMERO
    if grupos[1] and grupos[2]:
        area   = re.sub(r'\D', '', grupos[1])
        numero = re.sub(r'\D', '', grupos[2])
        if numero.startswith('15'):
            numero = numero[2:]
        if len(area) + len(numero) == 10 and not area.startswith('0'):
            return f"+549{area}{numero}"
        return None

    # Grupo 3: 0AREA NÚMERO
    if grupos[3] and grupos[4]:
        area   = re.sub(r'\D', '', grupos[3])
        numero = re.sub(r'\D', '', grupos[4])
        if numero.startswith('15'):
            numero = numero[2:]
        if len(area) + len(numero) == 10 and not area.startswith('0'):
            return f"+549{area}{numero}"
        return None

    # Grupo 4: AREA NÚMERO (local, sin prefijo)
    if grupos[5] and grupos[6]:
        area   = re.sub(r'\D', '', grupos[5])
        numero = re.sub(r'\D', '', grupos[6])
        if numero.startswith('15'):
            numero = numero[2:]
        if len(area) + len(numero) == 10 and not area.startswith('0'):
            return f"+549{area}{numero}"
        return None

    return None


def extraer_numeros_whatsapp(html: str) -> list[str]:
    """
    Extrae todos los números de WhatsApp únicos de un HTML,
    devueltos en formato E.164 (+549XXXXXXXXXX).

    Args:
        html: HTML crudo de la página.

    Returns:
        Lista de números únicos en formato E.164, ordenada.
    """
    encontrados: set[str] = set()
    for match in _RE_WHATSAPP.finditer(html):
        numero = normalizar_numero_ar(match)
        if numero:
            encontrados.add(numero)
    return sorted(encontrados)