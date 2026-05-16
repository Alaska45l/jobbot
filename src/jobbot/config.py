"""
config.py — JobBot Configuración Centralizada
Fuente única de verdad para variables de entorno, constantes de tiempo
y parámetros operativos del pipeline.

Python: 3.11+
Dependencias: stdlib únicamente (os)
"""
from __future__ import annotations

import os

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# Perfil del remitente
# ---------------------------------------------------------------------------
SENDER_NAME:   str = os.getenv("SENDER_NAME",   "Alaska")
GITHUB_USER:   str = os.getenv("GITHUB_USER",   "tu-usuario")
LINKEDIN_USER: str = os.getenv("LINKEDIN_USER", "tu-perfil")

# ---------------------------------------------------------------------------
# SMTP — leídas en tiempo de importación.
# ConfigSMTP en mailer.py hace la validación de obligatoriedad en runtime.
# ---------------------------------------------------------------------------
SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER: str = os.getenv("SMTP_USER", "")
SMTP_PASS: str = os.getenv("SMTP_PASS", "")

# ---------------------------------------------------------------------------
# Rate limiting / Jitter (segundos)
# ---------------------------------------------------------------------------
SMTP_JITTER_MIN_S: int = 180    # 3 minutos
SMTP_JITTER_MAX_S: int = 480    # 8 minutos

WA_JITTER_MIN_S: int = 180      # 3 minutos — mínimo recomendado por Meta
WA_JITTER_MAX_S: int = 450      # 7.5 minutos

# ---------------------------------------------------------------------------
# Cooldowns (días)
# ---------------------------------------------------------------------------
COOLDOWN_MAIL_DAYS:     int = 90
COOLDOWN_SCRAPING_DAYS: int = 7
COOLDOWN_WA_DAYS:       int = 7

# ---------------------------------------------------------------------------
# Límites operativos
# ---------------------------------------------------------------------------
WA_LIMITE_DIARIO: int = 30   # Meta puede banear si se superan ~40–50/día