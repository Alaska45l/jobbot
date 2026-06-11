"""Lightweight database row models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Empresa:
    id: int
    nombre: str
    dominio: str
    rubro: str | None
    perfil_cv: str | None
    score: int


@dataclass(frozen=True, slots=True)
class EmpresaATICMA:
    """Empresa cargada desde el listado ATICMA con campos extendidos."""
    id: int
    nombre: str
    dominio: str | None
    descripcion: str
    direccion: str
    ubicacion: str
    telefono: str
    email: str
    sitio_web: str | None
    perfil_cv: str | None
    score: int
    keywords_scraped: str       # JSON string de keywords extraídas del scraping
    descripcion_scraped: str    # "Sobre nosotros" extraído del sitio
    tiene_vacantes: bool
    fuente: str                 # 'dorking' | 'aticma'


@dataclass(frozen=True, slots=True)
class Contacto:
    id: int
    empresa_id: int
    email_o_link: str
    tipo: str
    prioridad: int


@dataclass(frozen=True, slots=True)
class Envio:
    id: int
    empresa_id: int
    fecha_envio: str
    cv_enviado: str
    asunto_usado: str
    estado: str
