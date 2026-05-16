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

