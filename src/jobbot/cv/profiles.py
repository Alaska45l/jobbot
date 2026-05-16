"""Resume profile definitions for Alaska Elaina Gonzalez."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CVProfile:
    code: str
    template_file: str
    title: str
    summary: str
    experience: tuple[str, ...]
    skills: tuple[str, ...]


CONTACT = {
    "nombre": "Alaska Elaina Gonzalez",
    "ubicacion": "Mar del Plata, Buenos Aires, Argentina",
    "email": "AlaskaGonzalez@outlook.com",
    "linkedin": "linkedin.com/in/alaska45l",
    "portfolio": "alaska45l.github.io",
    "github": "github.com/alaska45l",
}

EDUCATION: tuple[str, ...] = (
    "Licenciatura en Física - UNMDP, 2025-Presente",
    "Medicina, Ciclo Básico - UNMDP, 2024-2025",
    "TU en Desarrollo de Aplicaciones Informáticas, Ciclo Básico - UNICEN, 2022-2024",
)

CERTIFICATIONS: tuple[str, ...] = (
    "Técnica IT Nivel II",
    "Programación Multilenguaje - Mastermind",
    "Cambridge B2 English - First Certificate",
)

PROFILES: dict[str, CVProfile] = {
    "CV_Tech": CVProfile(
        code="CV_Tech",
        template_file="cv_tech.typ",
        title="Software Developer & Security Researcher",
        summary=(
            "Perfil de ciclo completo de software: análisis, diseño orientado a objetos, "
            "implementación full-stack, despliegue y mantenimiento. Foco técnico en Go, "
            "PostgreSQL, SvelteKit, Rust/Tauri, Linux y ciberseguridad ofensiva aplicada."
        ),
        experience=(
            "Lead Systems Engineer & Founder - INVARIANT SYSTEM, 2026-Presente: plataforma forense de tres capas con Go/Fiber, PostgreSQL y SvelteKit; utilidad desktop Rust/Tauri para flashing; ISO live Arch Linux con Python/Rich/LaTeX; CSRF, CSP, WebAuthn y CI/CD en Cloudflare.",
            "Software Developer & Security Researcher - Independiente, 2025-Presente: desarrollo full-stack Go+SvelteKit, herramientas desktop Rust/Tauri, despliegues Cloudflare y detección responsable de IDOR crítico en una plataforma e-commerce mayor.",
            "IT Support Technician - Freelance, 2019-Presente: troubleshooting HW/SW, instalación y hardening Windows/Linux, recuperación de datos y mantenimiento de redes.",
        ),
        skills=(
            "Go 1.26.2, Fiber v2, PostgreSQL, REST APIs, golang-jwt, go-webauthn",
            "SvelteKit, Svelte 5, TypeScript, Tailwind CSS v4, Vite, Cloudflare Pages/Workers",
            "Rust, Tauri v2, tokio, Python 3, Rich, Jinja2, C#, SQL, Git",
            "Pentesting, OWASP Top 10, IDOR/BOLA, Burp Suite, CVSS, Responsible Disclosure",
            "Linux Arch/Debian/Ubuntu, mkinitcpio, Tectonic/LaTeX, CI/CD",
        ),
    ),
    "CV_Admin_IT": CVProfile(
        code="CV_Admin_IT",
        template_file="cv_admin_it.typ",
        title="Administración, Operaciones y Soporte IT",
        summary=(
            "Perfil administrativo con base técnica sólida para PyMEs, estudios, clínicas y "
            "organizaciones que necesitan orden operativo, soporte a usuarios, documentación, "
            "facturación, Excel avanzado y mantenimiento IT cotidiano."
        ),
        experience=(
            "Operations & Admin Manager - Falucho/La MilaPizza/Tres Picos, 12/2023-01/2026: facturación AFIP, POS Morphi, control de stock, mantenimiento IT de sucursales, coordinación operativa y liderazgo de equipo.",
            "IT Support Technician - Freelance, 2019-Presente: soporte HW/SW, instalación Windows/Linux, hardening básico, redes TCP/IP, recuperación de datos y mantenimiento preventivo.",
            "Software Developer & Security Researcher - Independiente, 2025-Presente: automatización práctica, documentación técnica y herramientas internas para reducir tareas repetitivas.",
        ),
        skills=(
            "AFIP/ARCA, facturación, POS Morphi, control de stock, gestión documental",
            "Windows 10/11/Server, IIS, GPO, Microsoft 365, soporte a usuarios",
            "Excel avanzado, administración, atención a clientes y proveedores",
            "TCP/IP, subnetting, mantenimiento de redes, instalación y hardening de sistemas",
            "Python para automatización administrativa y reportes internos",
        ),
    ),
    "CV_Hybrid": CVProfile(
        code="CV_Hybrid",
        template_file="cv_hybrid.typ",
        title="Operaciones IT, Automatización y Soporte Técnico",
        summary=(
            "Perfil híbrido para equipos que cruzan operaciones e infraestructura: gestión "
            "administrativa, soporte IT, redes, automatización con Python, hardening de sistemas, "
            "documentación y coordinación de procesos en entornos de alta demanda."
        ),
        experience=(
            "Operations & Admin Manager - Falucho/La MilaPizza/Tres Picos, 12/2023-01/2026: operación diaria, stock, facturación AFIP, POS Morphi, soporte IT de sucursales y coordinación de equipo.",
            "IT Support Technician - Freelance, 2019-Presente: diagnóstico HW/SW, redes, recuperación de datos, instalación y hardening Windows/Linux, mantenimiento preventivo.",
            "Lead Systems Engineer & Founder - INVARIANT SYSTEM, 2026-Presente: automatización técnica, herramientas Python/Rich, plataforma Go/PostgreSQL/SvelteKit y prácticas de seguridad enterprise.",
            "Software Developer & Security Researcher - Independiente, 2025-Presente: scripting, herramientas internas, despliegues Cloudflare y seguridad aplicada.",
        ),
        skills=(
            "Operaciones, inventario, documentación, facturación AFIP, liderazgo de equipo",
            "Python, scripting, automatización de procesos, reportes y herramientas internas",
            "Windows Server, Linux, TCP/IP, subnetting, IIS, GPO, hardening y recuperación de datos",
            "Soporte IT, mantenimiento de sucursales, diagnóstico HW/SW, atención a usuarios",
            "Go, PostgreSQL, SvelteKit y Cloudflare como soporte técnico para sistemas internos",
        ),
    ),
}


def get_profile(perfil_cv: str) -> CVProfile:
    return PROFILES.get(perfil_cv, PROFILES["CV_Admin_IT"])

