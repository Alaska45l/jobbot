"""Resume profile definitions for Alaska Elaina Gonzalez.

Perfiles orientados a empresas ATICMA de Mar del Plata:
  - CV_IT_QA:       Software, QA, soporte, cloud, seguridad, IoT, infra.
  - CV_BackOffice:  E-commerce, CRM, logística, gestión, consultoría.
  - CV_Ciencia:     Biotech, green tech, calidad, laboratorio, ciencia.
"""

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
    puesto_objetivo: str


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
    # ── CV 1: Perfil Técnico IT / QA / Soporte ──────────────────────────
    "CV_IT_QA": CVProfile(
        code="CV_IT_QA",
        template_file="cv_it_qa.typ",
        title="Perfil Técnico IT / QA / Soporte",
        puesto_objetivo="QA Tester / Soporte IT / Desarrolladora Jr",
        summary=(
            "Perfil técnico orientado a QA, soporte IT, troubleshooting, desarrollo "
            "web y análisis de sistemas. Experiencia autodidacta en Go, PostgreSQL, "
            "SvelteKit, TypeScript, Tailwind CSS, Rust, Tauri, Python, y Cloudflare "
            "Workers/Pages. Capacidad demostrada para detectar vulnerabilidades "
            "(IDOR/BOLA), documentar fallos con precisión y resolver problemas de "
            "infraestructura. Busco integrarme a equipos de QA, soporte, "
            "infraestructura, cloud o seguridad."
        ),
        experience=(
            "Proyectos independientes (2024-Presente): Desarrollo full-stack con SvelteKit y Go/Fiber, "
            "aplicaciones desktop con Rust/Tauri, testing manual y automatizado, auditoría de seguridad "
            "con detección responsable de vulnerabilidad IDOR/BOLA crítica en plataforma e-commerce, "
            "uso de herramientas de IA para desarrollo y documentación.",
            "Soporte Técnico IT Freelance (2019-2025): Diagnóstico de hardware y software, "
            "instalación y hardening de Windows/Linux, configuración de redes TCP/IP, "
            "recuperación de datos, mantenimiento preventivo y correctivo.",
            "Falucho / La MilaPizza / Tres Picos (12/2023-01/2026): Gestión de sistemas POS Morphi, "
            "control de stock, facturación AFIP, troubleshooting de equipos, "
            "resolución de incidencias técnicas en múltiples sucursales.",
        ),
        skills=(
            "QA/Testing: Testing manual funcional, detección de edge cases, reporte de bugs, "
            "análisis de flujos, reproducción de errores, pensamiento sistémico",
            "Soporte IT: Troubleshooting HW/SW, redes TCP/IP, Windows/Linux, recuperación de datos, "
            "mantenimiento preventivo, documentación de incidencias",
            "Backend: Go/Fiber, Rust, Python 3, PostgreSQL, APIs REST, SQL, Git",
            "Frontend: SvelteKit, TypeScript, Tailwind CSS v4, Vite, HTML/CSS",
            "Cloud/DevOps: Cloudflare Workers/Pages, CI/CD, Linux Arch/Debian/Ubuntu",
            "Seguridad: OWASP Top 10, IDOR/BOLA, Burp Suite, CVSS, Responsible Disclosure",
            "IA/Automatización: Uso de herramientas IA para desarrollo, scraping, bots, automatización",
        ),
    ),

    # ── CV 2: Back Office / E-commerce / Operaciones Digitales ──────────
    "CV_BackOffice": CVProfile(
        code="CV_BackOffice",
        template_file="cv_backoffice.typ",
        title="Back Office Técnico / E-commerce / Operaciones Digitales",
        puesto_objetivo="Back Office / Operaciones E-commerce / Soporte de Gestión",
        summary=(
            "Perfil operativo con base técnica sólida, orientado a back office, "
            "e-commerce, control de datos, gestión de stock, sistemas de gestión y "
            "operaciones digitales. Experiencia real en entornos de alta demanda "
            "con POS, facturación, control de inventario y coordinación operativa. "
            "Capacidad para detectar inconsistencias, ordenar información, automatizar "
            "tareas repetitivas y proponer mejoras de proceso."
        ),
        experience=(
            "Falucho / La MilaPizza / Tres Picos (12/2023-01/2026): Gestión operativa integral, "
            "sistema POS Morphi, carga y control de productos, stock, facturación AFIP/ARCA, "
            "inventarios, generación de reportes, coordinación con proveedores, "
            "resolución de incidencias operativas en múltiples sucursales.",
            "Proyectos independientes - Operaciones digitales / Automatización / Web (2024-Presente): "
            "Desarrollo de herramientas de automatización, scraping de datos, "
            "gestión de catálogos digitales, documentación de procesos, "
            "optimización de flujos operativos con Python.",
            "Soporte Técnico IT Freelance (2019-2025): Diagnóstico y resolución de problemas "
            "de hardware/software, instalación de sistemas, redes, recuperación de datos.",
        ),
        skills=(
            "E-commerce/Operaciones: Carga de productos, control de stock, catálogos digitales, "
            "publicaciones, detección de errores de precio/inventario, logística básica",
            "Sistemas de gestión: POS Morphi, facturación AFIP/ARCA, control documental, "
            "gestión de proveedores, reportes operativos",
            "Datos/Documentación: Excel/Google Sheets avanzado, carga y control de datos, "
            "detección de inconsistencias, documentación de procesos",
            "Herramientas digitales: Google Workspace, Notion, VS Code, navegador/devtools, "
            "plataformas e-commerce, automatización básica",
            "Web/IT: HTML, CSS, JavaScript, troubleshooting, redes TCP/IP, Windows/Linux",
            "IA/Automatización: Python para automatización de tareas, scraping, "
            "herramientas IA para productividad",
        ),
    ),

    # ── CV 3: Calidad / Laboratorio / Ciencia Aplicada ──────────────────
    "CV_Ciencia": CVProfile(
        code="CV_Ciencia",
        template_file="cv_ciencia.typ",
        title="Calidad / Laboratorio / Ciencia Aplicada",
        puesto_objetivo="Control de Calidad / Auxiliar de Laboratorio / Documentación Técnica",
        summary=(
            "Perfil con formación parcial en Medicina y Física, combinado con "
            "experiencia en IT y control operativo. Orientado a calidad, laboratorio, "
            "documentación técnica, control de procesos y ciencia aplicada. "
            "Pensamiento analítico, registro preciso, capacidad para seguir protocolos "
            "y detectar desviaciones. Interés fuerte en biotecnología, green tech, "
            "industria alimenticia, producción, software de calidad y datos científicos."
        ),
        experience=(
            "Falucho / La MilaPizza / Tres Picos - Operaciones, control de stock y sistemas internos "
            "(12/2023-01/2026): Control de inventario con métricas de calidad, "
            "trazabilidad de productos, detección de errores en stock y facturación, "
            "documentación de procesos operativos, registro preciso de movimientos.",
            "Proyectos independientes - Análisis de sistemas / Documentación / Automatización "
            "(2024-Presente): Documentación técnica de sistemas, análisis de flujos y procesos, "
            "detección de fallos, automatización de tareas repetitivas, "
            "reportes y análisis de datos con Python.",
            "Soporte Técnico IT Freelance (2019-2025): Diagnóstico sistemático de problemas, "
            "documentación de incidencias, resolución de fallos de hardware/software.",
        ),
        skills=(
            "Ciencia aplicada: Biología, química, fisiología (medicina cursada), física (en curso), "
            "lectura crítica de evidencia, pensamiento científico, bioseguridad básica",
            "Calidad/Procesos: Control de calidad, trazabilidad, registro de desviaciones, "
            "documentación de protocolos, BPM/POES nociones, mejora continua",
            "Laboratorio/Producción: Observación metódica, seguimiento de protocolos, "
            "registro de datos experimentales, control de variables",
            "Datos: Excel/Sheets para análisis, Python para procesamiento, "
            "documentación técnica, reportes, visualización básica",
            "Sistemas/IT: Troubleshooting, redes, Windows/Linux, automatización, "
            "herramientas digitales, Git/VS Code",
            "Programación: Python, Go, HTML/CSS/JS, SQL, automatización de procesos, "
            "herramientas IA para análisis y documentación",
        ),
    ),
}

# Backward compatibility: map old profile names to new ones
_PROFILE_ALIASES: dict[str, str] = {
    "CV_Tech": "CV_IT_QA",
    "CV_Admin_IT": "CV_BackOffice",
    "CV_Hybrid": "CV_Ciencia",
}


def get_profile(perfil_cv: str) -> CVProfile:
    """Retorna el perfil de CV, soportando nombres legacy."""
    resolved = _PROFILE_ALIASES.get(perfil_cv, perfil_cv)
    return PROFILES.get(resolved, PROFILES["CV_IT_QA"])
