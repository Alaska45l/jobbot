"""Resume profile definitions for Alaska Elaina Gonzalez.

Perfiles orientados a empresas ATICMA de Mar del Plata:
  - CV_IT_QA:       Software, QA, soporte, cloud, seguridad, IoT, infra.
  - CV_BackOffice:  E-commerce, CRM, logistica, gestion, consultoria.
  - CV_Ciencia:     Biotech, green tech, calidad, laboratorio, ciencia.

El contenido se inyecta via {{ MARKERS }} en los templates Typst.
Los items de experiencia / educacion usan markup Typst embebido.
Los items de skills usan formato *bold label:* descripcion.
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
    education: tuple[str, ...]
    idiomas: str
    fortalezas: tuple[str, ...]
    puesto_objetivo: str


CONTACT = {
    "nombre": "Alaska Elaina Gonzalez",
    "ubicacion": "Zona Centro, Mar del Plata, PBA",
    "email": "AlaskaGonzalez\\@outlook.com",
    "telefono": "(2262) 63-3652",
    "portfolio": "alaska45l.github.io",
    "github": "github.com/alaska45l",
}

PROFILES: dict[str, CVProfile] = {
    # -- CV 1: Perfil Tecnico IT / QA / Soporte -------------------------
    "CV_IT_QA": CVProfile(
        code="CV_IT_QA",
        template_file="cv_it_qa.typ",
        title="Perfil Tecnico IT / QA / Soporte",
        puesto_objetivo="QA Tester / Soporte IT / Desarrolladora Jr",
        summary=(
            "Orientacion a *QA, soporte IT, troubleshooting, desarrollo web y "
            "analisis de sistemas*. Cuento con experiencia practica en "
            "diagnostico de hardware/software, instalacion y mantenimiento de "
            "sistemas operativos, soporte a usuarios, resolucion de incidentes "
            "y documentacion de problemas tecnicos.\n\n"
            "Ademas, desarrollo proyectos independientes con tecnologias "
            "modernas como *Go, PostgreSQL, SvelteKit, TypeScript, Tailwind "
            "CSS, Rust, Tauri, Python y Cloudflare Workers/Pages*, lo que me "
            "permite comprender los sistemas desde varias capas: frontend, "
            "backend, infraestructura, seguridad, despliegue y experiencia de "
            "usuario.\n\n"
            "Me destaco por mi pensamiento analitico, capacidad para detectar "
            "inconsistencias, reproducir errores, documentar fallos y aprender "
            "herramientas nuevas de forma autodidacta. Busco incorporarme a "
            "equipos de *QA, soporte tecnico, infraestructura IT, software a "
            "medida, integraciones, sistemas de gestion, seguridad informatica "
            "o cloud*."
        ),
        experience=(
            # Job 1
            "#grid(columns: (1fr, auto), "
            "text(10pt, weight: \"bold\")[Proyectos independientes de desarrollo, QA y auditoria tecnica], "
            "text(9pt, fill: rgb(\"#555555\"))[2024 -- Presente])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Desarrolladora / QA / Soporte tecnico independiente "
            "\\u{00B7} Necochea / Mar del Plata]\n\n"
            "- Desarrollo de sistemas web full-stack utilizando "
            "*Go, PostgreSQL, SvelteKit, TypeScript y Tailwind CSS*.\n"
            "- Construccion de herramientas de escritorio con "
            "*Rust y Tauri*, orientadas a diagnostico, automatizacion "
            "y operacion local.\n"
            "- Pruebas funcionales de aplicaciones propias: deteccion "
            "de bugs, revision de flujos, validacion de errores y "
            "documentacion de comportamiento esperado.\n"
            "- Auditoria tecnica de plataformas web, con foco en "
            "vulnerabilidades logicas, permisos, autenticacion, APIs "
            "y exposicion de informacion.\n"
            "- Deteccion y reporte responsable de una vulnerabilidad "
            "critica de permisos tipo *IDOR/BOLA* en una plataforma de "
            "comercio electronico.\n"
            "- Despliegue y mantenimiento de aplicaciones utilizando "
            "*Cloudflare Pages, Workers y herramientas de automatizacion "
            "con IA*.\n"
            "- Uso de agentes y herramientas como *OpenAI Codex, Claude "
            "Code, OpenCode y Google Antigravity* para revision de codigo, "
            "debugging y mejora de flujos tecnicos.",

            # Job 2
            "#grid(columns: (1fr, auto), "
            "text(10pt, weight: \"bold\")[Soporte Tecnico IT Freelance], "
            "text(9pt, fill: rgb(\"#555555\"))[2019 -- 2025])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Tecnica IT / Soporte de sistemas \\u{00B7} Necochea]\n\n"
            "- Diagnostico y resolucion de problemas en PCs, notebooks, "
            "impresoras, perifericos y redes domesticas o de pequenos "
            "comercios.\n"
            "- Instalacion, configuracion y mantenimiento de "
            "*Windows 10/11, Windows Server, Debian, Ubuntu y Fedora*.\n"
            "- Recuperacion de datos, optimizacion de rendimiento, "
            "instalacion de software y mantenimiento preventivo.\n"
            "- Configuracion basica de redes, routers, switches, "
            "conectividad, cuentas de usuario y politicas locales de "
            "seguridad.\n"
            "- Ensamblado y actualizacion de equipos segun necesidades "
            "de rendimiento, presupuesto y uso.\n"
            "- Explicacion clara de problemas tecnicos a usuarios no "
            "tecnicos.",

            # Job 3
            "#grid(columns: (1fr, auto), "
            "text(10pt, weight: \"bold\")[Falucho / La MilaPizza / Tres Picos], "
            "text(9pt, fill: rgb(\"#555555\"))[12/2023 -- 01/2026])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Operaciones, caja, sistemas POS y soporte interno "
            "\\u{00B7} Necochea / Mar del Plata]\n\n"
            "- Uso avanzado del sistema de gestion y punto de venta "
            "*Morphi*.\n"
            "- Carga, modificacion y eliminacion de productos, precios, "
            "descuentos y reportes.\n"
            "- Gestion de caja, facturacion electronica AFIP, arqueos "
            "y reportes diarios.\n"
            "- Control de stock, insumos, mermas y coordinacion de "
            "pedidos a proveedores.\n"
            "- Resolucion de incidentes tecnicos con terminales POS, "
            "impresoras fiscales, routers y sistemas de facturacion.\n"
            "- Capacitacion de personal en uso de sistemas internos "
            "y flujos operativos.",
        ),
        education=(
            # Fisica
            "#grid(columns: (1fr, auto), "
            "text(9.5pt, weight: \"bold\")[Licenciatura en Fisica], "
            "text(9pt, fill: rgb(\"#555555\"))[2025 -- Presente])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))[UNMDP]\n"
            "#text(8.5pt)[_Competencias asociadas:_ pensamiento logico, "
            "modelado de sistemas, analisis cuantitativo y resolucion de "
            "problemas.]",

            # Medicina
            "#grid(columns: (1fr, auto), "
            "text(9.5pt, weight: \"bold\")[Medicina -- Ciclo Basico], "
            "text(9pt, fill: rgb(\"#555555\"))[2024 -- 2025])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))[UNMDP]\n"
            "#text(8.5pt)[_Competencias asociadas:_ terminologia cientifica, "
            "bioseguridad, lectura tecnica, trabajo bajo presion y analisis "
            "de sistemas biologicos.]",

            # UNICEN
            "#grid(columns: (1fr, auto), "
            "text(9.5pt, weight: \"bold\")[Tecnicatura Universitaria en Desarrollo de Aplicaciones Informaticas], "
            "text(9pt, fill: rgb(\"#555555\"))[2022 -- 2024])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))[UNICEN]\n"
            "#text(8.5pt)[_Competencias asociadas:_ programacion estructurada, "
            "bases de datos, redes, arquitectura de software y ofimatica "
            "avanzada.]",
        ),
        skills=(
            "*QA / Testing:* testing funcional, reproduccion de bugs, "
            "documentacion de errores, revision de flujos, edge cases, "
            "validacion de APIs, analisis de permisos.",

            "*Soporte IT:* Windows 10/11, Windows Server, Linux, instalacion "
            "y configuracion de sistemas, diagnostico de hardware/software, "
            "redes TCP/IP, recuperacion de datos, hardening basico.",

            "*Backend:* Go, Fiber, PostgreSQL, SQL, Python, Rust, APIs REST, "
            "autenticacion JWT, WebAuthn, AWS SDK for Go.",

            "*Frontend:* SvelteKit, Svelte 5, TypeScript, Tailwind CSS, Vite, "
            "HTML, CSS, JavaScript.",

            "*Cloud / DevOps:* Cloudflare Pages, Cloudflare Workers, Git, "
            "GitHub, despliegue web, configuracion de entornos.",

            "*Seguridad informatica:* OWASP Top 10, IDOR/BOLA, Burp Suite, "
            "analisis de cabeceras HTTP, CVSS, divulgacion responsable.",

            "*Automatizacion e IA:* OpenAI Codex, Claude Code, OpenCode, "
            "prompting tecnico, automatizacion de flujos con agentes.",
        ),
        idiomas=(
            "Ingles nivel funcional alto. Lectura fluida de documentacion "
            "tecnica, reportes CVE, documentacion de APIs, foros tecnicos "
            "y material especializado."
        ),
        fortalezas=(
            "Pensamiento analitico y deteccion de patrones.",
            "Resolucion de problemas tecnicos.",
            "Capacidad para aprender herramientas nuevas rapidamente.",
            "Documentacion clara de errores y procesos.",
            "Buena respuesta ante presion operativa.",
            "Autonomia, curiosidad tecnica y orientacion a sistemas.",
        ),
    ),

    # -- CV 2: Back Office / E-commerce / Operaciones Digitales ---------
    "CV_BackOffice": CVProfile(
        code="CV_BackOffice",
        template_file="cv_backoffice.typ",
        title="Back Office Tecnico / E-commerce / Operaciones Digitales",
        puesto_objetivo="Back Office / Operaciones E-commerce / Soporte de Gestion",
        summary=(
            "Orientacion a *operaciones digitales, back office tecnico, "
            "e-commerce, control de datos, sistemas de gestion, stock y "
            "soporte interno*. Cuento con experiencia en operacion de "
            "sistemas POS, facturacion electronica, carga y control de "
            "productos, reportes, inventario, coordinacion de pedidos, "
            "resolucion de incidentes operativos y mejora de procesos.\n\n"
            "Mi experiencia combina trabajo real en operaciones comerciales "
            "con conocimientos tecnicos en software, bases de datos, "
            "automatizacion, sistemas web y herramientas digitales. Esto me "
            "permite entender tanto la logica del negocio como la estructura "
            "tecnica que sostiene sus procesos.\n\n"
            "Busco incorporarme a empresas de *e-commerce, CRM, "
            "logistica-stock, sistemas de gestion, paginas web, "
            "automatizacion, software a medida, marketing digital operativo "
            "o consultoria IT*, en roles donde pueda aportar orden, "
            "precision, analisis de informacion y mejora de procesos internos."
        ),
        experience=(
            # Job 1 — Falucho (first for backoffice)
            "#grid(columns: (1fr, auto), "
            "text(10pt, weight: \"bold\")[Falucho / La MilaPizza / Tres Picos], "
            "text(9pt, fill: rgb(\"#555555\"))[12/2023 -- 01/2026])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Operaciones, caja, sistemas POS y control de stock "
            "\\u{00B7} Necochea / Mar del Plata]\n\n"
            "- Operacion avanzada del sistema de gestion y punto de venta "
            "*Morphi*.\n"
            "- Carga, edicion y eliminacion de productos, precios, "
            "promociones, descuentos y configuraciones internas.\n"
            "- Gestion de pedidos de salon y delivery, seguimiento operativo "
            "y resolucion de inconsistencias.\n"
            "- Facturacion electronica mediante AFIP, apertura/cierre de "
            "caja, arqueos y reportes diarios.\n"
            "- Control de inventario, seguimiento de insumos, mermas, "
            "rotacion de productos y alertas de bajo stock.\n"
            "- Coordinacion de pedidos a proveedores segun demanda, "
            "disponibilidad y consumo.\n"
            "- Exportacion y revision de reportes de ventas para apoyar "
            "decisiones operativas.\n"
            "- Mantenimiento basico de terminales POS, impresoras fiscales, "
            "routers y sistemas internos.\n"
            "- Capacitacion de personal en uso de sistemas, carga de pedidos "
            "y flujos operativos.",

            # Job 2 — Proyectos independientes
            "#grid(columns: (1fr, auto), "
            "text(10pt, weight: \"bold\")[Proyectos independientes de desarrollo y automatizacion], "
            "text(9pt, fill: rgb(\"#555555\"))[2024 -- Presente])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Operaciones digitales / Automatizacion / Web "
            "\\u{00B7} Necochea / Mar del Plata]\n\n"
            "- Desarrollo de sistemas web y herramientas digitales con "
            "*Go, PostgreSQL, SvelteKit, TypeScript y Tailwind CSS*.\n"
            "- Creacion de automatizaciones orientadas a reducir tareas "
            "repetitivas y ordenar informacion.\n"
            "- Uso de herramientas de IA como *OpenAI Codex, Claude Code, "
            "OpenCode y Google Antigravity* para documentacion, analisis y "
            "mejora de procesos.\n"
            "- Revision de flujos web, formularios, autenticacion, errores "
            "funcionales y experiencia de usuario.\n"
            "- Organizacion de informacion tecnica y operativa para proyectos "
            "digitales.\n"
            "- Despliegue de sitios y aplicaciones mediante *Cloudflare Pages "
            "y Workers*.",

            # Job 3 — Soporte IT
            "#grid(columns: (1fr, auto), "
            "text(10pt, weight: \"bold\")[Soporte Tecnico IT Freelance], "
            "text(9pt, fill: rgb(\"#555555\"))[2019 -- 2025])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Soporte tecnico y mantenimiento informatico "
            "\\u{00B7} Necochea]\n\n"
            "- Diagnostico y resolucion de problemas de hardware, software, "
            "impresoras, conectividad y sistemas.\n"
            "- Instalacion y configuracion de Windows, Linux, software de "
            "oficina y herramientas de trabajo.\n"
            "- Recuperacion de datos, optimizacion de equipos y "
            "mantenimiento preventivo.\n"
            "- Asistencia a usuarios no tecnicos, explicacion clara de "
            "problemas y documentacion de soluciones.\n"
            "- Configuracion basica de redes, routers y perifericos.",
        ),
        education=(
            # Fisica
            "#grid(columns: (1fr, auto), "
            "text(9.5pt, weight: \"bold\")[Licenciatura en Fisica], "
            "text(9pt, fill: rgb(\"#555555\"))[2025 -- Presente])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Universidad Nacional de Mar del Plata]\n"
            "#text(8.5pt)[_Competencias asociadas:_ analisis cuantitativo, "
            "pensamiento logico, modelado de sistemas y resolucion de "
            "problemas.]",

            # Medicina
            "#grid(columns: (1fr, auto), "
            "text(9.5pt, weight: \"bold\")[Medicina -- Ciclo Basico], "
            "text(9pt, fill: rgb(\"#555555\"))[2024 -- 2025])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Universidad Nacional de Mar del Plata]\n"
            "#text(8.5pt)[_Competencias asociadas:_ organizacion de "
            "informacion cientifica, trabajo bajo presion, comunicacion con "
            "perfiles diversos y analisis de procesos.]",

            # UNICEN
            "#grid(columns: (1fr, auto), "
            "text(9.5pt, weight: \"bold\")[Tecnicatura Universitaria en Desarrollo de Aplicaciones Informaticas], "
            "text(9pt, fill: rgb(\"#555555\"))[2022 -- 2024])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))[UNICEN]\n"
            "#text(8.5pt)[_Competencias asociadas:_ bases de datos, "
            "programacion, sistemas, redes y herramientas digitales.]",
        ),
        skills=(
            "*E-commerce / Operaciones:* carga de productos, control de "
            "precios, revision de publicaciones, catalogo digital, stock, "
            "reportes, pedidos, control de inconsistencias.",

            "*Sistemas de gestion:* POS Morphi, facturacion AFIP, reportes "
            "de ventas, control de caja, carga de insumos, inventario y "
            "proveedores.",

            "*Datos y documentacion:* Excel avanzado, tablas dinamicas, "
            "formulas, control de datos, reportes operativos, documentacion "
            "de procesos.",

            "*Herramientas digitales:* Google Workspace, Microsoft Office, "
            "GitHub, Cloudflare, herramientas web, automatizacion con IA.",

            "*Web / IT:* HTML, CSS, JavaScript, SvelteKit, TypeScript, Go, "
            "PostgreSQL, soporte tecnico, troubleshooting, redes basicas.",

            "*IA y automatizacion:* prompting tecnico, agentes de codigo, "
            "automatizacion de flujos repetitivos, revision de procesos.",
        ),
        idiomas=(
            "Ingles nivel funcional alto. Lectura fluida de documentacion "
            "tecnica, herramientas digitales, plataformas web y material "
            "especializado."
        ),
        fortalezas=(
            "Orden y control de informacion.",
            "Deteccion de errores e inconsistencias.",
            "Pensamiento analitico aplicado a procesos.",
            "Capacidad para aprender sistemas nuevos.",
            "Autonomia operativa.",
            "Buena comunicacion escrita.",
            "Experiencia en entornos de presion y operacion real.",
        ),
    ),

    # -- CV 3: Calidad / Laboratorio / Ciencia Aplicada -----------------
    "CV_Ciencia": CVProfile(
        code="CV_Ciencia",
        template_file="cv_ciencia.typ",
        title="Calidad / Laboratorio / Ciencia Aplicada",
        puesto_objetivo="Control de Calidad / Auxiliar de Laboratorio / Documentacion Tecnica",
        summary=(
            "Orientacion a *calidad, laboratorio, documentacion tecnica, "
            "control de procesos, ciencia aplicada y analisis de "
            "informacion*. Cuento con formacion universitaria parcial en "
            "Medicina, Fisica y Desarrollo de Aplicaciones Informaticas, "
            "junto con experiencia laboral en operaciones, stock, "
            "facturacion, sistemas POS, soporte IT y resolucion de "
            "problemas.\n\n"
            "Me interesa trabajar en entornos donde se valoren el "
            "pensamiento analitico, el registro preciso, la observacion, "
            "la deteccion de desviaciones, el cumplimiento de procedimientos "
            "y la mejora de procesos. Tengo especial afinidad por areas "
            "vinculadas a *biotecnologia, green tech, industria alimentaria, "
            "software de produccion, automatizacion, logistica-stock, calidad, "
            "salud, datos cientificos y sistemas tecnicos*.\n\n"
            "Mi perfil combina base cientifica, criterio tecnico y "
            "experiencia operativa, lo que me permite adaptarme a tareas de "
            "control, documentacion, analisis y soporte en entornos "
            "productivos o tecnologicos."
        ),
        experience=(
            # Job 1 — Falucho
            "#grid(columns: (1fr, auto), "
            "text(10pt, weight: \"bold\")[Falucho / La MilaPizza / Tres Picos], "
            "text(9pt, fill: rgb(\"#555555\"))[12/2023 -- 01/2026])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Operaciones, control de stock y sistemas internos "
            "\\u{00B7} Necochea / Mar del Plata]\n\n"
            "- Control de inventario, insumos, mermas, rotacion de productos "
            "y alertas de bajo stock.\n"
            "- Coordinacion de pedidos a proveedores segun consumo, "
            "disponibilidad y necesidades operativas.\n"
            "- Carga y actualizacion de productos, precios, descuentos y "
            "configuraciones en sistema POS *Morphi*.\n"
            "- Revision de reportes diarios de ventas, caja y movimiento de "
            "productos.\n"
            "- Facturacion electronica AFIP, arqueos y control documental "
            "basico.\n"
            "- Deteccion de errores operativos en pedidos, carga de "
            "productos, stock y facturacion.\n"
            "- Mantenimiento basico de terminales POS, impresoras fiscales, "
            "routers y sistemas internos.\n"
            "- Capacitacion de personal en procedimientos de carga, uso de "
            "sistema y flujos de trabajo.",

            # Job 2 — Proyectos independientes
            "#grid(columns: (1fr, auto), "
            "text(10pt, weight: \"bold\")[Proyectos independientes de analisis tecnico y desarrollo], "
            "text(9pt, fill: rgb(\"#555555\"))[2024 -- Presente])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Analisis de sistemas / Documentacion / Automatizacion "
            "\\u{00B7} Necochea / Mar del Plata]\n\n"
            "- Desarrollo de herramientas y sistemas web orientados a "
            "ordenar informacion, automatizar procesos y reducir errores "
            "operativos.\n"
            "- Trabajo con *Go, PostgreSQL, SvelteKit, TypeScript, Python, "
            "Rust y Cloudflare*.\n"
            "- Documentacion de errores, flujos funcionales, procesos "
            "tecnicos y propuestas de mejora.\n"
            "- Analisis de seguridad y permisos en plataformas web, "
            "incluyendo reporte responsable de vulnerabilidad critica tipo "
            "*IDOR/BOLA*.\n"
            "- Uso de herramientas de IA para auditoria, documentacion, "
            "revision de codigo y analisis de procesos.\n"
            "- Capacidad para comprender sistemas desde multiples capas: "
            "usuario, proceso, datos, infraestructura y riesgo.",

            # Job 3 — Soporte IT
            "#grid(columns: (1fr, auto), "
            "text(10pt, weight: \"bold\")[Soporte Tecnico IT Freelance], "
            "text(9pt, fill: rgb(\"#555555\"))[2019 -- 2025])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Soporte tecnico y diagnostico informatico "
            "\\u{00B7} Necochea]\n\n"
            "- Diagnostico de problemas de hardware, software, redes, "
            "perifericos y sistemas operativos.\n"
            "- Instalacion y configuracion de Windows, Linux, herramientas "
            "de oficina y software tecnico.\n"
            "- Recuperacion de datos, optimizacion de rendimiento y "
            "mantenimiento preventivo.\n"
            "- Registro de problemas, explicacion de soluciones y asistencia "
            "a usuarios no tecnicos.\n"
            "- Configuracion basica de redes, routers y equipos de trabajo.",
        ),
        education=(
            # Medicina FIRST (most relevant to science profile)
            "#grid(columns: (1fr, auto), "
            "text(9.5pt, weight: \"bold\")[Medicina -- Ciclo Basico], "
            "text(9pt, fill: rgb(\"#555555\"))[2024 -- 2025])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Universidad Nacional de Mar del Plata]\n"
            "#text(8.5pt)[_Competencias asociadas:_ biologia humana, "
            "fisiologia, histologia, bioseguridad, terminologia medica, "
            "lectura cientifica, salud publica y analisis de sistemas "
            "biologicos.]",

            # Fisica
            "#grid(columns: (1fr, auto), "
            "text(9.5pt, weight: \"bold\")[Licenciatura en Fisica], "
            "text(9pt, fill: rgb(\"#555555\"))[2025 -- Presente])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Universidad Nacional de Mar del Plata]\n"
            "#text(8.5pt)[_Competencias asociadas:_ pensamiento logico, "
            "modelado de sistemas, analisis cuantitativo, medicion, "
            "abstraccion y resolucion de problemas.]",

            # UNICEN
            "#grid(columns: (1fr, auto), "
            "text(9.5pt, weight: \"bold\")[Tecnicatura Universitaria en Desarrollo de Aplicaciones Informaticas], "
            "text(9pt, fill: rgb(\"#555555\"))[2022 -- 2024])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))[UNICEN]\n"
            "#text(8.5pt)[_Competencias asociadas:_ programacion, bases de "
            "datos, redes, sistemas, documentacion tecnica y herramientas "
            "digitales.]",
        ),
        skills=(
            "*Ciencia aplicada:* biologia, fisiologia, quimica basica, "
            "salud, lectura critica de informacion cientifica, terminologia "
            "medica.",

            "*Calidad y procesos:* control de stock, deteccion de desvios, "
            "documentacion de procesos, reportes, seguimiento de insumos, "
            "control operativo.",

            "*Laboratorio / produccion:* disposicion para tareas de registro, "
            "control, observacion, preparacion, seguimiento de protocolos y "
            "trazabilidad.",

            "*Datos:* Excel avanzado, tablas dinamicas, formulas, control de "
            "informacion, reportes y revision de inconsistencias.",

            "*Sistemas:* POS Morphi, facturacion AFIP, soporte IT, Windows, "
            "Linux, redes basicas, troubleshooting.",

            "*Programacion y automatizacion:* Python, Go, PostgreSQL, "
            "SvelteKit, TypeScript, herramientas de IA, automatizacion de "
            "flujos y documentacion tecnica.",
        ),
        idiomas=(
            "Ingles nivel funcional alto. Lectura fluida de documentacion "
            "tecnica, cientifica y material especializado."
        ),
        fortalezas=(
            "Pensamiento analitico.",
            "Observacion detallada.",
            "Deteccion de errores y desviaciones.",
            "Capacidad para seguir y mejorar procesos.",
            "Aprendizaje autodidacta.",
            "Buena respuesta ante presion operativa.",
            "Interes fuerte por ciencia, tecnologia y sistemas complejos.",
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
