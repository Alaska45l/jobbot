"""Resume profile definitions for Alaska Elaina Gonzalez.

Perfiles orientados a empresas ATICMA de Mar del Plata:
  - CV_IT_QA:       Software, QA, soporte, cloud, seguridad, IoT, infra.
  - CV_BackOffice:  E-commerce, CRM, logistica, gestion, consultoria.
  - CV_Ciencia:     Biotech, green tech, control de calidad, laboratorio, ciencia.

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
    projects: tuple[str, ...] = ()


CONTACT = {
    "nombre": "Alaska Elaina Gonzalez",
    "ubicacion": "Zona Centro, Mar del Plata, PBA",
    "email": "alaska45lgon\\@gmail.com",
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
            "Perfil tecnico hibrido orientado a *QA manual, soporte IT, "
            "troubleshooting, documentacion y desarrollo web junior*. Puedo "
            "entender un sistema desde el usuario, la operacion y el codigo, "
            "con foco en detectar fallos, reproducir problemas y dejar "
            "registros claros para que el equipo pueda resolverlos.\n\n"
            "Cuento con experiencia practica en diagnostico de hardware y "
            "software, soporte a usuarios, sistemas POS, Linux/Windows, redes "
            "basicas, desarrollo web y revision funcional de aplicaciones. "
            "Trabajo principalmente con herramientas modernas como *Go, "
            "PostgreSQL, SvelteKit, TypeScript, Python, Rust/Tauri y "
            "Cloudflare*, y complemento esa base con conocimientos de "
            "ecosistemas muy usados en Argentina: *Java/Spring Boot, PHP "
            "con Laravel/Symfony y JavaScript con Node.js/Express, React y Vue*.\n\n"
            "Busco incorporarme a equipos donde pueda aportar en *QA, soporte "
            "tecnico, operaciones IT, documentacion, desarrollo junior o mejora "
            "de procesos tecnicos*."
        ),
        projects=(
            "*JobBot -- herramienta personal para organizar y seguir postulaciones laborales:* "
            "pipeline en Python con SQLite, generacion de CVs en Typst, "
            "clasificacion por perfil y panel TUI para controlar el estado del proceso.",
            "*Sistemas web full-stack -- Invariant.ar y Elixir Exclusive:* "
            "proyectos propios con Go, PostgreSQL, SvelteKit, TypeScript y "
            "Cloudflare, orientados a autenticacion, CRUD, despliegue y "
            "organizacion de datos.",
            "*Herramientas desktop -- Invariant Flasher, Probe.tex e Inercia:* "
            "apps y utilidades para diagnostico, operacion local y flujos "
            "reproducibles con foco en soporte tecnico.",
        ),
        experience=(
            # Job 1
            "#grid(columns: (1fr, auto), "
            "text(10pt, weight: \"bold\")[Proyectos independientes de desarrollo, QA y auditoria tecnica], "
            "text(9pt, fill: rgb(\"#555555\"))[2024 -- Presente])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Desarrolladora / QA / Soporte tecnico independiente "
            "\\u{00B7} Necochea / Mar del Plata]\n\n"
            "- Desarrollo de sistemas web y herramientas propias utilizando "
            "*Go, PostgreSQL, SvelteKit, TypeScript y Tailwind CSS*.\n"
            "- Pruebas funcionales de aplicaciones propias: deteccion "
            "de bugs, revision de flujos, validacion de errores y "
            "documentacion de comportamiento esperado.\n"
            "- Construccion de herramientas de escritorio con *Rust y Tauri*, "
            "orientadas a diagnostico y operacion local.\n"
            "- Auditoria tecnica de plataformas web, con foco en "
            "vulnerabilidades logicas, permisos, autenticacion, APIs "
            "y exposicion de informacion.\n"
            "- Deteccion y reporte responsable de una vulnerabilidad "
            "critica de permisos tipo *IDOR/BOLA* en una plataforma de "
            "comercio electronico.\n"
            "- Despliegue y mantenimiento de aplicaciones utilizando "
            "*Cloudflare Pages y Workers*.\n"
            "- Uso de IA aplicada como apoyo para revision de codigo, "
            "debugging, generacion de tests y documentacion tecnica.",

            # Job 2
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

            # Job 3
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
            "bioseguridad, lectura tecnica, criterio operativo y analisis "
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
            "*Foco principal:* QA manual, soporte IT, troubleshooting, "
            "documentacion de incidencias, desarrollo web junior y mejora de procesos.",

            "*Stack principal:* Go, PostgreSQL, SvelteKit, TypeScript, Python, "
            "Rust/Tauri, SQL, HTML, CSS, JavaScript y Git.",

            "*Ecosistemas frecuentes en Argentina:* Java con Spring Boot, "
            "PHP con Laravel/Symfony, JavaScript con Node.js/Express, React y Vue.",

            "*Soporte e infraestructura:* Windows 10/11, Windows Server, "
            "Linux, redes TCP/IP, instalacion de sistemas, diagnostico de "
            "hardware/software, recuperacion de datos y hardening basico.",

            "*Testing y documentacion:* reproduccion de bugs, casos de prueba, "
            "revision de flujos, validacion de APIs, analisis de permisos y "
            "registro claro de hallazgos.",

            "*Complementario:* seguridad web, OWASP Top 10, Cloudflare "
            "Pages/Workers, despliegue web e IA aplicada a revision de codigo, "
            "debugging, tests y documentacion tecnica.",
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
            "Priorizacion en entornos operativos.",
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
            "Orientacion a *back office tecnico, operaciones digitales, "
            "e-commerce, control de datos, documentacion y soporte de gestion*. "
            "Cuento con experiencia real en sistemas POS, facturacion "
            "electronica, carga y control de productos, reportes, inventario, "
            "coordinacion de pedidos y resolucion de incidencias operativas.\n\n"
            "Mi perfil combina trabajo operativo con base tecnica en software, "
            "bases de datos, herramientas web y soporte IT. Esto me permite "
            "entender tanto la logica diaria del negocio como los sistemas que "
            "sostienen sus procesos.\n\n"
            "Busco incorporarme a equipos donde pueda aportar *orden, precision, "
            "control de informacion, documentacion clara, soporte administrativo "
            "tecnico y mejora de procesos internos*."
        ),
        projects=(
            "*JobBot -- herramienta personal para organizar y seguir postulaciones laborales:* "
            "pipeline en Python con SQLite, generacion de CVs en Typst, "
            "clasificacion por perfil y panel TUI para controlar el estado del proceso.",
            "*Sistemas web -- Invariant.ar y Elixir Exclusive:* proyectos "
            "propios con Go, PostgreSQL, SvelteKit y TypeScript para organizar "
            "datos, flujos CRUD, autenticacion y despliegue web.",
            "*Herramientas de soporte operativo:* scripts y utilidades para "
            "ordenar informacion, revisar datos y documentar procesos; ejemplos: "
            "Invariant Flasher, Probe.tex e Inercia.",
        ),
        experience=(
            # Job 1 — Proyectos independientes
            "#grid(columns: (1fr, auto), "
            "text(10pt, weight: \"bold\")[Proyectos independientes de desarrollo y organizacion digital], "
            "text(9pt, fill: rgb(\"#555555\"))[2024 -- Presente])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Operaciones digitales / Documentacion / Web "
            "\\u{00B7} Necochea / Mar del Plata]\n\n"
            "- Desarrollo de sistemas web y herramientas digitales con "
            "*Go, PostgreSQL, SvelteKit, TypeScript y Tailwind CSS*.\n"
            "- Organizacion de datos, flujos CRUD, formularios y reportes "
            "para proyectos propios.\n"
            "- Revision de flujos web, formularios, autenticacion, errores "
            "funcionales y experiencia de usuario.\n"
            "- Organizacion de informacion tecnica y operativa para proyectos "
            "digitales.\n"
            "- Despliegue de sitios y aplicaciones mediante *Cloudflare Pages "
            "y Workers*.\n"
            "- Uso de IA aplicada como apoyo para documentacion, revision de "
            "procesos y mejora de textos tecnicos.",

            # Job 2 — Falucho / La MilaPizza / Tres Picos
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
            "informacion cientifica, organizacion de datos, comunicacion con "
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
            "*Foco principal:* back office tecnico, operaciones digitales, "
            "control de datos, documentacion, soporte de gestion y mejora de procesos.",

            "*Operaciones y gestion:* POS Morphi, facturacion AFIP, caja, "
            "stock, pedidos, proveedores, reportes y control de inconsistencias.",

            "*Datos y documentacion:* Excel avanzado, tablas dinamicas, "
            "formulas, control de informacion, reportes operativos y "
            "documentacion de procedimientos.",

            "*Stack practico:* Google Workspace, Microsoft Office, GitHub, "
            "Cloudflare, HTML, CSS, JavaScript, SvelteKit, TypeScript, Go y PostgreSQL.",

            "*Complementario:* soporte tecnico, troubleshooting, redes basicas "
            "e IA aplicada a documentacion, revision de procesos y mejora de textos.",
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
            "Experiencia en entornos operativos reales.",
        ),
    ),

    # -- CV 3: Control de Calidad / Laboratorio / Ciencia Aplicada ------
    "CV_Ciencia": CVProfile(
        code="CV_Ciencia",
        template_file="cv_ciencia.typ",
        title="Control de Calidad / Laboratorio / Ciencia Aplicada",
        puesto_objetivo="Control de Calidad / Auxiliar de Laboratorio / Documentacion Tecnica",
        summary=(
            "Orientacion a *Control de calidad, laboratorio, documentacion "
            "tecnica, control de procesos y analisis de informacion*. Cuento "
            "con formacion universitaria en Fisica, cursada previa en Medicina "
            "y base tecnica en Desarrollo de Aplicaciones Informaticas, junto "
            "con experiencia laboral en operaciones, stock, facturacion, "
            "sistemas POS, soporte IT y resolucion de problemas.\n\n"
            "Me interesa trabajar en entornos donde se valoren el pensamiento "
            "analitico, el registro preciso, la observacion, la deteccion de "
            "desviaciones, el cumplimiento de procedimientos y la mejora de "
            "procesos.\n\n"
            "Mi perfil combina base cientifica, criterio tecnico y experiencia "
            "operativa. Puedo aportar en tareas de *Control de calidad, "
            "documentacion, analisis, trazabilidad, soporte de sistemas y "
            "seguimiento de procesos*."
        ),
        projects=(
            "*JobBot -- herramienta personal para organizar y seguir postulaciones laborales:* "
            "pipeline en Python con SQLite, generacion de CVs en Typst, "
            "clasificacion por perfil y panel TUI para controlar el estado del proceso.",
            "*Sistemas web / paneles de datos -- Invariant.ar y Elixir Exclusive:* "
            "proyectos propios con Go, PostgreSQL, SvelteKit y TypeScript, "
            "orientados a ordenar informacion, validar flujos y documentar resultados.",
            "*Herramientas desktop -- Invariant Flasher, Probe.tex e Inercia:* "
            "apps y utilidades para diagnostico, operacion local y procedimientos "
            "reproducibles.",
        ),
        experience=(
            # Job 1 — Proyectos independientes
            "#grid(columns: (1fr, auto), "
            "text(10pt, weight: \"bold\")[Proyectos independientes de analisis tecnico y desarrollo], "
            "text(9pt, fill: rgb(\"#555555\"))[2024 -- Presente])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Analisis de sistemas / Documentacion / Automatizacion "
            "\\u{00B7} Necochea / Mar del Plata]\n\n"
            "- Desarrollo de herramientas y sistemas web orientados a "
            "ordenar informacion, validar procesos y reducir errores "
            "operativos.\n"
            "- Trabajo con *Go, PostgreSQL, SvelteKit, TypeScript, Python, "
            "Rust y Cloudflare*.\n"
            "- Documentacion de errores, flujos funcionales, procesos "
            "tecnicos y propuestas de mejora.\n"
            "- Analisis de seguridad y permisos en plataformas web, "
            "incluyendo reporte responsable de vulnerabilidad critica tipo "
            "*IDOR/BOLA*.\n"
            "- Uso de IA aplicada como apoyo para auditoria, documentacion, "
            "revision de codigo y analisis de procesos.\n"
            "- Capacidad para comprender sistemas desde multiples capas: "
            "usuario, proceso, datos, infraestructura y riesgo.",

            # Job 2 — Falucho / La MilaPizza / Tres Picos
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
            # Fisica
            "#grid(columns: (1fr, auto), "
            "text(9.5pt, weight: \"bold\")[Licenciatura en Fisica], "
            "text(9pt, fill: rgb(\"#555555\"))[2025 -- Presente])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Universidad Nacional de Mar del Plata]\n"
            "#text(8.5pt)[_Competencias asociadas:_ pensamiento logico, "
            "modelado de sistemas, analisis cuantitativo, medicion, "
            "abstraccion y resolucion de problemas.]",

            # Medicina
            "#grid(columns: (1fr, auto), "
            "text(9.5pt, weight: \"bold\")[Medicina -- Ciclo Basico], "
            "text(9pt, fill: rgb(\"#555555\"))[2024 -- 2025])\n"
            "#text(9pt, style: \"italic\", fill: rgb(\"#555555\"))"
            "[Universidad Nacional de Mar del Plata]\n"
            "#text(8.5pt)[_Competencias asociadas:_ biologia humana, "
            "fisiologia, histologia, bioseguridad, terminologia medica, "
            "lectura cientifica, salud publica y analisis de sistemas "
            "biologicos.]",

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
            "*Foco principal:* Control de calidad, laboratorio, documentacion "
            "tecnica, registro preciso, trazabilidad y seguimiento de procesos.",

            "*Base cientifica:* biologia, fisiologia, quimica basica, salud, "
            "lectura critica de informacion cientifica y terminologia medica.",

            "*Control de procesos:* control de stock, deteccion de desvios, "
            "reportes, seguimiento de insumos, control operativo y revision "
            "de inconsistencias.",

            "*Datos y documentacion:* Excel avanzado, tablas dinamicas, "
            "formulas, control de informacion, reportes y documentacion de procedimientos.",

            "*Stack practico:* Python, Go, PostgreSQL, SvelteKit, TypeScript, "
            "POS Morphi, Windows, Linux, soporte IT y troubleshooting.",

            "*Complementario:* seguridad web, Cloudflare e IA aplicada a "
            "documentacion tecnica, revision de procesos y analisis de datos.",
        ),
        idiomas=(
            "Ingles nivel funcional alto. Lectura fluida de documentacion "
            "tecnica, cientifica y material especializado."
        ),
        fortalezas=(
            "Pensamiento analitico.",
            "Observacion detallada.",
            "Deteccion de desviaciones e inconsistencias.",
            "Capacidad para seguir y mejorar procesos.",
            "Aprendizaje continuo.",
            "Priorizacion en entornos operativos.",
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
