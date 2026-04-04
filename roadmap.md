# <img src="https://api.iconify.design/material-symbols/robot.svg?color=%23007acc" width="34" height="34" align="center"> JobBot — Product Roadmap

Este documento detalla la evolución planificada para JobBot, pasando de un script de automatización personal a una herramienta integral B2B de OSINT y generación de leads.
<img src="https://api.iconify.design/material-symbols/check.svg?color=%23007acc" width="24" height="24" align="center"> Fase 1: MVP & Core Engine (v1.x) - Completado

El objetivo fundacional: crear una arquitectura asíncrona, determinista y de costo cero para búsqueda laboral.

    [x] Motor de OSINT: Scraping asíncrono sobre buscadores sin depender de APIs pagas.

    [x] Stealth Scraping: Implementación de Playwright con Chromium compartido y contextos aislados.

    [x] Lead Scoring Local: Algoritmo léxico para puntuar empresas y clasificar contactos (RRHH vs. General).

    [x] Base de Datos Resiliente: SQLite en modo WAL con claves foráneas para gestión de cooldowns anti-spam.

    [x] Mailer Engine: Envío automatizado con rotación de plantillas, adjuntos dinámicos y jitter (tiempos de espera aleatorios para cuidar la reputación del remitente).

    [x] Terminal UI (TUI): Dashboard en vivo con Rich, patrón snapshot y ventana deslizante para monitoreo sin cuellos de botella.

<img src="https://api.iconify.design/material-symbols/rocket.svg?color=%23007acc" width="24" height="24" align="center"> Fase 2: Hyper-Personalization & Omni-Channel (v2.0) - En desarrollo

Expansión de las vías de contacto y automatización de la creación de perfiles.

    [x] Módulo de WhatsApp (wa_sender.py):

        Incorporar expresiones regulares (_RE_WHATSAPP) en el Lead Scoring para cazar números locales.

        Usar un user_data_dir en Playwright para mantener una sesión de WhatsApp Web persistente.

        Envío de mensajes automatizados con rate limiting estricto para evitar baneos de Meta.

    [x] Generador de CVs Dinámico:

        Reemplazar los PDFs estáticos por plantillas HTML/CSS (Jinja2).

        Inyectar las keywords específicas encontradas durante el scraping de la empresa directamente en el currículum.

        Renderizar a PDF en milisegundos con typ o pdfkit antes de adjuntarlo.

    [ ] Mascota Interactiva en la TUI:

        Agregar un panel en la interfaz gráfica de la terminal con ASCII Art dinámico (estilo Claude Code).

        La mascota reaccionará al EstadoBot: durmiendo durante el rate limit, alerta durante el Dorking, o celebrando al encontrar un correo directo de RRHH.

    [ ] Módulo "Peces Gordos" (Bypass de ATS):

        Lista de dominios pre-cargados de las empresas corporativas más grandes de la región (que suelen usar plataformas tercerizadas de empleo).

        Scraping directo e intensivo a sus portales ignorando las exclusiones habituales de buscadores.

    [ ] Geospatial OSINT (Alternativa a Google Maps):

        Integración con la Overpass API (OpenStreetMap) (100% gratuita y Open Source).

        Extracción masiva de comercios por polígono geográfico y etiqueta (ej: clínicas, estudios contables) como semilla inicial, esquivando las restricciones y bloqueos de Google.

<img src="https://api.iconify.design/material-symbols/chart-data.svg?color=%23007acc" width="24" height="24" align="center"> Fase 3: Pro Release & GUI (v3.0) - Futuro

Conversión del proyecto en un software Desktop comercializable para agencias de reclutamiento y ventas B2B.

    [ ] Interfaz Gráfica de Usuario (GUI):

        Migración de la terminal a una aplicación de escritorio nativa utilizando CustomTkinter o PySide6.

        Formularios amigables para cargar variables (ciudad, rubros, archivos CV base).

    [ ] Modo "Agencia":

        Soporte para múltiples cuentas SMTP y rotación de remitentes.

        Tablero de analíticas integrado (Tasa de respuesta, correos rebotados, rubros más exitosos).

    [ ] Empaquetado y Distribución:

        Compilación de ejecutables standalone (.exe para Windows y binarios de Linux) usando PyInstaller o Nuitka para que usuarios sin conocimientos de Python puedan instalarlo con un clic.