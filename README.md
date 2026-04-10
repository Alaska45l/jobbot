<div align="center">
  <h1>JobBot v2.4</h1>
  <p><strong>Automated OSINT, Stealth Scraping & Dynamic Pipeline for Targeted Job Searching</strong></p>

  <img src="https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/Playwright-Stealth-2EAD33?style=flat-square&logo=playwright&logoColor=white" alt="Playwright">
  <img src="https://img.shields.io/badge/Typst-Dynamic_CV-239BBE?style=flat-square&logo=typst&logoColor=white" alt="Typst">
  <img src="https://img.shields.io/badge/SQLite-3-003B57?style=flat-square&logo=sqlite&logoColor=white" alt="SQLite3">
</div>

---

JobBot es un pipeline asíncrono de prospección B2B y contacto automatizado. Utiliza técnicas de dorking a nivel nacional para recolección de objetivos, scraping con evasión de firewalls (WAFs) para extraer datos de contacto, y un motor SMTP/WhatsApp que compila y envía currículums hiper-personalizados al vuelo.

## <img src="https://api.iconify.design/material-symbols/lightbulb.svg?color=%23007acc" width="24" height="24" align="center"> Why JobBot? (Architecture & Philosophy)

JobBot nace de una necesidad real y de la frustración con el ecosistema actual de reclutamiento. Después de meses de enviar solicitudes a través de portales tradicionales (ZonaJobs, Randstad, Bumeran, LinkedIn) con tasas de respuesta bajísimas, decidí cambiar la estrategia y volver a lo básico: entregar el CV directamente en la puerta de la empresa. Pero en lugar de hacerlo a pie una tarde de lluvia, decidí automatizarlo.

Al buscar herramientas de automatización de Cold Emailing u OSINT en GitHub, me encontré con un problema: casi todos los repositorios actuales son simples "wrappers" que requieren tarjetas de crédito para pagar costosas APIs de IA generativa (OpenAI, Claude) solo para leer un HTML básico.

Por eso construí JobBot bajo una filosofía técnica estricta:

* **Determinismo sobre Alucinación:** El bot no utiliza LLMs para tareas de clasificación. Emplea un motor léxico propio en Python puro con expresiones regulares para decidir el score del prospecto sin equivocarse.
* **Evasión Stealth:** Supera protecciones como Cloudflare o Datadome utilizando `playwright-stealth`, rotación de contextos, spoofing de zona horaria y simulación de biometría humana (movimientos de mouse y scroll aleatorio).
* **CVs Mutantes (Typst):** En lugar de enviar un PDF genérico, el orquestador lee la web de la empresa e inyecta las *keywords* exactas de la compañía en una plantilla Typst, compilando un PDF único en milisegundos antes de enviarlo.
* **Daemon 24/7 Resiliente:** Diseñado para correr de fondo en `tmux`. Cuenta con manejo seguro de señales de apagado (`SIGTERM`), timeouts estrictos anti-livelock y pausas aleatorias (Jitter) para evitar baneos de IP o de cuenta SMTP.

---

## <img src="https://api.iconify.design/material-symbols/layers-outline.svg?color=%23007acc" width="24" height="24" align="center"> Arquitectura y Características

* **Dorking Engine Nacional:** Automatiza consultas avanzadas leyendo listas de rubros dinámicas desde un archivo externo (`rubros.txt`).
* **Rich TUI (Terminal User Interface):** Un panel de control y telemetría inspirado en la estética retro-futurista de terminales de monitoreo.
* **Módulo WhatsApp Web:** Despacho automatizado de mensajes de presentación directa a líneas celulares extraídas de la web, con manejo de sesiones locales y rate-limits adaptables.
* **Smart Dispatcher (SMTP):** Cliente de correo con soporte para Dry-Run y colas asíncronas.

---

## <img src="https://api.iconify.design/material-symbols/settings-outline.svg?color=%23007acc" width="24" height="24" align="center"> Configuración del Entorno

### 1. Variables de Entorno (`.env`)
Crear un archivo `.env` en la raíz del proyecto para credenciales sensibles:

```env
# Servidor SMTP (Recomendado: Gmail con App Password)
SMTP_HOST="smtp.gmail.com"
SMTP_PORT="587"
SMTP_USER="tu_correo@gmail.com"
SMTP_PASS="tu_app_password_de_16_caracteres"

# Perfil del Remitente
SENDER_NAME="Tu Nombre"
GITHUB_USER="TuUsuarioGitHub"
LINKEDIN_USER="TuUsuarioLinkedIn"
```

### 2\. Dependencias Externas del Sistema

  * **Typst:** Se requiere el binario de Typst en el `PATH` para la compilación dinámica de CVs (`cargo install typst-cli` o vía su repositorio oficial).
  * **Plantilla Base:** La plantilla a renderizar debe ubicarse en `cvs/template.typ` (opcional: imagen en `cvs/perfil.jpg`).

-----

## <img src="https://api.iconify.design/material-symbols/terminal.svg?color=%23007acc" width="24" height="24" align="center"> Uso del Pipeline

El sistema se opera mediante el orquestador principal (`main.py`).

### Modo Daemon (Recomendado)

Ejecuta el ciclo completo (Dork -\> Scrape -\> Mail) en un loop infinito con manejo de timeouts y descansos anti-ban.

```bash
python main.py --auto --concurrencia 3
```

### Ejecuciones Manuales (Por Fases)

**Fase 1: Recolección de Semillas (Dorking)**

```bash
python main.py --dork --rubros-file rubros.txt --limite-dork 30
```

**Fase 2: Extracción y Scoring (Scraping)**

```bash
python main.py --scrape --concurrencia 3
```

**Fase 3: Despacho (Mailing o WhatsApp)**

```bash
# Auditoría en terminal (Dry-Run: No acciona envíos reales)
python main.py --mail --dry-run --min-score 55
python main.py --wa --dry-run

# Ejecución real
python main.py --mail --min-score 55
python main.py --wa
```

-----

## <img src="https://api.iconify.design/material-symbols/database.svg?color=%23007acc" width="24" height="24" align="center"> Gestión de Base de Datos

Toda la metadata se almacena en `jobbot.db`. Para reiniciar métricas, liberar el cooldown de 90 días o realizar un borrado de la base (Wipe), utilice su cliente SQL preferido:

```sql
-- Limpiar historial de envíos (reinicia el cooldown de SMTP)
DELETE FROM campanas_envios;

-- Wipe total (Borrar inteligencia recolectada)
DELETE FROM contactos;
DELETE FROM empresas;
```

-----

## <img src="https://api.iconify.design/material-symbols/list-alt-outline.svg?color=%23007acc" width="24" height="24" align="center"> Referencia CLI

| Argumento | Tipo | Default | Descripción |
| :--- | :--- | :--- | :--- |
| `--auto` | Flag | False | Inicia el Daemon de ejecución continua 24/7. |
| `--dork` | Flag | False | Ejecuta el módulo de búsqueda OSINT. |
| `--rubros-file` | String | rubros.txt | Archivo txt con la lista dinámica de rubros. |
| `--scrape` | Flag | False | Ejecuta el módulo Playwright Stealth. |
| `--concurrencia` | Int | 3 | Threads de navegadores en paralelo. |
| `--mail` | Flag | False | Ejecuta el motor SMTP con Typst. |
| `--wa` | Flag | False | Ejecuta el motor de envíos por WhatsApp Web. |
| `--min-score` | Int | 55 | Puntaje mínimo de la DB para prospectar. |
| `--dry-run` | Flag | False | Simula el envío sin accionar SMTP/WA. |