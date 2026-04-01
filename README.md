
<div align="center">
  <h1>JobBot</h1>
  <p><strong>Automated OSINT and Prospecting Pipeline for Targeted Job Searching</strong></p>

  <img src="https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/Playwright-Async-2EAD33?style=flat-square&logo=playwright&logoColor=white" alt="Playwright">
  <img src="https://img.shields.io/badge/SQLite-3-003B57?style=flat-square&logo=sqlite&logoColor=white" alt="SQLite3">
  <img src="https://img.shields.io/badge/License-MIT-blue?style=flat-square" alt="License MIT">
</div>

---

JobBot es un pipeline asíncrono de prospección y contacto automatizado diseñado para búsquedas laborales dirigidas. Utiliza técnicas de dorking para recolección de objetivos, scraping sigiloso para extracción de datos de contacto (Management/HR) y un motor SMTP con rate-limiting para el envío de currículums contextualizados según un sistema de Lead Scoring.

## <img src="https://api.iconify.design/material-symbols/account-tree.svg?color=%23007acc" width="24" height="24" align="center"> Arquitectura y Características

* **Dorking Engine**: Automatiza consultas avanzadas (operadores site:, intext:) en motores de búsqueda para sembrar la base de datos con dominios relevantes por zona y rubro.
* **Stealth Scraper (Playwright)**: Navegación asíncrona concurrente con evasión de huellas (webdriver spoofing, rotación de User-Agents/Viewports). Bloqueo de carga de media a nivel de red para optimización de ancho de banda.
* **Lead Scoring**: Analizador léxico local que evalúa el HTML renderizado para asignar un puntaje de relevancia (0-100+) y decidir qué perfil de CV (CV_Tech o CV_Admin_IT) se adapta mejor a la empresa objetivo.
* **Smart Dispatcher**: Cliente SMTP con soporte para Dry-Run, manejo de colas y tiempos de espera aleatorios (jitter) para evadir filtros de spam y bloqueos de cuenta.
* **Estado Persistente**: Sistema de cooldown de 90 días por dominio y registro de campañas basado en SQLite para evitar envíos duplicados.

---

## <img src="https://api.iconify.design/material-symbols/terminal.svg?color=%23007acc" width="24" height="24" align="center"> Uso del Pipeline

El sistema se opera mediante un script wrapper (start_bot.sh) que inyecta las variables de entorno y ejecuta la CLI en tres fases independientes.

### Fase 1: Recolección de Semillas (Dorking)
Alimenta la base de datos local con URLs candidatas basadas en los rubros especificados.

```bash
./start_bot.sh --dork --rubros "software house" "clínica" "estudio contable" --limite-dork 30
````

### Fase 2: Extracción y Scoring (Scraping)

Despliega headless browsers concurrentes para visitar las semillas, extraer correos corporativos y perfiles de LinkedIn, y calcular el score de la empresa.

```bash
./start_bot.sh --scrape --concurrencia 5
```

### Fase 3: Despacho SMTP (Mailing)

Filtra los objetivos por puntaje mínimo y ejecuta el envío de correos. Se recomienda encarecidamente usar --dry-run primero para auditar la construcción de los mensajes de forma segura.

```bash
# Auditoría en terminal (No abre conexión SMTP)
./start_bot.sh --mail --dry-run --min-score 30

# Ejecución real en lotes de 10 envíos
./start_bot.sh --mail --min-score 30 --limite 10
```

-----

## <img src="https://api.iconify.design/material-symbols/tune.svg?color=%23007acc" width="24" height="24" align="center"> Referencia CLI

| Argumento | Tipo | Default | Descripción |
| :--- | :--- | :--- | :--- |
| --dork | Flag | False | Ejecuta el módulo de búsqueda (DuckDuckGo). |
| --rubros | List | (Internos) | Rubros a buscar (ej. "QA testing" "soporte técnico"). |
| --limite-dork | Int | 10 | Cantidad de dominios a extraer por rubro. |
| --scrape | Flag | False | Ejecuta el módulo Playwright de extracción. |
| --concurrencia | Int | 3 | Threads de Playwright en paralelo. |
| --mail | Flag | False | Ejecuta el motor SMTP. |
| --min-score | Int | 55 | Puntaje mínimo de la DB para considerar la empresa apta. |
| --limite | Int | 0 | Máximo de correos a despachar en la ejecución actual. |
| --dry-run | Flag | False | Simula el envío y loguea el output generado sin enviar. |

-----

## <img src="https://api.iconify.design/material-symbols/settings.svg?color=%23007acc" width="24" height="24" align="center"> Configuración del Entorno

JobBot requiere credenciales válidas y configuración de variables de entorno para operar. Estas deben definirse en el archivo start\_bot.sh:

```bash
# Servidor SMTP (Recomendado: Gmail con App Password)
export SMTP_HOST="smtp.gmail.com"
export SMTP_PORT="587"
export SMTP_USER="tu_correo@gmail.com"
export SMTP_PASS="tu_app_password_de_16_caracteres"

# Perfil del Remitente
export SENDER_NAME="Tu Nombre"
export GITHUB_USER="TuUsuarioGitHub"
export LINKEDIN_USER="TuUsuarioLinkedIn"
```

**Estructura de Directorios Requerida:**
Los archivos PDF deben ubicarse en la carpeta cvs/ en la raíz del proyecto para que el motor de adjuntos los detecte:

  * cvs/CV\_Tech.pdf
  * cvs/CV\_Admin\_IT.pdf

-----

## <img src="https://api.iconify.design/material-symbols/database.svg?color=%23007acc" width="24" height="24" align="center"> Gestión de Base de Datos

Toda la metadata se almacena en jobbot.db. Para reiniciar métricas, liberar el cooldown de 90 días o realizar un borrado de la base (Wipe), utilice su cliente SQL preferido (ej. sqlite3):

```sql
-- Limpiar historial de envíos (reinicia el cooldown)
DELETE FROM campanas_envios;

-- Wipe total (Borrar inteligencia recolectada)
DELETE FROM contactos;
DELETE FROM empresas;
```

-----

*Disclaimer: Esta herramienta está diseñada para uso personal y optimización de tiempo. Configure siempre rate-limits responsables y evite enviar correos no solicitados a objetivos de baja puntuación.*