"""Email templates for ATICMA direct job application in Argentine Spanish."""

ASUNTOS: tuple[str, ...] = (
    "Postulación - Perfil técnico para {nombre_empresa}",
    "CV adjunto | Postulación espontánea - {nombre_empresa}",
    "Postulación directa | {nombre_empresa}",
    "{nombre_empresa} - Me interesa sumarme al equipo",
    "Candidatura espontánea | Perfil IT para {nombre_empresa}",
    "Postulación | Perfil analítico-técnico - {nombre_empresa}",
    "CV - Postulación para {nombre_empresa}",
    "Me interesa trabajar en {nombre_empresa}",
    "Postulación espontánea desde Mar del Plata",
    "Candidatura | Perfil técnico con experiencia operativa",
    "Postulación directa para {nombre_empresa}",
    "{nombre_empresa} - Postulación espontánea con CV adjunto",
)

CUERPOS: tuple[str, ...] = (
    """\
Buenos días,

Mi nombre es {nombre_remitente} y me comunico porque estoy interesada en \
sumarme al equipo de {nombre_empresa}.

Mi perfil combina experiencia técnica en desarrollo, QA y soporte IT con \
capacidad analítica y atención al detalle. Tengo formación en programación, \
sistemas y ciencias, y me destaco detectando errores, documentando procesos \
y resolviendo problemas técnicos de forma autónoma.

Adjunto mi CV para que puedan evaluarlo. Quedo a disposición para una \
entrevista o cualquier consulta.

{firma}""",

    """\
Hola,

Soy {nombre_remitente}, de Mar del Plata. Les escribo para postularme \
de forma directa a {nombre_empresa}.

Cuento con experiencia en soporte IT, desarrollo web, automatización y \
operaciones. Mi perfil es técnico-analítico: me desenvuelvo bien detectando \
fallos, ordenando información, documentando procesos y aprendiendo \
herramientas nuevas rápidamente.

Adjunto mi CV en PDF. Estoy disponible para ampliar información.

{firma}""",

    """\
Estimado equipo de {nombre_empresa}:

Me postulo de forma espontánea para oportunidades actuales o futuras en \
su equipo. Soy {nombre_remitente} y tengo experiencia en desarrollo de \
software, soporte técnico, operaciones y documentación.

Mi fortaleza principal es la capacidad de análisis: encuentro dónde se \
rompe el sistema, documento el problema con claridad y propongo soluciones. \
Tengo formación en programación, física y medicina.

Adjunto CV. Quedo atenta.

{firma}""",

    """\
Buenas tardes,

Soy {nombre_remitente}. Vi que {nombre_empresa} forma parte de la comunidad \
tecnológica de Mar del Plata y me interesa postularme.

Mi perfil es técnico con fuerte componente analítico: programación \
(Go, Python, TypeScript), troubleshooting, QA, seguridad aplicada y \
documentación técnica. Busco un entorno donde pueda aportar precisión, \
criterio y resolución de problemas.

Si el perfil resulta relevante, con gusto coordinamos una entrevista.

{firma}""",

    """\
Hola equipo de {nombre_empresa},

Les escribo para dejar mi candidatura directa. Soy {nombre_remitente} \
y busco integrarme a un equipo técnico donde mi capacidad de análisis, \
detección de errores y resolución de problemas tengan aplicación real.

Tengo experiencia en desarrollo full-stack, soporte IT, operaciones, \
QA y automatización. Resido en Mar del Plata y puedo adaptarme a \
modalidad presencial, híbrida o remota.

Adjunto mi CV para evaluación.

{firma}""",

    """\
Buenos días,

Soy {nombre_remitente} y les acerco mi postulación espontánea a \
{nombre_empresa}. Mi perfil combina desarrollo de software, soporte \
técnico, operaciones y pensamiento analítico.

En experiencias anteriores demostré capacidad para gestionar sistemas, \
detectar inconsistencias, documentar procesos y resolver problemas \
técnicos en entornos de presión.

Adjunto CV en PDF.

{firma}""",

    """\
Hola,

Me presento: soy {nombre_remitente}, de Mar del Plata. Estoy buscando \
sumarme a un equipo donde pueda aplicar mis habilidades técnicas y \
analíticas.

Tengo experiencia en programación (Go, Python, Rust), testing, soporte \
IT, redes, automatización y documentación. Mi formación incluye \
programación, física y medicina parcial. Me destaco encontrando \
problemas antes de que se conviertan en incidentes.

Si {nombre_empresa} tiene búsquedas abiertas o futuras, adjunto mi CV.

{firma}""",

    """\
Estimado equipo:

Soy {nombre_remitente}. Les envío mi CV para postulación espontánea \
en {nombre_empresa}.

Mi perfil está orientado a resolver problemas concretos: testear \
sistemas, detectar fallos, documentar procedimientos, ordenar datos \
y automatizar tareas repetitivas. Tengo experiencia real en entornos \
operativos y formación técnica autodidacta.

Quedo a disposición.

{firma}""",

    """\
Buenas tardes,

Soy {nombre_remitente}. Me interesa postularme a {nombre_empresa} \
porque creo que mi perfil técnico-analítico puede aportar valor.

Tengo experiencia con desarrollo web, bases de datos, Linux, redes, \
seguridad aplicada (OWASP), testing y operaciones. Trabajo bien con \
estructura y procesos, y prefiero resolver problemas técnicos antes \
que tareas sociales repetitivas.

Adjunto CV en PDF.

{firma}""",

    """\
Hola {nombre_empresa},

Les escribo de manera directa para presentar mi candidatura. Soy \
{nombre_remitente} y busco un equipo donde mi combinación de soporte \
IT, desarrollo, QA y capacidad analítica tenga aplicación práctica.

El CV adjunto resume mi experiencia técnica, operativa y formativa.

Muchas gracias por su tiempo.

{firma}""",

    """\
Buenos días,

Quería presentarme de forma directa. Soy {nombre_remitente}, con \
experiencia en desarrollo de software, soporte IT, testing y \
operaciones.

Si {nombre_empresa} necesita una persona que detecte errores antes \
que nadie, documente procesos con claridad y resuelva problemas \
técnicos sin depender de terceros, mi perfil puede resultarles útil.

Adjunto mi CV para evaluación.

{firma}""",

    """\
Estimados/as,

Mi nombre es {nombre_remitente}. Me postulo de forma directa a \
{nombre_empresa}.

Mi experiencia combina trabajo operativo real con formación en \
desarrollo de software, sistemas, ciencias y seguridad aplicada. \
Me destaco en detección de patrones, resolución de problemas, \
documentación clara y aprendizaje autodidacta.

Adjunto CV. Quedo atenta a una respuesta.

{firma}""",
)

FIRMA_TEMPLATE: str = """\
{nombre_remitente}
Mar del Plata, Buenos Aires
Email: {email_remitente}
Web: {github_user}.github.io/  |  linkedin.com/in/{linkedin_user}

PD: Este correo y su adjunto fueron generados con JobBot, \
una herramienta de automatización de búsqueda laboral que desarrollé en Python. \
Podés ver el código en: github.com/{github_user}/jobbot"""
