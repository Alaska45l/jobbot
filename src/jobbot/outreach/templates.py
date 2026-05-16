"""Cold outreach templates in Argentine Spanish."""

ASUNTOS: tuple[str, ...] = (
    "Perfil Admin/IT para {nombre_empresa}",
    "Postulación espontánea | {nombre_empresa}",
    "CV para futuras búsquedas",
    "{nombre_empresa} - candidatura espontánea",
    "Soporte IT y administración",
    "Interés en sumarme al equipo",
    "CV adjunto - perfil técnico operativo",
    "Consulta por oportunidades en {nombre_empresa}",
    "Perfil híbrido para operaciones e IT",
    "Presentación profesional - {nombre_empresa}",
    "Administración, soporte y automatización",
    "Candidatura desde Mar del Plata",
)

CUERPOS: tuple[str, ...] = (
    """\
Buenos días,

Mi nombre es {nombre_remitente} y me comunico para dejar mi candidatura espontánea \
en {nombre_empresa}.

Mi perfil combina administración de oficina con conocimientos técnicos en soporte IT, \
lo que me permite gestionar tareas operativas, resolver incidencias de sistemas, \
administrar accesos y documentar procesos internos de forma autónoma.

Adjunto mi CV para que puedan evaluarlo. Quedo a disposición ante cualquier consulta.

{firma}""",

    """\
Hola,

Soy {nombre_remitente} y les escribo desde Mar del Plata para compartir mi perfil \
con {nombre_empresa}.

Cuento con experiencia en gestión administrativa, atención a proveedores y clientes, \
herramientas de oficina y un fuerte componente técnico: soporte de primer nivel, \
scripting para automatizar tareas repetitivas y administración básica de redes.

Me pareció útil acercarles mi CV directamente por si el perfil encaja con alguna \
búsqueda actual o futura.

{firma}""",

    """\
Estimado equipo de {nombre_empresa}:

Soy {nombre_remitente} y me postulo de forma espontánea. Tengo experiencia en \
administración, operaciones y soporte IT, con disponibilidad para cubrir tareas \
operativas y técnicas dentro de equipos chicos o medianos.

Adjunto mi CV en PDF. Estoy disponible para una entrevista cuando lo consideren \
conveniente.

Saludos cordiales,

{firma}""",

    """\
Buenas tardes,

Mi nombre es {nombre_remitente}. Encontré información sobre {nombre_empresa} y me \
resultó interesante la posibilidad de sumarme al equipo.

Tengo experiencia cubriendo roles que suelen dividirse entre administración y soporte \
técnico: puedo redactar informes, coordinar con proveedores, diagnosticar fallas de \
red y automatizar tareas repetitivas con scripts.

Si el perfil les resulta relevante, con gusto ampliamos información.

{firma}""",

    """\
Hola equipo de {nombre_empresa},

Les escribo para dejar mi CV ante la posibilidad de que necesiten reforzar el área \
administrativa, operativa o de soporte técnico.

Soy {nombre_remitente}, con manejo de herramientas ofimáticas, facturación, gestión \
de stock, redes, sistemas operativos y automatización de procesos. Resido en Mar del \
Plata y puedo adaptarme a equipos administrativos, técnicos o mixtos.

Gracias por considerar mi postulación.

{firma}""",

    """\
Buenos días,

Soy {nombre_remitente}. Les comparto mi CV porque mi experiencia puede ser útil para \
{nombre_empresa} en tareas donde se cruzan administración, soporte a usuarios y mejora \
de procesos internos.

En trabajos anteriores combiné gestión operativa, facturación, control de stock, \
mantenimiento IT de sucursales y resolución de problemas técnicos cotidianos.

Quedo a disposición.

{firma}""",

    """\
Hola,

Me presento: soy {nombre_remitente}, de Mar del Plata. Trabajo con una combinación de \
software, infraestructura y operaciones administrativas.

Además de soporte técnico y redes, tengo experiencia real en entornos con presión \
operativa: control de stock, POS, facturación, coordinación de equipos y documentación \
de procesos. Por eso pensé que mi perfil podía ser pertinente para {nombre_empresa}.

Adjunto mi CV.

{firma}""",

    """\
Estimado equipo:

Soy {nombre_remitente}. Les envío mi CV para que lo tengan presente en búsquedas \
actuales o futuras de {nombre_empresa}.

Mi perfil está orientado a resolver problemas concretos: ordenar procesos, asistir a \
usuarios, mantener equipos, documentar procedimientos y automatizar tareas repetitivas \
cuando conviene hacerlo con Python o scripting.

Saludos,

{firma}""",

    """\
Buenas tardes,

Soy {nombre_remitente}. Me interesa acercar mi perfil a {nombre_empresa} porque puedo \
aportar en un punto intermedio entre administración, soporte técnico e implementación \
de herramientas internas.

Tengo experiencia con Windows/Linux, redes TCP/IP, Excel avanzado, facturación, POS, \
stock, recuperación de datos y desarrollo de utilidades para trabajo operativo.

Adjunto CV en PDF.

{firma}""",

    """\
Hola {nombre_empresa},

Les escribo de manera directa para evitar una postulación genérica por portal. Soy \
{nombre_remitente} y busco sumarme a una organización donde mi combinación de soporte \
IT, administración y automatización tenga uso práctico.

El CV adjunto resume mi experiencia en operaciones, sistemas, desarrollo y soporte.

Muchas gracias por su tiempo.

{firma}""",

    """\
Buenos días,

Quería dejarles una presentación breve. Soy {nombre_remitente}, con experiencia en \
gestión operativa, soporte IT, documentación, redes y automatización.

Si {nombre_empresa} necesita una persona que pueda moverse entre tareas administrativas \
y técnicas sin depender siempre de terceros, mi perfil puede resultarles útil.

Adjunto mi CV para evaluación.

{firma}""",

    """\
Estimados/as,

Mi nombre es {nombre_remitente}. Les acerco mi candidatura espontánea para \
{nombre_empresa}, con foco en administración, soporte técnico y mejora de procesos.

Mi experiencia combina trabajo operativo real con formación en desarrollo de software, \
sistemas, Linux/Windows, redes y seguridad aplicada. Esa mezcla me permite entender \
problemas de oficina y también resolver la parte técnica cuando aparece.

Quedo atenta.

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
