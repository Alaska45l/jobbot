// cv_backoffice.typ — Perfil Back Office / E-commerce / Operaciones Digitales
// Diseno profesional: foto con esquinas redondeadas, secciones con linea, sin emojis.
#set document(title: "CV - {{ NOMBRE }}")
#set page(
  paper: "a4",
  margin: (top: 1.2cm, bottom: 1.6cm, left: 1.4cm, right: 1.4cm),
)
#set text(font: "IBM Plex Sans", size: 9pt, fill: rgb("#1a1a1a"))
#set par(justify: true, leading: 0.55em)

#let accent = rgb("#2D5F5D")
#let rule-color = rgb("#2D5F5D")
#let light-gray = rgb("#555555")

// -- Section heading --
#let section(title) = {
  v(8pt)
  text(11pt, fill: accent, weight: "bold", tracking: 0.5pt)[#upper(title)]
  v(-2pt)
  line(length: 100%, stroke: 0.8pt + rule-color)
  v(4pt)
}

// ================================================================
// HEADER — sin GitHub (perfil no-dev)
// ================================================================

#grid(
  columns: (1fr, 3.7cm),
  gutter: 12pt,
  {
    text(24pt, weight: "bold", fill: rgb("#1a1a1a"))[{{ NOMBRE }}]
    v(-2pt)
    text(11pt, fill: accent)[{{ TITULO }}]
    v(4pt)
    text(8.5pt, fill: light-gray)[
      *Mail:* #link("mailto:{{ EMAIL }}")[{{ EMAIL }}]
      #h(8pt) *Tel:* {{ TELEFONO }}
      \
      *Portfolio:* #link("https://{{ PORTFOLIO }}")[{{ PORTFOLIO }}]
      #h(8pt) *Ubicacion:* {{ UBICACION }}
    ]
  },
  {
    align(right + top,
      box(
        clip: true,
        radius: 8pt,
        width: 3.5cm,
        height: 3.5cm,
        image("perfil.webp", width: 3.5cm, height: 3.5cm, fit: "cover"),
      )
    )
  },
)

// ================================================================
// PERFIL PROFESIONAL
// ================================================================

#section[Perfil Profesional]

{{ SUMMARY }}

// ================================================================
// EXPERIENCIA PROFESIONAL
// ================================================================

#section[Experiencia Profesional]

{{ EXPERIENCE }}

// ================================================================
// EDUCACION
// ================================================================

#section[Educacion]

{{ EDUCATION }}

// ================================================================
// HABILIDADES TECNICAS Y OPERATIVAS
// ================================================================

#section[Habilidades Tecnicas y Operativas]

{{ SKILLS }}

// ================================================================
// IDIOMAS
// ================================================================

#section[Idiomas]

{{ IDIOMAS }}

// ================================================================
// FORTALEZAS
// ================================================================

#section[Fortalezas]

{{ FORTALEZAS }}
