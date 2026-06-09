// ─────────────────────────────────────────────────────────────────────────────
// cv_it_qa.typ — IT · QA · Soporte Técnico
// Diseño cascading moderno con acentos pastel verde
// ─────────────────────────────────────────────────────────────────────────────

#set document(title: "CV - {{ NOMBRE }}")
#set page(
  paper: "a4",
  margin: (top: 1.2cm, bottom: 1.0cm, left: 1.3cm, right: 1.3cm),
)
#set text(font: "IBM Plex Sans", size: 9pt, fill: rgb("#2D2D2D"))
#set par(justify: true, leading: 0.55em)

// ── Paleta ──────────────────────────────────────────────────────────────────
#let accent     = rgb("#7BAE7F")
#let accent-lt  = rgb("#A8D5BA")
#let accent-dk  = rgb("#4A7A4E")
#let bg-pill    = rgb("#E8F5E9")
#let text-dark  = rgb("#1E1E1E")
#let text-muted = rgb("#555555")
#let divider-c  = rgb("#C8E6C9")

// ── Helpers ─────────────────────────────────────────────────────────────────
#let kw_list = ({{ KEYWORDS }})

#let divider() = {
  v(4pt)
  line(length: 100%, stroke: 0.6pt + divider-c)
  v(4pt)
}

#let section-title(body) = {
  divider()
  text(size: 11pt, weight: "bold", fill: accent-dk)[#body]
  v(3pt)
}

#let pill(content) = {
  box(
    fill: bg-pill,
    stroke: 0.5pt + accent-lt,
    inset: (x: 7pt, y: 3pt),
    radius: 10pt,
  )[#text(size: 7.5pt, fill: accent-dk, weight: "medium")[#content]]
}

// ═════════════════════════════════════════════════════════════════════════════
//  HEADER
// ═════════════════════════════════════════════════════════════════════════════

// Foto de perfil (descomentar si existe el archivo perfil.jpg)
// #place(top + right, dy: 0cm, dx: 0cm)[
//   #box(clip: true, radius: 6pt, width: 2.2cm)[
//     #image("perfil.jpg", width: 2.2cm)
//   ]
// ]

#text(size: 22pt, weight: "bold", fill: text-dark)[{{ NOMBRE }}]
#v(1pt)
#text(size: 11pt, weight: "semibold", fill: accent)[{{ TITULO }}]
#v(5pt)

// ── Contacto en fila horizontal ─────────────────────────────────────────────
#set text(size: 7.8pt, fill: text-muted)
#grid(
  columns: (auto, auto, auto, auto, auto),
  column-gutter: 12pt,
  [📍 {{ UBICACION }}],
  [✉ {{ EMAIL }}],
  [🔗 {{ LINKEDIN }}],
  [🌐 {{ PORTFOLIO }}],
  [💻 {{ GITHUB }}],
)
#set text(size: 9pt, fill: rgb("#2D2D2D"))

// ═════════════════════════════════════════════════════════════════════════════
//  ENFOQUE PARA LA EMPRESA
// ═════════════════════════════════════════════════════════════════════════════

#section-title[🎯 Enfoque para {{ EMPRESA }}]

#text(size: 8.5pt, fill: text-muted, style: "italic")[
  Postulación para: #text(weight: "semibold", fill: accent-dk)[{{ PUESTO_OBJETIVO }}]
]
#v(3pt)

#text(size: 8.5pt)[{{ PARRAFO_EMPRESA }}]
#v(4pt)

#{
  for kw in kw_list {
    pill(kw)
    h(4pt)
  }
}

// ═════════════════════════════════════════════════════════════════════════════
//  PERFIL
// ═════════════════════════════════════════════════════════════════════════════

#section-title[👤 Perfil técnico]

#text(size: 8.5pt)[{{ SUMMARY }}]

// ═════════════════════════════════════════════════════════════════════════════
//  EXPERIENCIA TÉCNICA
// ═════════════════════════════════════════════════════════════════════════════

#section-title[💼 Experiencia técnica]

#set list(marker: text(fill: accent)[●], indent: 4pt, body-indent: 6pt)
#text(size: 8.5pt)[
{{ EXPERIENCE }}
]

// ═════════════════════════════════════════════════════════════════════════════
//  STACK Y HERRAMIENTAS
// ═════════════════════════════════════════════════════════════════════════════

#section-title[🛠️ Stack y herramientas]

#set list(marker: text(fill: accent)[▸], indent: 4pt, body-indent: 6pt)
#text(size: 8.5pt)[
{{ SKILLS }}
]

// ═════════════════════════════════════════════════════════════════════════════
//  EDUCACIÓN Y CERTIFICACIONES
// ═════════════════════════════════════════════════════════════════════════════

#section-title[🎓 Educación y certificaciones]

#set list(marker: text(fill: accent)[◆], indent: 4pt, body-indent: 6pt)

#grid(
  columns: (1fr, 1fr),
  column-gutter: 16pt,
  [
    #text(size: 8pt, weight: "semibold", fill: accent-dk)[Formación académica]
    #text(size: 8.5pt)[
{{ EDUCATION }}
    ]
  ],
  [
    #text(size: 8pt, weight: "semibold", fill: accent-dk)[Certificaciones]
    #text(size: 8.5pt)[
{{ CERTIFICATIONS }}
    ]
  ],
)
