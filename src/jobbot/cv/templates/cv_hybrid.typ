#set document(title: "CV - {{ NOMBRE }}")
#set page(margin: 1.35cm)
#set text(size: 9.3pt)

#let accent = rgb("#7a4b12")
#let kw_list = ({{ KEYWORDS }})

= {{ NOMBRE }}
#text(fill: accent, weight: "bold")[{{ TITULO }}]

{{ UBICACION }} | {{ EMAIL }} | {{ LINKEDIN }} | {{ PORTFOLIO }} | {{ GITHUB }}

== Prioridades para {{ EMPRESA }}
#for kw in kw_list [
  #box(stroke: accent, inset: 3pt, radius: 2pt)[#kw]
  #h(3pt)
]

== Perfil híbrido
{{ SUMMARY }}

== Experiencia transversal
{{ EXPERIENCE }}

== Operaciones, sistemas y automatización
{{ SKILLS }}

== Educación y certificaciones
{{ EDUCATION }}

{{ CERTIFICATIONS }}

