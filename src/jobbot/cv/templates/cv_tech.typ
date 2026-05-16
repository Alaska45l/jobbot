#set document(title: "CV - {{ NOMBRE }}")
#set page(margin: 1.4cm)
#set text(size: 9.5pt)

#let accent = rgb("#1f5fbf")
#let kw_list = ({{ KEYWORDS }})

= {{ NOMBRE }}
#text(fill: accent, weight: "bold")[{{ TITULO }}]

{{ UBICACION }} | {{ EMAIL }} | {{ LINKEDIN }} | {{ PORTFOLIO }} | {{ GITHUB }}

== Enfoque para {{ EMPRESA }}
#for kw in kw_list [
  #box(stroke: accent, inset: 3pt, radius: 2pt)[#kw]
  #h(3pt)
]

== Perfil
{{ SUMMARY }}

== Experiencia relevante
{{ EXPERIENCE }}

== Stack y seguridad
{{ SKILLS }}

== Educación y certificaciones
{{ EDUCATION }}

{{ CERTIFICATIONS }}

