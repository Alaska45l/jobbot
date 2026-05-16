#set document(title: "CV - {{ NOMBRE }}")
#set page(margin: 1.4cm)
#set text(size: 9.5pt)

#let accent = rgb("#2f6f4e")
#let kw_list = ({{ KEYWORDS }})

= {{ NOMBRE }}
#text(fill: accent, weight: "bold")[{{ TITULO }}]

{{ UBICACION }} | {{ EMAIL }} | {{ LINKEDIN }} | {{ PORTFOLIO }} | {{ GITHUB }}

== Ajuste con {{ EMPRESA }}
#for kw in kw_list [
  #box(stroke: accent, inset: 3pt, radius: 2pt)[#kw]
  #h(3pt)
]

== Perfil
{{ SUMMARY }}

== Experiencia administrativa e IT
{{ EXPERIENCE }}

== Herramientas
{{ SKILLS }}

== Educación y certificaciones
{{ EDUCATION }}

{{ CERTIFICATIONS }}

