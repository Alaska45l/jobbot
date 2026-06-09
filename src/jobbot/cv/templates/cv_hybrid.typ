#set document(title: "CV - {{ NOMBRE }}")
#set page(margin: 1.35cm)
#set text(size: 9.3pt)

#let accent = rgb("#7a4b12")

= {{ NOMBRE }}
#text(fill: accent, weight: "bold")[{{ TITULO }}]

{{ UBICACION }} | {{ EMAIL }} | {{ PORTFOLIO }} | {{ GITHUB }}

== Perfil híbrido
{{ SUMMARY }}

== Experiencia transversal
{{ EXPERIENCE }}

== Operaciones, sistemas y automatización
{{ SKILLS }}

== Educación y certificaciones
{{ EDUCATION }}
