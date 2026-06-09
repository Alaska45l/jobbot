#set document(title: "CV - {{ NOMBRE }}")
#set page(margin: 1.4cm)
#set text(size: 9.5pt)

#let accent = rgb("#2f6f4e")

= {{ NOMBRE }}
#text(fill: accent, weight: "bold")[{{ TITULO }}]

{{ UBICACION }} | {{ EMAIL }} | {{ PORTFOLIO }} | {{ GITHUB }}

== Perfil
{{ SUMMARY }}

== Experiencia administrativa e IT
{{ EXPERIENCE }}

== Herramientas
{{ SKILLS }}

== Educación y certificaciones
{{ EDUCATION }}
