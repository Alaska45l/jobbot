#set document(title: "CV - {{ NOMBRE }}")
#set page(margin: 1.4cm)
#set text(size: 9.5pt)

#let accent = rgb("#1f5fbf")

= {{ NOMBRE }}
#text(fill: accent, weight: "bold")[{{ TITULO }}]

{{ UBICACION }} | {{ EMAIL }} | {{ PORTFOLIO }} | {{ GITHUB }}

== Perfil
{{ SUMMARY }}

== Experiencia relevante
{{ EXPERIENCE }}

== Stack y seguridad
{{ SKILLS }}

== Educación y certificaciones
{{ EDUCATION }}
