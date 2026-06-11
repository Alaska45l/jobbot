"""Scoring smoke tests."""

from jobbot.scoring.engine import analizar_empresa


def test_scoring_returns_result_for_empty_site() -> None:
    result = analizar_empresa("", dominio="example.com")
    assert result.perfil_cv in {"CV_IT_QA", "CV_BackOffice", "CV_Ciencia"}


def test_scoring_assigns_hybrid_for_mixed_ops_and_it() -> None:
    html = """
    <html><body>
      Empresa de logística con departamento de sistemas, soporte técnico,
      automatización de procesos, redes TCP/IP, administración e inventario.
      <a href="mailto:rrhh@example.com">rrhh@example.com</a>
    </body></html>
    """
    result = analizar_empresa(html, dominio="example.com")
    assert result.perfil_cv == "CV_Ciencia"


def test_scoring_ignores_linkedin_contacts() -> None:
    html = """
    <html><body>
      Empresa de software en Mar del Plata.
      <a href="mailto:rrhh@example.com">rrhh@example.com</a>
      <a href="https://www.linkedin.com/company/example">LinkedIn</a>
      <a href="https://www.linkedin.com/in/recruiter-example">Recruiter</a>
    </body></html>
    """
    result = analizar_empresa(html, dominio="example.com")
    assert result.contactos
    assert {contacto.tipo for contacto in result.contactos} <= {"RRHH", "General", "WhatsApp"}
