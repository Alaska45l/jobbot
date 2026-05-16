"""Scoring smoke tests."""

from jobbot.scoring.engine import analizar_empresa


def test_scoring_returns_result_for_empty_site() -> None:
    result = analizar_empresa("", dominio="example.com")
    assert result.perfil_cv in {"CV_Tech", "CV_Admin_IT", "CV_Hybrid"}


def test_scoring_assigns_hybrid_for_mixed_ops_and_it() -> None:
    html = """
    <html><body>
      Empresa de logística con departamento de sistemas, soporte técnico,
      automatización de procesos, redes TCP/IP, administración e inventario.
      <a href="mailto:rrhh@example.com">rrhh@example.com</a>
    </body></html>
    """
    result = analizar_empresa(html, dominio="example.com")
    assert result.perfil_cv == "CV_Hybrid"
