"""Outreach template tests."""

from jobbot.outreach.templates import (
    ASUNTOS,
    ASUNTOS_POR_PERFIL,
    CUERPOS,
    asuntos_para_perfil,
    variables_email_para_perfil,
)


def test_template_counts_and_placeholders() -> None:
    assert len(ASUNTOS) >= 12
    assert len(CUERPOS) >= 6
    for cuerpo in CUERPOS:
        assert "{nombre_empresa}" in cuerpo
        assert "{nombre_remitente}" in cuerpo
        assert "{firma}" in cuerpo
        assert "{linea_encaje}" in cuerpo


def test_subjects_are_profile_aware() -> None:
    assert set(ASUNTOS_POR_PERFIL) == {"CV_IT_QA", "CV_BackOffice", "CV_Ciencia"}
    assert asuntos_para_perfil("CV_Tech") == ASUNTOS_POR_PERFIL["CV_IT_QA"]
    assert "QA / Soporte IT" in asuntos_para_perfil("CV_IT_QA")[0]
    assert "Back Office" in asuntos_para_perfil("CV_BackOffice")[0]
    assert "Control de calidad" in asuntos_para_perfil("CV_Ciencia")[0]


def test_email_fragments_do_not_include_sensitive_outreach_terms() -> None:
    rendered = "\n".join(CUERPOS)
    fragments = variables_email_para_perfil("CV_IT_QA", "software")
    rendered += "\n".join(fragments.values())

    forbidden = ("LinkedIn", "JobBot", "scraping", "cold mailing", "envío automático")
    for term in forbidden:
        assert term.lower() not in rendered.lower()
