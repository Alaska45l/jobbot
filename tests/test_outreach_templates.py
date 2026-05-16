"""Outreach template tests."""

from jobbot.outreach.templates import ASUNTOS, CUERPOS


def test_template_counts_and_placeholders() -> None:
    assert len(ASUNTOS) >= 12
    assert len(CUERPOS) >= 10
    for cuerpo in CUERPOS:
        assert "{nombre_empresa}" in cuerpo
        assert "{nombre_remitente}" in cuerpo
        assert "{firma}" in cuerpo

