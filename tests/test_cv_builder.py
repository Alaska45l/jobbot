"""CV builder tests."""

from jobbot.cv.builder import _formatear_keywords_typst, _render_markers
from jobbot.cv.profiles import PROFILES


def test_formatear_keywords_typst_escapes_quotes() -> None:
    assert _formatear_keywords_typst(["Python", 'API "REST"']) == (
        '"Python", "API \\"REST\\""'
    )


def test_three_cv_profiles_exist() -> None:
    assert {"CV_IT_QA", "CV_BackOffice", "CV_Ciencia"} <= set(PROFILES)
    assert all(profile.projects for profile in PROFILES.values())


def test_render_markers_replaces_known_values() -> None:
    assert _render_markers("Hola {{ EMPRESA }}", {"EMPRESA": "Acme"}) == "Hola Acme"
