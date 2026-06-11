"""Dorking query tests."""

from jobbot.core.orchestrator import DORK_ZONA_DEFAULT, _construir_query_dork


def test_dorking_query_targets_mar_del_plata() -> None:
    query = _construir_query_dork("software house", zona=DORK_ZONA_DEFAULT)
    assert '"Mar del Plata"' in query
    assert '"software house"' in query
