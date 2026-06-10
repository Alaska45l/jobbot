"""Database smoke tests."""

import pytest

from jobbot.db.manager import init_db, insert_contacto


def test_init_db_tmp_path(tmp_path) -> None:
    db_path = tmp_path / "jobbot.db"
    init_db(db_path)
    assert db_path.exists()


def test_insert_contacto_rejects_linkedin() -> None:
    with pytest.raises(ValueError):
        insert_contacto(1, "https://www.linkedin.com/company/example", "LinkedIn", 2)
