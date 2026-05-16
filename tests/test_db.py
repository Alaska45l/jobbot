"""Database smoke tests."""

from jobbot.db.manager import init_db


def test_init_db_tmp_path(tmp_path) -> None:
    db_path = tmp_path / "jobbot.db"
    init_db(db_path)
    assert db_path.exists()

