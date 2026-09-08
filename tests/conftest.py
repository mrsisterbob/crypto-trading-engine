"""Test fixtures for the Crypto Alert & Paper-Trading Engine.

Every test runs against a throwaway SQLite file so the real crypto_engine.db is never touched.
database.DB_PATH is read fresh inside get_db_conn() on every connection, and analytics_engine
reaches the DB only through `import database` attribute access, so monkeypatching the module
attribute is enough to fully isolate both modules.
"""
import pytest

import database


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    db_file = tmp_path / "test_engine.db"
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    database.init_db()
    yield
