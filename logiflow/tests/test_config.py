"""
tests/test_config.py

Tests common/config.py's fail-fast behavior. Both tests import the module
once, cleanly, before patching anything -- then patch dotenv.load_dotenv
at its source module (not common.config's already-bound reference to it)
so a subsequent reload() doesn't silently repopulate a deleted var from
the real .env file sitting in the working directory.
"""

import importlib

import pytest


def test_fails_fast_on_missing_required_var(monkeypatch):
    import common.config as config_module  # first import, real load_dotenv, must succeed

    monkeypatch.setattr("dotenv.load_dotenv", lambda *a, **kw: None)
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.setenv("POSTGRES_DB", "x")
    monkeypatch.setenv("POSTGRES_PASSWORD", "x")
    monkeypatch.setenv("MINIO_ROOT_USER", "x")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "x")

    with pytest.raises(ValueError, match="POSTGRES_USER"):
        importlib.reload(config_module)


def test_database_url_builds_correctly(monkeypatch):
    import common.config as config_module

    monkeypatch.setattr("dotenv.load_dotenv", lambda *a, **kw: None)
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("MINIO_ROOT_USER", "x")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "x")

    importlib.reload(config_module)
    assert config_module.settings.database_url == "postgresql://testuser:testpass@localhost:5432/testdb"