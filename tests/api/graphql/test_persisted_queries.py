from __future__ import annotations

import json

import pytest

from dev_health_ops.api.graphql import persisted
from dev_health_ops.api.graphql.errors import PersistedQueryError


def test_register_and_load_persisted_query_in_memory():
    persisted.clear_cache()
    persisted.register_query("q-test", "query { health }", "Health query")

    assert persisted.load_persisted_query("q-test") == "query { health }"
    listed = persisted.list_persisted_queries()
    assert {
        "id": "q-test",
        "description": "Health query",
        "schema_version": persisted.get_schema_version(),
    } in listed


def test_load_persisted_query_missing_raises_error():
    persisted.clear_cache()

    with pytest.raises(PersistedQueryError, match="not found"):
        persisted.load_persisted_query("missing")


def test_load_persisted_query_from_registry_file(tmp_path, monkeypatch):
    persisted.clear_cache()
    registry_path = tmp_path / "persisted_queries.json"
    registry_path.write_text(
        json.dumps(
            {
                "queries": [
                    {
                        "id": "q-file",
                        "query": "query { version }",
                        "schema_version": persisted.get_schema_version(),
                        "description": "version query",
                    }
                ]
            }
        )
    )

    monkeypatch.setattr(persisted, "_REGISTRY_PATH", registry_path)
    persisted.clear_cache()

    assert persisted.load_persisted_query("q-file") == "query { version }"


def test_schema_version_mismatch_raises_error():
    persisted.clear_cache()
    persisted.register_query("q-old", "query { old }", "old")
    persisted._QUERY_CACHE["q-old"].schema_version = "0.9"  # type: ignore[misc]

    with pytest.raises(PersistedQueryError, match="current version"):
        persisted.load_persisted_query("q-old")
