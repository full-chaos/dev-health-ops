"""No-drift check: committed static export vs. a fresh in-memory render (CHAOS-2692).

Imports the exact document-building/serialization function
``export_schemas.py`` uses (does not shell out) — brief D7's no-drift test,
run as part of the standing unit-test tier rather than a new CI workflow.
"""

from __future__ import annotations

from pathlib import Path

from dev_health_ops.api.external_ingest.export_schemas import render_schema_json

_REPO_ROOT = Path(__file__).resolve().parents[3]
_COMMITTED_PATH = _REPO_ROOT / "docs" / "api" / "external-ingest" / "v1" / "schema.json"


def test_static_export_matches_committed_artifact():
    generated = render_schema_json()
    committed = _COMMITTED_PATH.read_text()

    assert generated == committed, (
        f"{_COMMITTED_PATH} is stale — run `python3 -m "
        "dev_health_ops.api.external_ingest.export_schemas --out "
        "docs/api/external-ingest/v1/schema.json` and commit the diff"
    )
