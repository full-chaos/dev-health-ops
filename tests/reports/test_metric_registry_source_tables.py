"""Every report registry source_table must be a table the migrations create."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dev_health_ops.reports.metric_registry import METRIC_REGISTRY

ROOT = Path(__file__).resolve().parents[2]
CHAIN = ROOT / "internal/chmigrate/testdata/python_chain_contract2.json"


def _created_names(node: Any, found: set[str]) -> None:
    if isinstance(node, dict):
        if "name" in node and "create" in node:
            found.add(node["name"])
        for value in node.values():
            _created_names(value, found)
    elif isinstance(node, list):
        for value in node:
            _created_names(value, found)


def test_every_registry_source_table_is_a_migrated_table() -> None:
    created: set[str] = set()
    _created_names(json.loads(CHAIN.read_text(encoding="utf-8")), created)
    assert created, "the migration chain contract lists no tables"

    unknown = {
        definition.source_table
        for definition in METRIC_REGISTRY.values()
        if definition.source_table not in created
    }
    assert unknown == set()
