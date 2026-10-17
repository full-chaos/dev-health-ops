"""CHAOS-3383: every Python writer of ``repos`` carries the repository anchor.

``repos`` is a ReplacingMergeTree(last_synced): the newest version of a key
wins, so ``provider_repo_id`` (migration 134) and ``identity_path`` (migration
138) survive a later write only if the writer copies them from the version it
replaces. The Go writers do it under their stored-version contracts
(internal/storedversion census); this module holds the Python writer to the
same rule and scans ``src`` for another one.
"""

from __future__ import annotations

import ast
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dev_health_ops.storage.clickhouse import ClickHouseStore

SRC = Path(__file__).resolve().parents[1] / "src" / "dev_health_ops"
ORG_ID = "org-acme"
NOW = datetime(2026, 10, 17, tzinfo=timezone.utc)
HELD_ID = uuid.UUID("c7198fbc-1945-3717-05d8-eb78866b4e79")
NEW_ID = uuid.UUID("1ebaec64-a77c-6b80-bc85-38e95e16d90e")


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


class _FakeClient:
    """One stored GitLab repository whose anchor a later write must keep."""

    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[list[Any]], list[str]]] = []
        self.anchor_queries = 0

    def query(self, sql: str, parameters: dict[str, Any] | None = None) -> _Result:
        params = parameters or {}
        if "identity_path" in sql and "provider_repo_id" in sql:
            self.anchor_queries += 1
            assert ORG_ID in params["org_ids"]
            if str(HELD_ID) not in params["ids"]:
                return _Result([])
            return _Result([(ORG_ID, str(HELD_ID), "4711", "acme/api")])
        return _Result([])

    def insert(
        self, table: str, matrix: list[list[Any]], *, column_names: list[str], **_: Any
    ) -> None:
        self.inserts.append((table, matrix, column_names))


def _store() -> tuple[ClickHouseStore, _FakeClient]:
    store = ClickHouseStore("clickhouse://fake")
    client = _FakeClient()
    store.client = client
    store.org_id = ORG_ID
    return store, client


def _repo(repo_id: uuid.UUID, name: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=repo_id,
        repo=name,
        ref=None,
        created_at=NOW,
        settings={"source": "gitlab"},
        tags=["gitlab"],
        provider="gitlab",
        source_id=None,
    )


@pytest.mark.asyncio
async def test_insert_repo_carries_the_stored_anchor() -> None:
    store, client = _store()

    await store.insert_repo(_repo(HELD_ID, "acme/api"))
    await store.insert_repo(_repo(NEW_ID, "acme/other"))

    assert client.anchor_queries == 2
    rows = []
    for table, matrix, columns in client.inserts:
        assert table == "repos"
        assert "provider_repo_id" in columns and "identity_path" in columns
        rows.extend(dict(zip(columns, row, strict=True)) for row in matrix)
    by_id = {row["id"]: row for row in rows}
    assert (
        by_id[str(HELD_ID)]["provider_repo_id"],
        by_id[str(HELD_ID)]["identity_path"],
    ) == ("4711", "acme/api")
    assert (
        by_id[str(NEW_ID)]["provider_repo_id"],
        by_id[str(NEW_ID)]["identity_path"],
    ) == ("", "")


_INSERT_NAMES = {"insert", "_insert_rows", "insert_rows"}


def _inserts_into_repos(call: ast.Call) -> bool:
    """A call that writes the ``repos`` table: ``x.insert("repos", ...)``,
    ``x._insert_rows("repos", ...)`` or ``asyncio.to_thread(x.insert, "repos", ...)``."""
    if not any(isinstance(a, ast.Constant) and a.value == "repos" for a in call.args):
        return False
    func = call.func
    if isinstance(func, ast.Attribute) and func.attr in _INSERT_NAMES:
        return True
    return any(
        isinstance(a, ast.Attribute) and a.attr in _INSERT_NAMES for a in call.args
    )


def _repos_insert_functions() -> list[tuple[Path, str, str]]:
    found: list[tuple[Path, str, str]] = []
    for path in sorted(SRC.rglob("*.py")):
        if "migrations" in path.parts:
            continue
        source = path.read_text(encoding="utf-8")
        if '"repos"' not in source:
            continue
        for node in ast.walk(ast.parse(source)):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            for call in ast.walk(node):
                if isinstance(call, ast.Call) and _inserts_into_repos(call):
                    found.append(
                        (path, node.name, ast.get_source_segment(source, node) or "")
                    )
    return found


def test_every_python_repos_writer_carries_the_anchor() -> None:
    writers = _repos_insert_functions()
    # A scan that finds nothing is not a pass: the writer known on 2026-10-17.
    assert ("clickhouse.py", "insert_repo") in {
        (path.name, name) for path, name, _ in writers
    }
    for path, name, body in writers:
        assert "REPO_ANCHOR_COLUMNS" in body, (
            f"{path.relative_to(SRC)}::{name} inserts into repos without "
            "provider_repo_id and identity_path (CHAOS-3383): carry them with "
            "storage.repo_anchor"
        )
        assert "fetch_repo_anchor" in body or "_carry_repo_anchor" in body, (
            f"{path.relative_to(SRC)}::{name} does not carry the anchor of the "
            "version it replaces"
        )
