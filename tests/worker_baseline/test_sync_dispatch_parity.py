from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/worker/observe_sync_dispatch_parity.py"


def _load_helper() -> ModuleType:
    spec = importlib.util.spec_from_file_location("sync_dispatch_parity", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


helper = _load_helper()


def test_snapshot_token_is_environment_only_and_strictly_validated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYNC_DISPATCH_PARITY_SNAPSHOT_ID", "000003A1-1")
    assert helper._snapshot_id() == "000003A1-1"

    monkeypatch.setenv("SYNC_DISPATCH_PARITY_SNAPSHOT_ID", "x'; SELECT 1; --")
    with pytest.raises(helper.ParityHelperError, match="snapshot_unavailable"):
        helper._snapshot_id()

    source = SCRIPT.read_text(encoding="utf-8")
    assert '"--snapshot"' not in source
    assert "SYNC_DISPATCH_PARITY_SNAPSHOT_ID" in source


def test_cutoff_is_normalized_to_utc_and_database_uri_stays_out_of_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cutoff = helper._parse_cutoff("2026-07-23T05:00:00.123456-07:00")
    assert cutoff == datetime(2026, 7, 23, 12, 0, 0, 123456, tzinfo=timezone.utc)

    monkeypatch.setenv(
        "SYNC_DISPATCH_PARITY_DATABASE_URI", "postgres://user:secret@db.example/app"
    )
    assert helper._database_uri() == "postgresql+psycopg2://user:secret@db.example/app"


def test_helper_emits_only_generic_errors(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fail(**_: object) -> dict[str, object]:
        raise helper.ParityHelperError("postgresql://user:secret@db.example/app")

    monkeypatch.setattr(helper, "observe_imported_snapshot", fail)

    assert helper.main(["--cutoff", "2026-07-23T12:00:00Z", "--limit", "1"]) == 1
    output = json.loads(capsys.readouterr().out)
    assert output == {"status": "error", "reason": "unavailable"}


def test_helper_source_uses_one_read_only_imported_snapshot_and_no_mutations() -> None:
    source = SCRIPT.read_text(encoding="utf-8").upper()
    for required in (
        "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "SET TRANSACTION SNAPSHOT",
        "OBSERVE_DUE_OUTBOX_ROWS",
        '"ROLLBACK"',
    ):
        assert required in source
    for forbidden in ("INSERT ", "UPDATE ", "DELETE ", "FOR UPDATE", "SKIP LOCKED"):
        assert forbidden not in source
