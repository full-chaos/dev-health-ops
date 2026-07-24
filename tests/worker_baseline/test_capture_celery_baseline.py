from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/worker/capture_celery_baseline.py"
CAPTURE = ROOT / (
    ".github/docs-legacy/architecture/evidence/go-worker-migration/v0-celery-baseline/capture.json"
)
SCHEMA = CAPTURE.with_name("capture.schema.json")


def _load_recorder() -> ModuleType:
    spec = importlib.util.spec_from_file_location("capture_celery_baseline", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


recorder = _load_recorder()


def test_task_log_parser_retains_only_safe_aggregate_fields() -> None:
    line = (
        "2026-07-21T17:00:00Z [INFO] Task "
        "dev_health_ops.workers.tasks.run_sync_unit"
        "[aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee] succeeded in 1.25s: "
        "{'org_id': 'ffffffff-1111-4222-8333-444444444444', "
        "'token': 'do-not-retain'}"
    )

    assert recorder.parse_task_log_line(line) == (
        "dev_health_ops.workers.tasks.run_sync_unit",
        "success",
        1.25,
    )


@pytest.mark.parametrize(
    ("suffix", "expected"),
    [
        ("retry: Retry in 60s", "retry"),
        ("raised unexpected: RuntimeError('redacted')", "failure"),
        ("revoked", "discard"),
    ],
)
def test_task_log_parser_classifies_non_success_outcomes(
    suffix: str, expected: str
) -> None:
    line = (
        "Task dev_health_ops.workers.tasks.run_sync_unit"
        "[aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee] "
        f"{suffix}"
    )

    parsed = recorder.parse_task_log_line(line)

    assert parsed is not None
    assert parsed[1] == expected
    assert parsed[2] is None


def test_queue_aggregation_preserves_empty_and_observed_age_profiles() -> None:
    depth, age = recorder.aggregate_queue_samples(
        [
            {
                "queues": [
                    {
                        "queue": "default",
                        "depth": 2,
                        "oldest_age_seconds": 4.5,
                        "age_observable": True,
                    },
                    {
                        "queue": "metrics",
                        "depth": 0,
                        "oldest_age_seconds": None,
                        "age_observable": False,
                    },
                ]
            },
            {
                "queues": [
                    {
                        "queue": "default",
                        "depth": 0,
                        "oldest_age_seconds": None,
                        "age_observable": False,
                    },
                    {
                        "queue": "metrics",
                        "depth": 3,
                        "oldest_age_seconds": None,
                        "age_observable": False,
                    },
                ]
            },
        ]
    )

    assert depth["general"]["max"] == 2.0
    assert age["general"]["max"] == 4.5
    assert age["heavy"]["nonempty_samples_without_enqueued_at"] == 1
    assert age["heavy"]["status"] == "not_observed_during_sampling_window"


def test_sanitizer_rejects_raw_identifiers_and_secret_bearing_shapes() -> None:
    with pytest.raises(recorder.CaptureError, match="forbidden raw-data keys"):
        recorder.assert_sanitized({"payload": {"safe": False}})

    with pytest.raises(recorder.CaptureError, match="UUID-shaped"):
        recorder.assert_sanitized({"value": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"})

    with pytest.raises(recorder.CaptureError, match="DSN"):
        recorder.assert_sanitized({"value": "redis://user:password@host/0"})

    with pytest.raises(recorder.CaptureError, match="absolute host path"):
        recorder.assert_sanitized({"value": "/Users/example/private/repo"})


def test_committed_capture_matches_recorder_contract_and_redaction() -> None:
    capture = json.loads(CAPTURE.read_text(encoding="utf-8"))
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))

    recorder.validate_capture_shape(capture)
    assert set(schema["required"]) <= capture.keys()
    assert schema["properties"]["scope"]["const"] == capture["scope"]
    assert capture["authoritative_for_baseline"] is True
    assert capture["sources"]["raw_evidence_retained"] is False
    assert capture["redaction"]["automated_validation"] == "pass"


def test_recorder_contains_no_stack_mutation_commands() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    for forbidden in (
        '"stop"',
        '"restart"',
        '"kill"',
        '"rm"',
        '"down"',
        '"up"',
        '"scale"',
        '"flushdb"',
        '"del"',
    ):
        assert forbidden not in source
