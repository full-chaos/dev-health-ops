from __future__ import annotations

import pytest

from dev_health_ops.api.internal.worker_workgraph import _evidence, _scope_arguments


def test_scope_arguments_reloads_only_allowlisted_workgraph_fields() -> None:
    row = {
        "org_id": "00000000-0000-4000-8000-000000000009",
        "model_ref": "gpt-test",
        "llm_concurrency": 2,
    }
    assert _scope_arguments(
        "workgraph.build",
        {"from_date": "2026-07-01", "heuristic_window": 7},
        row,
    ) == {
        "from_date": "2026-07-01",
        "heuristic_window": 7,
        "org_id": "00000000-0000-4000-8000-000000000009",
    }


def test_scope_arguments_rejects_callable_or_credential_injection() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        _scope_arguments(
            "investment.materialize",
            {"from_date": "2026-07-01", "callable": "os.system"},
            {
                "org_id": "00000000-0000-4000-8000-000000000009",
                "model_ref": "gpt-test",
                "llm_concurrency": 1,
            },
        )


def test_evidence_is_canonical_and_bounded() -> None:
    assert _evidence({"z": 1, "a": ["evidence"]}) == {
        "a": ["evidence"],
        "z": 1,
    }
    with pytest.raises(ValueError, match="durable bound"):
        _evidence({"output": "x" * 5000})
