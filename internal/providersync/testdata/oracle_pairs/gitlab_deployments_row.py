from __future__ import annotations

import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.oracle_pairs._gitlab_deployments_common import (
    run_producer,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
MODEL_SOURCE = REPO_ROOT / "src/dev_health_ops/models/git.py"


def _reflected_fields() -> frozenset[str]:
    return class_annotated_field_names(MODEL_SOURCE.read_text(), "Deployment")


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    rows, _, _ = run_producer(case, apply_until=False)
    if len(rows) != 1:
        raise ValueError(f"deployment oracle expected one row, got {len(rows)}")
    return vars(rows[0])


oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/deployments/row",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "last_synced": "stamped from the Go handler normalized collection instant",
            "started_at": "Go derives started_at/finished_at from the deployment's own nested deployable object rather than copying created_at; Python's fixed copy is the declared baseline defect this row diverges from -- this oracle harness never supplies a deployable fixture, so Go's own row leaves it nil here",
            "finished_at": "see started_at",
            "merged_at": "Go only attributes merged_at/pull_request_number to a merge request whose own state is merged; Python's first-nonmerged-MR fallback is the declared baseline defect this row diverges from",
            "pull_request_number": "see merged_at",
            "lifecycle_lookup_failed": "writer-internal signaling for the write-once lifecycle carry-forward guard -- never an INSERT column, Python has no equivalent field",
        },
    )
)
