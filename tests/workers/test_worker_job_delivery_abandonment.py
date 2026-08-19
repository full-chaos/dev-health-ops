from __future__ import annotations

from typing import cast

from sqlalchemy import CheckConstraint, Table

from dev_health_ops.models import WorkerJobDeliveryAbandonment


def test_delivery_abandonment_model_keeps_only_minimal_terminal_evidence() -> None:
    table = cast(Table, WorkerJobDeliveryAbandonment.__table__)

    assert set(table.columns.keys()) == {
        "dedupe_key",
        "job_kind",
        "abandoned_at",
        "attempt_count",
        "last_error_code",
    }
    assert table.primary_key.columns.keys() == ["dedupe_key"]
    assert table.c.last_error_code.nullable
    assert {
        constraint.name
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    } == {"ck_worker_job_delivery_abandonments_attempt_count"}
    assert {index.name for index in table.indexes} == {
        "ix_worker_job_delivery_abandonments_kind_time"
    }

    # Full delivery material is intentionally absent: retention deletes it
    # with the outbox row instead of creating a second sensitive payload store.
    for forbidden in (
        "args",
        "payload_hash",
        "last_error_detail",
        "correlation_id",
        "organization_id",
    ):
        assert forbidden not in table.c
