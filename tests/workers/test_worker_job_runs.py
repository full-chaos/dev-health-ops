from __future__ import annotations

from typing import cast

from sqlalchemy import Table, create_engine, inspect

from dev_health_ops.models import Base, WorkerJobRun


def test_worker_job_run_model_has_durable_claim_and_completion_constraints() -> None:
    engine = create_engine("sqlite:///:memory:")
    try:
        Base.metadata.create_all(engine, tables=[cast(Table, WorkerJobRun.__table__)])
        inspector = inspect(engine)
        constraints = {
            constraint["name"]
            for constraint in inspector.get_check_constraints("worker_job_runs")
        }
        assert constraints >= {
            "ck_worker_job_run_status",
            "ck_worker_job_run_attempt_count",
            "ck_worker_job_run_claim_state",
            "ck_worker_job_run_result_state",
        }
        unique_constraints = inspector.get_unique_constraints("worker_job_runs")
        assert any(
            constraint["name"] == "uq_worker_job_run_key"
            and constraint["column_names"] == ["job_kind", "idempotency_key"]
            for constraint in unique_constraints
        )
    finally:
        engine.dispose()
