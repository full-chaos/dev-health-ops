from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dev_health_ops.models import Base
from dev_health_ops.models.investment_batch import (
    InvestmentBatchItem,
    InvestmentBatchItemStatus,
    InvestmentBatchJob,
    InvestmentBatchJobStatus,
)
from dev_health_ops.work_graph.investment.batch_store import (
    InvestmentBatchItemSpec,
    InvestmentBatchStore,
)
from tests._helpers import tables_of

_TABLES = tables_of(InvestmentBatchJob, InvestmentBatchItem)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=_TABLES)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _item_spec(index: int = 0, *, custom_id: str | None = None):
    return InvestmentBatchItemSpec(
        work_unit_id="wu-1",
        component_index=index,
        custom_id=custom_id or f"run-{index}",
        input_hash="hash-1",
    )


def test_create_job_scopes_items_by_org(db_session):
    store = InvestmentBatchStore(db_session, "org-a")
    job = store.create_job(
        provider="openai",
        model="gpt-5-mini",
        run_id="run-1",
        prompt_version="prompt-v1",
        contract_version="contract-v1",
        items=[_item_spec()],
    )

    assert job.org_id == "org-a"
    assert job.total_items == 1
    assert job.items[0].org_id == "org-a"
    assert job.items[0].status == InvestmentBatchItemStatus.PENDING.value


def test_item_idempotency_lookup_allows_retry_jobs(db_session):
    store = InvestmentBatchStore(db_session, "org-a")
    store.create_job(
        provider="openai",
        model="gpt-5-mini",
        run_id="run-1",
        prompt_version="prompt-v1",
        contract_version="contract-v1",
        items=[_item_spec()],
    )
    db_session.commit()

    store.create_job(
        provider="openai",
        model="gpt-5-mini",
        run_id="run-2",
        prompt_version="prompt-v1",
        contract_version="contract-v1",
        items=[_item_spec(custom_id="other-custom")],
    )
    db_session.commit()

    assert db_session.query(InvestmentBatchItem).count() == 2


def test_item_idempotency_allows_contract_version_bump(db_session):
    store = InvestmentBatchStore(db_session, "org-a")
    store.create_job(
        provider="openai",
        model="gpt-5-mini",
        run_id="run-1",
        prompt_version="prompt-v1",
        contract_version="contract-v1",
        items=[_item_spec()],
    )
    db_session.commit()

    store.create_job(
        provider="openai",
        model="gpt-5-mini",
        run_id="run-2",
        prompt_version="prompt-v1",
        contract_version="contract-v2",
        items=[_item_spec(custom_id="other-custom")],
    )
    db_session.commit()

    assert db_session.query(InvestmentBatchItem).count() == 2


def test_custom_id_constraint_is_per_job(db_session):
    store = InvestmentBatchStore(db_session, "org-a")
    with pytest.raises(IntegrityError):
        store.create_job(
            provider="openai",
            model="gpt-5-mini",
            run_id="run-1",
            prompt_version="prompt-v1",
            contract_version="contract-v1",
            items=[_item_spec(0, custom_id="dup"), _item_spec(1, custom_id="dup")],
        )
        db_session.commit()


def test_status_transitions_update_terminal_counts(db_session):
    store = InvestmentBatchStore(db_session, "org-a")
    job = store.create_job(
        provider="openai",
        model="gpt-5-mini",
        run_id="run-1",
        prompt_version="prompt-v1",
        contract_version="contract-v1",
        items=[_item_spec()],
    )
    item = job.items[0]

    store.transition_job(
        job, InvestmentBatchJobStatus.SUBMITTED, provider_job_id="batch-1"
    )
    store.transition_item(item, InvestmentBatchItemStatus.VALIDATED)
    completed, failed = store.terminal_counts(job)

    assert job.status == InvestmentBatchJobStatus.SUBMITTED.value
    assert job.provider_job_id == "batch-1"
    assert completed == 1
    assert failed == 0
    assert job.completed_items == 1


def test_fallback_items_count_as_failed_terminal_outcomes(db_session):
    store = InvestmentBatchStore(db_session, "org-a")
    job = store.create_job(
        provider="openai",
        model="gpt-5-mini",
        run_id="run-1",
        prompt_version="prompt-v1",
        contract_version="contract-v1",
        items=[_item_spec()],
    )
    item = job.items[0]

    store.transition_item(item, InvestmentBatchItemStatus.FALLBACK)
    completed, failed = store.terminal_counts(job)

    assert completed == 1
    assert failed == 1
    assert job.completed_items == 1
    assert job.failed_items == 1
