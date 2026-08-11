"""CHAOS-3566: record the RetentionService.execute_policy residual explicitly.

`RetentionService` is a separate, schedule-driven cleanup path (see
`api/services/retention.py`) -- NOT an org-deletion completeness mechanism.
`OrganizationDeletionService` (org_deletion.py) plus the derived-store
registry (derived_store_registry.py) are the only place deletion
completeness for an org's derived stores is enforced.

This pins the actual scope of `execute_policy` today: only
`RetentionResourceType.AUDIT_LOGS` is implemented. Every other declared
resource type returns a "not implemented" error and deletes/counts nothing.
If a future change implements a second resource type, this test's assertion
about the SPECIFIC untouched types should shrink accordingly -- it must never
be read as "retention deletes everything a policy names".
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.retention import RetentionService
from dev_health_ops.models.git import Base
from dev_health_ops.models.retention import OrgRetentionPolicy, RetentionResourceType
from dev_health_ops.models.users import Organization
from tests._helpers import tables_of


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "retention-residual.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn, tables=tables_of(Organization, OrgRetentionPolicy)
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resource_type",
    [
        RetentionResourceType.METRICS_DAILY.value,
        RetentionResourceType.WORK_ITEMS.value,
        RetentionResourceType.GIT_COMMITS.value,
        RetentionResourceType.SYNC_LOGS.value,
    ],
)
async def test_execute_policy_only_implements_audit_logs(session_maker, resource_type):
    org_id = uuid.uuid4()
    async with session_maker() as session:
        session.add(Organization(id=org_id, slug="acme", name="Acme"))
        policy = OrgRetentionPolicy(
            org_id=org_id,
            resource_type=resource_type,
            retention_days=90,
            is_active=True,
        )
        session.add(policy)
        await session.flush()
        policy_id = policy.id

        count, error = await RetentionService(session).execute_policy(
            org_id, policy_id, dry_run=True
        )

        assert count == 0
        assert error == f"Cleanup not implemented for resource type: {resource_type}"
        # Confirms this is a no-op, not a silent partial delete: the policy's
        # run metadata is untouched (execute_policy returns before the
        # dry_run-gated metadata-update branch for every non-audit_logs type).
        assert policy.last_run_at is None
        assert policy.last_run_deleted_count is None
