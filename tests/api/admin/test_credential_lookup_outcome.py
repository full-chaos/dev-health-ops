"""issue 3694: get_decrypted_credentials_by_id conflated three distinct
"falsy" reasons into one -- (1) no row at all, (2) a row with no stored
payload, (3) a row whose payload exists but fails to decrypt (a
key-mismatch class of failure, previously silent to every caller). This
tests CredentialLookupOutcome / get_decrypted_credentials_by_id_with_outcome
at the service layer against a REAL (sqlite) session -- not a mock of the
decrypt call -- so case 3 is plant-verified against the real
decrypt_value/json.loads path, not assumed.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")

from dev_health_ops.api.services.configuration import (
    CredentialLookupOutcome,
    IntegrationCredentialsService,
)
from dev_health_ops.metrics.prometheus import (
    INTEGRATION_CREDENTIAL_DECRYPT_FAILED_TOTAL,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import IntegrationCredential
from tests._helpers import tables_of

_TABLES = tables_of(IntegrationCredential)

ORG_ID = str(uuid.uuid4())


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "cred-lookup-outcome.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed(
    session_maker, *, name: str = "default", credentials: dict | None = None
) -> str:
    """Seed one real, decryptable credential row and return its id.

    ``name`` defaults to "default" but every test that seeds MORE THAN
    ONE row in the same org must pass distinct names -- the service's own
    ``set()`` is an upsert keyed on (org_id, provider, name), so two
    same-named seeds silently collapse into ONE row and corrupting one
    would corrupt the other's expected-clean payload too.
    """
    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        cred = await svc.set(
            provider="github",
            credentials=credentials or {"token": "ghp_real"},
            name=name,
        )
        await session.commit()
        return str(cred.id)


async def _corrupt_encrypted_payload(session_maker, credential_id: str) -> None:
    """Overwrite the row's ciphertext with garbage that cannot decrypt --
    the real path a key rotation or storage corruption would produce.
    Direct SQL, bypassing the service's own encrypt/decrypt entirely, so
    this genuinely exercises decrypt_value/json.loads raising, not a
    mocked-away shortcut."""
    async with session_maker() as session:
        await session.execute(
            update(IntegrationCredential)
            .where(IntegrationCredential.id == uuid.UUID(credential_id))
            .values(credentials_encrypted="not-valid-ciphertext-at-all")
        )
        await session.commit()


async def _blank_out_payload(session_maker, credential_id: str) -> None:
    async with session_maker() as session:
        await session.execute(
            update(IntegrationCredential)
            .where(IntegrationCredential.id == uuid.UUID(credential_id))
            .values(credentials_encrypted=None)
        )
        await session.commit()


@pytest.mark.asyncio
async def test_ok_case_returns_the_real_decrypted_dict(session_maker):
    credential_id = await _seed(session_maker, credentials={"token": "ghp_real"})

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        (
            decrypted,
            cred,
            outcome,
        ) = await svc.get_decrypted_credentials_by_id_with_outcome(credential_id)

    assert outcome is CredentialLookupOutcome.OK
    assert decrypted == {"token": "ghp_real"}
    assert cred is not None


@pytest.mark.asyncio
async def test_not_found_case_for_a_genuinely_absent_id(session_maker):
    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        (
            decrypted,
            cred,
            outcome,
        ) = await svc.get_decrypted_credentials_by_id_with_outcome(str(uuid.uuid4()))

    assert outcome is CredentialLookupOutcome.NOT_FOUND
    assert decrypted is None
    assert cred is None


@pytest.mark.asyncio
async def test_not_found_case_for_a_different_orgs_row(session_maker):
    """Cross-tenant posture unchanged: a row that exists, but scoped to a
    DIFFERENT org, is indistinguishable from no row at all -- still
    NOT_FOUND, never a different outcome that would leak its existence."""
    credential_id = await _seed(session_maker)

    async with session_maker() as session:
        other_org_svc = IntegrationCredentialsService(session, str(uuid.uuid4()))
        (
            decrypted,
            cred,
            outcome,
        ) = await other_org_svc.get_decrypted_credentials_by_id_with_outcome(
            credential_id
        )

    assert outcome is CredentialLookupOutcome.NOT_FOUND
    assert decrypted is None
    assert cred is None


@pytest.mark.asyncio
async def test_no_payload_case_when_the_row_has_nothing_stored(session_maker):
    credential_id = await _seed(session_maker)
    await _blank_out_payload(session_maker, credential_id)

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        (
            decrypted,
            cred,
            outcome,
        ) = await svc.get_decrypted_credentials_by_id_with_outcome(credential_id)

    assert outcome is CredentialLookupOutcome.NO_PAYLOAD
    assert decrypted is None
    # Distinct from NOT_FOUND: the row itself IS returned -- it exists.
    assert cred is not None


@pytest.mark.asyncio
async def test_decrypt_failed_case_is_plant_verified_against_a_real_corrupt_row(
    session_maker,
):
    """Plant-verified per the issue's own requirement: corrupt a REAL
    encrypted payload (not a mock) and observe the case-3 path actually
    fire against the real decrypt_value/json.loads call."""
    credential_id = await _seed(session_maker)
    await _corrupt_encrypted_payload(session_maker, credential_id)

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        (
            decrypted,
            cred,
            outcome,
        ) = await svc.get_decrypted_credentials_by_id_with_outcome(credential_id)

    assert outcome is CredentialLookupOutcome.DECRYPT_FAILED
    assert decrypted is None
    # Distinct from NOT_FOUND and NO_PAYLOAD: the row exists and HAD a
    # payload -- it just can't be read anymore.
    assert cred is not None


@pytest.mark.asyncio
async def test_decrypt_failed_case_increments_the_structured_metric(session_maker):
    """The issue's other explicit requirement: a decrypt failure must be
    COUNTABLE, not just a grep-able log line."""
    credential_id = await _seed(session_maker)
    await _corrupt_encrypted_payload(session_maker, credential_id)

    before = INTEGRATION_CREDENTIAL_DECRYPT_FAILED_TOTAL.labels(
        provider="github"
    )._value.get()

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        await svc.get_decrypted_credentials_by_id_with_outcome(credential_id)

    after = INTEGRATION_CREDENTIAL_DECRYPT_FAILED_TOTAL.labels(
        provider="github"
    )._value.get()
    assert after == before + 1, (
        "a real decrypt failure must increment "
        "INTEGRATION_CREDENTIAL_DECRYPT_FAILED_TOTAL, labeled by provider"
    )


@pytest.mark.asyncio
async def test_no_payload_case_never_increments_the_decrypt_failure_metric(
    session_maker,
):
    """Case 2 (no payload) must NOT be conflated with case 3 in the
    metric either -- only a genuine decrypt failure counts."""
    credential_id = await _seed(session_maker)
    await _blank_out_payload(session_maker, credential_id)

    before = INTEGRATION_CREDENTIAL_DECRYPT_FAILED_TOTAL.labels(
        provider="github"
    )._value.get()

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)
        await svc.get_decrypted_credentials_by_id_with_outcome(credential_id)

    after = INTEGRATION_CREDENTIAL_DECRYPT_FAILED_TOTAL.labels(
        provider="github"
    )._value.get()
    assert after == before, "a missing payload is not a decrypt failure"


@pytest.mark.asyncio
async def test_the_two_tuple_wrapper_is_unchanged_for_every_existing_caller(
    session_maker,
):
    """get_decrypted_credentials_by_id (the pre-existing 2-tuple method)
    must behave byte-for-byte as before for all of its existing callers,
    across all three falsy cases plus the OK case -- it is a thin,
    outcome-discarding wrapper, not a second implementation that could
    drift from the outcome-aware one."""
    ok_id = await _seed(session_maker, name="ok", credentials={"token": "ghp_real"})
    no_payload_id = await _seed(session_maker, name="no-payload")
    await _blank_out_payload(session_maker, no_payload_id)
    decrypt_failed_id = await _seed(session_maker, name="decrypt-failed")
    await _corrupt_encrypted_payload(session_maker, decrypt_failed_id)

    async with session_maker() as session:
        svc = IntegrationCredentialsService(session, ORG_ID)

        decrypted, cred = await svc.get_decrypted_credentials_by_id(ok_id)
        assert decrypted == {"token": "ghp_real"}
        assert cred is not None

        decrypted, cred = await svc.get_decrypted_credentials_by_id(no_payload_id)
        assert decrypted is None
        assert cred is not None

        decrypted, cred = await svc.get_decrypted_credentials_by_id(decrypt_failed_id)
        assert decrypted is None
        assert cred is not None

        decrypted, cred = await svc.get_decrypted_credentials_by_id(str(uuid.uuid4()))
        assert decrypted is None
        assert cred is None
