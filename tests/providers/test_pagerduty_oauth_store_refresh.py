from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import anyio
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.models.settings import ProviderOAuthCredential
from dev_health_ops.providers.pagerduty.oauth import OAuthTokens, PagerDutyOAuthConfig
from dev_health_ops.providers.pagerduty.oauth_lifecycle import (
    ClientCredentialsCacheKey,
    ClientCredentialsRequest,
    ClientCredentialsTokenCacheRegistry,
    get_client_credentials_access_token_keyed,
    get_valid_access_token,
)
from dev_health_ops.providers.pagerduty.oauth_storage import (
    OAuthRotationConflictError,
    PagerDutyOAuthCredentialRepository,
    VersionedOAuthTokens,
)


@pytest.fixture(autouse=True)
def encryption_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")


@pytest_asyncio.fixture
async def session() -> AsyncIterator[AsyncSession]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(ProviderOAuthCredential.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as database_session:
        yield database_session
    await engine.dispose()


def tokens(
    access_token: str,
    *,
    refresh_token: str | None = "refresh",
    expires_at: datetime | None = None,
    granted_scopes: frozenset[str] = frozenset({"Users.read"}),
) -> OAuthTokens:
    return OAuthTokens(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=expires_at or datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=granted_scopes,
    )


@pytest.mark.asyncio
async def test_create_or_replace_reconnects_with_new_binding_and_version(
    session: AsyncSession,
) -> None:
    repository = PagerDutyOAuthCredentialRepository(session, "org", "primary")

    first_version = await repository.create_or_replace(
        tokens("first"), binding_id="binding-first", account_id="account-first"
    )
    second_version = await repository.create_or_replace(
        tokens("second"),
        binding_id="binding-second",
        account_id="account-second",
        account_display="Primary account",
    )

    stored = await repository.get()

    assert first_version == 1
    assert second_version == 2
    assert stored is not None
    assert stored.tokens.access_token == "second"
    assert stored.version == 2
    assert stored.binding_id == "binding-second"


@pytest.mark.asyncio
async def test_rotate_rejects_a_binding_mismatch(session: AsyncSession) -> None:
    repository = PagerDutyOAuthCredentialRepository(session, "org")
    await repository.create_or_replace(tokens("first"), binding_id="binding-first")

    with pytest.raises(OAuthRotationConflictError):
        await repository.rotate(
            1,
            tokens("second"),
            expected_binding_id="different-binding",
        )


@pytest.mark.asyncio
async def test_rotate_rejects_a_version_conflict(session: AsyncSession) -> None:
    repository = PagerDutyOAuthCredentialRepository(session, "org")
    await repository.create_or_replace(tokens("first"), binding_id="binding-first")
    await repository.rotate(1, tokens("second"), expected_binding_id="binding-first")

    with pytest.raises(OAuthRotationConflictError):
        await repository.rotate(1, tokens("third"), expected_binding_id="binding-first")


@pytest.mark.asyncio
async def test_get_status_metadata_does_not_decrypt_tokens(
    session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(UTC)
    session.add(
        ProviderOAuthCredential(
            org_id="org",
            provider="pagerduty",
            credential_name="default",
            token_encrypted="undecryptable-garbage",
            version=3,
            created_at=now,
            updated_at=now,
            binding_id="binding",
            expires_at=now + timedelta(hours=1),
            granted_scopes=["Users.read", "Incidents.read"],
            has_refresh_token=True,
            account_id="account",
            account_display="Operations",
        )
    )
    await session.flush()
    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_storage.decrypt_value",
        lambda _: (_ for _ in ()).throw(AssertionError("must not decrypt")),
    )

    metadata = await PagerDutyOAuthCredentialRepository(
        session, "org"
    ).get_status_metadata()

    assert metadata is not None
    assert metadata.binding_id == "binding"
    assert metadata.granted_scopes == frozenset({"Incidents.read", "Users.read"})
    assert metadata.has_refresh_token is True
    assert metadata.account_id == "account"
    assert metadata.account_display == "Operations"
    assert metadata.version == 3


@dataclass
class SerializingCredentialStore:
    current: VersionedOAuthTokens
    second_initial_read: anyio.Event = field(default_factory=anyio.Event)
    rotations: int = 0
    _row_lock: anyio.Lock = field(default_factory=anyio.Lock)
    _get_calls: int = 0

    async def get(self) -> VersionedOAuthTokens:
        self._get_calls += 1
        if self._get_calls == 2:
            self.second_initial_read.set()
        return self.current

    async def get_for_update(self) -> VersionedOAuthTokens:
        await self._row_lock.acquire()
        return self.current

    async def rotate(
        self,
        current_version: int,
        tokens: OAuthTokens,
        *,
        expected_binding_id: str,
    ) -> int:
        if (
            current_version != self.current.version
            or expected_binding_id != self.current.binding_id
        ):
            self._row_lock.release()
            raise OAuthRotationConflictError("conflict")
        self.rotations += 1
        self.current = VersionedOAuthTokens(
            tokens,
            current_version + 1,
            expected_binding_id,
        )
        self._row_lock.release()
        return self.current.version

    async def delete(self) -> None:
        return None


@pytest.mark.asyncio
async def test_concurrent_refresh_is_serialized_per_credential_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refresh_started = anyio.Event()
    allow_refresh = anyio.Event()
    refresh_calls = 0

    async def refresh(_: PagerDutyOAuthConfig, __: str) -> OAuthTokens:
        nonlocal refresh_calls
        refresh_calls += 1
        refresh_started.set()
        await allow_refresh.wait()
        return tokens("fresh")

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_lifecycle.refresh_tokens", refresh
    )
    store = SerializingCredentialStore(
        VersionedOAuthTokens(
            tokens("stale", expires_at=datetime.now(UTC) - timedelta(seconds=1)),
            1,
            "binding",
        )
    )
    config = PagerDutyOAuthConfig("id", "secret", "uri")
    results: list[str] = []

    async def load_token() -> None:
        results.append(await get_valid_access_token(store, config))

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(load_token)
        await refresh_started.wait()
        task_group.start_soon(load_token)
        await store.second_initial_read.wait()
        allow_refresh.set()

    assert results == ["fresh", "fresh"]
    assert refresh_calls == 1
    assert store.rotations == 1


@pytest.mark.asyncio
async def test_keyed_client_credentials_cache_does_not_cross_account_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issued_subdomains: list[str] = []

    async def exchange(
        _: PagerDutyOAuthConfig, *, scopes: set[str], subdomain: str, region: str
    ) -> OAuthTokens:
        issued_subdomains.append(subdomain)
        return tokens(
            f"token-{subdomain}-{region}",
            refresh_token=None,
            granted_scopes=frozenset(scopes),
        )

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_lifecycle.client_credentials",
        exchange,
    )
    registry = ClientCredentialsTokenCacheRegistry()
    config = PagerDutyOAuthConfig("id", "secret", "uri")
    request_one = ClientCredentialsRequest(frozenset({"Users.read"}), "one", "us")
    request_two = ClientCredentialsRequest(frozenset({"Users.read"}), "two", "us")
    key_one: ClientCredentialsCacheKey = (
        "org-one",
        "primary",
        request_one.scopes,
        request_one.subdomain,
        request_one.region,
        "secret-one",
    )
    key_two: ClientCredentialsCacheKey = (
        "org-two",
        "primary",
        request_two.scopes,
        request_two.subdomain,
        request_two.region,
        "secret-two",
    )

    first = await get_client_credentials_access_token_keyed(
        registry, key_one, config, request_one
    )
    second = await get_client_credentials_access_token_keyed(
        registry, key_two, config, request_two
    )
    first_again = await get_client_credentials_access_token_keyed(
        registry, key_one, config, request_one
    )

    assert first == "token-one-us"
    assert second == "token-two-us"
    assert first_again == "token-one-us"
    assert issued_subdomains == ["one", "two"]


@pytest.mark.asyncio
async def test_reads_force_fresh_state_after_external_rotation(
    session: AsyncSession,
) -> None:
    from sqlalchemy import update

    repository = PagerDutyOAuthCredentialRepository(session, "org", "primary")
    await repository.create_or_replace(tokens("first"), binding_id="binding-first")

    # Populate the session identity map at version 1.
    initial = await repository.get()
    assert initial is not None
    assert initial.version == 1

    # Rotate the row via a Core UPDATE, which bypasses the ORM identity map.
    await session.execute(
        update(ProviderOAuthCredential)
        .where(
            ProviderOAuthCredential.org_id == "org",
            ProviderOAuthCredential.provider == "pagerduty",
            ProviderOAuthCredential.credential_name == "primary",
        )
        .values(version=42)
    )
    await session.flush()

    # populate_existing must force a DB re-read, not return the cached v1 object.
    refreshed = await repository.get()
    assert refreshed is not None
    assert refreshed.version == 42
    locked = await repository.get_for_update()
    assert locked is not None
    assert locked.version == 42
