from __future__ import annotations

import importlib
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import IdentityMapping
from dev_health_ops.models.users import Membership, Organization, User
from tests._clickhouse_team_store import FakeClickHouseTeamStore
from tests._helpers import tables_of

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
admin_router_module = importlib.import_module("dev_health_ops.api.admin")

ORG_ID = str(uuid.uuid4())
USER_ID = str(uuid.uuid4())
ADMIN_EMAIL = "admin@example.com"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "identities.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(User, Organization, Membership, IdentityMapping),
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def client(session_maker):
    # CHAOS-2600 CS5: identity *reads* (list) stay on Postgres, but identity
    # *mutations* reflect membership into the ClickHouse team catalog rather
    # than writing Postgres IdentityMapping rows.
    ch_store = FakeClickHouseTeamStore()
    app = FastAPI()
    app.include_router(admin_router_module.router)

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    async def _ch_store_override():
        yield ch_store

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: (
        AuthenticatedUser(
            user_id=USER_ID,
            email=ADMIN_EMAIL,
            org_id=ORG_ID,
            role="owner",
            is_superuser=False,
        )
    )
    app.dependency_overrides[admin_router_module.get_session] = _session_override
    app.dependency_overrides[admin_router_module.get_clickhouse_store] = (
        _ch_store_override
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client, session_maker, ch_store

    app.dependency_overrides.clear()


async def _seed_ch_team(ch_store: FakeClickHouseTeamStore, team_id: str) -> None:
    await ch_store.insert_teams(
        [
            {
                "id": team_id,
                "name": team_id,
                "members": [],
                "is_active": 1,
                "org_id": ORG_ID,
            }
        ]
    )


async def _seed_ch_identity(
    ch_store: FakeClickHouseTeamStore,
    canonical_id: str,
    *,
    email: str | None = None,
    display_name: str | None = None,
    provider_identities: dict[str, list[str]] | None = None,
    team_ids: list[str] | None = None,
    is_active: int = 1,
) -> None:
    import json

    await ch_store.insert_identities(
        [
            {
                "org_id": ORG_ID,
                "canonical_id": canonical_id,
                "email": email,
                "display_name": display_name,
                "provider_identities": json.dumps(provider_identities or {}),
                "team_ids": team_ids or [],
                "is_active": is_active,
            }
        ]
    )


@pytest.mark.asyncio
async def test_list_identities_empty(client):
    async_client, _, _ = client
    response = await async_client.get("/api/v1/admin/identities")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_create_identity_returns_response_shape(client):
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-1")
    payload = {
        "canonical_id": "alice@example.com",
        "display_name": "Alice Smith",
        "email": "alice@example.com",
        "provider_identities": {"github": ["alice-gh"]},
        "team_ids": ["team-1"],
    }
    response = await async_client.post("/api/v1/admin/identities", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["canonical_id"] == "alice@example.com"
    assert data["display_name"] == "Alice Smith"
    assert data["email"] == "alice@example.com"
    assert data["provider_identities"] == {"github": ["alice-gh"]}
    assert data["team_ids"] == ["team-1"]
    assert data["is_active"] is True
    assert "id" in data
    assert "created_at" in data
    assert "updated_at" in data


@pytest.mark.asyncio
async def test_create_identity_reflects_membership_into_clickhouse(client):
    # CHAOS-2600 CS5: an identity linked to a team unions its facets into the
    # ClickHouse team's members; no Postgres IdentityMapping row is written.
    async_client, session_maker, ch_store = client
    await _seed_ch_team(ch_store, "platform")
    payload = {
        "canonical_id": "bob@example.com",
        "display_name": "Bob Jones",
        "email": "bob@example.com",
        "provider_identities": {"jira": ["bob-jira-id"]},
        "team_ids": ["platform"],
    }
    response = await async_client.post("/api/v1/admin/identities", json=payload)
    assert response.status_code == 200

    row = ch_store.rows[(ORG_ID, "platform")]
    assert set(row["members"]) == {"bob@example.com", "bob-jira-id"}

    # No Postgres IdentityMapping row was written.
    async with session_maker() as session:
        result = await session.execute(
            select(IdentityMapping).where(
                IdentityMapping.canonical_id == "bob@example.com",
                IdentityMapping.org_id == ORG_ID,
            )
        )
        assert result.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_create_identity_unknown_team_id_404_writes_nothing(client):
    # CHAOS-2600 CS5: with no Postgres IdentityMapping fallback, an unknown
    # team_id must 404 rather than silently dropping the membership write.
    async_client, _, ch_store = client
    response = await async_client.post(
        "/api/v1/admin/identities",
        json={
            "canonical_id": "ghost@example.com",
            "display_name": "Ghost",
            "email": "ghost@example.com",
            "provider_identities": {"github": ["ghost-gh"]},
            "team_ids": ["does-not-exist"],
        },
    )
    assert response.status_code == 404
    assert "does-not-exist" in response.json()["detail"]
    # No team row was created or mutated by the failed request.
    assert ch_store.rows == {}


@pytest.mark.asyncio
async def test_create_identity_mixed_team_ids_is_atomic(client):
    # Atomicity: one valid + one unknown team_id => 404 and NEITHER team is
    # mutated (the valid team must not be written before the bad one is hit).
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "valid-team")

    response = await async_client.post(
        "/api/v1/admin/identities",
        json={
            "canonical_id": "carol@example.com",
            "display_name": "Carol",
            "email": "carol@example.com",
            "provider_identities": {"github": ["carol-gh"]},
            "team_ids": ["valid-team", "missing-team"],
        },
    )
    assert response.status_code == 404
    assert "missing-team" in response.json()["detail"]
    # The valid team was seeded with no members and must remain untouched.
    assert ch_store.rows[(ORG_ID, "valid-team")]["members"] == []


@pytest.mark.asyncio
async def test_identity_record_id_is_stable_and_reassignment_moves_membership(client):
    # The identity id is deterministic per (org, canonical_id), so a repeat
    # upsert returns the same id. Reassigning team_ids MOVES membership: the
    # identity is removed from the team it left and added to the new one.
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-a")
    await _seed_ch_team(ch_store, "team-b")
    canonical_id = "carol@example.com"

    r1 = await async_client.post(
        "/api/v1/admin/identities",
        json={
            "canonical_id": canonical_id,
            "display_name": "Carol",
            "email": canonical_id,
            "provider_identities": {"github": ["carol-gh"]},
            "team_ids": ["team-a"],
        },
    )
    assert r1.status_code == 200
    original_id = r1.json()["id"]
    assert set(ch_store.rows[(ORG_ID, "team-a")]["members"]) == {
        "carol@example.com",
        "carol-gh",
    }

    r2 = await async_client.post(
        "/api/v1/admin/identities",
        json={
            "canonical_id": canonical_id,
            "display_name": "Carol Updated",
            "email": canonical_id,
            "provider_identities": {"github": ["carol-gh"], "jira": ["carol-jira"]},
            "team_ids": ["team-b"],
        },
    )
    assert r2.status_code == 200
    data = r2.json()
    assert data["id"] == original_id
    assert data["display_name"] == "Carol Updated"
    assert data["team_ids"] == ["team-b"]

    # team-a no longer contains carol's facets; team-b now does.
    assert ch_store.rows[(ORG_ID, "team-a")]["members"] == []
    assert set(ch_store.rows[(ORG_ID, "team-b")]["members"]) == {
        "carol@example.com",
        "carol-gh",
        "carol-jira",
    }


@pytest.mark.asyncio
async def test_identity_facet_change_replaces_member_facet_in_team(client):
    # Changing an identity's email replaces the facet in a retained team: the
    # old email must be removed and the new email present.
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-a")
    canonical_id = "dave"

    await async_client.post(
        "/api/v1/admin/identities",
        json={
            "canonical_id": canonical_id,
            "email": "e1@x",
            "provider_identities": {},
            "team_ids": ["team-a"],
        },
    )
    assert "e1@x" in ch_store.rows[(ORG_ID, "team-a")]["members"]

    await async_client.post(
        "/api/v1/admin/identities",
        json={
            "canonical_id": canonical_id,
            "email": "e2@x",
            "provider_identities": {},
            "team_ids": ["team-a"],
        },
    )
    members = set(ch_store.rows[(ORG_ID, "team-a")]["members"])
    assert "e2@x" in members
    assert "e1@x" not in members


@pytest.mark.asyncio
async def test_identity_omitted_email_is_preserved_not_removed(client):
    # An omitted email is PRESERVED by the store, so it must NOT be treated as a
    # removed facet on a retained team (facets derive from the stored record).
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-a")
    canonical_id = "fred"

    await async_client.post(
        "/api/v1/admin/identities",
        json={
            "canonical_id": canonical_id,
            "email": "e1@x",
            "provider_identities": {},
            "team_ids": ["team-a"],
        },
    )
    assert "e1@x" in ch_store.rows[(ORG_ID, "team-a")]["members"]

    # Re-POST changing ONLY provider_identities (email omitted entirely).
    resp = await async_client.post(
        "/api/v1/admin/identities",
        json={
            "canonical_id": canonical_id,
            "provider_identities": {"github": ["fred-gh"]},
            "team_ids": ["team-a"],
        },
    )
    assert resp.status_code == 200
    # The preserved email is still on the record and still in team-a members.
    assert resp.json()["email"] == "e1@x"
    members = set(ch_store.rows[(ORG_ID, "team-a")]["members"])
    assert "e1@x" in members
    assert "fred-gh" in members


@pytest.mark.asyncio
async def test_confirm_member_adds_complete_stored_facet_set(client):
    # Confirming a member into a team must add the COMPLETE stored facet set
    # (email + canonical + all provider ids), not just the new provider id.
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-b")
    # Pre-existing identity record with an email + a github id.
    await _seed_ch_identity(
        ch_store,
        "gita@example.com",
        email="gita@example.com",
        provider_identities={"github": ["gita-gh"]},
    )

    resp = await async_client.post(
        "/api/v1/admin/teams/team-b/confirm-members",
        json={
            "team_id": "team-b",
            "links": [
                {
                    "provider_identity": "gita-gl",
                    "provider": "gitlab",
                    "canonical_id": "gita@example.com",
                    "action": "link",
                }
            ],
        },
    )
    assert resp.status_code == 200
    members = set(ch_store.rows[(ORG_ID, "team-b")]["members"])
    # Complete set: email + canonical_id + existing github + new gitlab id.
    assert {"gita@example.com", "gita-gh", "gita-gl"} <= members


@pytest.mark.asyncio
async def test_confirm_inferred_member_rejects_provider_id_conflict(client):
    # A jira account_id already owned by a DIFFERENT canonical => 409; the same
    # canonical => ok.
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-c")
    # account_id ACC1 already belongs to canonical "owner@example.com".
    await _seed_ch_identity(
        ch_store,
        "owner@example.com",
        email="owner@example.com",
        provider_identities={"jira": ["ACC1"]},
    )
    # The intruder canonical exists (so the link-existence 404 doesn't mask the
    # provider-id conflict) but does NOT own ACC1.
    await _seed_ch_identity(
        ch_store, "intruder@example.com", email="intruder@example.com"
    )

    # Confirming ACC1 under a DIFFERENT canonical must 409.
    conflict = await async_client.post(
        "/api/v1/admin/teams/team-c/confirm-inferred-members",
        json={
            "team_id": "team-c",
            "members": [
                {
                    "account_id": "ACC1",
                    "action": "add",
                    "canonical_id": "intruder@example.com",
                }
            ],
        },
    )
    assert conflict.status_code == 409
    assert "ACC1" in conflict.json()["detail"]

    # Confirming ACC1 under the SAME canonical is allowed.
    ok = await async_client.post(
        "/api/v1/admin/teams/team-c/confirm-inferred-members",
        json={
            "team_id": "team-c",
            "members": [
                {
                    "account_id": "ACC1",
                    "action": "add",
                    "canonical_id": "owner@example.com",
                }
            ],
        },
    )
    assert ok.status_code == 200
    members = set(ch_store.rows[(ORG_ID, "team-c")]["members"])
    assert {"owner@example.com", "ACC1"} <= members


@pytest.mark.asyncio
async def test_confirm_members_batch_is_atomic_on_conflict(client):
    # A mixed confirm batch where a LATER link conflicts must leave ZERO
    # mutations: the earlier valid member's identity row is not created and the
    # team.members are unchanged (validation is all-or-nothing).
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-x")
    # ACC2 is already owned by "boss@example.com" (github id "boss-gh").
    await _seed_ch_identity(
        ch_store,
        "boss@example.com",
        email="boss@example.com",
        provider_identities={"github": ["boss-gh"]},
    )

    resp = await async_client.post(
        "/api/v1/admin/teams/team-x/confirm-members",
        json={
            "team_id": "team-x",
            "links": [
                {
                    "provider_identity": "valid-gh",
                    "provider": "github",
                    "canonical_id": "valid@example.com",
                    "action": "create",
                },
                {
                    "provider_identity": "boss-gh",
                    "provider": "github",
                    "canonical_id": "thief@example.com",
                    "action": "create",
                },
            ],
        },
    )
    assert resp.status_code == 409
    # The valid member was NOT created and the team was NOT mutated.
    assert (ORG_ID, "valid@example.com") not in ch_store.identities
    assert ch_store.rows[(ORG_ID, "team-x")]["members"] == []


@pytest.mark.asyncio
async def test_confirm_members_link_to_missing_identity_404(client):
    # A "link" action targets an EXISTING identity; a missing one => 404 with
    # no mutation (a link must never silently create).
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-y")

    resp = await async_client.post(
        "/api/v1/admin/teams/team-y/confirm-members",
        json={
            "team_id": "team-y",
            "links": [
                {
                    "provider_identity": "ghost-gh",
                    "provider": "github",
                    "canonical_id": "ghost@example.com",
                    "action": "link",
                }
            ],
        },
    )
    assert resp.status_code == 404
    assert (ORG_ID, "ghost@example.com") not in ch_store.identities
    assert ch_store.rows[(ORG_ID, "team-y")]["members"] == []


@pytest.mark.asyncio
async def test_confirm_inferred_explicit_missing_canonical_404(client):
    # An explicit canonical_id LINKS to an existing identity; a missing one =>
    # 404 with no mutation.
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-z")

    resp = await async_client.post(
        "/api/v1/admin/teams/team-z/confirm-inferred-members",
        json={
            "team_id": "team-z",
            "members": [
                {
                    "account_id": "ACC9",
                    "action": "add",
                    "canonical_id": "nobody@example.com",
                }
            ],
        },
    )
    assert resp.status_code == 404
    assert (ORG_ID, "nobody@example.com") not in ch_store.identities
    assert ch_store.rows[(ORG_ID, "team-z")]["members"] == []


@pytest.mark.asyncio
async def test_confirm_inferred_does_not_overwrite_stored_email(client):
    # Fill-only: an inferred email must NOT overwrite a value already on the
    # stored identity record (no silent clobber of a curated email).
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-f")
    await _seed_ch_identity(
        ch_store,
        "curated@example.com",
        email="e1@x",
        provider_identities={"jira": ["ACCF"]},
    )

    resp = await async_client.post(
        "/api/v1/admin/teams/team-f/confirm-inferred-members",
        json={
            "team_id": "team-f",
            "members": [
                {
                    "account_id": "ACCF",
                    "action": "add",
                    "canonical_id": "curated@example.com",
                    "email": "e2@x",
                }
            ],
        },
    )
    assert resp.status_code == 200
    # The stored email is preserved; the inferred e2@x did not clobber it.
    assert ch_store.identities[(ORG_ID, "curated@example.com")]["email"] == "e1@x"
    members = set(ch_store.rows[(ORG_ID, "team-f")]["members"])
    assert "e1@x" in members
    assert "e2@x" not in members


def _confirm_members_self_contradictory_payload() -> dict:
    # Two CREATE links share one (provider, provider_identity) but resolve to
    # DIFFERENT canonicals — neither exists in the store yet, so only an
    # intra-batch check can catch it.
    return {
        "links": [
            {
                "provider_identity": "dup-gh",
                "provider": "github",
                "canonical_id": "a@example.com",
                "action": "create",
            },
            {
                "provider_identity": "dup-gh",
                "provider": "github",
                "canonical_id": "b@example.com",
                "action": "create",
            },
        ],
    }


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.asyncio
async def test_confirm_members_intra_batch_conflict_409_no_mutation(client, reverse):
    # A self-contradictory batch (same provider id -> two canonicals) must 409
    # before any write, regardless of member order.
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-ib")
    body = {"team_id": "team-ib", **_confirm_members_self_contradictory_payload()}
    if reverse:
        body["links"] = list(reversed(body["links"]))

    resp = await async_client.post(
        "/api/v1/admin/teams/team-ib/confirm-members", json=body
    )
    assert resp.status_code == 409
    # Zero mutations: neither identity row created, team.members unchanged.
    assert (ORG_ID, "a@example.com") not in ch_store.identities
    assert (ORG_ID, "b@example.com") not in ch_store.identities
    assert ch_store.rows[(ORG_ID, "team-ib")]["members"] == []


@pytest.mark.asyncio
async def test_confirm_members_same_provider_id_same_canonical_ok(client):
    # The same provider id mapped to the SAME canonical twice is idempotent.
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-ok")
    resp = await async_client.post(
        "/api/v1/admin/teams/team-ok/confirm-members",
        json={
            "team_id": "team-ok",
            "links": [
                {
                    "provider_identity": "same-gh",
                    "provider": "github",
                    "canonical_id": "same@example.com",
                    "action": "create",
                },
                {
                    "provider_identity": "same-gh",
                    "provider": "github",
                    "canonical_id": "same@example.com",
                    "action": "create",
                },
            ],
        },
    )
    assert resp.status_code == 200
    assert (ORG_ID, "same@example.com") in ch_store.identities
    members = set(ch_store.rows[(ORG_ID, "team-ok")]["members"])
    assert {"same@example.com", "same-gh"} <= members


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.asyncio
async def test_confirm_inferred_intra_batch_mint_vs_explicit_409(client, reverse):
    # One Jira account_id appears twice: once minted (-> jira:{account_id}) and
    # once with an explicit DIFFERENT existing canonical -> 409, no mutation.
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-im")
    await _seed_ch_identity(
        ch_store, "explicit@example.com", email="explicit@example.com"
    )
    members = [
        {"account_id": "DUP", "action": "add"},  # minted -> jira:DUP
        {
            "account_id": "DUP",
            "action": "add",
            "canonical_id": "explicit@example.com",
        },
    ]
    if reverse:
        members = list(reversed(members))

    resp = await async_client.post(
        "/api/v1/admin/teams/team-im/confirm-inferred-members",
        json={"team_id": "team-im", "members": members},
    )
    assert resp.status_code == 409
    assert (ORG_ID, "jira:DUP") not in ch_store.identities
    # The pre-seeded explicit identity must not have gained the team or account.
    explicit = ch_store.identities[(ORG_ID, "explicit@example.com")]
    assert "team-im" not in explicit["team_ids"]
    assert ch_store.rows[(ORG_ID, "team-im")]["members"] == []


@pytest.mark.asyncio
async def test_confirm_inferred_intra_batch_two_explicit_canonicals_409(client):
    # One account_id claimed by two DIFFERENT explicit canonicals -> 409.
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-2c")
    await _seed_ch_identity(ch_store, "c1@example.com", email="c1@example.com")
    await _seed_ch_identity(ch_store, "c2@example.com", email="c2@example.com")

    resp = await async_client.post(
        "/api/v1/admin/teams/team-2c/confirm-inferred-members",
        json={
            "team_id": "team-2c",
            "members": [
                {"account_id": "X", "action": "add", "canonical_id": "c1@example.com"},
                {"account_id": "X", "action": "add", "canonical_id": "c2@example.com"},
            ],
        },
    )
    assert resp.status_code == 409
    assert "team-2c" not in ch_store.identities[(ORG_ID, "c1@example.com")]["team_ids"]
    assert "team-2c" not in ch_store.identities[(ORG_ID, "c2@example.com")]["team_ids"]
    assert ch_store.rows[(ORG_ID, "team-2c")]["members"] == []


@pytest.mark.asyncio
async def test_confirm_inferred_intra_batch_two_mints_same_account_ok(client):
    # The same account_id minted twice resolves to the SAME canonical -> ok.
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-2m")
    resp = await async_client.post(
        "/api/v1/admin/teams/team-2m/confirm-inferred-members",
        json={
            "team_id": "team-2m",
            "members": [
                {"account_id": "M", "action": "add"},
                {"account_id": "M", "action": "add"},
            ],
        },
    )
    assert resp.status_code == 200
    assert (ORG_ID, "jira:M") in ch_store.identities
    members = set(ch_store.rows[(ORG_ID, "team-2m")]["members"])
    assert {"jira:M", "M"} <= members


@pytest.mark.asyncio
async def test_create_identity_persists_record_to_clickhouse(client):
    # The identity record itself is persisted to the ClickHouse `identities` table.
    async_client, _, ch_store = client
    await _seed_ch_team(ch_store, "team-1")
    await async_client.post(
        "/api/v1/admin/identities",
        json={
            "canonical_id": "erin@example.com",
            "display_name": "Erin",
            "email": "erin@example.com",
            "provider_identities": {"github": ["erin-gh"]},
            "team_ids": ["team-1"],
        },
    )
    row = ch_store.identities[(ORG_ID, "erin@example.com")]
    assert row["email"] == "erin@example.com"
    assert row["team_ids"] == ["team-1"]


@pytest.mark.asyncio
async def test_list_identities_returns_clickhouse_record(client):
    # list_identities reads the ClickHouse-native catalog, not Postgres.
    async_client, _, ch_store = client
    await _seed_ch_identity(
        ch_store,
        "ch-only@example.com",
        email="ch-only@example.com",
        provider_identities={"github": ["ch-gh"]},
        team_ids=["t1"],
    )
    response = await async_client.get("/api/v1/admin/identities")
    assert response.status_code == 200
    body = response.json()
    assert [m["canonical_id"] for m in body] == ["ch-only@example.com"]
    assert body[0]["provider_identities"] == {"github": ["ch-gh"]}
    assert body[0]["team_ids"] == ["t1"]


@pytest.mark.asyncio
async def test_list_identities_active_only_filter(client):
    async_client, _, ch_store = client
    await _seed_ch_identity(
        ch_store, "active@example.com", email="active@example.com", is_active=1
    )
    await _seed_ch_identity(
        ch_store, "inactive@example.com", email="inactive@example.com", is_active=0
    )

    # Default active_only=True
    response = await async_client.get("/api/v1/admin/identities")
    assert response.status_code == 200
    canonical_ids = [m["canonical_id"] for m in response.json()]
    assert "active@example.com" in canonical_ids
    assert "inactive@example.com" not in canonical_ids


@pytest.mark.asyncio
async def test_list_identities_includes_inactive_when_false(client):
    async_client, _, ch_store = client
    await _seed_ch_identity(
        ch_store, "active2@example.com", email="active2@example.com", is_active=1
    )
    await _seed_ch_identity(
        ch_store, "inactive2@example.com", email="inactive2@example.com", is_active=0
    )

    response = await async_client.get("/api/v1/admin/identities?active_only=false")
    assert response.status_code == 200
    canonical_ids = [m["canonical_id"] for m in response.json()]
    assert "active2@example.com" in canonical_ids
    assert "inactive2@example.com" in canonical_ids


@pytest.mark.asyncio
async def test_find_by_provider_identity_resolves_from_clickhouse():
    # ClickHouseIdentityStore.find_by_provider_identity resolves a stored
    # provider id from the ClickHouse-native identity catalog.
    from typing import cast

    from dev_health_ops.api.services.configuration.clickhouse_identity_admin import (
        ClickHouseIdentityStore,
    )
    from dev_health_ops.storage.clickhouse import ClickHouseStore

    ch_store = FakeClickHouseTeamStore()
    await _seed_ch_identity(
        ch_store,
        "frank@example.com",
        email="frank@example.com",
        provider_identities={"jira": ["frank-jira-id"], "github": ["frank-gh"]},
    )
    id_store = ClickHouseIdentityStore(cast(ClickHouseStore, ch_store), ORG_ID)

    matched = await id_store.find_by_provider_identity("jira", "frank-jira-id")
    assert matched is not None
    assert matched.canonical_id == "frank@example.com"

    assert await id_store.find_by_provider_identity("jira", "nobody") is None
