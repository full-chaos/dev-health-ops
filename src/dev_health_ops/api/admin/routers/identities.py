from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    ConfirmInferredMembersRequest,
    ConfirmInferredMembersResponse,
    ConfirmMembersRequest,
    ConfirmMembersResponse,
    IdentityMappingCreate,
    IdentityMappingResponse,
    JiraActivityInferenceResponse,
    TeamMembersDiscoverResponse,
)
from dev_health_ops.api.services.configuration import (
    AmbiguousCredentialError,
    IntegrationCredentialsService,
    JiraActivityInferenceService,
    TeamMembershipService,
)
from dev_health_ops.api.services.configuration.clickhouse_identity_admin import (
    ClickHouseIdentityStore,
)
from dev_health_ops.api.services.configuration.clickhouse_team_admin import (
    ClickHouseTeamAdminService,
    member_facets,
)
from dev_health_ops.storage.clickhouse import ClickHouseStore

from .common import get_clickhouse_store, get_session

router = APIRouter()


def _identity_mapping_response(mapping: object) -> IdentityMappingResponse:
    return IdentityMappingResponse.model_validate(
        {
            "id": str(getattr(mapping, "id")),
            "canonical_id": getattr(mapping, "canonical_id"),
            "display_name": getattr(mapping, "display_name"),
            "email": getattr(mapping, "email"),
            "provider_identities": getattr(mapping, "provider_identities") or {},
            "team_ids": list(getattr(mapping, "team_ids") or []),
            "is_active": getattr(mapping, "is_active"),
            "created_at": getattr(mapping, "created_at"),
            "updated_at": getattr(mapping, "updated_at"),
        }
    )


def _stored_facets(record: object) -> set[str]:
    """Member facets derived from a STORED ClickHouse identity record.

    Always derive facets from the resolved/stored record (which preserves
    omitted fields), never from the raw request payload — otherwise an
    omitted-but-preserved facet would be wrongly treated as removed.
    """
    return member_facets(
        canonical_id=getattr(record, "canonical_id", None),
        email=getattr(record, "email", None),
        display_name=getattr(record, "display_name", None),
        provider_identities=getattr(record, "provider_identities", None),
    )


async def _assert_provider_identities_unowned(
    id_store: ClickHouseIdentityStore,
    *,
    canonical_id: str,
    provider_identities: dict[str, list[str]],
) -> None:
    """Reject (409) if any provider identity is already owned by a DIFFERENT
    canonical identity.

    A provider identity (e.g. a Jira account id) must map to at most one
    canonical identity — otherwise ``find_by_provider_identity`` becomes
    order-dependent. Mirrors the conflict semantics the retired Postgres
    ``match_and_confirm`` enforced.
    """
    for provider, identities in provider_identities.items():
        for identity in identities:
            owner = await id_store.find_by_provider_identity(provider, identity)
            if owner is not None and owner.canonical_id != canonical_id:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Provider identity '{provider}:{identity}' is already "
                        f"linked to a different canonical identity "
                        f"'{owner.canonical_id}'"
                    ),
                )


def _assert_no_intra_batch_conflict(
    seen: dict[tuple[str, str], str],
    *,
    canonical_id: str,
    provider_identities: dict[str, list[str]],
) -> None:
    """Reject (409) if a provider identity is claimed by two DIFFERENT canonical
    identities WITHIN the same request batch.

    The store-snapshot check (:func:`_assert_provider_identities_unowned`) only
    sees rows that exist before the batch runs, so a self-contradictory batch
    (two non-skip members sharing one provider identity but different
    canonicals) would pass it and then write conflicting ownership in PASS 2.
    ``seen`` records each ``(provider, identity)`` -> canonical claimed so far;
    a divergent claim raises here, before any mutation. Same canonical is
    idempotent and allowed.
    """
    for provider, identities in provider_identities.items():
        for identity in identities:
            key = (provider, identity)
            prior = seen.get(key)
            if prior is not None and prior != canonical_id:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Provider identity '{provider}:{identity}' is claimed by "
                        f"two different canonical identities in the same request "
                        f"('{prior}' and '{canonical_id}')"
                    ),
                )
            seen[key] = canonical_id


@router.get("/identities", response_model=list[IdentityMappingResponse])
async def list_identities(
    active_only: bool = True,
    store: ClickHouseStore = Depends(get_clickhouse_store),
    org_id: str = Depends(get_admin_org_id),
) -> list[IdentityMappingResponse]:
    # ClickHouse is the identity system of record (CHAOS-2600 CS5).
    id_store = ClickHouseIdentityStore(store, org_id)
    mappings = await id_store.list_all(active_only=active_only)
    return [_identity_mapping_response(mapping) for mapping in mappings]


@router.post("/identities", response_model=IdentityMappingResponse)
async def create_or_update_identity(
    payload: IdentityMappingCreate,
    store: ClickHouseStore = Depends(get_clickhouse_store),
    org_id: str = Depends(get_admin_org_id),
) -> IdentityMappingResponse:
    # ClickHouse is the system of record for BOTH the identity record and
    # identity->team membership (CHAOS-2600 CS5). Updating an identity uses
    # REPLACEMENT semantics: it must remove the identity's facets from teams it
    # left, and drop changed-away facets from teams it stayed in. Membership is
    # edited surgically (add/remove specific facets) so Auto Import / catalog
    # members are preserved.
    team_svc = ClickHouseTeamAdminService(store, org_id)
    id_store = ClickHouseIdentityStore(store, org_id)

    # Atomic validate-then-write: preflight ALL team_ids and 404 before any
    # mutation so the write is all-or-nothing (no partial membership writes).
    missing = [tid for tid in payload.team_ids if await team_svc.get(tid) is None]
    if missing:
        raise HTTPException(
            status_code=404, detail=f"Unknown team_id(s): {sorted(set(missing))}"
        )

    # A submitted provider identity must not already belong to a different
    # canonical identity (keeps find_by_provider_identity deterministic).
    await _assert_provider_identities_unowned(
        id_store,
        canonical_id=payload.canonical_id,
        provider_identities=payload.provider_identities,
    )

    old = await id_store.get(payload.canonical_id)
    # Facets must come from the RESOLVED records, not the raw payload. The store
    # preserves omitted email/display_name/provider_identities, so deriving from
    # the payload would treat an omitted-but-preserved facet as removed.
    old_facets = _stored_facets(old) if old is not None else set()
    old_team_ids = set(old.team_ids) if old is not None else set()

    new_team_ids = set(payload.team_ids)

    stored = await id_store.create_or_update(
        canonical_id=payload.canonical_id,
        display_name=payload.display_name,
        email=payload.email,
        provider_identities=payload.provider_identities,
        team_ids=payload.team_ids,
    )
    new_facets = _stored_facets(stored)
    stale_facets = old_facets - new_facets

    # Teams the identity left entirely: drop ALL of its old facets.
    for team_id in old_team_ids - new_team_ids:
        await team_svc.remove_members(team_id, old_facets)
    # Teams retained or newly joined: drop changed-away facets, then add the
    # current (complete, resolved) facet set.
    for team_id in new_team_ids:
        if stale_facets:
            await team_svc.remove_members(team_id, stale_facets)
        await team_svc.add_members(team_id, sorted(new_facets))

    return _identity_mapping_response(stored)


@router.get(
    "/teams/{team_id}/discover-members",
    response_model=TeamMembersDiscoverResponse,
)
async def discover_team_members(
    team_id: str,
    provider: str = Query(..., pattern="^(github|gitlab|jira|linear)$"),
    credential_id: str | None = Query(None),
    credential_name: str | None = Query(None),
    session: AsyncSession = Depends(get_session),
    store: ClickHouseStore = Depends(get_clickhouse_store),
    org_id: str = Depends(get_admin_org_id),
) -> TeamMembersDiscoverResponse:
    # Team catalog lives in ClickHouse (CHAOS-2600 CS5); the Postgres session
    # is still used for credentials + identity matching reads only.
    team = await ClickHouseTeamAdminService(store, org_id).get(team_id)
    if team is None:
        raise HTTPException(status_code=404, detail="Team not found")

    creds_svc = IntegrationCredentialsService(session, org_id)
    try:
        credential, decrypted = await creds_svc.resolve_with_fallback(
            provider, name=credential_name, credential_id=credential_id
        )
    except AmbiguousCredentialError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if credential is None or decrypted is None:
        raise HTTPException(
            status_code=404,
            detail=f"No credentials found for provider '{provider}'",
        )

    config: dict[str, Any] = getattr(credential, "config") or {}
    membership_svc = TeamMembershipService(session, org_id)
    # Admin/imported teams carry their provider key in the ClickHouse team id
    # (e.g. ``gh:backend``); fall back to it as the provider team identifier.
    provider_team_id = str(team_id)

    if provider == "github":
        token = decrypted.get("token")
        org_name_value = config.get("org")
        org_name = org_name_value if isinstance(org_name_value, str) else None
        team_slug = provider_team_id.removeprefix("gh:")
        if not token or not org_name:
            raise HTTPException(
                status_code=400,
                detail="GitHub credentials require token and config.org",
            )
        members = await membership_svc.discover_members_github(
            token=token,
            org_name=org_name,
            team_slug=team_slug,
        )
    elif provider == "gitlab":
        token = decrypted.get("token")
        group_path = provider_team_id.removeprefix("gl:")
        url_value = config.get("url", "https://gitlab.com")
        url = url_value if isinstance(url_value, str) else "https://gitlab.com"
        if not token or not group_path:
            raise HTTPException(
                status_code=400,
                detail="GitLab credentials require token and team provider path",
            )
        members = await membership_svc.discover_members_gitlab(
            token=token,
            group_path=group_path,
            url=url,
        )
    elif provider == "linear":
        api_key = decrypted.get("apiKey") or decrypted.get("api_key")
        if not api_key:
            raise HTTPException(
                status_code=400,
                detail="Linear credentials require apiKey",
            )
        members = await membership_svc.discover_members_linear(
            api_key=api_key,
            team_key=provider_team_id,
        )
    else:
        email = decrypted.get("email")
        api_token = decrypted.get("api_token") or decrypted.get("token")
        jira_config_url = config.get("url")
        jira_url = jira_config_url if isinstance(jira_config_url, str) else None
        if jira_url is None:
            decrypted_url = decrypted.get("url")
            jira_url = decrypted_url if isinstance(decrypted_url, str) else None
        project_key = provider_team_id
        if ":" in project_key:
            project_key = project_key.split(":", 1)[1]
        team_project_keys = list(getattr(team, "project_keys") or [])
        if not project_key and team_project_keys:
            project_key = str(team_project_keys[0])
        if not email or not api_token or not jira_url or not project_key:
            raise HTTPException(
                status_code=400,
                detail="Jira credentials require email, api_token, url, and project key",
            )
        members = await membership_svc.discover_members_jira(
            email=email,
            api_token=api_token,
            url=jira_url,
            project_key=project_key,
        )

    # Match against the ClickHouse-native identity catalog (CHAOS-2600 CS5).
    matched = await membership_svc.match_members(
        members, identity_store=ClickHouseIdentityStore(store, org_id)
    )
    return TeamMembersDiscoverResponse(
        team_id=team_id,
        provider=provider,
        members=matched,
        total=len(matched),
    )


@router.post(
    "/teams/{team_id}/confirm-members",
    response_model=ConfirmMembersResponse,
)
async def confirm_team_members(
    team_id: str,
    payload: ConfirmMembersRequest,
    store: ClickHouseStore = Depends(get_clickhouse_store),
    org_id: str = Depends(get_admin_org_id),
) -> ConfirmMembersResponse:
    # ClickHouse is the system of record for teams AND identities (CHAOS-2600
    # CS5). Confirming a member upserts the identity record (merging this team
    # into its team_ids) and unions its facets into the ClickHouse team
    # ``members``. No Postgres ``IdentityMapping`` is written.
    if payload.team_id != team_id:
        raise HTTPException(
            status_code=400, detail="team_id mismatch between path and body"
        )

    team_svc = ClickHouseTeamAdminService(store, org_id)
    team = await team_svc.get(team_id)
    if team is None:
        raise HTTPException(status_code=404, detail="Team not found")

    id_store = ClickHouseIdentityStore(store, org_id)
    linked = 0
    created = 0
    skipped = 0

    # PASS 1 — validate the WHOLE batch with NO mutation. ClickHouse has no
    # multi-statement transactions, so a 409/404 must be raised before any
    # upsert/add_members runs, otherwise a later failure leaves earlier members
    # partially applied. Collect the actionable links for pass 2.
    #
    # ``seen`` closes the intra-batch hole: the store check only sees the
    # PRE-mutation snapshot, so two same-request members sharing one provider
    # identity but resolving to DIFFERENT canonicals would both pass the store
    # check yet write conflicting ownership. Track (provider, identity) ->
    # canonical within this request and reject a divergent mapping.
    seen: dict[tuple[str, str], str] = {}
    actionable: list[Any] = []
    for link in payload.links:
        if link.action == "skip":
            skipped += 1
            continue
        # A "link" action attaches to an EXISTING identity — it must exist; a
        # link action must not silently create one. "create" makes a new record.
        if link.action == "link" and await id_store.get(link.canonical_id) is None:
            raise HTTPException(
                status_code=404,
                detail=f"Identity '{link.canonical_id}' not found",
            )
        # The provider identity must not already belong to a different canonical
        # in the store, nor to a different canonical earlier in this batch.
        await _assert_provider_identities_unowned(
            id_store,
            canonical_id=link.canonical_id,
            provider_identities={link.provider: [link.provider_identity]},
        )
        _assert_no_intra_batch_conflict(
            seen,
            canonical_id=link.canonical_id,
            provider_identities={link.provider: [link.provider_identity]},
        )
        actionable.append(link)

    # PASS 2 — apply. The whole batch validated, so these mutations run.
    facets: set[str] = set()
    for link in actionable:
        # Keep the identity record authoritative: merge this team into the
        # identity's team_ids and record the provider identity. Upsert FIRST,
        # then add the COMPLETE stored facet set (so the identity's existing
        # email / other-provider facets also land in team.members).
        existing = await id_store.get(link.canonical_id)
        team_ids = sorted({*(existing.team_ids if existing else []), team_id})
        providers = dict(existing.provider_identities) if existing else {}
        provider_ids = sorted(
            {*providers.get(link.provider, []), link.provider_identity}
        )
        providers[link.provider] = provider_ids
        stored = await id_store.create_or_update(
            canonical_id=link.canonical_id,
            provider_identities=providers,
            team_ids=team_ids,
        )
        facets |= _stored_facets(stored)
        if link.action == "link":
            linked += 1
        else:  # "create"
            created += 1

    if facets:
        await team_svc.add_members(team_id, sorted(facets))
    return ConfirmMembersResponse(linked=linked, created=created, skipped=skipped)


@router.get(
    "/teams/{team_id}/infer-members",
    response_model=JiraActivityInferenceResponse,
)
async def infer_team_members_from_jira_activity(
    team_id: str,
    window_days: int = Query(90, ge=1, le=365),
    credential_id: str | None = Query(None),
    credential_name: str | None = Query(None),
    session: AsyncSession = Depends(get_session),
    store: ClickHouseStore = Depends(get_clickhouse_store),
    org_id: str = Depends(get_admin_org_id),
) -> JiraActivityInferenceResponse:
    # Team catalog lives in ClickHouse (CHAOS-2600 CS5).
    team = await ClickHouseTeamAdminService(store, org_id).get(team_id)
    if team is None:
        raise HTTPException(status_code=404, detail="Team not found")

    team_project_keys = list(getattr(team, "project_keys") or [])
    project_key = str(team_project_keys[0]) if team_project_keys else None
    # Jira teams use their bare project key as the team id (no provider
    # prefix); use it as the project key when none is configured explicitly.
    if not project_key and ":" not in team_id:
        project_key = team_id
    if not project_key:
        raise HTTPException(
            status_code=400,
            detail="Team does not have a Jira project key configured",
        )

    creds_svc = IntegrationCredentialsService(session, org_id)
    try:
        credential, decrypted = await creds_svc.resolve_with_fallback(
            "jira", name=credential_name, credential_id=credential_id
        )
    except AmbiguousCredentialError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if credential is None or decrypted is None:
        raise HTTPException(
            status_code=404,
            detail="No credentials found for provider 'jira'",
        )

    config: dict[str, Any] = getattr(credential, "config") or {}
    email = decrypted.get("email")
    api_token = decrypted.get("api_token") or decrypted.get("token")
    jira_config_url = config.get("url")
    jira_url = jira_config_url if isinstance(jira_config_url, str) else None
    if jira_url is None:
        decrypted_url = decrypted.get("url")
        jira_url = decrypted_url if isinstance(decrypted_url, str) else None
    if not email or not api_token or not jira_url:
        raise HTTPException(
            status_code=400,
            detail="Jira credentials require email, api_token, and url",
        )

    inference_svc = JiraActivityInferenceService(session, org_id)
    inferred_members = await inference_svc.infer_members(
        email=email,
        api_token=api_token,
        jira_url=jira_url,
        project_key=project_key,
        window_days=window_days,
    )

    # Enrich against the ClickHouse-native identity catalog (CHAOS-2600 CS5).
    id_store = ClickHouseIdentityStore(store, org_id)
    for member in inferred_members:
        matched = await id_store.find_by_provider_identity("jira", member.account_id)
        if matched is not None:
            if not member.display_name and matched.display_name:
                member.display_name = str(matched.display_name)
            if not member.email and matched.email:
                member.email = str(matched.email)

    return JiraActivityInferenceResponse(
        team_id=team_id,
        project_key=project_key,
        window_days=window_days,
        inferred_members=inferred_members,
        total=len(inferred_members),
    )


@router.post(
    "/teams/{team_id}/confirm-inferred-members",
    response_model=ConfirmInferredMembersResponse,
)
async def confirm_inferred_team_members(
    team_id: str,
    payload: ConfirmInferredMembersRequest,
    store: ClickHouseStore = Depends(get_clickhouse_store),
    org_id: str = Depends(get_admin_org_id),
) -> ConfirmInferredMembersResponse:
    # ClickHouse is the system of record for teams AND identities (CHAOS-2600
    # CS5). Confirming an inferred Jira member upserts the identity record
    # (merging this team into its team_ids) and unions its facets into the
    # ClickHouse team ``members``. No Postgres ``IdentityMapping`` is written.
    if payload.team_id != team_id:
        raise HTTPException(status_code=400, detail="team_id in path/body must match")

    team_svc = ClickHouseTeamAdminService(store, org_id)
    team = await team_svc.get(team_id)
    if team is None:
        raise HTTPException(status_code=404, detail="Team not found")

    id_store = ClickHouseIdentityStore(store, org_id)
    linked = 0
    created = 0
    skipped = 0

    # PASS 1 — validate the WHOLE batch with NO mutation (ClickHouse has no
    # multi-statement transactions; a 409/404 must fire before any write).
    # Each actionable entry is (member, canonical_id). ``seen`` rejects an
    # intra-batch conflict: the same account_id resolving to two different
    # canonicals within this request (e.g. once minted to jira:{account_id} and
    # once with an explicit different canonical, or two explicit canonicals).
    seen: dict[tuple[str, str], str] = {}
    actionable: list[tuple[Any, str]] = []
    for member in payload.members:
        account_id = str(getattr(member, "account_id", "") or "")
        if member.action != "add" or not account_id:
            skipped += 1
            continue
        # An explicit canonical_id LINKS to an existing identity — it must
        # exist; only the no-canonical path may create (jira:{account_id}).
        if member.canonical_id:
            canonical_id = member.canonical_id
            if await id_store.get(canonical_id) is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Identity '{canonical_id}' not found",
                )
        else:
            canonical_id = f"jira:{account_id}"
        # The jira account_id must not already belong to a different canonical
        # in the store, nor to a different canonical earlier in this batch.
        await _assert_provider_identities_unowned(
            id_store,
            canonical_id=canonical_id,
            provider_identities={"jira": [account_id]},
        )
        _assert_no_intra_batch_conflict(
            seen,
            canonical_id=canonical_id,
            provider_identities={"jira": [account_id]},
        )
        actionable.append((member, canonical_id))

    # PASS 2 — apply.
    facets: set[str] = set()
    for member, canonical_id in actionable:
        account_id = str(getattr(member, "account_id", "") or "")
        existing = await id_store.get(canonical_id)
        team_ids = sorted({*(existing.team_ids if existing else []), team_id})
        providers = dict(existing.provider_identities) if existing else {}
        providers["jira"] = sorted({*providers.get("jira", []), account_id})
        # Fill-only: an inferred email/display_name must not overwrite a value
        # already on the stored record (which could silently clobber a manually
        # curated email and leave stale facets on the identity's other teams).
        display_name = member.display_name
        email = member.email
        if existing is not None:
            if existing.display_name:
                display_name = existing.display_name
            if existing.email:
                email = existing.email
        # Upsert FIRST, then add the COMPLETE stored facet set to the team.
        stored = await id_store.create_or_update(
            canonical_id=canonical_id,
            display_name=display_name,
            email=email,
            provider_identities=providers,
            team_ids=team_ids,
        )
        facets |= _stored_facets(stored)
        if member.canonical_id:
            linked += 1
        else:
            created += 1

    if facets:
        await team_svc.add_members(team_id, sorted(facets))
    return ConfirmInferredMembersResponse(
        linked=linked, created=created, skipped=skipped
    )
