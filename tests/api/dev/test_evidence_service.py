from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from dev_health_ops.api.dev.contracts import FreshnessState
from dev_health_ops.api.dev.evidence_service import (
    MAX_EXPANSION_BYTES,
    EvidenceAvailability,
    EvidenceCandidate,
    EvidenceRecord,
    EvidenceReferenceSigner,
    EvidenceService,
    SourceSearchResult,
    sanitize_untrusted_text,
)
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ResolvedTimeRange,
    ScopeRef,
    ScopeResolution,
    ScopeResolutionOutcome,
    ScopeResolveRequest,
    TimeRangeRequest,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)
SECRET = "evidence-test-secret-that-is-at-least-thirty-two-bytes"


class Entitlement:
    async def require(self, _org_id: str) -> None:
        return None


def _time_range() -> ResolvedTimeRange:
    return ResolvedTimeRange(
        timezone="UTC",
        utc_start=NOW - timedelta(days=30),
        utc_end=NOW,
        local_start=(NOW - timedelta(days=30)).isoformat(),
        local_end=NOW.isoformat(),
        comparison_utc_start=NOW - timedelta(days=60),
        comparison_utc_end=NOW - timedelta(days=30),
        comparison_local_start=(NOW - timedelta(days=60)).isoformat(),
        comparison_local_end=(NOW - timedelta(days=30)).isoformat(),
    )


def _resolution(*, entity: AuthorizedEntity | None = None) -> ScopeResolution:
    return ScopeResolution(
        outcome=ScopeResolutionOutcome.EXACT,
        entities=(
            entity or AuthorizedEntity(EntityKind.REPOSITORY, "repo-a", "Repo A"),
        ),
        team_filters=(),
        candidates=(),
        time_range=_time_range(),
    )


class Authorizer:
    def __init__(self) -> None:
        self.denied = False
        self.calls: list[tuple[str, str, ScopeResolveRequest]] = []

    async def resolve(
        self, org_id: str, permission_fingerprint: str, request: ScopeResolveRequest
    ) -> ScopeResolution:
        self.calls.append((org_id, permission_fingerprint, request))
        if self.denied or org_id != "org-a" or permission_fingerprint == "revoked":
            return ScopeResolution(
                outcome=ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND,
                entities=(),
                team_filters=(),
                candidates=(),
                time_range=_time_range(),
            )
        ref = request.explicit_refs[0]
        if ref.kind is EntityKind.ORGANIZATION:
            return _resolution(
                entity=AuthorizedEntity(EntityKind.ORGANIZATION, "org-a", "Org A")
            )
        if ref.kind is EntityKind.ISSUE:
            return _resolution(
                entity=AuthorizedEntity(EntityKind.ISSUE, ref.value, "Issue", "repo-a")
            )
        if ref.kind is EntityKind.REPOSITORY and all(
            item.value == "repo-a" for item in request.explicit_refs
        ):
            return _resolution()
        return ScopeResolution(
            outcome=ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND,
            entities=(),
            team_filters=(),
            candidates=(),
            time_range=_time_range(),
        )


class Adapter:
    source_system = "work_items"

    def __init__(
        self, records: list[EvidenceRecord], *, fail_search: bool = False
    ) -> None:
        self.records = records
        self.fail_search = fail_search
        self.expand_calls = 0

    async def search(self, **_kwargs: object) -> SourceSearchResult:
        if self.fail_search:
            raise RuntimeError("source detail must not escape")
        return SourceSearchResult(
            self.source_system,
            EvidenceAvailability.AVAILABLE
            if self.records
            else EvidenceAvailability.NO_MATCHES,
            tuple(self.records),
        )

    async def expand(
        self, *, evidence: object, **_kwargs: object
    ) -> EvidenceRecord | None:
        self.expand_calls += 1
        entity_id = getattr(evidence, "entity_id")
        return next(
            (item for item in self.records if item.entity_id == entity_id), None
        )


class FailingACR:
    source_system = "acr"

    async def search(self, **_kwargs: object) -> SourceSearchResult:
        raise TimeoutError("acr unavailable")

    async def expand(self, **_kwargs: object) -> EvidenceRecord | None:
        raise TimeoutError("acr unavailable")


class ACRAdapter:
    source_system = "acr"

    def __init__(self) -> None:
        self.record = EvidenceRecord(
            source_system="acr",
            source_version="acr-evidence.v1",
            entity_type="review_finding",
            entity_id="acr-finding-1",
            display_label="Authorized review finding",
            observed_at=NOW,
            freshness=FreshnessState.FRESH,
            provenance="native",
            confidence=0.9,
            repository_ids=("repo-a",),
            raw_excerpt="Bounded ACR finding",
        )

    async def search(self, **_kwargs: object) -> SourceSearchResult:
        return SourceSearchResult("acr", EvidenceAvailability.AVAILABLE, (self.record,))

    async def expand(self, **_kwargs: object) -> EvidenceRecord | None:
        return self.record


def _record(index: int = 1, *, excerpt: str | None = None) -> EvidenceRecord:
    return EvidenceRecord(
        source_system="work_items",
        source_version="native.v1",
        entity_type="issue",
        entity_id=f"issue-{index}",
        display_label=f"Issue {index}",
        observed_at=NOW - timedelta(minutes=index),
        freshness=FreshnessState.FRESH,
        provenance="native",
        confidence=1.0,
        repository_ids=("repo-a",),
        raw_excerpt=excerpt or f"Ask Dev work {index}",
    )


def _request(
    kind: EntityKind = EntityKind.REPOSITORY, value: str = "repo-a"
) -> ScopeResolveRequest:
    return ScopeResolveRequest(
        explicit_refs=(ScopeRef(kind, value),),
        time_range=TimeRangeRequest(preset_days=30),
    )


@pytest.mark.asyncio
async def test_durable_ref_reopens_with_new_service_and_reauthorizes_repository() -> (
    None
):
    authorizer = Authorizer()
    adapter = Adapter([_record()])
    first = EvidenceService(
        entitlement=Entitlement(),
        authorizer=authorizer,
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[adapter],
    )
    searched = await first.search(
        org_id="org-a",
        permission_fingerprint="permission-v1",
        scope_request=_request(),
        query="Ask Dev",
    )
    persisted_ref = searched.evidence[0].model_copy(deep=True)

    # Simulate a retained conversation reopening in another process/request:
    # no locator registry is shared with this service instance.
    reopened = EvidenceService(
        entitlement=Entitlement(),
        authorizer=authorizer,
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[adapter],
    )
    expanded = await reopened.expand(
        org_id="org-a",
        permission_fingerprint="permission-v2",
        scope_request=_request(),
        evidence=[persisted_ref],
    )

    assert expanded.expansions[0].state is EvidenceAvailability.AVAILABLE
    assert expanded.expansions[0].safe_excerpt == (
        "UNTRUSTED_DATA\nAsk Dev work 1\nEND_UNTRUSTED_DATA"
    )
    assert adapter.expand_calls == 1
    # Original scope plus repository-specific current authorization.
    assert len(authorizer.calls) == 3
    assert authorizer.calls[-1][2].explicit_refs == (
        ScopeRef(EntityKind.REPOSITORY, "repo-a"),
    )


@pytest.mark.asyncio
async def test_permission_revocation_denies_reopened_ref_without_source_lookup() -> (
    None
):
    authorizer = Authorizer()
    adapter = Adapter([_record()])
    service = EvidenceService(
        entitlement=Entitlement(),
        authorizer=authorizer,
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[adapter],
    )
    searched = await service.search(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        query="Ask Dev",
    )
    expanded = await service.expand(
        org_id="org-a",
        permission_fingerprint="revoked",
        scope_request=_request(),
        evidence=searched.evidence,
    )

    assert expanded.expansions[0].state is EvidenceAvailability.UNAUTHORIZED
    assert expanded.expansions[0].warning == "not_found"
    assert adapter.expand_calls == 0


@pytest.mark.asyncio
async def test_cross_tenant_and_tampered_refs_are_existence_neutral() -> None:
    authorizer = Authorizer()
    adapter = Adapter([_record()])
    service = EvidenceService(
        entitlement=Entitlement(),
        authorizer=authorizer,
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[adapter],
    )
    searched = await service.search(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        query="Ask Dev",
    )
    tampered = searched.evidence[0].model_copy(
        update={"evidence_ref_id": "ev1_0000000000000000000000000000000000000000"}
    )
    cross_tenant = await service.expand(
        org_id="org-b",
        permission_fingerprint="allowed",
        scope_request=_request(),
        evidence=[searched.evidence[0]],
    )
    tampered_result = await service.expand(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        evidence=[tampered],
    )

    assert cross_tenant.expansions[0].warning == "not_found"
    assert tampered_result.expansions[0].warning == "not_found"
    assert {cross_tenant.expansions[0].state, tampered_result.expansions[0].state} == {
        EvidenceAvailability.UNAUTHORIZED
    }
    assert adapter.expand_calls == 0


@pytest.mark.asyncio
async def test_native_search_survives_optional_acr_failure_and_ranking_is_deterministic() -> (
    None
):
    authorizer = Authorizer()
    records = [_record(2), _record(1)]
    service = EvidenceService(
        entitlement=Entitlement(),
        authorizer=authorizer,
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[Adapter(records)],
        acr_adapter=FailingACR(),
    )
    first = await service.search(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        query="Ask Dev",
    )
    second = await service.search(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        query="Ask Dev",
    )

    assert [item.evidence_ref_id for item in first.evidence] == [
        item.evidence_ref_id for item in second.evidence
    ]
    assert len(first.evidence) == 2
    assert first.query_version == "search-evidence.v1"
    assert first.ranking_version == "evidence-ranking.v1"
    assert (
        next(item for item in first.source_states if item.source_system == "acr").state
        is EvidenceAvailability.UNAVAILABLE
    )


@pytest.mark.asyncio
async def test_acr_assists_without_replacing_native_evidence() -> None:
    service = EvidenceService(
        entitlement=Entitlement(),
        authorizer=Authorizer(),
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[Adapter([_record()])],
        acr_adapter=ACRAdapter(),
    )
    searched = await service.search(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        query="finding",
    )
    assert {item.source_system for item in searched.evidence} == {
        "work_items",
        "acr",
    }
    acr_ref = next(item for item in searched.evidence if item.source_system == "acr")
    expanded = await service.expand(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        evidence=[acr_ref],
    )
    assert expanded.expansions[0].state is EvidenceAvailability.AVAILABLE
    assert "Bounded ACR finding" in (expanded.expansions[0].safe_excerpt or "")


@pytest.mark.asyncio
async def test_search_and_expansion_hard_bounds() -> None:
    authorizer = Authorizer()
    adapter = Adapter([_record(index, excerpt="x" * 10_000) for index in range(1, 101)])
    service = EvidenceService(
        entitlement=Entitlement(),
        authorizer=authorizer,
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[adapter],
    )
    searched = await service.search(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        query="Issue",
    )
    assert len(searched.evidence) == 25
    with pytest.raises(ValueError, match="At most 10"):
        await service.expand(
            org_id="org-a",
            permission_fingerprint="allowed",
            scope_request=_request(),
            evidence=searched.evidence[:11],
        )
    expanded = await service.expand(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        evidence=searched.evidence[:10],
    )
    assert expanded.serialized_bytes <= MAX_EXPANSION_BYTES
    assert all(
        item.safe_excerpt is None or item.safe_excerpt.startswith("UNTRUSTED_DATA\n")
        for item in expanded.expansions
    )


def test_sanitization_keeps_injection_inert_and_removes_unsafe_markup_links_secrets() -> (
    None
):
    value = (
        '<script>request_tool("shell")</script> '
        "[click me](javascript:alert(1)) <https://evil.example/a> "
        "Authorization: Bearer secret-secret password=hunter2 sk-live-secret"
    )
    sanitized = sanitize_untrusted_text(value)

    assert sanitized is not None
    assert "<script>" not in sanitized
    assert "javascript:" not in sanitized
    assert "https://evil.example" not in sanitized
    assert "hunter2" not in sanitized
    assert "secret-secret" not in sanitized
    # Source text remains data. It is not executed or interpreted as a tool request.
    assert 'request_tool("shell")' in sanitized


@pytest.mark.asyncio
async def test_unsafe_structured_source_url_is_not_rendered() -> None:
    record = replace(_record(), source_url="javascript:alert(1)")
    service = EvidenceService(
        entitlement=Entitlement(),
        authorizer=Authorizer(),
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[Adapter([record])],
    )
    result = await service.search(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        query="Ask Dev",
    )
    assert result.evidence[0].link is None


@pytest.mark.asyncio
async def test_authorized_https_link_and_redacted_expansion_are_preserved() -> None:
    record = replace(
        _record(),
        source_url="https://jira.example/browse/ISSUE-1",
        authorized_link_hosts=("jira.example",),
        redacted=True,
    )
    service = EvidenceService(
        entitlement=Entitlement(),
        authorizer=Authorizer(),
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[Adapter([record])],
    )
    searched = await service.search(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        query="Ask Dev",
    )
    assert searched.evidence[0].link is not None
    assert searched.evidence[0].link.source_url == "https://jira.example/browse/ISSUE-1"
    expanded = await service.expand(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        evidence=searched.evidence,
    )
    assert expanded.expansions[0].state is EvidenceAvailability.REDACTED


@pytest.mark.asyncio
async def test_organization_wide_search_admits_evidence_from_multiple_repositories() -> (
    None
):
    """CHAOS-3300 attack.unrelated-evidence.availability: the other half of
    the unrelated-evidence attack. The negative control (elsewhere in this
    file's coverage of expand()) proves a *named* repository subject
    excludes an unrelated repository. This proves the converse: an
    organization-wide resolved scope -- no committed repository subject --
    must NOT silently restrict search() to a single repository. ``search()``
    merges every adapter's records with no repository filter of its own
    (see evidence_service.py's ``records = [record for result in
    source_results ...]``); the non-obvious part is ``valid_entity_ids``,
    computed via ``_authorized_entity_ids``, which excludes ORGANIZATION and
    REPOSITORY kinds by design -- so an org-wide resolution's only entity
    (kind ORGANIZATION) yields an *empty* ``valid_entity_ids``, and
    ``_authorize_expansion`` treats an empty ``valid_entity_ids`` as
    unrestricted (``if evidence.valid_entity_ids and not ... <= allowed``),
    not as "nothing authorized". This is exactly the property this test
    pins: an org-wide resolution admits an entity it never explicitly named.
    """
    authorizer = Authorizer()
    repo_a_record = _record(1)
    repo_b_record = replace(
        _record(2),
        entity_id="issue-repo-b-1",
        repository_ids=("repo-b",),
    )
    adapter = Adapter([repo_a_record, repo_b_record])
    service = EvidenceService(
        entitlement=Entitlement(),
        authorizer=authorizer,
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[adapter],
    )

    searched = await service.search(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(kind=EntityKind.ORGANIZATION, value="org-a"),
        query="Ask Dev",
    )

    observed_repository_ids = {
        repository_id
        for item in searched.evidence
        for repository_id in item.repository_ids
    }
    assert observed_repository_ids == {"repo-a", "repo-b"}
    # The non-obvious authorization property itself: an org-wide resolution
    # commits no explicit non-organization/repository entity, so every
    # returned ref's valid_entity_ids is empty -- which _authorize_expansion
    # treats as unrestricted, not as "nothing is authorized".
    assert all(item.valid_entity_ids == [] for item in searched.evidence)


@pytest.mark.asyncio
async def test_no_matches_is_distinct_from_unconfigured_optional_source() -> None:
    result = await EvidenceService(
        entitlement=Entitlement(),
        authorizer=Authorizer(),
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[Adapter([])],
    ).search(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        query="no such evidence",
    )
    states = {item.source_system: item.state for item in result.source_states}
    assert states == {
        "work_items": EvidenceAvailability.NO_MATCHES,
        "acr": EvidenceAvailability.UNCONFIGURED,
    }


# ---------------------------------------------------------------------------
# CHAOS-3633: two same-kind records about one entity must not collide onto
# one handle.
# ---------------------------------------------------------------------------


class _TwoRecordResolver:
    """Resolves two DISTINCT records of the same kind about one entity.

    Keyed by ``candidate.locator`` -- the source's own record identity
    (CHAOS-3646, ``EvidenceCandidate.locator`` docstring) -- never by
    ``entity_id``, which both records share on purpose: that sharing is
    exactly the CHAOS-3633 scenario (two reviews on one PR, two incidents
    about one project, ...).
    """

    source_system = "reviews"

    def __init__(self, records_by_locator: dict[str, EvidenceRecord]) -> None:
        self._records = records_by_locator

    async def resolve(
        self, *, org_id: str, scope: object, candidate: EvidenceCandidate
    ) -> EvidenceRecord | None:
        return self._records.get(candidate.locator)


def _same_entity_record(locator: str, *, label: str) -> EvidenceRecord:
    return EvidenceRecord(
        source_system="reviews",
        source_version="native.v1",
        entity_type="pull_request",
        entity_id="issue-1",
        display_label=label,
        observed_at=NOW,
        freshness=FreshnessState.FRESH,
        provenance="native",
        confidence=1.0,
        repository_ids=(),
        raw_excerpt=f"{label} content, locator {locator}",
    )


@pytest.mark.asyncio
async def test_two_same_kind_records_about_one_entity_mint_distinct_handles() -> None:
    """CHAOS-3633 RED-first: the defect this test demonstrates is a
    denial-of-service on legitimate, distinct evidence. Before the fix,
    ``EvidenceReferenceSigner._payload`` binds only
    ``(org, source_system, source_version, entity_type, entity_id,
    repositories)`` -- none of which differ between these two records, since
    they are two DIFFERENT records about the SAME entity. Both mint the
    identical ``evidence_ref_id``, and a caller enforcing "no repeated index
    handle" (the frozen graph packet contract) refuses the second, genuine
    record as though it were a duplicate of the first.
    """

    records = {
        "review-1": _same_entity_record("review-1", label="First review"),
        "review-2": _same_entity_record("review-2", label="Second review"),
    }
    service = EvidenceService(
        entitlement=Entitlement(),
        authorizer=Authorizer(),
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[],
        candidate_resolvers=[_TwoRecordResolver(records)],
    )
    candidates = [
        EvidenceCandidate(
            source_system="reviews",
            entity_type="pull_request",
            entity_id="issue-1",
            locator=locator,
        )
        for locator in ("review-1", "review-2")
    ]
    result = await service.admit(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(EntityKind.ISSUE, "issue-1"),
        candidates=candidates,
    )
    first, second = result.admissions
    assert first.state is EvidenceAvailability.AVAILABLE
    assert second.state is EvidenceAvailability.AVAILABLE
    assert first.evidence is not None
    assert second.evidence is not None
    assert first.evidence.display_label == "First review"
    assert second.evidence.display_label == "Second review"
    # The defect: without a distinct record identity in the signed payload,
    # these two DIFFERENT records mint the SAME handle.
    assert first.evidence.evidence_ref_id != second.evidence.evidence_ref_id


def test_adjacent_field_concatenation_cannot_collide_payloads() -> None:
    """Contract-owner condition 1: the signed payload is a JSON object with
    a distinct named key per field, never a concatenation of
    ``entity_type``/``record_locator`` -- so ``(entity_type="a",
    record_locator="bc")`` and ``(entity_type="ab", record_locator="c")``
    -- which a naive ``entity_type + record_locator`` string join WOULD
    equate, both reducing to ``"abc"`` -- must sign to different bytes."""

    left = EvidenceRecord(
        source_system="reviews",
        source_version="native.v1",
        entity_type="a",
        entity_id="issue-1",
        display_label="Left",
        observed_at=NOW,
        freshness=FreshnessState.FRESH,
        provenance="native",
        confidence=1.0,
        record_locator="bc",
    )
    right = replace(left, entity_type="ab", record_locator="c")
    signer = EvidenceReferenceSigner(SECRET)
    assert signer.issue("org-a", left) != signer.issue("org-a", right)


class _RecordLocatorStrippingSigner(EvidenceReferenceSigner):
    """A DELIBERATELY weakened encoder: identical to the real signer except
    it drops ``record_locator`` from the signed payload -- i.e. exactly
    today's pre-CHAOS-3633 behaviour. Exists only so the strip/forge tests
    below can be shown failing against it, proving those tests actually
    discriminate rather than passing by construction."""

    @staticmethod
    def _payload(org_id: str, evidence) -> bytes:
        repository_ids = sorted(evidence.repository_ids)
        payload = {
            "org": org_id,
            "source": evidence.source_system,
            "source_version": evidence.source_version,
            "entity_type": evidence.entity_type,
            "entity_id": evidence.entity_id,
            "repositories": repository_ids,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _locator_bearing_record(locator: str) -> EvidenceRecord:
    return EvidenceRecord(
        source_system="reviews",
        source_version="native.v1",
        entity_type="pull_request",
        entity_id="issue-1",
        display_label="A review",
        observed_at=NOW,
        freshness=FreshnessState.FRESH,
        provenance="native",
        confidence=1.0,
        repository_ids=(),
        record_locator=locator,
    )


@pytest.mark.parametrize(
    "signer_cls,discriminates",
    [
        pytest.param(EvidenceReferenceSigner, True, id="real_signer"),
        pytest.param(_RecordLocatorStrippingSigner, False, id="weakened_signer"),
    ],
)
def test_stripping_the_signed_locator_is_caught_only_by_the_real_signer(
    signer_cls: type[EvidenceReferenceSigner], discriminates: bool
) -> None:
    """Contract-owner condition 2, strip direction: a ref signed WITH a
    locator, then presented for verification WITHOUT one, must fail.
    Parametrized over the real signer and the deliberately weakened one
    above so this property is observed FAILING against the weak encoder
    (``discriminates=False``) before it is trusted to pass against the real
    one -- a strip test that could never fail proves nothing."""

    signer = signer_cls(SECRET)
    signed = _locator_bearing_record("review-1")
    handle = signer.issue("org-a", signed)
    stripped = replace(signed, record_locator=None)
    verified = signer.verify(
        "org-a",
        SimpleNamespace(
            evidence_ref_id=handle,
            source_system=stripped.source_system,
            source_version=stripped.source_version,
            entity_type=stripped.entity_type,
            entity_id=stripped.entity_id,
            repository_ids=stripped.repository_ids,
            record_locator=stripped.record_locator,
        ),
    )
    # A stripped locator must NEVER verify against the real signer
    # (``discriminates=True``), and -- observed here, not assumed -- DOES
    # verify against the deliberately weakened one, which is exactly what
    # makes the weakened encoder weak and this parametrization meaningful.
    assert verified is not discriminates


@pytest.mark.parametrize(
    "signer_cls,discriminates",
    [
        pytest.param(EvidenceReferenceSigner, True, id="real_signer"),
        pytest.param(_RecordLocatorStrippingSigner, False, id="weakened_signer"),
    ],
)
def test_forging_a_different_locator_is_caught_only_by_the_real_signer(
    signer_cls: type[EvidenceReferenceSigner], discriminates: bool
) -> None:
    """Contract-owner condition 2, forge direction: a ref signed for one
    locator, then presented with a DIFFERENT locator substituted in, must
    fail against the real signer -- and, observed here rather than assumed,
    does NOT fail against the deliberately weakened one, proving this test
    discriminates instead of passing by construction."""

    signer = signer_cls(SECRET)
    signed = _locator_bearing_record("review-1")
    handle = signer.issue("org-a", signed)
    forged = replace(signed, record_locator="review-2")
    verified = signer.verify(
        "org-a",
        SimpleNamespace(
            evidence_ref_id=handle,
            source_system=forged.source_system,
            source_version=forged.source_version,
            entity_type=forged.entity_type,
            entity_id=forged.entity_id,
            repository_ids=forged.repository_ids,
            record_locator=forged.record_locator,
        ),
    )
    assert verified is not discriminates


@pytest.mark.asyncio
async def test_non_vacuity_admission_forces_distinct_locators_even_if_the_resolver_forgets() -> (
    None
):
    """Contract-owner condition 3: same-kind multi-record admission MUST set
    the locator -- unset-by-default must not be able to silently
    reintroduce CHAOS-3633. The resolver below is deliberately "lazy": it
    never sets ``record_locator`` on the ``EvidenceRecord`` it returns (the
    field defaults to ``None``, exactly as every pre-fix resolver would).
    ``EvidenceService.admit`` must still force distinctness by binding the
    submitted candidate's own locator -- see ``admit``'s docstring -- so
    this passes not because the resolver did the right thing, but because
    the service does not let it get this wrong.
    """

    class _LazyResolver:
        source_system = "reviews"

        async def resolve(self, *, org_id, scope, candidate):
            # Deliberately ignores ``candidate.locator`` when building the
            # record -- ``record_locator`` stays at its ``None`` default.
            return EvidenceRecord(
                source_system="reviews",
                source_version="native.v1",
                entity_type="pull_request",
                entity_id="issue-1",
                display_label=f"Review at {candidate.locator}",
                observed_at=NOW,
                freshness=FreshnessState.FRESH,
                provenance="native",
                confidence=1.0,
                repository_ids=(),
            )

    service = EvidenceService(
        entitlement=Entitlement(),
        authorizer=Authorizer(),
        signer=EvidenceReferenceSigner(SECRET),
        native_adapters=[],
        candidate_resolvers=[_LazyResolver()],
    )
    candidates = [
        EvidenceCandidate(
            source_system="reviews",
            entity_type="pull_request",
            entity_id="issue-1",
            locator=locator,
        )
        for locator in ("review-1", "review-2")
    ]
    result = await service.admit(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(EntityKind.ISSUE, "issue-1"),
        candidates=candidates,
    )
    first, second = result.admissions
    assert first.evidence is not None and second.evidence is not None
    assert first.evidence.evidence_ref_id != second.evidence.evidence_ref_id
    assert first.evidence.record_locator == "review-1"
    assert second.evidence.record_locator == "review-2"


@pytest.mark.asyncio
async def test_a_genuine_handle_collision_within_one_round_is_refused_not_silently_merged() -> (
    None
):
    """Defense-in-depth backstop for condition 3: even if two DIFFERENT
    locators somehow minted the identical handle -- a payload-encoding
    regression, not something reachable through the real signer today --
    the second admission must be refused rather than silently returned as
    though it were a distinct, valid record. Exercised with the
    deliberately weakened (locator-blind) signer, which is exactly the kind
    of regression this guard exists to catch.
    """

    class _TwoLocatorResolver:
        source_system = "reviews"

        async def resolve(self, *, org_id, scope, candidate):
            return _locator_bearing_record(candidate.locator)

    service = EvidenceService(
        entitlement=Entitlement(),
        authorizer=Authorizer(),
        signer=_RecordLocatorStrippingSigner(SECRET),
        native_adapters=[],
        candidate_resolvers=[_TwoLocatorResolver()],
    )
    candidates = [
        EvidenceCandidate(
            source_system="reviews",
            entity_type="pull_request",
            entity_id="issue-1",
            locator=locator,
        )
        for locator in ("review-1", "review-2")
    ]
    result = await service.admit(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(EntityKind.ISSUE, "issue-1"),
        candidates=candidates,
    )
    first, second = result.admissions
    assert first.state is EvidenceAvailability.AVAILABLE
    assert first.evidence is not None
    assert second.state is EvidenceAvailability.UNAVAILABLE
    assert second.evidence is None
    assert second.warning == "ambiguous_record_identity"
