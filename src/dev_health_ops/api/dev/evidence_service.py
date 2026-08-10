"""Authorized, bounded evidence search and expansion for Ask Dev.

Evidence references are durable descriptors, not cached payloads.  Their opaque
IDs are deterministic HMACs over the tenant and canonical source locator.  A
retained ``dev_answer.v1`` therefore contains everything needed to locate the
record again without duplicating source content.  Expansion still re-resolves
the current scope and permissions for every reference; a valid signature is
never an authorization decision.
"""

from __future__ import annotations

import hashlib
import hmac
import html
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from html.parser import HTMLParser
from typing import Protocol
from urllib.parse import urlsplit

from .contracts import (
    DevCitationLink,
    DevEvidenceFlags,
    DevEvidenceRef,
    FreshnessState,
)
from .entitlement import AskDevEntitlementAuthorizer
from .scope_service import (
    EntityKind,
    ScopeRef,
    ScopeResolution,
    ScopeResolutionOutcome,
    ScopeResolveRequest,
)

SEARCH_EVIDENCE_VERSION = "search-evidence.v1"
EVIDENCE_RANKING_VERSION = "evidence-ranking.v1"
GET_EVIDENCE_VERSION = "get-evidence.v1"
#: CHAOS-3646. Versioned like the other two entry points, because the
#: admission result is a shape a trial artifact reads.
ADMIT_EVIDENCE_VERSION = "admit-evidence.v1"
MAX_SEARCH_REFS = 25
#: The same bound as a search page. An admission round is a discovery layer
#: asking "resolve these for me", and a caller that could ask for more than a
#: search can return would be using admission to enumerate.
MAX_ADMISSION_CANDIDATES = 25
MAX_EXPANSION_REFS = 10
MAX_EXPANSION_BYTES = 64 * 1024
MAX_SOURCE_CANDIDATES = 100
MAX_RAW_EXCERPT_CHARS = 32 * 1024
UNTRUSTED_DATA_START = "UNTRUSTED_DATA\n"
UNTRUSTED_DATA_END = "\nEND_UNTRUSTED_DATA"


class EvidenceAvailability(StrEnum):
    AVAILABLE = "available"
    NO_MATCHES = "no_matches"
    UNAVAILABLE = "unavailable"
    UNCONFIGURED = "unconfigured"
    UNAUTHORIZED = "unauthorized"
    REDACTED = "redacted"
    STALE = "stale"


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    source_system: str
    source_version: str
    entity_type: str
    entity_id: str
    display_label: str
    observed_at: datetime
    freshness: FreshnessState
    provenance: str
    confidence: float
    repository_ids: tuple[str, ...] = ()
    raw_excerpt: str | None = None
    internal_path: str | None = None
    source_url: str | None = None
    authorized_link_hosts: tuple[str, ...] = ()
    stale: bool = False
    unavailable: bool = False
    redacted: bool = False
    deleted: bool = False
    uncertain: bool = False
    conflicting: bool = False
    #: CHAOS-3633. The source's own identity for this record, distinct from
    #: ``entity_id`` -- see ``DevEvidenceRef.record_locator``. ``search``/
    #: ``expand`` never set this. ``EvidenceService.admit`` overwrites it
    #: unconditionally with the submitted candidate's own ``locator``
    #: (never trusting a resolver to remember to set it): see ``admit``'s
    #: docstring for why that is always correct for the admission path.
    record_locator: str | None = None
    #: CHAOS-3675 PR2. An EXPLICIT, typed resolver capability, independent
    #: of whatever ``entity_id`` holds. ``True`` only when a resolver has
    #: independently confirmed, from the canonical row itself, that this
    #: record has no directly-authorizable :class:`~.scope_service.EntityKind`
    #: entity at all (a deployment with no linked PR, an incident with no
    #: linked repository). ``entity_id`` stays whatever the record's own
    #: descriptive identity is in that case (``DevEvidenceRef.entity_id``
    #: requires a non-empty string; it is never repurposed as an
    #: authorization signal) -- this flag, not the VALUE of ``entity_id``,
    #: is what tells :meth:`EvidenceService.admit` to skip entity
    #: authorization. A resolver that CAN derive an authorizable entity
    #: must always derive it and leave this ``False``: gating on
    #: ``entity_id``'s value instead would let a resolver bug (or a
    #: resolver that took the cheap path instead of deriving) launder its
    #: way past the entity check merely by returning something falsy.
    #:
    #: **Requires ``repository_ids`` to be non-empty.** ``admit`` refuses
    #: (never silently admits) a record with this flag set AND empty
    #: ``repository_ids``: ``_authorize_expansion``'s entity check and its
    #: repository check are BOTH truthy-gated, so an entity-less,
    #: repository-less record would skip both and be admitted
    #: unconditionally to any caller with a generally-valid resolution --
    #: an authorization bypass, not a convenience. Every resolver that sets
    #: this flag must resolve a repository to attach.
    no_authorizable_entity: bool = False


@dataclass(frozen=True, slots=True)
class SourceSearchResult:
    source_system: str
    state: EvidenceAvailability
    records: tuple[EvidenceRecord, ...] = ()
    watermark: str | None = None
    warning: str | None = None


@dataclass(frozen=True, slots=True)
class EvidenceSearchResult:
    evidence: tuple[DevEvidenceRef, ...]
    source_states: tuple[SourceSearchResult, ...]
    query_version: str = SEARCH_EVIDENCE_VERSION
    ranking_version: str = EVIDENCE_RANKING_VERSION


@dataclass(frozen=True, slots=True)
class EvidenceCandidate:
    """A record a discovery layer POINTS AT, carrying no authority of its own.

    CHAOS-3646. The CHAOS-3619 trial recorded a canonical bypass as an
    architectural fact: the graph arm can discover authentic world evidence
    it cannot cite, because the frame's canonical set does not contain it and
    nothing could put it there. This type is one half of the path across
    that boundary, and **its field set is the guarantee**.

    There is no ``evidence_ref_id`` here, and no ``display_label``,
    ``citation_text``, ``observed_at``, ``confidence``, ``provenance`` or
    ``freshness``. A discovery layer therefore cannot hand across a handle it
    minted or a claim about the record's content, because the dataclass has
    nowhere to put one -- every field a reader would take as canonical comes
    back from the source during resolution, not from the candidate. That is
    checked by a field-set test rather than left to review, because "the
    graph never mints authority" is the whole property and a widened
    dataclass would repeal it silently.

    :attr:`locator` is the SOURCE's own identity for the record, and it is
    deliberately distinct from :attr:`entity_id`, which is the entity the
    record is *about*. CHAOS-3633 is exactly this distinction going wrong:
    two records of one kind about one entity are two records, and a candidate
    that could not tell them apart would admit whichever one the source
    happened to return.
    """

    source_system: str
    entity_type: str
    entity_id: str
    locator: str
    repository_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EvidenceAdmission:
    """One candidate's outcome. ``evidence`` is ``None`` unless admitted."""

    candidate: EvidenceCandidate
    state: EvidenceAvailability
    evidence: DevEvidenceRef | None = None
    warning: str | None = None

    @property
    def admitted(self) -> bool:
        return self.evidence is not None


@dataclass(frozen=True, slots=True)
class EvidenceAdmissionResult:
    """Every candidate's outcome, in the order they were submitted.

    One entry per candidate, always. A refused candidate that simply vanished
    from the result would be indistinguishable from one that was never
    submitted, and a caller cannot disclose a drop it cannot see.
    """

    admissions: tuple[EvidenceAdmission, ...]
    query_version: str = ADMIT_EVIDENCE_VERSION

    @property
    def admitted(self) -> tuple[DevEvidenceRef, ...]:
        return tuple(
            item.evidence for item in self.admissions if item.evidence is not None
        )


@dataclass(frozen=True, slots=True)
class EvidenceExpansion:
    evidence: DevEvidenceRef
    state: EvidenceAvailability
    safe_excerpt: str | None
    serialized_bytes: int
    warning: str | None = None


@dataclass(frozen=True, slots=True)
class EvidenceExpansionResult:
    expansions: tuple[EvidenceExpansion, ...]
    serialized_bytes: int
    query_version: str = GET_EVIDENCE_VERSION


class EvidenceSourceAdapter(Protocol):
    source_system: str

    async def search(
        self,
        *,
        org_id: str,
        scope: ScopeResolution,
        query: str,
        limit: int,
    ) -> SourceSearchResult: ...

    async def expand(
        self,
        *,
        org_id: str,
        scope: ScopeResolution,
        evidence: DevEvidenceRef,
    ) -> EvidenceRecord | None: ...


class EvidenceCandidateResolver(Protocol):
    """A source that can resolve a discovery layer's locator to its record.

    CHAOS-3646, and deliberately a **separate** protocol from
    :class:`EvidenceSourceAdapter` rather than a method added to it. Every
    existing adapter answers "search my records" and "expand this ref I
    already minted"; neither question is "here is an identity someone else
    discovered -- do you have it". Widening the existing protocol would make
    every adapter in the tree claim an ability it does not have, and a
    default implementation returning ``None`` would make an unimplemented
    source indistinguishable from a genuinely absent record.

    ``EvidenceRecord | None`` is the whole contract: the source either has the
    record or it does not, and the discovery layer's belief about it is never
    consulted. A resolver that echoed its input back would be the graph
    minting authority through a canonical-looking door, which is why the
    trial's resolver reads the world rather than the projection.
    """

    # A read-only ``@property``, not a plain annotation, for the reason
    # ``IdentityPayload`` above gives: a plain Protocol annotation demands a
    # SETTABLE attribute, which a frozen dataclass resolver is not. Read-only
    # is also the honest shape -- nothing may reassign a resolver's source
    # system -- and it still admits the mutable-attribute adapters that
    # ``EvidenceSourceAdapter`` describes.
    @property
    def source_system(self) -> str: ...

    async def resolve(
        self,
        *,
        org_id: str,
        scope: ScopeResolution,
        candidate: EvidenceCandidate,
    ) -> EvidenceRecord | None: ...


class EvidenceScopeAuthorizer(Protocol):
    async def resolve(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: ScopeResolveRequest,
    ) -> ScopeResolution: ...


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


_MARKDOWN_LINK = re.compile(r"!?\[([^\]\n]{0,512})\]\([^\)\n]{0,2048}\)")
_MARKDOWN_AUTOLINK = re.compile(r"<(?:(?:https?|mailto):)[^>\n]{1,2048}>", re.I)
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|access[_-]?token|password|secret|authorization)\s*[:=]\s*([^\s,;]{1,512})"
)
_BEARER = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{8,512}")
_TOKEN_PREFIX = re.compile(r"\b(?:gh[opsu]_|sk-)[A-Za-z0-9_-]{8,512}")


def sanitize_untrusted_text(value: str | None, *, max_chars: int = 2_048) -> str | None:
    """Return inert plain text with HTML, links, and common secrets removed."""
    if not value:
        return None
    bounded = value[:MAX_RAW_EXCERPT_CHARS]
    parser = _TextExtractor()
    try:
        parser.feed(bounded)
        parser.close()
        text = " ".join(parser.parts)
    except Exception:
        text = bounded.replace("<", " ").replace(">", " ")
    text = html.unescape(text)
    text = _MARKDOWN_LINK.sub(lambda match: match.group(1), text)
    text = _MARKDOWN_AUTOLINK.sub("[link removed]", text)
    text = _BEARER.sub("Bearer [REDACTED]", text)
    text = _SECRET_ASSIGNMENT.sub(lambda match: f"{match.group(1)}=[REDACTED]", text)
    text = _TOKEN_PREFIX.sub("[REDACTED]", text)
    text = " ".join(text.split())
    if not text:
        return None
    return text[:max_chars]


def render_untrusted_excerpt(value: str | None) -> str | None:
    """Delimit sanitized source text for safe model/tool-result composition."""
    sanitized = sanitize_untrusted_text(value, max_chars=MAX_RAW_EXCERPT_CHARS)
    if sanitized is None:
        return None
    return f"{UNTRUSTED_DATA_START}{sanitized}{UNTRUSTED_DATA_END}"


def _safe_link(record: EvidenceRecord) -> DevCitationLink | None:
    if record.internal_path:
        parsed = urlsplit(record.internal_path)
        if (
            record.internal_path.startswith("/")
            and not record.internal_path.startswith("//")
            and not parsed.scheme
            and not parsed.netloc
            and "\\" not in record.internal_path
        ):
            return DevCitationLink(internal_path=record.internal_path)
    if record.source_url:
        parsed = urlsplit(record.source_url)
        allowed = {host.casefold() for host in record.authorized_link_hosts}
        if (
            parsed.scheme == "https"
            and parsed.hostname
            and parsed.hostname.casefold() in allowed
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
        ):
            return DevCitationLink(source_url=record.source_url)
    return None


def _authorized_outcome(resolution: ScopeResolution) -> bool:
    return resolution.outcome in {
        ScopeResolutionOutcome.EXACT,
        ScopeResolutionOutcome.FILTERED,
        ScopeResolutionOutcome.INHERITED,
        ScopeResolutionOutcome.ORGANIZATION_FALLBACK,
    }


def _authorized_repository_ids(resolution: ScopeResolution) -> set[str]:
    result: set[str] = set()
    for entity in resolution.entities:
        if entity.kind is EntityKind.REPOSITORY:
            result.add(entity.canonical_id)
        if entity.repository_id:
            result.add(entity.repository_id)
    return result


def _authorized_entity_ids(resolution: ScopeResolution) -> set[str]:
    return {
        entity.canonical_id
        for entity in resolution.entities
        if entity.kind not in {EntityKind.ORGANIZATION, EntityKind.REPOSITORY}
    }


class IdentityPayload(Protocol):
    """CHAOS-3296 round 5: exactly the fields
    :meth:`EvidenceReferenceSigner._payload` binds into its HMAC, and
    nothing else -- ``DevEvidenceRef``, ``EvidenceRecord``, and a
    verifier-only candidate a caller reconstructs from a re-derived
    identity plus its own already-authorized ambient scope (never from the
    fact or handle being checked; see ``investigation_plans.executor.
    _CandidateIdentity``) are all structurally this shape. Widening
    ``_payload`` to this protocol -- rather than the ``DevEvidenceRef |
    EvidenceRecord`` union it used before -- is what lets a caller
    recompute the actual signature via THIS code (never a parallel
    reimplementation) without constructing a full contract object it has
    no other use for.
    """

    # Read-only (``@property``, not plain annotations): every real
    # implementer -- ``DevEvidenceRef`` (pydantic, frozen), ``EvidenceRecord``
    # (frozen dataclass), ``_CandidateIdentity`` (frozen dataclass) -- exposes
    # these as immutable attributes; a plain Protocol annotation requires a
    # SETTABLE attribute for structural matching, which none of them are.
    @property
    def source_system(self) -> str: ...
    @property
    def source_version(self) -> str: ...
    @property
    def entity_type(self) -> str: ...
    @property
    def entity_id(self) -> str: ...
    @property
    def repository_ids(self) -> Sequence[str]: ...
    #: CHAOS-3633. ``None`` for every identity that predates this field --
    #: see ``DevEvidenceRef.record_locator`` for the collision this closes
    #: and the backward-compatibility argument for the ``None`` default.
    @property
    def record_locator(self) -> str | None: ...


class SignedIdentity(IdentityPayload, Protocol):
    """:class:`IdentityPayload` plus the claimed handle itself -- what
    :meth:`EvidenceReferenceSigner.verify` needs on top of what
    ``_payload`` needs, to compare the recomputed signature against."""

    @property
    def evidence_ref_id(self) -> str: ...


class EvidenceReferenceSigner:
    """Issue and verify stable non-enumerable evidence handles."""

    def __init__(self, secret: str | bytes) -> None:
        key = secret.encode() if isinstance(secret, str) else secret
        if len(key) < 32:
            raise ValueError(
                "Evidence reference signing secret must be at least 32 bytes"
            )
        self._key = hashlib.sha256(b"ask-dev-evidence-v1\0" + key).digest()

    @staticmethod
    def _payload(org_id: str, evidence: IdentityPayload) -> bytes:
        repository_ids = sorted(evidence.repository_ids)
        payload: dict[str, object] = {
            "org": org_id,
            "source": evidence.source_system,
            "source_version": evidence.source_version,
            "entity_type": evidence.entity_type,
            "entity_id": evidence.entity_id,
            "repositories": repository_ids,
        }
        # CHAOS-3633. Present only when set, so a payload with no locator
        # (every native ``search``/``expand`` ref today, and every
        # already-persisted ``dev_answer.v1`` reference) is byte-for-byte
        # what this function produced before the field existed -- zero
        # migration, every currently-issued handle keeps verifying.
        #
        # The key is a distinct, fixed, named JSON field -- never
        # concatenated with ``entity_type``/``entity_id`` -- so
        # ``(entity_type="a", record_locator="bc")`` and
        # ``(entity_type="ab", record_locator="c")`` cannot collide the way
        # naive string concatenation would
        # (``test_adjacent_field_concatenation_cannot_collide_payloads``
        # locks this).
        if evidence.record_locator is not None:
            payload["record_locator"] = evidence.record_locator
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()

    def issue(self, org_id: str, record: EvidenceRecord) -> str:
        digest = hmac.new(self._key, self._payload(org_id, record), hashlib.sha256)
        return f"ev1_{digest.hexdigest()[:40]}"

    def verify(self, org_id: str, evidence: SignedIdentity) -> bool:
        expected = hmac.new(
            self._key, self._payload(org_id, evidence), hashlib.sha256
        ).hexdigest()[:40]
        return hmac.compare_digest(evidence.evidence_ref_id, f"ev1_{expected}")


class EvidenceService:
    def __init__(
        self,
        *,
        entitlement: AskDevEntitlementAuthorizer,
        authorizer: EvidenceScopeAuthorizer,
        signer: EvidenceReferenceSigner,
        native_adapters: Sequence[EvidenceSourceAdapter],
        acr_adapter: EvidenceSourceAdapter | None = None,
        candidate_resolvers: Sequence[EvidenceCandidateResolver] = (),
    ) -> None:
        adapters = [*native_adapters]
        if len({adapter.source_system for adapter in adapters}) != len(adapters):
            raise ValueError("Evidence adapter source systems must be unique")
        resolvers = [*candidate_resolvers]
        if len({resolver.source_system for resolver in resolvers}) != len(resolvers):
            raise ValueError(
                "Evidence candidate resolver source systems must be unique"
            )
        self._entitlement = entitlement
        self._authorizer = authorizer
        self._signer = signer
        self._native = tuple(adapters)
        self._acr = acr_adapter
        #: CHAOS-3646. Empty by default, and that default is a STRUCTURAL
        #: off-switch rather than a convenience: no existing construction of
        #: this service passes the argument, so :meth:`admit` can only refuse
        #: in every process that ships today -- with the graph arm's flag on
        #: or off. The flag is the second lock, not the only one.
        self._resolvers = {resolver.source_system: resolver for resolver in resolvers}

    async def search(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope_request: ScopeResolveRequest,
        query: str,
        limit: int = MAX_SEARCH_REFS,
    ) -> EvidenceSearchResult:
        await self._entitlement.require(org_id)
        query = " ".join(query.split())
        if not query or len(query) > 2_048:
            raise ValueError("Evidence query must contain 1 to 2048 characters")
        if limit < 1 or limit > MAX_SEARCH_REFS:
            raise ValueError(
                f"Evidence search limit must be between 1 and {MAX_SEARCH_REFS}"
            )
        resolution = await self._authorizer.resolve(
            org_id, permission_fingerprint, scope_request
        )
        if not _authorized_outcome(resolution):
            return EvidenceSearchResult(
                evidence=(),
                source_states=(
                    SourceSearchResult(
                        source_system="authorized_scope",
                        state=EvidenceAvailability.UNAUTHORIZED,
                    ),
                ),
            )

        source_results: list[SourceSearchResult] = []
        for adapter in self._native:
            try:
                result = await adapter.search(
                    org_id=org_id,
                    scope=resolution,
                    query=query,
                    limit=MAX_SOURCE_CANDIDATES,
                )
            except Exception:
                result = SourceSearchResult(
                    source_system=adapter.source_system,
                    state=EvidenceAvailability.UNAVAILABLE,
                    warning="source_unavailable",
                )
            source_results.append(result)

        if self._acr is not None:
            try:
                source_results.append(
                    await self._acr.search(
                        org_id=org_id,
                        scope=resolution,
                        query=query,
                        limit=MAX_SOURCE_CANDIDATES,
                    )
                )
            except Exception:
                source_results.append(
                    SourceSearchResult(
                        source_system=self._acr.source_system,
                        state=EvidenceAvailability.UNAVAILABLE,
                        warning="optional_acr_unavailable",
                    )
                )
        else:
            source_results.append(
                SourceSearchResult(
                    source_system="acr",
                    state=EvidenceAvailability.UNCONFIGURED,
                )
            )

        valid_entity_ids = tuple(sorted(_authorized_entity_ids(resolution)))
        records = [record for result in source_results for record in result.records]
        records.sort(key=lambda record: self._rank_key(query, record))
        refs = tuple(
            self._to_ref(org_id, record, valid_entity_ids=valid_entity_ids)
            for record in records[:limit]
        )
        return EvidenceSearchResult(
            evidence=refs,
            source_states=tuple(source_results),
        )

    async def expand(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope_request: ScopeResolveRequest,
        evidence: Sequence[DevEvidenceRef],
    ) -> EvidenceExpansionResult:
        await self._entitlement.require(org_id)
        if len(evidence) > MAX_EXPANSION_REFS:
            raise ValueError(
                f"At most {MAX_EXPANSION_REFS} evidence refs may be expanded"
            )
        adapters = {adapter.source_system: adapter for adapter in self._native}
        if self._acr is not None:
            adapters[self._acr.source_system] = self._acr

        expansions: list[EvidenceExpansion] = []
        total = 0
        for ref in evidence:
            # Deliberately re-resolve for every ref so one denied locator cannot
            # inherit another ref's successful authorization decision.
            resolution = await self._authorizer.resolve(
                org_id, permission_fingerprint, scope_request
            )
            state, warning = await self._authorize_expansion(
                org_id,
                permission_fingerprint,
                scope_request,
                resolution,
                ref,
            )
            record: EvidenceRecord | None = None
            if state is EvidenceAvailability.AVAILABLE:
                adapter = adapters.get(ref.source_system)
                if adapter is None:
                    state = EvidenceAvailability.UNCONFIGURED
                    warning = "source_unconfigured"
                else:
                    try:
                        record = await adapter.expand(
                            org_id=org_id,
                            scope=resolution,
                            evidence=ref,
                        )
                    except Exception:
                        state = EvidenceAvailability.UNAVAILABLE
                        warning = "source_unavailable"
                    if record is None and state is EvidenceAvailability.AVAILABLE:
                        state = EvidenceAvailability.NO_MATCHES
                        warning = "evidence_deleted_or_unavailable"
            excerpt = render_untrusted_excerpt(record.raw_excerpt) if record else None
            if record and record.redacted:
                state = EvidenceAvailability.REDACTED
            if record and record.freshness is FreshnessState.STALE:
                state = EvidenceAvailability.STALE
            encoded = len((excerpt or "").encode("utf-8"))
            if total + encoded > MAX_EXPANSION_BYTES:
                remaining = MAX_EXPANSION_BYTES - total
                if remaining <= 0:
                    excerpt = None
                    encoded = 0
                    warning = "expansion_byte_limit_reached"
                else:
                    excerpt = (
                        (excerpt or "")
                        .encode("utf-8")[:remaining]
                        .decode("utf-8", errors="ignore")
                    )
                    encoded = len(excerpt.encode("utf-8"))
                    warning = "expansion_byte_limit_reached"
            total += encoded
            expansions.append(
                EvidenceExpansion(
                    evidence=ref,
                    state=state,
                    safe_excerpt=excerpt,
                    serialized_bytes=encoded,
                    warning=warning,
                )
            )
        return EvidenceExpansionResult(tuple(expansions), total)

    async def admit(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope_request: ScopeResolveRequest,
        candidates: Sequence[EvidenceCandidate],
    ) -> EvidenceAdmissionResult:
        """Resolve, authorize and mint handles for discovered candidates.

        CHAOS-3646. The bounded admission path: a discovery layer -- the
        CHAOS-3617 graph arm is the first -- points at records it believes
        exist, and **this service** decides whether they do, whether this
        principal may see them, and what their canonical identity is. The
        discovery layer never mints, and an unresolvable or unauthorized
        candidate is refused in the vocabulary refusals already use.

        Order is load-bearing: resolve, then **mint, then authorize**.
        Authorizing before minting would need a second authorization surface
        shaped like :meth:`_authorize_expansion` but taking a candidate, and
        two authorization surfaces over one decision is the defect class this
        whole seam exists to prevent. Minting first means authorization runs
        over exactly the object the caller would receive, through exactly the
        code path expansion uses -- called, not copied.

        Nothing here relaxes a check, and one existing check is made to do
        real work rather than duplicated. :meth:`_authorize_expansion` runs
        the signature comparison, the ``valid_entity_ids`` containment check,
        and the separate repository re-resolution with organization fallback
        disabled, all unmodified.

        The subtlety is what ``valid_entity_ids`` is set to. ``search`` mints
        with the whole authorized set, which makes the containment check
        compare a set to itself -- harmless there, because an adapter is
        handed the scope and returns records within it. Admission mints with
        **the record's own entity**, so the same unmodified check becomes the
        real question: is the entity this record is about one this principal
        may see? A resolver that returned a record outside the grant is
        refused by the existing code path rather than by a second one written
        here, and the emitted ref also stops claiming validity for entities
        that have nothing to do with it.

        CHAOS-3633. ``record_locator`` on the minted ref is always the
        submitted candidate's own ``locator`` -- overwritten unconditionally
        after resolution, never left to whatever (if anything) the resolver
        put on the ``EvidenceRecord`` it returned. That is safe, not merely
        convenient: :class:`EvidenceCandidateResolver` -- see its docstring
        -- answers exactly one question, "does the source have the record at
        THIS locator", so a non-``None`` result is definitionally the record
        for ``candidate.locator``. A resolver author cannot forget to make
        two same-kind records about one entity mint distinct handles,
        because this line does it regardless of what the resolver returns.
        ``_minted_this_round`` below is the defense-in-depth backstop for
        anything that could still defeat that -- a payload-encoding
        regression, a future second minting path -- rather than the primary
        mechanism.
        """

        await self._entitlement.require(org_id)
        if len(candidates) > MAX_ADMISSION_CANDIDATES:
            raise ValueError(
                f"At most {MAX_ADMISSION_CANDIDATES} evidence candidates may be "
                "admitted in one round"
            )

        admissions: list[EvidenceAdmission] = []
        #: CHAOS-3633 non-vacuity backstop. Maps a minted ``evidence_ref_id``
        #: to the locator that minted it THIS round. If a second,
        #: DIFFERENT locator ever mints the identical handle, that is a
        #: silent collision no matter its cause, and it is refused here
        #: rather than returned as though it were a distinct, valid record.
        _minted_this_round: dict[str, str] = {}
        for candidate in candidates:
            resolver = self._resolvers.get(candidate.source_system)
            if resolver is None:
                admissions.append(
                    EvidenceAdmission(
                        candidate=candidate,
                        state=EvidenceAvailability.UNCONFIGURED,
                        warning="source_unconfigured",
                    )
                )
                continue
            # Re-resolved for EVERY candidate, exactly as ``expand`` does and
            # for the same reason: one denied locator must not inherit
            # another's successful authorization decision.
            resolution = await self._authorizer.resolve(
                org_id, permission_fingerprint, scope_request
            )
            if not _authorized_outcome(resolution):
                admissions.append(
                    EvidenceAdmission(
                        candidate=candidate,
                        state=EvidenceAvailability.UNAUTHORIZED,
                        warning="not_found",
                    )
                )
                continue
            try:
                record = await resolver.resolve(
                    org_id=org_id, scope=resolution, candidate=candidate
                )
            except Exception:
                admissions.append(
                    EvidenceAdmission(
                        candidate=candidate,
                        state=EvidenceAvailability.UNAVAILABLE,
                        warning="source_unavailable",
                    )
                )
                continue
            if record is None:
                admissions.append(
                    EvidenceAdmission(
                        candidate=candidate,
                        state=EvidenceAvailability.NO_MATCHES,
                        warning="evidence_deleted_or_unavailable",
                    )
                )
                continue
            # Minted over the record the SOURCE returned, never over the
            # candidate -- CONTENT always comes from ``record``, never
            # ``candidate``. A candidate that pointed at one record and got
            # another back still yields a truthful handle, because the
            # handle describes what the source actually had.
            #
            # ``record_locator`` is the one exception, and it is an
            # ADDRESS, not content: ``candidate.locator`` -- see class
            # docstring -- is undisputedly the discovery layer's identity
            # for the record it asked about, and ``resolver.resolve``
            # already confirmed a genuine record exists at exactly that
            # locator (see ``admit``'s own docstring, CHAOS-3633).
            #
            # ``valid_entity_ids`` is the record's OWN entity -- see the
            # docstring. That single-element set is what turns the existing
            # containment check in ``_authorize_expansion`` from a tautology
            # into the admission path's entity authorization.
            #
            # CHAOS-3675 PR2, entity-less records (deployments with no
            # linked PR, incidents with no linked repository): empty ONLY
            # when the resolver has EXPLICITLY set
            # ``no_authorizable_entity`` -- never inferred from
            # ``entity_id``'s value, which stays a normal, non-empty
            # descriptive identity either way (``DevEvidenceRef.entity_id``
            # requires at least one character). Gating on the flag alone,
            # not on ``entity_id`` being falsy, is what stops a resolver
            # bug from silently earning repository-only authorization by
            # merely returning something empty. An empty
            # ``valid_entity_ids`` skips the entity containment check
            # entirely (``_authorize_expansion``'s own truthy gate),
            # leaving authorization to the unmodified, independent
            # ``repository_ids`` check below it.
            record = replace(record, record_locator=candidate.locator)
            if record.no_authorizable_entity and not record.repository_ids:
                # An empty ``valid_entity_ids`` skips the entity check;
                # ``_authorize_expansion``'s repository check is ALSO
                # truthy-gated (``if evidence.repository_ids:``). A record
                # with neither has no authorization anchor at all -- both
                # checks would skip and it would be admitted
                # unconditionally to any caller with a generally-valid
                # resolution, regardless of scope. Refused here rather
                # than trusted to every current and future resolver's own
                # discipline never to produce that combination.
                admissions.append(
                    EvidenceAdmission(
                        candidate=candidate,
                        state=EvidenceAvailability.UNAUTHORIZED,
                        warning="no_authorization_anchor",
                    )
                )
                continue
            valid_entity_ids = (
                () if record.no_authorizable_entity else (record.entity_id,)
            )
            ref = self._to_ref(org_id, record, valid_entity_ids=valid_entity_ids)
            state, warning = await self._authorize_expansion(
                org_id,
                permission_fingerprint,
                scope_request,
                resolution,
                ref,
            )
            if state is not EvidenceAvailability.AVAILABLE:
                admissions.append(
                    EvidenceAdmission(candidate=candidate, state=state, warning=warning)
                )
                continue
            colliding_locator = _minted_this_round.get(ref.evidence_ref_id)
            if colliding_locator is not None and colliding_locator != candidate.locator:
                admissions.append(
                    EvidenceAdmission(
                        candidate=candidate,
                        state=EvidenceAvailability.UNAVAILABLE,
                        warning="ambiguous_record_identity",
                    )
                )
                continue
            _minted_this_round[ref.evidence_ref_id] = candidate.locator
            admissions.append(
                EvidenceAdmission(
                    candidate=candidate,
                    state=EvidenceAvailability.AVAILABLE,
                    evidence=ref,
                )
            )
        return EvidenceAdmissionResult(tuple(admissions))

    async def _authorize_expansion(
        self,
        org_id: str,
        permission_fingerprint: str,
        scope_request: ScopeResolveRequest,
        resolution: ScopeResolution,
        evidence: DevEvidenceRef,
    ) -> tuple[EvidenceAvailability, str | None]:
        if not _authorized_outcome(resolution) or not self._signer.verify(
            org_id, evidence
        ):
            return EvidenceAvailability.UNAUTHORIZED, "not_found"
        allowed_entities = _authorized_entity_ids(resolution)
        if (
            evidence.valid_entity_ids
            and not set(evidence.valid_entity_ids) <= allowed_entities
        ):
            return EvidenceAvailability.UNAUTHORIZED, "not_found"
        if evidence.repository_ids:
            repository_resolution = await self._authorizer.resolve(
                org_id,
                permission_fingerprint,
                ScopeResolveRequest(
                    explicit_refs=tuple(
                        ScopeRef(EntityKind.REPOSITORY, repository_id)
                        for repository_id in evidence.repository_ids
                    ),
                    team_filter_refs=scope_request.team_filter_refs,
                    time_range=scope_request.time_range,
                    allow_organization_fallback=False,
                ),
            )
            if not _authorized_outcome(repository_resolution) or not set(
                evidence.repository_ids
            ) <= _authorized_repository_ids(repository_resolution):
                return EvidenceAvailability.UNAUTHORIZED, "not_found"
        return EvidenceAvailability.AVAILABLE, None

    def _to_ref(
        self,
        org_id: str,
        record: EvidenceRecord,
        *,
        valid_entity_ids: tuple[str, ...],
    ) -> DevEvidenceRef:
        return DevEvidenceRef(
            schema_version="dev_evidence_ref.v1",
            evidence_ref_id=self._signer.issue(org_id, record),
            source_system=record.source_system,
            source_version=record.source_version,
            entity_type=record.entity_type,
            entity_id=record.entity_id,
            display_label=sanitize_untrusted_text(record.display_label, max_chars=256)
            or "Evidence",
            link=_safe_link(record),
            observed_at=record.observed_at,
            freshness=record.freshness,
            provenance=record.provenance,
            confidence=record.confidence,
            citation_text=sanitize_untrusted_text(record.raw_excerpt),
            repository_ids=list(record.repository_ids),
            valid_entity_ids=list(valid_entity_ids),
            record_locator=record.record_locator,
            flags=DevEvidenceFlags(
                stale=record.stale or record.freshness is FreshnessState.STALE,
                unavailable=record.unavailable,
                redacted=record.redacted,
                deleted=record.deleted,
                uncertain=record.uncertain,
                conflicting=record.conflicting,
                untrusted_content=True,
            ),
        )

    @staticmethod
    def _rank_key(query: str, record: EvidenceRecord) -> tuple[object, ...]:
        query_terms = tuple(part.casefold() for part in query.split())
        label = record.display_label.casefold()
        excerpt = (record.raw_excerpt or "").casefold()
        matches = sum(3 for term in query_terms if term in label) + sum(
            1 for term in query_terms if term in excerpt
        )
        source_precedence: Mapping[str, int] = {
            "work_items": 0,
            "work_units": 1,
            "pull_requests": 2,
            "reviews": 3,
            "ci_runs": 4,
            "deployments": 5,
            "incidents": 6,
            "commits": 7,
            "acr": 8,
        }
        freshness_penalty = 0 if record.freshness is FreshnessState.FRESH else 1
        observed = record.observed_at.astimezone(UTC).timestamp()
        return (
            -matches,
            source_precedence.get(record.source_system, 99),
            freshness_penalty,
            -observed,
            record.entity_type,
            record.entity_id,
        )
