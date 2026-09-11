"""Operator-side writes and diagnostics for ``go_api_routing_state``.

``go_api_registry.py`` is the REQUEST path's access layer: it reads a
routing row (``lookup_routing_state``), appends an immutable candidate
build (``register_candidate_build``) and records proof outcomes
(``record_proof_run``). What it has never had is a **writer for the
routing row itself** -- the one mutable row that decides whether an
operation is served by Go at all.

That gap is not academic. Between 2026-09-01 and 2026-09-07 the only
code in this repository that could write a ``RoutingState`` row was a
test helper (``tests/api/graphql/test_go_api_livelocal.py``'s
``_seed_candidate_and_enable_canary``). Twelve rows had been seeded by
hand at a schema digest that a same-day SDL change (#2065,
``33b3f3f21d``) immediately obsoleted; every dispatch then missed, fell
back to Python, and reported nothing -- for six days. This module is the
committed, tested writer that was missing, and
:func:`routing_status_rows` is the read that makes the failure visible
instead of silent.

Nothing here is on a request path. It is called by
``dev-hops go-api routing`` (:mod:`dev_health_ops.api.graphql.go_api_cli`)
and by the startup drift check in :mod:`dev_health_ops.api._lifespan`.

**Every function is explicit about failure.** A lookup that cannot reach
the database raises; it never returns "no rows", because "the registry is
unreachable" and "nothing is enabled" are the two states this whole
subsystem exists to tell apart (the same reasoning
``go_api_registry_telemetry``'s ``result`` label documents).
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast

import sqlalchemy as sa
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement

from dev_health_ops.models.go_api_registry import ProofRun, RoutingState

from .go_api_registry import register_candidate_build

__all__ = [
    "DISABLE_MODES",
    "ENABLEMENT_PROOF_CITED_MISMATCH_STATE",
    "ENABLEMENT_PROOF_STAGE",
    "ENABLEMENT_PROOF_TERMINAL_STATE",
    "ENABLEMENT_PROOF_TERMINAL_STATES",
    "ENABLEMENT_TARGET_MODES",
    "ENABLEMENT_TARGET_MODE_ANY_ROUTE",
    "ENABLEMENT_TARGET_MODE_EDGE_ONLY",
    "MEASUREMENT_ROUTE_EDGE",
    "MEASUREMENT_ROUTE_PROOF",
    "AuthorizingReceipt",
    "build_enablement_proof_select",
    "build_enablement_receipt_select",
    "OperationStatus",
    "count_rows_by_schema_digest",
    "enable_operation",
    "enablement_receipts",
    "operations_with_enablement_proof",
    "routing_status_rows",
    "upsert_routing_state",
]

#: The proof a routing row must have behind it before ``enable`` will turn
#: an operation on without ``--acknowledge-unproven``. Plan section 5's
#: five-stage gate runs dual_run -> deployed_executed -> shadow -> canary;
#: ``deployed_executed`` is the first stage that proves THIS candidate
#: build actually served the operation through real ingress, auth, parse,
#: validate, dispatch and a real database -- "a constructor, health check,
#: direct resolver test, or bare 200 does not qualify". Requiring the
#: EARLIER stage (dual_run) would accept a local comparison as evidence
#: the deployed binary works; requiring a LATER one (shadow/canary) would
#: be unsatisfiable, since reaching them is what enabling the row does.
ENABLEMENT_PROOF_STAGE = "deployed_executed"

#: A ``match`` always counts. Nothing else counts on its own: a
#: ``deployed_executed`` run that terminated in ``timeout``/``fallback``/
#: ``unsupported``/``proof_failed`` is evidence the operation is NOT
#: ready, and treating any terminal state as "proof exists" would make the
#: gate worse than absent, because it would look enforced.
ENABLEMENT_PROOF_TERMINAL_STATE = "match"

#: The ONE exception, added by CHAOS-5484: a
#: ``mismatch`` in which every difference is a named defect in the PYTHON
#: baseline. Those are the divergences where Python is wrong and Go is
#: right (CHAOS-5447/5448/5449/5450), so refusing them would mean an
#: operation could never be enabled until Python was fixed -- which is
#: backwards, since enabling Go is how the Python defect stops being
#: served.
#:
#: The receipt is NOT rewritten. ``terminal_state`` stays ``mismatch``
#: forever; this changes only how a receipt is READ.
ENABLEMENT_PROOF_CITED_MISMATCH_STATE = "mismatch"

#: Every terminal state the preflight can be satisfied by, for operator
#: messages that have to name them. ``mismatch`` appears here only under
#: the citation conditions in :func:`build_enablement_proof_select`;
#: listing it unqualified would misdescribe the rule.
ENABLEMENT_PROOF_TERMINAL_STATES = (
    ENABLEMENT_PROOF_TERMINAL_STATE,
    ENABLEMENT_PROOF_CITED_MISMATCH_STATE,
)

#: Measurement routes, mirroring go_api_proof_run's CHECK constraint and
#: internal/goapiproof's RouteEdge/RouteProof.
MEASUREMENT_ROUTE_EDGE = "edge"
MEASUREMENT_ROUTE_PROOF = "proof"

#: Target modes `enable` can be asked for, and the route rule each one
#: carries.
#:
#: * ``canary``  -- any recorded route. A shadow operation can ONLY be
#:                  measured through ``/query/proof``, because
#:                  PostgresSwitch.Enabled admits canary|primary only and
#:                  the deployed build will not execute a shadow operation
#:                  on ``/query`` at all. Requiring edge evidence here
#:                  would mean an operation could only be proven after it
#:                  had already been enabled.
#: * ``primary`` -- ``edge`` only. Promotion to primary is promotion to
#:                  served traffic, and ``/query/proof`` is a
#:                  measurement-only handler unreachable from the product
#:                  edge: a proof-route receipt says the build CAN serve
#:                  the operation, not that the edge DOES.
#:
#: A NULL route satisfies neither. "Any route" is not "no route", and
#: admitting unknown provenance is the same failure shape as reading a
#: DEFAULT 0 as an assertion.
ENABLEMENT_TARGET_MODE_ANY_ROUTE = "canary"
ENABLEMENT_TARGET_MODE_EDGE_ONLY = "primary"
ENABLEMENT_TARGET_MODES = (
    ENABLEMENT_TARGET_MODE_ANY_ROUTE,
    ENABLEMENT_TARGET_MODE_EDGE_ONLY,
)

#: Modes `disable` may set. All three make an operation UNREACHABLE to a
#: real client, which is the whole point of the verb:
#:
#: * ``python``   -- the documented safe default; identical in effect to
#:                   having no row at all.
#: * ``disabled`` -- same reachability, but records a deliberate decision
#:                   rather than a default, so an operator reading the
#:                   table later can tell "turned off" from "never on".
#: * ``shadow``   -- the client still receives Python's response (plan §5
#:                   stage 4), and the dispatcher logs loudly that the
#:                   shadow executor does not exist. Protective today even
#:                   though the comparison half is unimplemented.
#:
#: `canary`/`primary` are deliberately absent: turning an operation ON is
#: `enable`'s job, and it has preflights this verb intentionally does not.
DISABLE_MODES = ("python", "disabled", "shadow")


@dataclass(frozen=True)
class OperationStatus:
    """One row of ``dev-hops go-api routing status``.

    ``digest_state`` is the field the 2026-09-01 outage had no way to
    report:

    * ``MATCH``    -- a routing row exists at the LIVE schema digest.
    * ``STALE``    -- rows exist for this operation, but only at OTHER
      schema digests (``stale_digests``). This is the silent-death shape:
      the rows look present in ``psql`` and are unreachable in fact.
    * ``MISSING``  -- no row at any digest. Never enabled, or cleaned up.

    * ``DOCUMENT_DRIFT`` -- a row exists at the LIVE schema digest but
      carries a DOCUMENT digest the catalog no longer names. It gets its
      OWN status row, carrying its own ``document_digest`` and ``mode``,
      so the operator sees which digest is actually in the table.
    * ``UNREGISTERED`` -- a row exists at the LIVE schema digest for an
      operation the catalog does not register at all (renamed or retired).
      Its own status row too: iterating the catalog alone named it
      nowhere.

    ``DOCUMENT_DRIFT`` is the silent-death shape one level down: the edge
    resolves a request to an operation through the catalog, so such a row
    cannot be dispatched. It used to be reported ``MATCH`` under the
    CATALOG's digest -- a row an operator reads as serving that in fact
    serves nothing. Folding it into ``STALE`` would widen
    what STALE means (rows at other SCHEMA digests), and reporting it
    ``MISSING`` would be the other half of the original error:
    ``count_rows_by_schema_digest`` groups by schema digest alone, so the
    row would be named nowhere at all. Never silent, so it gets a name.

    ``proven`` is separate from and orthogonal to ``digest_state``: a row
    can be live and reachable while nothing ever proved the deployed
    build serves it (see :data:`ENABLEMENT_PROOF_STAGE`). ``status``
    renders that as ``UNPROVEN`` so an acknowledged-unproven enablement
    stays visible for as long as it is in force, not just in the log line
    written the moment it happened.
    """

    operation: str
    document_digest: str
    digest_state: str
    mode: str | None = None
    current_candidate_build: str | None = None
    rollout_percentage: int | None = None
    owner: str | None = None
    updated_at: datetime | None = None
    stale_digests: tuple[str, ...] = ()
    proven: bool = False

    @property
    def reachable(self) -> bool:
        """Whether a real request would be served by Go right now.

        Mirrors ``go_api_dispatcher``'s ``_REACHABLE_MODES`` and
        ``routeswitch.PostgresSwitch``'s ``reachableModes`` exactly:
        ``canary`` and ``primary`` only. ``shadow`` is NOT reachable (the
        client still gets Python's response), and ``python``/``disabled``
        are the safe default a missing row already gives.
        """
        return self.digest_state == "MATCH" and self.mode in ("canary", "primary")


async def upsert_routing_state(
    session: AsyncSession,
    *,
    schema_digest: str,
    document_digest: str,
    selected_operation: str,
    candidate_build: str,
    mode: str,
    rollout_percentage: int = 100,
    owner: str = "go",
    review_evidence: str | None = None,
    recorded_by: str | None = None,
) -> None:
    """Insert or update ONE routing row, idempotently.

    ``ON CONFLICT DO UPDATE`` on the 3-column primary key, so re-running
    an enablement is a no-op in effect rather than an error: an operator
    recovering from a digest move should be able to run the same command
    twice without thinking about it. ``updated_at`` is refreshed on every
    write (including a no-change one) precisely so "when was this last
    asserted" is answerable -- the column is the only timestamp on the
    row, and a rollout that was re-confirmed today reads very differently
    from one nothing has touched since September.

    The four-column foreign key to ``go_api_candidate_build`` means the
    candidate build MUST already be registered; :func:`enable_operation`
    does that first, in the same transaction. Callers using this function
    directly are responsible for the same ordering.
    """
    now = datetime.now(timezone.utc)
    stmt = (
        pg_insert(RoutingState)
        .values(
            schema_digest=schema_digest,
            document_digest=document_digest,
            selected_operation=selected_operation,
            current_candidate_build=candidate_build,
            owner=owner,
            mode=mode,
            rollout_percentage=rollout_percentage,
            review_evidence=review_evidence,
            recorded_by=recorded_by,
            updated_at=now,
        )
        .on_conflict_do_update(
            index_elements=[
                "schema_digest",
                "document_digest",
                "selected_operation",
            ],
            set_={
                "current_candidate_build": candidate_build,
                "owner": owner,
                "mode": mode,
                "rollout_percentage": rollout_percentage,
                "review_evidence": review_evidence,
                "recorded_by": recorded_by,
                "updated_at": now,
            },
        )
    )
    await session.execute(stmt)


async def enable_operation(
    session: AsyncSession,
    *,
    schema_digest: str,
    document_digest: str,
    selected_operation: str,
    candidate_build: str,
    mode: str,
    rollout_percentage: int = 100,
    review_evidence: str | None = None,
    recorded_by: str | None = None,
) -> None:
    """Register the candidate build, then point the routing row at it.

    Both writes go through ONE session so a failure to write the routing
    row cannot leave a candidate build registered for a rollout that
    never happened. The order matters and is not interchangeable: the
    routing row carries a 4-column foreign key to the candidate-build
    row, so registering second would violate it.
    """
    await register_candidate_build(
        session,
        schema_digest=schema_digest,
        document_digest=document_digest,
        selected_operation=selected_operation,
        candidate_build=candidate_build,
    )
    await upsert_routing_state(
        session,
        schema_digest=schema_digest,
        document_digest=document_digest,
        selected_operation=selected_operation,
        candidate_build=candidate_build,
        mode=mode,
        rollout_percentage=rollout_percentage,
        review_evidence=review_evidence,
        recorded_by=recorded_by,
    )


@dataclass(frozen=True)
class ModeChange:
    """One row `disable` would change, or did.

    ``applied`` is False for a dry run. ``current_mode`` is None when no row
    exists at the live digest -- reported as "nothing to disable" rather
    than invented, because writing a `python` row for an operation that was
    never enabled would manufacture history.
    """

    operation: str
    document_digest: str
    current_mode: str | None
    new_mode: str
    candidate_build: str | None
    applied: bool = False

    @property
    def is_noop(self) -> bool:
        return self.current_mode is None or self.current_mode == self.new_mode


async def plan_disable(
    session: AsyncSession,
    *,
    schema_digest: str,
    operations: Mapping[str, str],
    new_mode: str,
    expected_candidate_build: str | None = None,
) -> tuple[list[ModeChange], list[str]]:
    """Work out what `disable` would change, WITHOUT changing anything.

    Returns ``(changes, problems)``. ``problems`` is non-empty only for a
    condition an operator must resolve -- currently just a candidate-build
    guard mismatch, meaning someone repointed the row since they looked.

    Deliberately performs no digest-agreement or reachability check. This
    is the OFF ramp: it has to work when the planes disagree, when
    query-api is down, and when the operator is in a hurry. `enable`'s
    preflights exist to stop traffic moving to an unproven plane; none of
    that reasoning applies to moving traffic back.
    """
    if new_mode not in DISABLE_MODES:
        raise ValueError(
            f"invalid disable mode {new_mode!r}, expected one of {DISABLE_MODES}"
        )
    result = await session.execute(
        select(RoutingState).where(RoutingState.schema_digest == schema_digest)
    )
    # Keyed by (operation, document digest), because that is how
    # go_api_routing_state is keyed -- (schema_digest, document_digest,
    # selected_operation). Keying by operation alone collapses two
    # document versions under one schema onto whichever row the scan
    # returned last, producing a rollback plan that named the wrong row's
    # mode. A disable verb reporting the wrong current_mode is a verb an
    # operator cannot check.
    live = {
        (row.selected_operation, row.document_digest): row
        for row in result.scalars().all()
    }

    changes: list[ModeChange] = []
    problems: list[str] = []
    for operation, document_digest in sorted(operations.items()):
        # The CATALOG's document digest, not any row that happens to
        # carry this operation name: the caller asked to disable a
        # specific registered document.
        row = live.get((operation, document_digest))
        if row is not None and expected_candidate_build is not None:
            if row.current_candidate_build != expected_candidate_build:
                problems.append(
                    f"{operation}: row points at candidate build "
                    f"{row.current_candidate_build}, not the "
                    f"{expected_candidate_build} you named -- someone has "
                    "repointed it since you looked; re-run `status` and "
                    "decide again"
                )
                continue
        changes.append(
            ModeChange(
                operation=operation,
                document_digest=document_digest,
                current_mode=row.mode if row is not None else None,
                new_mode=new_mode,
                candidate_build=row.current_candidate_build
                if row is not None
                else None,
            )
        )
    return changes, problems


async def apply_disable(
    session: AsyncSession,
    *,
    schema_digest: str,
    changes: Sequence[ModeChange],
    review_evidence: str | None = None,
    recorded_by: str | None = None,
    expected_candidate_build: str | None = None,
) -> list[ModeChange]:
    """Write the mode changes `plan_disable` produced.

    Only rows that actually exist are touched: a `ModeChange` with
    ``current_mode is None`` is skipped, never inserted. Turning something
    off must not be able to turn something on -- enforced twice, by the
    mode check below and by using UPDATE rather than an upsert.

    ``expected_candidate_build`` becomes part of the WHERE, so the
    candidate-build guard is atomic with the write instead of a
    time-of-check/time-of-use read in `plan_disable`. A row repointed in
    between simply does not match, and is reported by its absence from the
    returned list.
    """
    applied: list[ModeChange] = []
    for change in changes:
        if change.current_mode is None:
            continue
        # Enforced HERE, at the write, not only in plan_disable: a
        # hand-built ModeChange(new_mode="primary") reached this
        # function and turned routing ON -- the one thing an off-ramp must
        # never be able to do. An invariant checked only by the caller is
        # an invariant the next caller breaks.
        if change.new_mode not in DISABLE_MODES:
            raise ValueError(
                f"apply_disable refuses mode {change.new_mode!r}: this verb "
                f"may only set {DISABLE_MODES}. Turning an operation ON is "
                "`enable`'s job, and it has preflights this path does not."
            )
        conditions = [
            RoutingState.schema_digest == schema_digest,
            RoutingState.document_digest == change.document_digest,
            RoutingState.selected_operation == change.operation,
        ]
        if expected_candidate_build is not None:
            # The guard has to be part of the WRITE, not a separate earlier
            # read: plan_disable checks it, then anyone can
            # repoint the row before apply_disable fires. Putting it in the
            # WHERE makes the check and the write one atomic statement, so
            # a repointed row is simply not matched.
            conditions.append(
                RoutingState.current_candidate_build == expected_candidate_build
            )
        # `AsyncSession.execute` is annotated `Result[Any]`, which has no
        # `rowcount`; an UPDATE returns a `CursorResult` at runtime, which
        # does. Cast rather than `# type: ignore`, so the reason stays
        # legible and swapping this for a non-DML statement fails review
        # instead of being quietly covered.
        statement = (
            update(RoutingState)
            .where(*conditions)
            .values(
                mode=change.new_mode,
                review_evidence=review_evidence,
                recorded_by=recorded_by,
                updated_at=datetime.now(timezone.utc),
            )
        )
        result = cast("CursorResult[Any]", await session.execute(statement))
        if result.rowcount == 0:
            # Only reachable with the guard on: the row moved between plan
            # and apply. Reported by omission from `applied`, never as a
            # silent success.
            continue
        applied.append(
            ModeChange(
                operation=change.operation,
                document_digest=change.document_digest,
                current_mode=change.current_mode,
                new_mode=change.new_mode,
                candidate_build=change.candidate_build,
                applied=True,
            )
        )
    return applied


async def operations_with_enablement_proof(
    session: AsyncSession,
    *,
    schema_digest: str,
    candidate_build: str,
    operations: Mapping[str, str],
    target_mode: str,
) -> frozenset[str]:
    """Which of ``operations`` have a ``deployed_executed``/``match``
    proof run for EXACTLY this ``(schema_digest, candidate_build)``.

    Scoped to the exact tuple on purpose: plan section 8.3's rule is that
    a proof is evidence for one immutable 4-column key and is "never
    carried forward across any of the four changing". ``operations`` is
    therefore a mapping of ``operation -> document_digest``, not a bare
    list of names: the document digest is one of those four columns. A proof recorded
    against an older build, or against the SDL as it was before a schema
    move, says nothing about the build being enabled now -- which is the
    entire lesson of the digest move this command exists to recover from.
    """
    if not operations:
        return frozenset()
    result = await session.execute(
        build_enablement_proof_select(
            schema_digest=schema_digest,
            candidate_build=candidate_build,
            operations=operations,
            target_mode=target_mode,
        )
    )
    return frozenset(result.scalars().all())


def _admissible_terminal_state() -> ColumnElement[bool]:
    """Which terminal states authorize an enablement.

    A ``match``, or a ``mismatch`` in which every difference was cited
    against a named Python defect. The three conditions on the mismatch
    arm are all load-bearing and none is belt-and-braces:

    * ``differences_outside_baseline_defect == 0`` -- nothing diverged
      that no declared defect covers.
    * ``baseline_defect IS NOT NULL`` -- and this is the subtle one. That
      counter is ``NOT NULL DEFAULT 0``, so a bare ``== 0`` cannot tell
      "computed, and every difference was cited" apart from "this row
      predates the column and nothing ever computed it". Requiring a
      citation is what disambiguates: only a row that actually named a
      ticket can promote.
    * ``cardinality(...) > 0`` -- because an empty array is a writer that
      had the column and cited nothing, and without this the IS NOT NULL
      test could be satisfied by writing ``'{}'``.
    """
    return and_(
        # A whitespace-only candidate build is as unmatchable as an absent
        # one while LOOKING present -- and ``proven`` is keyed on it.
        sa.func.btrim(ProofRun.candidate_build, _BLANK_CUTSET) != "",
        # The binding is part of the rule, on both arms, uniformly.
        #
        # This change added two counting rules proveOne did not have at
        # the base build -- an $.http.* difference and an unbound edge
        # mismatch now count into differences_outside_baseline_defect --
        # so every row the OLD writer wrote with mismatch/edge/outside=0
        # and a citation became PRIMARY-admissible retroactively, tied to
        # no replica and possibly carrying an uncited HTTP difference.
        # Those are the pre-0129 rows: build_binding IS NULL.
        #
        # per_request excludes them and nothing this writer produces: the
        # proof route always binds per request, an edge match without the
        # header is downgraded to `unsupported`, and an edge mismatch
        # without it already has outside >= 1. It ALSO excludes the sound
        # pre-0129 MATCH rows: after 0129 every operation
        # proven before it reads UNPROVEN and `enable` refuses it until
        # go-api-prove re-runs at the deployed build (JOB 6 does).
        ProofRun.build_binding == BUILD_BINDING_PER_REQUEST,
        _terminal_state_admits(),
    )


#: THE definition of "names nothing", identical to
#: goapiproof.blankCitationCutset by construction: space, tab, newline,
#: vertical tab, form feed, carriage return, NBSP.
#:
#: Explicit rather than Unicode's space class because the two engines
#: cannot share that definition. Measured on this Postgres (UTF8):
#: ``btrim(E'\u00a0', <this set>) = ''`` is TRUE while
#: ``E'\u00a0' ~ '^[[:space:]]+$'`` is FALSE, and U+2028 is the exact
#: reverse -- the explicit set and POSIX ``[[:space:]]`` have opposite
#: gaps, and neither equals Go's ``unicode.IsSpace``.
#:
#: The SQL definition is the authority and Go implements it, because SQL
#: reads rows from producers Go has never seen. Pinned character by
#: character by ``TestTheBlankDefinitionIsIdenticalInBothEngines``.
_BLANK_CUTSET = " \t\n\v\f\r\xa0"

#: The only binding a receipt from the current writer can carry on an
#: admissible row. See the comment at the clause for why both arms.
BUILD_BINDING_PER_REQUEST = "per_request"


def _terminal_state_admits() -> ColumnElement[bool]:
    return or_(
        ProofRun.terminal_state == ENABLEMENT_PROOF_TERMINAL_STATE,
        and_(
            ProofRun.terminal_state == ENABLEMENT_PROOF_CITED_MISMATCH_STATE,
            ProofRun.differences_outside_baseline_defect == 0,
            # No ``baseline_defect IS NOT NULL`` here, deliberately:
            # ``cardinality(NULL) > 0`` is NULL, not TRUE, so a NULL
            # citation list is already excluded by the cardinality clause.
            # Both the NULL and empty-array cases are in the shared
            # admission table, so this is covered rather than argued.
            func.cardinality(ProofRun.baseline_defect) > 0,
            # ...but ``cardinality(ARRAY[''])`` is 1, so a mismatch citing a
            # single EMPTY ticket read as fully cited and authorized a
            # promotion. Executed against real PostgreSQL before this
            # clause existed: "EMPTY-STRING CITATION admitted as enablement
            # proof: true". A citation that names nothing is not a
            # citation, and ``btrim`` because whitespace names nothing
            # either.
            #
            # Stated in BOTH implementations rather than only at the
            # writer: rows already in the table, a future writer and a
            # manual repair all reach the predicate. The shared admission
            # fixture is what keeps the two statements from drifting.
            ~sa.exists(
                sa.select(sa.literal(1))
                .select_from(sa.func.unnest(ProofRun.baseline_defect).alias("citation"))
                .where(
                    sa.or_(
                        # A SQL NULL element: cardinality() counts it and
                        # one-argument btrim(NULL) is NULL, not '', so it
                        # passed BOTH predicates and promoted to primary,
                        # executed through the CLI.
                        sa.column("citation").is_(None),
                        sa.func.btrim(sa.column("citation"), _BLANK_CUTSET) == "",
                    )
                )
            ),
        ),
    )


def _admissible_route(target_mode: str) -> ColumnElement[bool]:
    """Which measurement routes authorize enabling into ``target_mode``.

    See :data:`ENABLEMENT_TARGET_MODES` for why the two modes differ. Both
    branches require the route to be RECORDED: a NULL is a pre-0128 row
    that says nothing about how it was measured, and "any route" is not
    "no route".
    """
    if target_mode == ENABLEMENT_TARGET_MODE_EDGE_ONLY:
        return ProofRun.measurement_route == MEASUREMENT_ROUTE_EDGE
    return ProofRun.measurement_route.is_not(None)


def build_enablement_proof_select(
    *,
    schema_digest: str,
    candidate_build: str,
    operations: Mapping[str, str],
    target_mode: str,
) -> Select[tuple[str]]:
    """The SELECT :func:`operations_with_enablement_proof` executes.

    Extracted so a test can COMPILE THE PRODUCTION PREDICATE rather than a
    hand-rebuilt copy of it. The previous test reconstructed
    the same clauses and compiled those, which proves only that the test
    agrees with itself -- a regression in the real function would not have
    failed it. The behaviour needs no database to verify, so the seam is
    worth having.
    """
    # Fail CLOSED on a mode this function has no rule for. An unknown
    # target mode must never fall through to the more permissive branch:
    # that is how a promotion to served traffic would quietly accept
    # measurement-only evidence.
    if target_mode not in ENABLEMENT_TARGET_MODES:
        raise ValueError(
            f"build_enablement_proof_select: unknown target mode {target_mode!r} "
            f"-- expected one of {', '.join(ENABLEMENT_TARGET_MODES)}. Refusing "
            "to compile a predicate whose route rule is undefined."
        )

    return (
        select(ProofRun.selected_operation)
        .where(
            *_enablement_proof_conditions(
                schema_digest=schema_digest,
                candidate_build=candidate_build,
                operations=operations,
                target_mode=target_mode,
            )
        )
        .distinct()
    )


def _enablement_proof_conditions(
    *,
    schema_digest: str,
    candidate_build: str,
    operations: Mapping[str, str],
    target_mode: str,
) -> list[ColumnElement[bool]]:
    """The WHERE clauses of the rule -- ONE list, read by both selects.

    :func:`build_enablement_proof_select` (which operations are proven) and
    :func:`build_enablement_receipt_select` (WHICH receipt proves each) must
    never disagree about admissibility, so neither restates a clause.
    """
    # The key is FOUR columns, and `document_digest` is not optional --
    # it was missing, so a proof recorded against a
    # DIFFERENT registered document could authorize an enablement.
    # Matched as an explicit tuple-OR rather than two independent `IN`
    # lists: `selected_operation IN (...) AND document_digest IN (...)`
    # is a cross product and would accept exactly the mismatch under test.
    return [
        ProofRun.schema_digest == schema_digest,
        ProofRun.candidate_build == candidate_build,
        ProofRun.stage == ENABLEMENT_PROOF_STAGE,
        _admissible_terminal_state(),
        _admissible_route(target_mode),
        # `or_()` with no arguments is dropped from the WHERE clause
        # entirely by SQLAlchemy (a warning, not an error) rather than
        # compiling to FALSE, so an empty `operations` mapping would
        # silently remove the one clause that scopes the query to the
        # operations asked about -- the exact cross-product this
        # function's own comment above warns against. `sa.false()` as a
        # standing first argument keeps the clause always present.
        or_(
            sa.false(),
            *(
                and_(
                    ProofRun.selected_operation == operation,
                    ProofRun.document_digest == document_digest,
                )
                for operation, document_digest in operations.items()
            ),
        ),
    ]


@dataclass(frozen=True)
class AuthorizingReceipt:
    """The receipt that authorizes enabling one operation.

    `enable` printed the same output for a ``match`` admission and a
    fully-cited ``mismatch`` admission, and
    nothing -- not the output, not the routing row, not a log line -- named
    the receipt that carried the decision. A predicate regression that
    admitted the wrong receipt would have looked exactly like a correct
    enablement. This is what `enable` now prints, logs and writes into the
    row's ``review_evidence``.
    """

    operation: str
    receipt_id: str
    terminal_state: str
    baseline_defect: tuple[str, ...]
    measurement_route: str | None
    build_binding: str | None
    observed_at: datetime | None
    #: What the receipt's citation covered and did not, by finding shape, as
    #: the Go writer recorded them in the receipt's provenance.
    #: ``None`` when the receipt carries no such provenance -- an
    #: older writer, or an operator's free text -- which is said, never
    #: printed as an empty coverage.
    covered_by_shape: Mapping[str, int] | None = None
    outside_by_shape: Mapping[str, int] | None = None

    def shape_counts(self) -> str:
        """``covered[null=7 value=384] outside[]`` -- go-api-prove's format."""
        if self.covered_by_shape is None or self.outside_by_shape is None:
            return "shape counts unrecorded"

        def render(counts: Mapping[str, int]) -> str:
            return " ".join(f"{key}={counts[key]}" for key in sorted(counts))

        return (
            f"covered[{render(self.covered_by_shape)}] "
            f"outside[{render(self.outside_by_shape)}]"
        )

    def evidence(self) -> str:
        """One line naming the receipt, for logs and ``review_evidence``."""
        cited = (
            f" baseline_defect={','.join(self.baseline_defect)}"
            if self.baseline_defect
            else ""
        )
        return (
            f"proof_receipt={self.receipt_id} terminal_state={self.terminal_state}"
            f"{cited} measurement_route={self.measurement_route} "
            f"build_binding={self.build_binding} {self.shape_counts()}"
        )


#: Every field name ``ReceiptProvenance`` (internal/goapiproof/run.go) may
#: put in a receipt's ``review_evidence`` JSON. Any one of them present
#: marks the payload as the writer's own object, independent of which
#: particular fields that receipt happened to carry.
_RECEIPT_PROVENANCE_FIELDS = frozenset(
    {
        "operator",
        "measurement_route",
        "edge_build_binding",
        "routing_row_build",
        "refusal",
        "measured_operations",
        "attempted_operations",
        "covered_by_shape",
        "outside_by_shape",
    }
)


def _recorded_shape_counts(
    review_evidence: str | None,
) -> tuple[dict[str, int] | None, dict[str, int] | None]:
    """The per-shape counts the Go writer put in a receipt's provenance.

    The writer's provenance is a JSON object (``ReceiptProvenance``,
    internal/goapiproof/run.go); every field on it -- including
    ``covered_by_shape``/``outside_by_shape`` themselves -- is
    ``omitempty``. A MATCH receipt has no findings to shape-classify, so
    both shape keys are legitimately absent while other fields
    (``measurement_route`` at least) are always present -- that object is
    still recognisably the writer's, and its counts read as "nothing of
    that kind" (``{}``), not unrecorded. Recognised means the payload has
    ANY of the writer's own field names, not only the two shape keys --
    checking only those two treated a genuine MATCH receipt's provenance
    the same as an operator's unrelated JSON, both missing both shape
    keys, and both rendered "unrecorded" instead of "covered[] outside[]"
    (a regression the e2e harness's own case for a match receipt catches:
    a match admission must print its shape counts too, not only a cited
    mismatch's). A payload with NONE of the writer's field names (``{}``, an
    operator's own JSON, or free text that is not JSON at all) is
    "unrecorded" (``None``), and so is a key that IS present but is not
    the writer's shape: not a JSON object, or holding a non-integer (bool
    included) count -- a malformed key invalidates the whole object
    rather than being read as "nothing of that kind" (F3, CHAOS-5484
    opus-r9). Counts are only ever integers.
    """
    try:
        payload = json.loads(review_evidence) if review_evidence else None
    except ValueError:
        return None, None
    if not isinstance(payload, dict):
        return None, None
    if payload.keys().isdisjoint(_RECEIPT_PROVENANCE_FIELDS):
        return None, None

    def counts(key: str) -> dict[str, int] | None:
        if key not in payload:
            return {}
        value = payload[key]
        if not isinstance(value, dict):
            return None
        result: dict[str, int] = {}
        for name, number in value.items():
            if not isinstance(number, int) or isinstance(number, bool):
                return None
            result[str(name)] = number
        return result

    covered, outside = counts("covered_by_shape"), counts("outside_by_shape")
    if covered is None or outside is None:
        return None, None
    return covered, outside


def build_enablement_receipt_select(
    *,
    schema_digest: str,
    candidate_build: str,
    operations: Mapping[str, str],
    target_mode: str,
) -> Select[Any]:
    """The newest admissible receipt per operation, under the SAME rule.

    ``DISTINCT ON (selected_operation)`` ordered by ``observed_at`` DESC
    then ``id``, so the receipt named is deterministic and is the latest
    measurement that satisfies every clause.
    """
    if target_mode not in ENABLEMENT_TARGET_MODES:
        raise ValueError(
            f"build_enablement_receipt_select: unknown target mode {target_mode!r} "
            f"-- expected one of {', '.join(ENABLEMENT_TARGET_MODES)}"
        )
    return (
        select(
            ProofRun.selected_operation,
            ProofRun.id,
            ProofRun.terminal_state,
            ProofRun.baseline_defect,
            ProofRun.measurement_route,
            ProofRun.build_binding,
            ProofRun.observed_at,
            ProofRun.review_evidence,
        )
        .where(
            *_enablement_proof_conditions(
                schema_digest=schema_digest,
                candidate_build=candidate_build,
                operations=operations,
                target_mode=target_mode,
            )
        )
        .order_by(
            ProofRun.selected_operation,
            ProofRun.observed_at.desc(),
            ProofRun.id,
        )
        .distinct(ProofRun.selected_operation)
    )


async def enablement_receipts(
    session: AsyncSession,
    *,
    schema_digest: str,
    candidate_build: str,
    operations: Mapping[str, str],
    target_mode: str,
) -> dict[str, AuthorizingReceipt]:
    """``{operation: the receipt that authorizes it}`` -- the proven set
    :func:`operations_with_enablement_proof` returns, plus WHICH receipt."""
    if not operations:
        return {}
    result = await session.execute(
        build_enablement_receipt_select(
            schema_digest=schema_digest,
            candidate_build=candidate_build,
            operations=operations,
            target_mode=target_mode,
        )
    )
    receipts: dict[str, AuthorizingReceipt] = {}
    for row in result.all():
        covered, outside = _recorded_shape_counts(row[7])
        receipts[row[0]] = AuthorizingReceipt(
            operation=row[0],
            receipt_id=str(row[1]),
            terminal_state=row[2],
            baseline_defect=tuple(row[3] or ()),
            measurement_route=row[4],
            build_binding=row[5],
            observed_at=row[6],
            covered_by_shape=covered,
            outside_by_shape=outside,
        )
    return receipts


async def count_rows_by_schema_digest(session: AsyncSession) -> dict[str, int]:
    """``{schema_digest: row_count}`` over the whole routing table.

    The startup drift check's single query. Deliberately unfiltered and
    un-paginated: the table holds one row per (schema version, document,
    operation) triple -- tens of rows, not a scan risk -- and the whole
    point is to see the digests nobody asked about, including the dead
    ones.
    """
    result = await session.execute(
        select(RoutingState.schema_digest, func.count())
        .select_from(RoutingState)
        .group_by(RoutingState.schema_digest)
    )
    return {digest: count for digest, count in result.all()}


async def routing_status_rows(
    session: AsyncSession,
    *,
    live_schema_digest: str,
    catalog: Sequence[tuple[str, str]],
) -> list[OperationStatus]:
    """Per registered operation: its row at the live digest, or why not.

    ``catalog`` is ``[(operation, document_digest), ...]`` from
    ``go_api_operations.json`` -- the registrydump-generated inventory,
    never a hand list. Reporting is driven by the CATALOG rather than by
    the table so an operation that has no row at all is reported as
    ``MISSING`` instead of simply being absent from the output; "nothing
    printed" is exactly how the six-day outage stayed invisible.

    Rows in the table for operations NOT in the catalog are not reported
    here -- they cannot be dispatched (the edge resolves a request to an
    operation via the catalog), so they are stale by construction. The
    per-digest totals from :func:`count_rows_by_schema_digest` are what
    surface those.
    """
    result = await session.execute(select(RoutingState))
    rows = list(result.scalars().all())

    # Trap #120: go_api_routing_state is keyed by (schema_digest,
    # document_digest, selected_operation). A map keyed on the operation
    # ALONE collapses two document versions under one schema onto
    # whichever row the scan returned last -- nondeterministically, so the
    # answer changes between runs against the same data, and status
    # reported the wrong row's mode.
    live_by_document: dict[tuple[str, str], RoutingState] = {}
    live_documents_by_operation: dict[str, set[str]] = {}
    stale_digests_by_operation: dict[str, set[str]] = {}
    for row in rows:
        if row.schema_digest == live_schema_digest:
            live_by_document[(row.selected_operation, row.document_digest)] = row
            live_documents_by_operation.setdefault(row.selected_operation, set()).add(
                row.document_digest
            )
        else:
            stale_digests_by_operation.setdefault(row.selected_operation, set()).add(
                row.schema_digest
            )

    # Grouped by (candidate build, the mode's own route rule), because
    # CHAOS-5484 made admissibility depend on where the row IS. A row at
    # `primary` is serving real traffic and needs EDGE evidence; a row
    # anywhere else is admissible on any recorded route. Reporting every
    # row against the laxer rule would mark a primary row PROVEN on
    # measurement-only evidence -- the exact claim the split exists to
    # stop -- and reporting every row against the stricter one would mark
    # a legitimately-proven shadow row UNPROVEN, since a shadow operation
    # cannot be measured on the edge at all.
    # Trap #120 again, and the reason it kept recurring: the grouping is a
    # SECOND keyed map, and re-keying only the live-row map above left it
    # collapsing. `grouped[...][operation] = document` keys the inner dict
    # by operation, so two documents sharing a (build, target mode) lost
    # one of them; and `operations_with_enablement_proof` returns operation
    # NAMES, so proof found for either document marked BOTH proven:
    # `routing status --json` exited 0 reporting
    # "proven": true for a catalog document with no proof of its own,
    # borrowed from a drifted row at a different build and target mode.
    #
    # The proof key is (schema_digest, document_digest, selected_operation,
    # candidate_build) plus the target mode. It is carried WHOLE from here
    # to the lookup and back: the result set below is keyed by the same
    # (operation, document) pair the query asked about, never by name.
    proven: set[tuple[str, str]] = set()
    # Keyed by (build, target mode, DOCUMENT): the document was found
    # missing from this key while the two downstream halves were already
    # fixed: `grouped[...][operation] = document` collapsed two live
    # documents of one operation that shared a build and a mode, BEFORE
    # the query was built, so only one was ever asked about -- and which
    # one survived was dictionary insertion order, i.e. the Postgres scan
    # order. Executed through the CLI, the catalog's document reported
    # `proven: False` with its own receipt sitting in go_api_proof_run.
    #
    # `operations_with_enablement_proof` takes a mapping of operation to
    # document, so one call can only ever ask about ONE document per
    # operation. The document therefore belongs in the GROUP key, not in
    # the value: a map cannot hold the pair the query needs to distinguish.
    # That is why re-keying the other two maps was not enough.
    grouped: dict[tuple[str, str, str], dict[str, str]] = {}
    for (operation, document), row in live_by_document.items():
        target_mode = (
            ENABLEMENT_TARGET_MODE_EDGE_ONLY
            if row.mode == ENABLEMENT_TARGET_MODE_EDGE_ONLY
            else ENABLEMENT_TARGET_MODE_ANY_ROUTE
        )
        # The row's OWN document_digest, not the catalog's: a row whose
        # document has drifted from the catalog must not borrow the
        # catalog's proof, and vice versa.
        grouped.setdefault((row.current_candidate_build, target_mode, document), {})[
            operation
        ] = document
    for (build, target_mode, _document), operations in sorted(grouped.items()):
        found = await operations_with_enablement_proof(
            session,
            schema_digest=live_schema_digest,
            candidate_build=build,
            operations=operations,
            target_mode=target_mode,
        )
        # Re-pair each returned name with the document THIS group asked
        # about, so a name can never carry proof across a document, a
        # build or a target mode.
        proven |= {(operation, operations[operation]) for operation in found}

    # Every document the catalog registers per operation. A drifted row is a
    # live row at a document the catalog does not name AT ALL -- subtracting
    # only the current entry's digest made each of two registered documents
    # flag the other as drift.
    catalog_documents: dict[str, set[str]] = {}
    for operation, document_digest in catalog:
        catalog_documents.setdefault(operation, set()).add(document_digest)

    statuses: list[OperationStatus] = []
    reported_drift: set[tuple[str, str]] = set()
    for operation, document_digest in catalog:
        # The catalog's EXACT document, never whichever row carries this
        # operation name.
        live_row = live_by_document.get((operation, document_digest))
        stale = tuple(sorted(stale_digests_by_operation.get(operation, ())))
        if live_row is not None:
            statuses.append(
                OperationStatus(
                    operation=operation,
                    document_digest=document_digest,
                    digest_state="MATCH",
                    mode=live_row.mode,
                    current_candidate_build=live_row.current_candidate_build,
                    rollout_percentage=live_row.rollout_percentage,
                    owner=live_row.owner,
                    updated_at=live_row.updated_at,
                    stale_digests=stale,
                    proven=(operation, document_digest) in proven,
                )
            )
        elif stale:
            statuses.append(
                OperationStatus(
                    operation=operation,
                    document_digest=document_digest,
                    digest_state="STALE",
                    stale_digests=stale,
                )
            )
        else:
            statuses.append(
                OperationStatus(
                    operation=operation,
                    document_digest=document_digest,
                    digest_state="MISSING",
                )
            )

        # A live-schema row this operation has at a document the catalog
        # does NOT name gets its own row, after the catalog's. Its own
        # digest and mode are on it, because "which digest is actually
        # serving" is the question an operator has when they see this.
        #
        # Emitted per drifted document rather than as a list on the
        # catalog's row: two drifted documents are two rows in the table
        # and reading them as one would repeat, one level down, the
        # collapsing this whole change exists to stop.
        for drifted_digest in sorted(
            live_documents_by_operation.get(operation, set())
            - catalog_documents[operation]
        ):
            if (operation, drifted_digest) in reported_drift:
                continue
            reported_drift.add((operation, drifted_digest))
            drifted_row = live_by_document[(operation, drifted_digest)]
            statuses.append(
                OperationStatus(
                    operation=operation,
                    document_digest=drifted_digest,
                    digest_state="DOCUMENT_DRIFT",
                    mode=drifted_row.mode,
                    current_candidate_build=drifted_row.current_candidate_build,
                    rollout_percentage=drifted_row.rollout_percentage,
                    owner=drifted_row.owner,
                    updated_at=drifted_row.updated_at,
                    proven=(operation, drifted_digest) in proven,
                )
            )

    # A live row for an operation the catalog does not register at all
    # (renamed or retired) cannot be dispatched either -- the edge resolves
    # a request to an operation THROUGH the catalog -- and iterating the
    # catalog named it nowhere, while the migration page named it.
    # Its own state, its own row, after the catalog's.
    for operation, document_digest in sorted(live_by_document):
        if operation in catalog_documents:
            continue
        row = live_by_document[(operation, document_digest)]
        statuses.append(
            OperationStatus(
                operation=operation,
                document_digest=document_digest,
                digest_state="UNREGISTERED",
                mode=row.mode,
                current_candidate_build=row.current_candidate_build,
                rollout_percentage=row.rollout_percentage,
                owner=row.owner,
                updated_at=row.updated_at,
                proven=(operation, document_digest) in proven,
            )
        )
    return statuses
