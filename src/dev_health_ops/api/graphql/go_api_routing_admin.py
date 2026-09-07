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

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.go_api_registry import ProofRun, RoutingState

from .go_api_registry import register_candidate_build

logger = logging.getLogger(__name__)

__all__ = [
    "ENABLEMENT_PROOF_STAGE",
    "ENABLEMENT_PROOF_TERMINAL_STATE",
    "OperationStatus",
    "count_rows_by_schema_digest",
    "enable_operation",
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

#: Only a ``match`` counts. A ``deployed_executed`` proof run that
#: terminated in ``mismatch``/``timeout``/``fallback`` is evidence the
#: operation is NOT ready -- treating any terminal state as "proof exists"
#: would make the gate worse than absent, because it would look enforced.
ENABLEMENT_PROOF_TERMINAL_STATE = "match"


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
    )


async def operations_with_enablement_proof(
    session: AsyncSession,
    *,
    schema_digest: str,
    candidate_build: str,
    operations: Iterable[str],
) -> frozenset[str]:
    """Which of ``operations`` have a ``deployed_executed``/``match``
    proof run for EXACTLY this ``(schema_digest, candidate_build)``.

    Scoped to the exact tuple on purpose: plan section 8.3's rule is that
    a proof is evidence for one immutable 4-column key and is "never
    carried forward across any of the four changing". A proof recorded
    against an older build, or against the SDL as it was before a schema
    move, says nothing about the build being enabled now -- which is the
    entire lesson of the digest move this command exists to recover from.
    """
    wanted = list(operations)
    if not wanted:
        return frozenset()
    result = await session.execute(
        select(ProofRun.selected_operation)
        .where(
            ProofRun.schema_digest == schema_digest,
            ProofRun.candidate_build == candidate_build,
            ProofRun.selected_operation.in_(wanted),
            ProofRun.stage == ENABLEMENT_PROOF_STAGE,
            ProofRun.terminal_state == ENABLEMENT_PROOF_TERMINAL_STATE,
        )
        .distinct()
    )
    return frozenset(result.scalars().all())


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

    live_by_operation: dict[str, RoutingState] = {}
    stale_digests_by_operation: dict[str, set[str]] = {}
    for row in rows:
        if row.schema_digest == live_schema_digest:
            live_by_operation[row.selected_operation] = row
        else:
            stale_digests_by_operation.setdefault(row.selected_operation, set()).add(
                row.schema_digest
            )

    live_builds = {row.current_candidate_build for row in live_by_operation.values()}
    proven: frozenset[str] = frozenset()
    for build in sorted(live_builds):
        proven |= await operations_with_enablement_proof(
            session,
            schema_digest=live_schema_digest,
            candidate_build=build,
            operations=[
                operation
                for operation, row in live_by_operation.items()
                if row.current_candidate_build == build
            ],
        )

    statuses: list[OperationStatus] = []
    for operation, document_digest in catalog:
        live_row = live_by_operation.get(operation)
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
                    proven=operation in proven,
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
    return statuses
