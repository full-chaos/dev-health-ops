"""Valkey-backed counters for the Ask Dev platform monthly allowance (CHAOS-3522).

WHY. ``dev_runs`` used to be re-scanned (``count() + sum(case ...)``) on
every single admission check AND every admin usage read, for the whole
current-month window. The window only grows over the month, so this was a
constant, ever-growing aggregate query for what is really just two counters
per org per month. Counters belong in Valkey, the same way the impersonation
cache (``impersonation_cache.py``) and slowapi's own rate-limit buckets
(``middleware/rate_limit.py``) already keep shared, cross-replica state
there instead of re-deriving it from Postgres on every request.

``dev_runs`` remains the single source of truth. The counter is a cache of
an aggregate over it, kept current by two write points (admission reserves
worst-case; finalization reconciles down/up to the real cost) and healed
from ``dev_runs`` whenever it might be wrong (cold key, Valkey outage
recovery, or an explicit operator reconcile).

NO LUA. The original design used a Lua ``EVAL`` script for atomic
check-and-increment. Verified directly: this repo's ``fakeredis`` supports
no ``EVAL`` at all (sync or async client), the same gap
``tests/test_token_pool.py`` already works around with a
``_fakeredis_supports_lua()`` probe, and CI provisions no live Valkey
service for the unit tier -- a Lua-based admit would be permanently
skip-gated in CI, never actually exercised. Every atomic step here instead
uses a single Valkey command (``HINCRBY``, ``HSETNX``), each atomic on its
own, composed with explicit compensation on the reject path. Algebraically
this produces the identical accept/reject boundary as the old SQL query:
``new_requests > limit`` iff ``existing_requests >= limit``, and
``new_cost > limit`` iff ``existing_cost + reservation > limit`` (the exact
SQL predicates). The only cost is a narrow window where a concurrent
rejected admission could transiently inflate the visible count before its
own compensating HINCRBY lands -- self-correcting within microseconds, and
the failure direction is conservative (a spurious reject under heavy
contention at the exact boundary, never a spurious admit).

KNOWN LIMITATION, stated rather than left for an operator to discover
(CHAOS-3522 review round 2): admit()'s compensation is two sequential
Valkey commands (the speculative HINCRBY, then the compensating HINCRBY on
rejection), not one atomic step. A process crash or network partition
between them -- not an ordinary Valkey error, which the try/except here
already catches and compensates for -- leaves the counter with a permanent
CONSERVATIVE overcount: never an undercount, so it can only ever cause a
spurious future rejection, not a spurious admit. Rare (it requires the
process to die mid-function, not merely a request to fail), but not
impossible, and the counter does not self-heal it -- the remedy is the
explicit operator reconcile (POST .../platform-allowance/reconcile,
force_reconcile() below), which recomputes from dev_runs and overwrites
whatever drift accumulated.

FAILURE POLICY mirrors impersonation_cache.py: fail-correct, not fail-stale.
A Valkey error trips a short circuit breaker; while it is open, every
operation computes straight from ``dev_runs`` and does not attempt to write
the counter (there is nothing trustworthy to write into). The first
operation after the breaker closes forces one recompute-from-``dev_runs``
for whichever org happens to touch it first, before resuming counter mode
for that org -- STATED LIMITATION: this clears staleness for that one org
in THIS process only; other orgs, and other processes that had their own
open breaker, remain on their own stale counter (undercounting, which for
a cost cap is over-admission risk) until their own next cold-key heal, their
own first post-recovery touch, or an explicit operator reconcile. There is
no cross-process signal that "the outage is over, everyone should recompute
now" -- only a per-process, per-org, first-touch trigger.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dev.org_policy import TERMINAL_RUN_STATES, platform_month_window
from dev_health_ops.models.dev_persistence import DevRun

logger = logging.getLogger(__name__)

_KEY_PREFIX = "askdev:allowance:"

# Grace past reset_at before the key's TTL lapses -- generous so a slow
# clock, a delayed sweep, or an org that touches the boundary right at
# month-end never loses its own-month data early. The NEXT month gets its
# own, separate key regardless (the suffix embeds YYYY-MM), so this is not
# "how long until reset" -- it is "how long an abandoned key survives".
_TTL_GRACE_SECONDS = 5 * 24 * 3600

# Circuit breaker: after a Valkey error, skip the counter store for a short
# window instead of paying a connect timeout on every request.
_CIRCUIT_SECONDS = 5.0
_circuit_open_until = 0.0

# Amendment 3: set whenever the breaker trips: the next successful operation
# for ANY org must force a recompute-from-dev_runs for THAT org before
# trusting the counter, since admissions during the outage went through the
# SQL-only fallback and never touched Valkey. Consumed (cleared) by that one
# operation -- see module docstring for the stated cross-org/cross-process
# limitation.
_needs_recovery_recompute = False

_client: Any | None = None


class AdmitOutcome(Enum):
    ADMITTED = "admitted"
    REQUEST_LIMIT_EXCEEDED = "request_limit_exceeded"
    COST_LIMIT_EXCEEDED = "cost_limit_exceeded"


@dataclass(frozen=True, slots=True)
class AllowanceCounts:
    requests: int
    cost_microusd: int


def _key(org_id: str, window_start: datetime) -> str:
    # CHAOS-3522 amendment 4: the suffix is derived from the SAME
    # platform_month_window() the SQL fallback and the admin read both call
    # -- never an independently formatted "%Y-%m" that could silently
    # diverge if the window function ever stops being calendar-month-UTC.
    return f"{_KEY_PREFIX}{org_id}:{window_start.year:04d}-{window_start.month:02d}"


def _ttl_seconds(reset_at: datetime, now: datetime) -> int:
    remaining = (reset_at - now).total_seconds() + _TTL_GRACE_SECONDS
    return max(1, int(remaining))


def _get_client() -> Any | None:
    """Lazily create the shared Valkey client; None when unconfigured."""
    global _client
    if _client is not None:
        return _client
    redis_url = os.getenv("REDIS_URL", "")
    if not redis_url:
        return None
    import valkey.asyncio as aioredis

    _client = aioredis.from_url(
        redis_url,
        socket_connect_timeout=0.5,
        socket_timeout=0.5,
    )
    return _client


def _circuit_is_open() -> bool:
    return time.monotonic() < _circuit_open_until


def _trip_circuit(exc: Exception) -> None:
    global _circuit_open_until, _needs_recovery_recompute
    _circuit_open_until = time.monotonic() + _CIRCUIT_SECONDS
    _needs_recovery_recompute = True
    logger.warning(
        "Valkey ask-dev allowance counters unavailable, bypassing for %.0fs: %s",
        _CIRCUIT_SECONDS,
        exc,
    )


def _consume_recovery_recompute() -> bool:
    """True exactly once per trip -> recover cycle; False otherwise."""
    global _needs_recovery_recompute
    if _needs_recovery_recompute:
        _needs_recovery_recompute = False
        return True
    return False


async def _recompute_from_dev_runs(
    session: AsyncSession,
    *,
    org_id: str,
    window_start: datetime,
    reset_at: datetime,
    per_run_reservation_microusd: int,
) -> AllowanceCounts:
    """The single aggregate query over ``dev_runs`` -- ground truth.

    Used for the Valkey-down fallback, lazy cold-key init, breaker-recovery
    recompute, and the explicit operator reconcile. One definition, so the
    "what counts as charged" logic (terminal -> the real cost, zero if none
    was ever recorded; non-terminal -> the worst-case reservation, since the
    run may still bill up to the per-call estimate) cannot drift between call
    sites the way it had before this module existed (ask_dev.py and
    persistence/service.py each had their own copy).

    CHAOS-3573: a terminal run's ``estimated_cost_microusd`` is NULL only
    when it never dispatched a single provider call --
    ``ProviderBudget.require()`` (``orchestrator.py``) always turns it into a
    concrete int the instant a call is reserved, before dispatch, and never
    back to None afterward, so every terminal write's cost column is exactly
    the real, fully-accounted spend for that run at the moment it reached
    ``finish()``. A terminal-but-NULL run is therefore a genuine
    zero-cost run (a preflight rejection, a disabled feature, a
    provider-unavailable 503 -- never a run that spent and lost the number).
    Charging it the worst-case reservation here would keep exactly the
    phantom spend this function exists to heal away, and would silently
    re-inflate the Valkey counter back to worst-case on the very next
    cold-key init / breaker recovery / operator reconcile after
    ``reconcile_terminal_cost`` (below) has already corrected it there --
    the two call sites must agree, or "fixed" doesn't survive a heal cycle.
    """

    charged_cost = case(
        (
            DevRun.state.in_(TERMINAL_RUN_STATES),
            func.coalesce(DevRun.estimated_cost_microusd, 0),
        ),
        else_=per_run_reservation_microusd,
    )
    statement = select(
        func.count(DevRun.id), func.coalesce(func.sum(charged_cost), 0)
    ).where(
        DevRun.org_id == uuid.UUID(org_id),
        DevRun.provider_source == "platform",
        DevRun.started_at >= window_start,
        DevRun.started_at < reset_at,
    )
    request_used, cost_used = (await session.execute(statement)).one()
    return AllowanceCounts(
        requests=int(request_used or 0), cost_microusd=int(cost_used or 0)
    )


async def _ensure_initialized(
    client: Any,
    *,
    key: str,
    ttl_seconds: int,
    baseline: AllowanceCounts,
) -> None:
    """Create the hash with a recomputed baseline iff nobody has yet -- PER
    FIELD, not via a single sentinel field gating a later ``HSET``.

    Amendment 1: two concurrent first-of-month callers both miss the key and
    both recompute a baseline from ``dev_runs``. A blind ``HSET`` from
    whichever one runs second would silently overwrite increments the first
    one's caller already applied in between -- the exact race this function
    exists to kill.

    A single ``HSETNX(key, "initialized", "1")`` sentinel gating a
    SEPARATE, later ``HSET`` of the real fields does NOT close that race: the
    sentinel write and the baseline ``HSET`` are two distinct round trips, so
    a THIRD caller (an ordinary admit, not another initializer) can observe
    the key "exist" (the sentinel field landed) before ``requests`` /
    ``cost_microusd`` do, race ahead with its own ``HINCRBY`` (which
    auto-vivifies the missing field at 0 + its own delta), and then lose that
    increment entirely the moment the sentinel-winner's baseline ``HSET``
    lands and overwrites the field outright. This was CHAOS-3522 review
    round 2's finding against the first version of this function.

    ``HSETNX`` directly on ``requests`` and ``cost_microusd`` has no such
    window: each field is independently atomic from the instant it exists,
    whichever write reaches it first -- a losing initializer's no-op, or a
    concurrent ``HINCRBY`` auto-vivifying it. There is no state in which the
    key "partially exists" from an outside caller's perspective in a way
    that can be raced.

    ORDERING INVARIANT this correctness depends on (also stated at every
    call site: admit() here, and append_user_message_and_run in
    persistence/service.py): a request's own init-if-absent AND its own
    admit HINCRBY must both complete BEFORE its DevRun row is inserted into
    Postgres. If a row landed first, a concurrent caller's baseline
    recompute (a live query over dev_runs) could already include that row,
    and the original request's own increment would ALSO count it -- a
    double count no amount of Valkey-side atomicity can prevent, because it
    is an ordering problem between two different stores, not a Valkey race.
    """

    await client.hsetnx(key, "requests", baseline.requests)
    await client.hsetnx(key, "cost_microusd", baseline.cost_microusd)
    await client.expire(key, ttl_seconds)


async def _read_hash(client: Any, key: str) -> AllowanceCounts | None:
    values = await client.hmget(key, ["requests", "cost_microusd"])
    requests_raw, cost_raw = values
    if requests_raw is None or cost_raw is None:
        return None
    return AllowanceCounts(requests=int(requests_raw), cost_microusd=int(cost_raw))


async def admit(
    session: AsyncSession,
    *,
    org_id: str,
    request_limit: int,
    cost_limit_microusd: int,
    per_run_reservation_microusd: int,
    now: datetime | None = None,
) -> AdmitOutcome:
    """Reserve one request + the worst-case cost, or refuse.

    Semantics are pinned exactly to the SQL version this replaces (order
    matters: a request-limit rejection is reported even when the cost limit
    is ALSO exceeded, matching persistence/service.py's original check
    order):

      reject (request) iff existing_requests >= request_limit
      reject (cost)    iff existing_cost + reservation > cost_limit

    ORDERING INVARIANT the caller must uphold (also stated in
    _ensure_initialized and at the call site in
    persistence/service.py.append_user_message_and_run): this function must
    run, and its outcome be honoured, BEFORE the corresponding DevRun row is
    inserted into Postgres. A cold-key recompute here reads dev_runs live --
    if the caller's own row had already landed, a concurrent cold-key
    recompute for the SAME org could double-count it (once from the SQL
    scan, once from this call's own increment). Admission-before-insert is
    what keeps every baseline recompute a valid prefix of the real history.
    """

    now = now or datetime.now(UTC)
    window_start, reset_at = platform_month_window(now)

    if _circuit_is_open():
        return await _admit_via_sql_only(
            session,
            org_id=org_id,
            window_start=window_start,
            reset_at=reset_at,
            request_limit=request_limit,
            cost_limit_microusd=cost_limit_microusd,
            per_run_reservation_microusd=per_run_reservation_microusd,
        )

    client = _get_client()
    if client is None:
        return await _admit_via_sql_only(
            session,
            org_id=org_id,
            window_start=window_start,
            reset_at=reset_at,
            request_limit=request_limit,
            cost_limit_microusd=cost_limit_microusd,
            per_run_reservation_microusd=per_run_reservation_microusd,
        )

    key = _key(org_id, window_start)
    try:
        if _consume_recovery_recompute():
            baseline = await _recompute_from_dev_runs(
                session,
                org_id=org_id,
                window_start=window_start,
                reset_at=reset_at,
                per_run_reservation_microusd=per_run_reservation_microusd,
            )
            await client.hset(
                key,
                mapping={
                    "requests": baseline.requests,
                    "cost_microusd": baseline.cost_microusd,
                },
            )
            await client.expire(key, _ttl_seconds(reset_at, now))
        elif await _read_hash(client, key) is None:
            baseline = await _recompute_from_dev_runs(
                session,
                org_id=org_id,
                window_start=window_start,
                reset_at=reset_at,
                per_run_reservation_microusd=per_run_reservation_microusd,
            )
            await _ensure_initialized(
                client,
                key=key,
                ttl_seconds=_ttl_seconds(reset_at, now),
                baseline=baseline,
            )

        new_requests = int(await client.hincrby(key, "requests", 1))
        if new_requests > request_limit:
            await client.hincrby(key, "requests", -1)
            return AdmitOutcome.REQUEST_LIMIT_EXCEEDED

        new_cost = int(
            await client.hincrby(key, "cost_microusd", per_run_reservation_microusd)
        )
        if new_cost > cost_limit_microusd:
            await client.hincrby(key, "cost_microusd", -per_run_reservation_microusd)
            await client.hincrby(key, "requests", -1)
            return AdmitOutcome.COST_LIMIT_EXCEEDED

        return AdmitOutcome.ADMITTED
    except Exception as exc:  # noqa: BLE001 - fail-correct: fall back to SQL
        _trip_circuit(exc)
        return await _admit_via_sql_only(
            session,
            org_id=org_id,
            window_start=window_start,
            reset_at=reset_at,
            request_limit=request_limit,
            cost_limit_microusd=cost_limit_microusd,
            per_run_reservation_microusd=per_run_reservation_microusd,
        )


async def _admit_via_sql_only(
    session: AsyncSession,
    *,
    org_id: str,
    window_start: datetime,
    reset_at: datetime,
    request_limit: int,
    cost_limit_microusd: int,
    per_run_reservation_microusd: int,
) -> AdmitOutcome:
    """The original query, unmodified in spirit: read-only, no Valkey write.

    Used whenever the counter store cannot be trusted right now (breaker
    open, unconfigured). Does not attempt to reserve into Valkey -- there is
    nothing there to correctly reserve into. This is intentionally the exact
    same admit/reject arithmetic as the counter path so a Valkey outage does
    not change enforcement, only where the read comes from.
    """

    counts = await _recompute_from_dev_runs(
        session,
        org_id=org_id,
        window_start=window_start,
        reset_at=reset_at,
        per_run_reservation_microusd=per_run_reservation_microusd,
    )
    if counts.requests >= request_limit:
        return AdmitOutcome.REQUEST_LIMIT_EXCEEDED
    if counts.cost_microusd + per_run_reservation_microusd > cost_limit_microusd:
        return AdmitOutcome.COST_LIMIT_EXCEEDED
    return AdmitOutcome.ADMITTED


async def reconcile_terminal_cost(
    session: AsyncSession,
    *,
    org_id: str,
    provider_source: str | None,
    state: str,
    estimated_cost_microusd: int | None,
    per_run_reservation_microusd: int,
    now: datetime | None = None,
) -> None:
    """Adjust the reserved worst-case down (or up) to the real cost.

    Called exactly once per run, from the SAME code path in
    ``update_run`` that performs the actual terminal-state mutation --
    never from the early-return branch that answers a repeat call on an
    already-terminal run unmutated. That placement, not an assumption about
    caller behaviour, is what makes this exactly-once (see
    test_persistence_v2.py's contract test driving a terminal transition
    twice through the real path).

    A no-op only when there is no ``provider`` this counter tracks, or the
    state is not terminal. ``estimated_cost_microusd`` being None is NOT a
    no-op (CHAOS-3573 -- reversing CHAOS-3522(b)'s original choice here):
    every terminal write's cost column comes from
    ``ProviderBudget.usage.estimated_cost_microusd``
    (``orchestrator.py``/``orchestrator_persistence.py``'s ``finish()``),
    which ``require()`` turns into a concrete int the instant a provider
    call is reserved and never resets to None afterward. NULL at a terminal
    write therefore means this run never dispatched a single provider call
    -- a preflight rejection, a disabled feature, a provider-unavailable
    503 -- genuinely zero spend, never a run that spent and lost the
    number. (A crash that loses a run's cost entirely bypasses this
    function altogether: ``force_terminal_fallback`` /
    ``recover_stale_non_terminal_run`` write ``state`` directly and never
    call ``update_run``'s terminal branch, so they never reach here --
    those stay fail-closed by construction, unaffected by this change.)
    Reconciling NULL to 0 here is what makes this function agree with
    ``_recompute_from_dev_runs``'s identical ``charged_cost`` rule above --
    the two must match, or a cold-key heal / breaker recovery / operator
    reconcile would re-inflate the counter this call just corrected.
    """

    if provider_source != "platform" or state not in TERMINAL_RUN_STATES:
        return

    now = now or datetime.now(UTC)
    window_start, _reset_at = platform_month_window(now)
    real_cost = estimated_cost_microusd if estimated_cost_microusd is not None else 0
    delta = real_cost - per_run_reservation_microusd
    if delta == 0:
        return

    client = _get_client()
    if client is None or _circuit_is_open():
        return

    key = _key(org_id, window_start)
    try:
        if not await client.exists(key):
            # Nothing to reconcile against -- the admission that reserved
            # this run's worst-case either predates this module or was
            # itself served purely via SQL fallback and never touched
            # Valkey. Do not fabricate a baseline here: a recompute at
            # finalize time races the very admission this run's own request
            # made, double counting is possible. The next lazy heal or
            # operator reconcile is the correct place to fix this, not here.
            logger.warning(
                "askdev allowance counter missing at finalize for org=%s key=%s; "
                "skipping reconcile (will heal on next cold-key read/admit or "
                "explicit reconcile)",
                org_id,
                key,
            )
            return
        await client.hincrby(key, "cost_microusd", delta)
    except Exception as exc:  # noqa: BLE001 - never block a run's terminal write
        _trip_circuit(exc)
        logger.warning(
            "askdev allowance counter reconcile failed for org=%s: %s", org_id, exc
        )


async def read_counts(
    session: AsyncSession,
    *,
    org_id: str,
    per_run_reservation_microusd: int,
    now: datetime | None = None,
) -> tuple[AllowanceCounts, datetime, datetime]:
    """Read-only counts for the admin usage surface. Returns (counts, window_start, reset_at)."""

    now = now or datetime.now(UTC)
    window_start, reset_at = platform_month_window(now)

    if _circuit_is_open():
        counts = await _recompute_from_dev_runs(
            session,
            org_id=org_id,
            window_start=window_start,
            reset_at=reset_at,
            per_run_reservation_microusd=per_run_reservation_microusd,
        )
        return counts, window_start, reset_at

    client = _get_client()
    if client is None:
        counts = await _recompute_from_dev_runs(
            session,
            org_id=org_id,
            window_start=window_start,
            reset_at=reset_at,
            per_run_reservation_microusd=per_run_reservation_microusd,
        )
        return counts, window_start, reset_at

    key = _key(org_id, window_start)
    try:
        if _consume_recovery_recompute():
            baseline = await _recompute_from_dev_runs(
                session,
                org_id=org_id,
                window_start=window_start,
                reset_at=reset_at,
                per_run_reservation_microusd=per_run_reservation_microusd,
            )
            await client.hset(
                key,
                mapping={
                    "requests": baseline.requests,
                    "cost_microusd": baseline.cost_microusd,
                },
            )
            await client.expire(key, _ttl_seconds(reset_at, now))
            return baseline, window_start, reset_at

        cached = await _read_hash(client, key)
        if cached is not None:
            return cached, window_start, reset_at

        baseline = await _recompute_from_dev_runs(
            session,
            org_id=org_id,
            window_start=window_start,
            reset_at=reset_at,
            per_run_reservation_microusd=per_run_reservation_microusd,
        )
        await _ensure_initialized(
            client, key=key, ttl_seconds=_ttl_seconds(reset_at, now), baseline=baseline
        )
        # Read back rather than trust `baseline`: a concurrent racer may have
        # won _ensure_initialized and already incremented past it.
        cached = await _read_hash(client, key)
        return (cached or baseline), window_start, reset_at
    except Exception as exc:  # noqa: BLE001 - fail-correct: fall back to SQL
        _trip_circuit(exc)
        counts = await _recompute_from_dev_runs(
            session,
            org_id=org_id,
            window_start=window_start,
            reset_at=reset_at,
            per_run_reservation_microusd=per_run_reservation_microusd,
        )
        return counts, window_start, reset_at


async def force_reconcile(
    session: AsyncSession,
    *,
    org_id: str,
    per_run_reservation_microusd: int,
    now: datetime | None = None,
) -> tuple[AllowanceCounts, datetime, datetime]:
    """Unconditionally recompute from ``dev_runs`` and overwrite the counter.

    The explicit operator escape hatch (CHAOS-3522, origin of the ticket:
    resetting a local/dev org's allowance today needs literal SQL surgery on
    ``dev_runs``). Deliberately bypasses the HSETNX race guard -- this is a
    single, intentional administrative action, not a concurrent lazy heal,
    and it is allowed to clobber whatever the counter currently holds
    because ``dev_runs`` is the source of truth being asked for directly.

    Does not raise on a Valkey error -- if the write fails, the recomputed
    value is still returned (accurate as of `now`) even though it was not
    persisted to the cache; the caller/response should surface that the
    counter is thereby stale until it next heals.
    """

    now = now or datetime.now(UTC)
    window_start, reset_at = platform_month_window(now)
    counts = await _recompute_from_dev_runs(
        session,
        org_id=org_id,
        window_start=window_start,
        reset_at=reset_at,
        per_run_reservation_microusd=per_run_reservation_microusd,
    )

    client = _get_client()
    if client is None:
        return counts, window_start, reset_at

    key = _key(org_id, window_start)
    try:
        await client.hset(
            key,
            mapping={
                "requests": counts.requests,
                "cost_microusd": counts.cost_microusd,
            },
        )
        await client.expire(key, _ttl_seconds(reset_at, now))
    except Exception as exc:  # noqa: BLE001 - return the accurate value regardless
        _trip_circuit(exc)
        logger.warning(
            "askdev allowance force_reconcile write failed for org=%s: %s", org_id, exc
        )

    return counts, window_start, reset_at
