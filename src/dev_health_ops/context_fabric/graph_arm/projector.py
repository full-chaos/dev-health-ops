"""CHAOS-3676: bounded retry, failure classification around a projection write.

``GraphArmStore.write_projection`` had no retry and no failure
classification: a transient failure (an unreachable/timed-out backend, per
CHAOS-3631's :class:`~.store.GraphOperationTimeoutError`) and a permanent one
(a malformed batch, an over-budget embedding run, a wrong-organization
projection, the projection flag being off) both just propagated on the first
attempt. :func:`project_with_retry` is the driver that sits between a future
worker task and the store.

**Idempotency is not re-proven here.** It is established one layer down, at
the Graphiti/FalkorDB write itself: every node and edge is written via Cypher
``MERGE`` keyed on a uuid :mod:`.identity` derives deterministically from the
canonical id, so writing the same projection twice re-sets identical
properties rather than duplicating anything. That is what makes retrying a
write that may have partially succeeded safe in the first place — a retry
after a mid-write timeout is a repeat of the same upsert, not a duplicate.

**What this module does not do**, named explicitly because CHAOS-3500's
required scope is broader than this one issue: it does not enqueue or
dequeue a ``worker_job_outbox`` row, does not register a Celery task, and
does not decide where a "project this organization's canonical delta" job
comes from. Those are separate, already-flagged follow-up work. What this
module produces — a structured :class:`ProjectionOutcome` distinguishing
success, exhausted-transient-retry and permanent failure — is exactly the
shape a future worker task needs to decide ``self.retry(...)`` versus
writing a ``worker_job_outbox`` row to ``dead``, without that task having to
inspect exception types itself.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from .budgets import DEFAULT_BUDGETS, TrialBudgets
from .projection import GraphProjection, ProjectionError, build_projection
from .records import IngestionBatch
from .store import (
    EmbeddingBudgetExceededError,
    ProjectionDisabledError,
    StoreUnavailableError,
    WriteResult,
)

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_RETRY_BACKOFF_S",
    "ProjectingStore",
    "ProjectionFailureClass",
    "ProjectionOutcome",
    "project_with_retry",
]

#: How many total attempts a transient failure gets before the driver reports
#: exhaustion. Small and fixed by default: a caller that wants more has to
#: say so, and CHAOS-3631 requires that retries "do not amplify load against
#: an already-struggling backend" -- a large default would violate that on
#: its own.
DEFAULT_MAX_ATTEMPTS = 3

#: Base backoff between retries, in seconds. Doubles per attempt (capped),
#: so total wall-clock across an exhausted run stays small and predictable.
DEFAULT_RETRY_BACKOFF_S = 1.0

#: The ceiling a single backoff sleep may reach, however many attempts are
#: configured. Without a cap, a caller passing a large ``max_attempts`` would
#: also get exponentially large individual sleeps -- bounding attempts alone
#: does not bound the wait between them.
_MAX_SINGLE_BACKOFF_S = 8.0

#: Exceptions that mean "this specific attempt could not be judged" rather
#: than "this projection is invalid" -- retryable up to the bound.
#: ``GraphOperationTimeoutError`` (CHAOS-3631) is a subclass and is covered
#: by this catch.
_TRANSIENT_EXCEPTIONS: tuple[type[Exception], ...] = (StoreUnavailableError,)

#: Exceptions that mean "retrying the identical call cannot help" -- refused
#: immediately, on the first and only attempt.
_PERMANENT_EXCEPTIONS: tuple[type[Exception], ...] = (
    EmbeddingBudgetExceededError,
    ProjectionDisabledError,
    PermissionError,
)


class ProjectingStore(Protocol):
    """The one method :func:`project_with_retry` needs from a store.

    A ``Protocol`` rather than importing ``GraphArmStore`` as the parameter
    type: tests exercise the retry/classification logic against fakes that
    implement exactly this method, and structural typing keeps the driver
    honest about how little of the store it actually depends on.
    """

    async def write_projection(
        self, projection: GraphProjection, *, budgets: TrialBudgets = DEFAULT_BUDGETS
    ) -> WriteResult: ...


class ProjectionFailureClass(StrEnum):
    """Why a :class:`ProjectionOutcome` failed, for a caller to act on.

    The two members map directly onto what a future ``worker_job_outbox``
    consumer needs to decide: ``TRANSIENT_EXHAUSTED`` means "retry again
    later, via the outbox's own backoff" (this driver already tried and
    could not get through); ``PERMANENT`` means "stop retrying, mark the
    outbox row dead" -- retrying an identical call cannot change the
    outcome.
    """

    TRANSIENT_EXHAUSTED = "transient_exhausted"
    PERMANENT = "permanent"


@dataclass(frozen=True, slots=True)
class ProjectionOutcome:
    """The result of one :func:`project_with_retry` call.

    Exactly one of "success" and "failure" shape is valid, enforced in
    ``__post_init__`` rather than left to convention: a caller must be able
    to trust that ``write_result is not None`` if and only if
    ``success is True``, without also checking ``failure_class``.
    """

    success: bool
    attempts: int
    write_result: WriteResult | None = None
    failure_class: ProjectionFailureClass | None = None
    failure_detail: str | None = None

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError(
                f"a projection outcome reporting {self.attempts} attempts is "
                "incoherent; project_with_retry always makes at least one"
            )
        if self.success:
            if self.write_result is None:
                raise ValueError(
                    "a successful outcome must carry a write_result -- a "
                    "success with nothing written is not distinguishable "
                    "from a caller-side bug that forgot to check"
                )
            if self.failure_class is not None or self.failure_detail is not None:
                raise ValueError(
                    "a successful outcome must carry no failure detail; "
                    "carrying both would leave a reader to guess which one "
                    "is authoritative"
                )
        else:
            if self.write_result is not None:
                raise ValueError(
                    "a failed outcome must carry no write_result -- nothing "
                    "was durably written on the attempt this outcome reports"
                )
            if self.failure_class is None or self.failure_detail is None:
                raise ValueError(
                    "a failed outcome must carry a failure_class and a "
                    "failure_detail; a failure with neither tells a caller "
                    "nothing to act on"
                )


def _detail(operation: str, attempts: int, exc: Exception) -> str:
    """A content-safe description of a failure. Never the exception's text.

    Deliberately excludes ``str(exc)``. ``ProjectionError`` messages can
    embed a source-supplied display label (two conflicting labels for one
    canonical id, for instance), and this detail is meant to be logged and
    carried on an outcome a worker task persists -- so it is built from a
    fixed template, the operation name this module chose, the attempt count,
    and the exception's TYPE name only. None of those can carry entity
    content, whatever the exception's own message says.
    """

    detail = (
        f"graph projection operation {operation!r} failed on attempt "
        f"{attempts} with {type(exc).__name__}"
    )
    refusal = getattr(exc, "refusal", None)
    if refusal is not None:
        # ``IdentifierRefusal.safe_detail`` is a closed-vocabulary diagnostic;
        # never append ``str(exc)`` because the rejected source value must not
        # become worker telemetry.
        detail = f"{detail}: {refusal.safe_detail()}"
    return detail


async def project_with_retry(
    store: ProjectingStore,
    batch: IngestionBatch,
    *,
    budgets: TrialBudgets = DEFAULT_BUDGETS,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_s: float = DEFAULT_RETRY_BACKOFF_S,
    sleep: Callable[[float], Awaitable[None]] | None = None,
) -> ProjectionOutcome:
    """Project ``batch`` onto ``store``, retrying only transient write failures.

    Two phases, and only the second retries:

    1. :func:`~.projection.build_projection` runs exactly once. It is pure
       and deterministic -- the same batch always yields the same
       :class:`~.projection.GraphProjection` or the same
       :class:`~.projection.ProjectionError` -- so retrying it could never
       change the outcome. A ``ProjectionError`` here is reported as
       ``PERMANENT`` with ``attempts=1``; the store is never called.
    2. ``store.write_projection`` is attempted up to ``max_attempts`` times.
       :data:`_TRANSIENT_EXCEPTIONS` (backend unreachable/timed out) are
       retried with capped exponential backoff between attempts, never
       after the last one. :data:`_PERMANENT_EXCEPTIONS` (over budget, the
       projection flag off, a wrong-organization write) stop the loop on
       the attempt that raised them -- retrying an identical call cannot
       change a permanent refusal.

    Any exception outside both classified sets propagates unchanged, on the
    attempt it was raised. This driver retries exactly the failure shapes it
    has a stated reason to believe are transient; guessing about an
    unclassified exception (retrying it, or silently reclassifying it) would
    risk masking a real defect behind an apparent retry success.
    """

    sleeper = sleep or _default_sleep()

    try:
        projection = build_projection(batch, budgets=budgets)
    except ProjectionError as exc:
        detail = _detail("build_projection", 1, exc)
        logger.warning("%s", detail)
        return ProjectionOutcome(
            success=False,
            attempts=1,
            failure_class=ProjectionFailureClass.PERMANENT,
            failure_detail=detail,
        )

    attempts = 0
    last_detail: str | None = None
    while attempts < max_attempts:
        attempts += 1
        try:
            result = await store.write_projection(projection, budgets=budgets)
        except _PERMANENT_EXCEPTIONS as exc:
            detail = _detail("write_projection", attempts, exc)
            logger.warning("%s", detail)
            return ProjectionOutcome(
                success=False,
                attempts=attempts,
                failure_class=ProjectionFailureClass.PERMANENT,
                failure_detail=detail,
            )
        except _TRANSIENT_EXCEPTIONS as exc:
            last_detail = _detail("write_projection", attempts, exc)
            if attempts >= max_attempts:
                break
            backoff = min(backoff_s * (2 ** (attempts - 1)), _MAX_SINGLE_BACKOFF_S)
            logger.warning(
                "%s; retrying (attempt %d/%d) after %.1fs",
                last_detail,
                attempts + 1,
                max_attempts,
                backoff,
            )
            await sleeper(backoff)
            continue
        else:
            return ProjectionOutcome(
                success=True, attempts=attempts, write_result=result
            )

    assert last_detail is not None  # every path to here set it
    return ProjectionOutcome(
        success=False,
        attempts=attempts,
        failure_class=ProjectionFailureClass.TRANSIENT_EXHAUSTED,
        failure_detail=last_detail,
    )


def _default_sleep() -> Callable[[float], Awaitable[None]]:
    """Deferred import of ``asyncio.sleep`` as the default backoff sleeper.

    Deferred rather than imported at module scope purely to keep the
    parameter's default expression simple to read; there is no lazy-import
    concern here the way there is for Graphiti.
    """

    import asyncio

    return asyncio.sleep
