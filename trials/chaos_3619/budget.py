"""The per-case wall clock. This is the CHAOS-3625 fix.

CHAOS-3625: ``InvestigationPacketProducer`` is a synchronous Protocol, so a
blocking producer stalls the event loop. The issue names two fix shapes and
leaves the choice to trial-infrastructure time:

  (a) an async Protocol with a bounded await;
  (b) a post-producer wall-clock budget that abandons the shadow record past
      the deadline, recording the abandonment and never falling silent.

**This trial implements (b), and the reasoning is worth stating because the
choice is not obvious.** (a) reshapes the Protocol CHAOS-3618 froze, which
means touching the seam contract, the orchestrator wiring and the native
producer for a defect whose only merged exposure is a producer nothing
constructs in production — both seam flags default off, and the seam call
already sits after ``terminal()`` persistence so a slow producer delays a
response and never durability. (b) changes no contract, and the trial runner
needs its own per-case wall clock regardless: an arm that hangs on case 17
must not silently consume the whole trial.

The honest limitation, stated rather than glossed: a wall-clock check
**after** a synchronous call cannot interrupt that call. It measures and
records; it does not preempt. Interruption genuinely requires (a), or a
process boundary. What this buys is that a slow producer is *recorded as
slow* with its own disposition instead of being indistinguishable from a
fast one, and that a single hung case cannot masquerade as a scored one.
:func:`enforce` therefore never claims to have stopped anything.

The trial runner additionally caps total wall clock, because "each case
finished within its own budget" and "the sweep finished" are different
claims and a 39-case sweep of individually-acceptable slow cases is still a
sweep nobody waits for.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

__all__ = [
    "DEFAULT_PER_CASE_TIMEOUT_SECONDS",
    "BudgetOutcome",
    "enforce",
]

#: Generous on purpose. This is a hang detector, not a performance target:
#: a bound tight enough to catch a slow case would turn latency measurement
#: into a pass/fail gate, and the trial reports latency rather than judging
#: it. Overridable per run and recorded in the artifact binding, so a result
#: set always says which bound produced it.
DEFAULT_PER_CASE_TIMEOUT_SECONDS = 120.0


@dataclass(frozen=True, slots=True)
class BudgetOutcome:
    """How one bounded call ended.

    ``value`` is present only when ``exceeded`` is false, and that is the
    invariant the whole module exists to keep: a caller cannot accidentally
    use the result of a call that blew its budget, because there is nothing
    to use.
    """

    elapsed_seconds: float
    limit_seconds: float
    exceeded: bool
    value: object | None = None
    fault: BaseException | None = None

    @property
    def detail(self) -> str:
        if self.exceeded:
            return (
                f"the arm returned after {self.elapsed_seconds:.3f}s, past the "
                f"{self.limit_seconds:.1f}s per-case budget; recorded as NOT RUN "
                "and not retried -- the first honest result stands"
            )
        if self.fault is not None:
            return (
                f"{type(self.fault).__name__}: {self.fault} "
                f"(after {self.elapsed_seconds:.3f}s)"
            )
        return f"completed in {self.elapsed_seconds:.3f}s"


def enforce(
    call: Callable[[], object],
    *,
    limit_seconds: float = DEFAULT_PER_CASE_TIMEOUT_SECONDS,
    clock: Callable[[], float] = time.monotonic,
) -> BudgetOutcome:
    """Run ``call``, time it, and report whether it blew its budget.

    Does **not** interrupt: see the module docstring. A synchronous callable
    cannot be preempted from the calling thread, and pretending otherwise --
    by naming this ``timeout`` or by returning a value alongside
    ``exceeded`` -- would be a stronger claim than the mechanism supports.

    Exceptions are caught and returned rather than propagated, so one arm
    faulting on one case cannot end the sweep. The runner turns a fault into
    its own disposition; a sweep that aborted on the first raise would lose
    every case after it, which is the silent coverage loss this trial's own
    predecessor was caught by.

    ``clock`` is injected so the budget's behaviour can be tested without
    sleeping. A test that sleeps to prove a timeout is a test that is slow
    and flaky in exactly the environments that matter.
    """

    if limit_seconds <= 0:
        raise ValueError(
            f"a per-case budget must be positive; got {limit_seconds!r}. A "
            "zero or negative budget would mark every case NOT RUN and the "
            "sweep would report a clean absence of measurement"
        )
    started = clock()
    value: object | None = None
    fault: BaseException | None = None
    try:
        value = call()
    except Exception as raised:  # noqa: BLE001 - deliberately total
        fault = raised
    elapsed = clock() - started
    exceeded = elapsed > limit_seconds
    return BudgetOutcome(
        elapsed_seconds=elapsed,
        limit_seconds=limit_seconds,
        exceeded=exceeded,
        # Withheld past the deadline, so a late value cannot be scored as if
        # it had arrived in time.
        value=None if exceeded else value,
        fault=None if exceeded else fault,
    )
