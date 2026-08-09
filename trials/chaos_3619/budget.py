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

The honest limitation of (b), stated rather than glossed: a wall-clock check
**after** a synchronous call cannot interrupt that call. :func:`enforce`
measures and records; it does not preempt, and it never claims to have
stopped anything.

**That residual is real, and it is not accepted -- it is bounded.** The
question of whether a genuinely wedged producer can stall the sweep was
settled by inspection rather than assumed:

* the **native** arm cannot wedge on transport. ``native_arm`` imports no
  HTTP, socket, database or subprocess module, and ``projection.py``
  contains no ``await``, no ``.execute``/``.fetch`` and no client handle: it
  is pure computation over contract objects the run already materialised. It
  could only wedge on an infinite loop.
* the **graph** arm can. It drives FalkorDB over a redis connection, and
  ``GraphArmStore.for_org`` constructs ``FalkorDriver(host, port, password,
  database)`` with **no socket timeout of any kind**. A half-open connection
  or an unresponsive server blocks indefinitely, and the trial store is a
  container shared with other lanes.

So the exposure is specific and one-sided, which makes it worth bounding
rather than documenting. :func:`hard_bound` runs the callable on a worker
thread and joins with a deadline, so the *sweep* always proceeds and the
case is recorded ``NOT_RUN_TIMEOUT`` with its budget named.

What that still does not do, said plainly because it is the residual that
survives: Python cannot kill a thread. A wedged producer's thread **leaks**
for the remainder of the process. :class:`BudgetOutcome` therefore carries
``abandoned_thread``, the runner records it per case, and the sweep refuses
to describe itself as a clean run while any thread is outstanding -- an
abandoned worker still holds a connection to the shared store, and a later
lane debugging that container deserves to find it in the artifact rather
than by bisecting. Killing the wedged work genuinely requires a process
boundary, which is out of proportion to a trial that has never observed a
wedge.

The trial runner additionally caps total wall clock, because "each case
finished within its own budget" and "the sweep finished" are different
claims and a 39-case sweep of individually-acceptable slow cases is still a
sweep nobody waits for.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Protocol

__all__ = [
    "DEFAULT_PER_CASE_TIMEOUT_SECONDS",
    "BudgetOutcome",
    "Worker",
    "enforce",
    "hard_bound",
]


class Worker(Protocol):
    """The three things :func:`hard_bound` needs from a worker.

    Narrower than ``threading.Thread`` on purpose. The abandoned path is the
    one that matters most and is the hardest to reach honestly: observing it
    with a real thread means actually wedging one for the length of the
    timeout, which is slow, environment-dependent, and tempts the next
    person to shrink the bound until the guard stops discriminating. A
    protocol lets a test supply a worker that is *always* still running,
    keeping the assertion on this module's own logic.
    """

    def start(self) -> None: ...  # pragma: no cover - protocol

    def join(
        self, timeout: float | None = None
    ) -> None: ...  # pragma: no cover - protocol

    def is_alive(self) -> bool: ...  # pragma: no cover - protocol


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
    #: Set only by :func:`hard_bound`, and only when the worker was still
    #: running at the deadline. The thread is not killable and keeps whatever
    #: connection it holds; the runner surfaces this per case so a leaked
    #: worker against the shared trial store is a recorded fact rather than
    #: something a later lane discovers by bisecting a misbehaving container.
    abandoned_thread: bool = False

    @property
    def detail(self) -> str:
        if self.abandoned_thread:
            return (
                f"the arm was still running at the {self.limit_seconds:.1f}s "
                "per-case deadline and was abandoned on its worker thread, "
                "which Python cannot kill; the sweep continued, the case is "
                "NOT RUN and not retried, and the thread leaks for the "
                "remainder of the process holding whatever it held"
            )
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


def hard_bound(
    call: Callable[[], object],
    *,
    limit_seconds: float = DEFAULT_PER_CASE_TIMEOUT_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    spawn: Callable[..., Worker] | None = None,
) -> BudgetOutcome:
    """Run ``call`` on a worker thread and stop waiting at the deadline.

    The difference from :func:`enforce` is the one that matters for a wedged
    producer: ``enforce`` cannot return until the call does, so a producer
    blocked on a socket with no timeout stalls the whole sweep. This returns
    at the deadline whether or not the worker has finished.

    It does **not** kill the worker, and the naming reflects that: the bound
    is on how long the RUNNER waits, not on how long the work runs. A worker
    still running at the deadline is reported through
    ``BudgetOutcome.abandoned_thread`` and keeps running, holding whatever it
    holds. Anything stronger needs a process boundary.

    Daemon threads, so a leaked worker cannot keep the interpreter alive at
    exit and turn a recorded timeout into a hung sweep.

    ``spawn`` is injected for the tests, which need to observe the abandoned
    path without actually wedging a socket.
    """

    if limit_seconds <= 0:
        raise ValueError(
            f"a per-case budget must be positive; got {limit_seconds!r}. A "
            "zero or negative budget would mark every case NOT RUN and the "
            "sweep would report a clean absence of measurement"
        )

    box: dict[str, object] = {}

    def target() -> None:
        try:
            box["value"] = call()
        except Exception as raised:  # noqa: BLE001 - deliberately total
            box["fault"] = raised

    factory: Callable[..., Worker] = spawn or threading.Thread
    worker = factory(target=target, daemon=True)
    started = clock()
    worker.start()
    worker.join(timeout=limit_seconds)
    elapsed = clock() - started

    if worker.is_alive():
        return BudgetOutcome(
            elapsed_seconds=elapsed,
            limit_seconds=limit_seconds,
            exceeded=True,
            value=None,
            fault=None,
            abandoned_thread=True,
        )

    outcome = BudgetOutcome(
        elapsed_seconds=elapsed,
        limit_seconds=limit_seconds,
        exceeded=elapsed > limit_seconds,
        value=box.get("value"),
        fault=box.get("fault"),  # type: ignore[arg-type]
    )
    # Same withholding rule as ``enforce``: a value that arrived after the
    # deadline (the join returned late, or the clock crossed it during
    # teardown) must not be scorable as if it had arrived in time.
    if outcome.exceeded:
        return replace(outcome, value=None, fault=None)
    return outcome
