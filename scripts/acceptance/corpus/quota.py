"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: scripted-provider quota coordination.

Recon finding this exists to close: the compose-configured platform ceiling
(``tests/acceptance/compose.ask-dev.yml``: ``ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX=1000``,
``ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD=200_000_000``) is enforced ONLY
by the real admission path at request time
(``DevPersistenceService._enforce_platform_allowance`` ->
``DevMonthlyCostLimitExceeded``) -- there is no in-band coordinator. Phase
1c's ``test_ask_dev_quota_headroom.py`` proves the ceiling *should* clear
134 cases x 3 runs with margin, at an offline allowance-accounting level;
it does not track or bound spend *during* a real corpus run.

Without this module, a corpus run that overruns the ceiling (a case
retried more than planned, a future larger corpus, a lower ceiling in some
other environment) would see its N-th case fail with a raw
``DevMonthlyCostLimitExceeded``/429 that looks exactly like a per-case
product defect. ``QuotaBudget`` tracks the SAME estimate
``test_ask_dev_quota_headroom.py`` already validates has headroom, locally,
case by case, and fails loud with an actionable message (how many
requests/cost remain, which case tripped it) BEFORE the real admission path
would 429 -- turning an opaque per-case failure into a diagnosable
quota-coordination one.

This is advisory bookkeeping, not enforcement: the real ceiling lives
server-side and is what actually protects the platform. A caller that never
calls :meth:`QuotaBudget.reserve` is not blocked from anything -- this
module only helps the runner behave well against a shared, real limit.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from dev_health_ops.llm.agent.openai_compatible import _estimated_cost_microusd

__all__ = [
    "DEFAULT_MODEL",
    "DEFAULT_ROUNDS_PER_RUN",
    "QuotaBudget",
    "QuotaConfigurationError",
    "QuotaExhaustedError",
    "estimate_run_cost_microusd",
]

#: The scripted acceptance model id (scripted_openai_service.py). Pricing is
#: looked up by this exact string in ``openai_compatible._PLATFORM_MODEL_PRICES``.
DEFAULT_MODEL = "ask-dev-scripted-v1"

#: orchestrator.py's ``DevRunLimits`` default model-round cap -- the
#: worst-case number of provider round-trips one run can make. Mirrors
#: ``test_ask_dev_quota_headroom.py``'s identical constant so both proofs
#: use the same worst-case assumption.
DEFAULT_ROUNDS_PER_RUN = 4

_REQUEST_MAX_ENV = "ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX"
_COST_MAX_ENV = "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD"


class QuotaConfigurationError(Exception):
    """The environment does not declare a usable quota ceiling.

    Raised rather than defaulting to "unlimited": a corpus run against a
    stack that forgot to set these (compose misconfiguration, or a
    developer running against a different environment entirely) must not
    silently proceed un-budgeted.
    """


class QuotaExhaustedError(Exception):
    """A reservation would exceed the tracked request or cost ceiling."""


def estimate_run_cost_microusd(
    *,
    model: str = DEFAULT_MODEL,
    input_tokens: int,
    output_tokens: int,
    rounds: int = DEFAULT_ROUNDS_PER_RUN,
) -> int:
    """The real production pricing function, applied to a per-round token
    estimate and summed over the worst-case round count one run can reach.

    Raises ``ValueError`` (not a silent 0) if ``model`` has no priced entry
    -- an unpriced model falling back to "free" would make every reservation
    against it vacuous.
    """

    per_round = _estimated_cost_microusd(
        model=model, input_tokens=input_tokens, output_tokens=output_tokens
    )
    if per_round is None:
        raise ValueError(
            f"{model!r} has no priced entry in _PLATFORM_MODEL_PRICES -- "
            "refusing to silently treat this reservation as free"
        )
    return per_round * rounds


@dataclass(slots=True)
class QuotaBudget:
    """Tracks estimated request/cost spend against a fixed monthly ceiling.

    Not thread-safe and not process-shared -- one instance per corpus-runner
    pytest session, mirroring the single-process, sequential-by-default
    nature of the acceptance run itself.
    """

    max_requests: int
    max_cost_microusd: int
    spent_requests: int = 0
    spent_cost_microusd: int = 0

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> QuotaBudget:
        """Build a budget from the same env vars the compose stack sets.

        Raises :class:`QuotaConfigurationError` if either is missing,
        non-numeric, or non-positive -- an un-set or zero ceiling is
        treated as a configuration error, never as "no limit".
        """

        source = env if env is not None else os.environ
        max_requests = _positive_int(source, _REQUEST_MAX_ENV)
        max_cost_microusd = _positive_int(source, _COST_MAX_ENV)
        return cls(max_requests=max_requests, max_cost_microusd=max_cost_microusd)

    def remaining_requests(self) -> int:
        return self.max_requests - self.spent_requests

    def remaining_cost_microusd(self) -> int:
        return self.max_cost_microusd - self.spent_cost_microusd

    def reserve(self, *, case_id: str, requests: int, cost_microusd: int) -> None:
        """Record spend for one case, or raise before recording anything.

        A failed reservation leaves the budget untouched (no partial
        spend) -- a caller that catches :class:`QuotaExhaustedError` and
        aborts the run gets an accurate "spent so far" figure for its
        failure report.
        """

        if self.spent_requests + requests > self.max_requests:
            raise QuotaExhaustedError(
                f"case {case_id!r} needs {requests} request(s) but only "
                f"{self.remaining_requests()} remain of the "
                f"{self.max_requests}-request monthly ceiling "
                f"({self.spent_requests} already spent this run)"
            )
        if self.spent_cost_microusd + cost_microusd > self.max_cost_microusd:
            raise QuotaExhaustedError(
                f"case {case_id!r} needs {cost_microusd} microUSD but only "
                f"{self.remaining_cost_microusd()} remain of the "
                f"{self.max_cost_microusd}-microUSD monthly ceiling "
                f"({self.spent_cost_microusd} already spent this run)"
            )
        self.spent_requests += requests
        self.spent_cost_microusd += cost_microusd

    def release(self, *, requests: int, cost_microusd: int) -> None:
        """Credit back a reservation that turned out not to be used.

        Codex round-1 finding (MEDIUM, confirmed): ``reserve`` is called
        BEFORE the case's HTTP request is even attempted (a worst-case
        pre-charge, so a case that trips the ceiling never touches the
        network at all). Without a release path, a pre-admission failure
        (the conversation-creation call itself failing, a network error
        before any real server-side usage occurred) leaves that spend
        charged locally with no corresponding real usage -- silently
        eating into budget later cases could have legitimately used. A
        caller should call this from a ``finally``/exception handler around
        the request it guarded, crediting back exactly what it reserved.

        Never raises on over-release (clamped at zero) -- a caller
        reporting a slightly-wrong credit-back is far preferable to a
        second exception masking the original failure that triggered the
        release in the first place.
        """

        self.spent_requests = max(0, self.spent_requests - requests)
        self.spent_cost_microusd = max(0, self.spent_cost_microusd - cost_microusd)


def _positive_int(source: Mapping[str, str], env_name: str) -> int:
    raw = source.get(env_name)
    if raw is None:
        raise QuotaConfigurationError(
            f"{env_name} is not set -- a corpus run must not proceed "
            "un-budgeted against an unknown platform quota ceiling"
        )
    try:
        value = int(raw)
    except ValueError as exc:
        raise QuotaConfigurationError(f"{env_name}={raw!r} is not an integer") from exc
    if value <= 0:
        raise QuotaConfigurationError(f"{env_name}={value} must be positive")
    return value
