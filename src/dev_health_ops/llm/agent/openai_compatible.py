"""OpenAI-compatible adapter for Ask Dev's provider-neutral agent contract."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from enum import StrEnum
from typing import Any, cast
from urllib.parse import urlsplit

from dev_health_ops.llm.providers._http import make_hardened_async_httpx_client
from dev_health_ops.llm.providers.openai_capabilities import (
    build_wire_tool_name_map,
    chat_completion_reasoning_effort,
    sanitize_tool_name,
    supports_parallel_tool_calls,
    supports_temperature,
)
from dev_health_ops.logging_config import pin_content_carrying_client_loggers

from .budget_policy import model_family_budget
from .contracts import (
    AgentDecisionResult,
    AgentDisambiguation,
    AgentFinalAnswer,
    AgentMessage,
    AgentMessageRole,
    AgentProviderCapabilities,
    AgentRefusal,
    AgentToolDefinition,
    AgentToolRequest,
    AgentUsage,
    CancellationSignal,
    StreamingMode,
    StructuredOutputMode,
    ToolDecisionMode,
)
from .errors import (
    AgentProviderError,
    AgentProviderErrorCode,
    safe_agent_provider_error,
)

READINESS_VERSION = "ask-dev-agent-v3"
"""Bumped v2 -> v3 for CHAOS-3254: the outbound wire contract changed (native
tool requests now send ``parallel_tool_calls``, gated by model family -- see
``supports_parallel_tool_calls``). A v2-certified endpoint has never been
asked to accept the new parameter, so it must be re-certified rather than
treated as still current. ``provider_fingerprint`` folds this constant in,
so bumping it invalidates every existing stored readiness record for every
provider instance (see ``AgentReadinessRecord.is_current``)."""
PLATFORM_PRICE_BOOK_VERSION = "openai-2026-07-29"

_DECISION_FIELDS = frozenset(
    {
        "kind",
        "tool_id",
        "arguments",
        "call_id",
        "value",
        "prompt",
        "candidates",
        "code",
        "message",
    }
)


class _SequentialToolContractViolation(ValueError):
    """A provider returned more than one native tool call in one decision.

    Ask Dev's runtime is a sequential bounded state machine: exactly one tool
    request per model decision. This is distinct from other malformed/invalid
    provider responses (``AgentProviderErrorCode.INVALID_RESPONSE``) because
    it must be classified as a stable provider/decision contract error rather
    than an opaque application ``internal_error`` (CHAOS-3254).
    """


_STRUCTURAL_SCHEMA_KEYS = frozenset(
    {
        "$defs",
        "$ref",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "anyOf",
        "enum",
        "const",
    }
)

# Server-owned conservative prices in microUSD per million tokens. Cached input
# is intentionally charged at the full input rate because the normalized usage
# contract does not expose cached-token detail.
# Source snapshot: https://developers.openai.com/api/docs/models/gpt-5-mini
#
# CHAOS-3219 Wave 4 (Codex finding, HIGH, 2026-08-05): "ask-dev-scripted-v1"
# (dev_health_ops.llm.agent.scripted_openai_service.SCRIPTED_OPENAI_MODEL,
# the deterministic acceptance provider, wired in as source=PLATFORM by
# production_runtime.py) previously had NO entry here, so
# _estimated_cost_microusd returned None for every call. ProviderBudget.add
# (orchestrator.py) never reconciles a None-cost call down from its
# admission-time reservation (ProviderBudget.require's
# estimated_cost_per_call_microusd, currently 1_000_000) -- it stays stuck
# at the reservation, unbounded by what actually happened. Over a run's
# model rounds that meant every acceptance run charged the org's monthly
# allowance ~1M-4M microusd regardless of real usage, exhausting the
# default 100_000_000-microusd org allowance in a few dozen runs -- far
# short of the ~134-case corpus this environment exists to run. Pricing it
# here (deliberately tiny -- it is a fixed, deterministic test double, not
# a real model with real inference cost) lets every call report a real,
# small, non-None cost, so ProviderBudget.add's reconciliation branch
# replaces the reservation with the true (near-zero) figure instead of
# leaving it stuck.
# CHAOS-3552: "gpt-5-nano" had no entry either, and it is what the dev stack
# actually configures (ops/.env LLM_MODEL). The same defect as the scripted
# model above, one level more serious because it is a REAL model on a real
# endpoint: measured at 4 rounds x the US$1 reservation, a run booked US$4.00
# against a real cost of US$0.018 -- a 222x overcharge that exhausted the
# default US$100 monthly allowance after 25 runs, against a request cap of
# 1,000. That is what CHAOS-3523's "the allowance is gating platform runs"
# turned out to be.
# Source snapshot: https://developers.openai.com/api/docs/models/gpt-5-nano
_PLATFORM_MODEL_PRICES: dict[str, tuple[int, int]] = {
    "gpt-5-mini": (250_000, 2_000_000),
    "gpt-5-nano": (50_000, 400_000),
    "ask-dev-scripted-v1": (1_000, 1_000),
}

#: The deterministic acceptance provider's model id
#: (``scripted_openai_service.SCRIPTED_OPENAI_MODEL``), named so the carve-out
#: keys on it exactly rather than on a bare literal.
SCRIPTED_FIXTURE_MODEL = "ask-dev-scripted-v1"

#: Models exempt from the unpriced-model configuration error, by EXACT id.
#:
#: Exactly one entry, and a test pins the count. A test double is not an
#: unpriced production model: it has no inference cost to get wrong, and the
#: acceptance stack runs on it, so failing construction here would take that
#: stack down for a fault it cannot have.
#:
#: Membership is exact -- deliberately NOT the prefix match
#: ``_estimated_cost_microusd`` uses for real models below. A carve-out in that
#: style would admit ``ask-dev-scripted-v1-v2`` and every other stem-sharer,
#: which is how a deliberate exception becomes an accidental default. The
#: anti-widening control in ``test_chaos_3552_platform_price_book.py`` asserts
#: each such neighbour still fails loud.
_CARVE_OUT_MODELS = frozenset({SCRIPTED_FIXTURE_MODEL})

#: Providers that run on the operator's OWN hardware, so a call genuinely
#: costs nothing on our platform key. Mirrors
#: ``policy._LOCAL_PLATFORM_PROVIDERS``. This -- not the endpoint URL -- is
#: what licenses reporting a cost of zero.
_SELF_HOSTED_PROVIDERS = frozenset({"local", "ollama", "lmstudio"})


class PlatformCostMetering(StrEnum):
    """Whether a platform provider's cost can be metered, and if not, why not.

    CHAOS-3552. The distinction is load-bearing because the two "no price"
    cases want OPPOSITE handling, and collapsing them is precisely the bug: a
    model nobody priced is today indistinguishable from a deployment that
    cannot be priced, so both silently book the US$1 reservation as the cost.
    """

    #: A real price exists for this provider/model/endpoint.
    PRICED = "priced"
    #: The deterministic acceptance fixture -- see ``_CARVE_OUT_MODELS``.
    FIXTURE = "fixture"
    #: Self-hosted, or any non-official endpoint. Genuinely has no dollar cost
    #: to report, and pricing it at OpenAI's rates would FABRICATE one. Not an
    #: error -- the run proceeds unmetered.
    UNMETERED_SELF_HOSTED = "unmetered_self_hosted"
    #: A real OpenAI model on the official endpoint that this build has no
    #: price for. Operator misconfiguration; books the reservation, loudly.
    UNPRICED_CONFIGURATION_ERROR = "unpriced_configuration_error"
    #: An openai-compatible endpoint that is NOT OpenAI's own -- Azure OpenAI,
    #: OpenRouter, a corporate gateway, a forwarding proxy. Billability is
    #: UNKNOWN, so it books the reservation loudly rather than reporting zero.
    #:
    #: Distinct from ``UNMETERED_SELF_HOSTED`` and the distinction is the whole
    #: point. An earlier revision classified by URL -- "not api.openai.com"
    #: implied "free" -- and reported Azure, OpenRouter and every paid gateway
    #: at cost 0. That is fail-OPEN: real spend accrues against an allowance
    #: that never moves and nobody is throttled or told, which is strictly
    #: worse than the stuck-reservation overcharge it replaced. Only the
    #: PROVIDER NAME can say "this is the operator's own hardware"; a URL
    #: cannot.
    UNKNOWN_BILLABILITY = "unknown_billability"


def _official_openai_endpoint(base_url: str | None) -> bool:
    """Whether ``base_url`` is OpenAI's own API.

    Mirrors ``llm.budget._official_openai_endpoint``. Duplicated deliberately
    and temporarily: importing ``llm.budget`` here would pull SQLAlchemy, the
    session factory and the licensing models into this provider's import graph
    for one predicate. Stage 2 (CHAOS-3560) deletes this module's price book in
    favour of ``budget.reliable_price`` and takes the duplication with it;
    until then a differential test pins the two equal rather than letting them
    drift -- which is the failure this whole ticket is about.
    """

    if not (base_url or "").strip():
        return True
    try:
        parsed = urlsplit(str(base_url))
    except ValueError:
        return False
    return parsed.scheme == "https" and parsed.hostname == "api.openai.com"


def _normalized_model_id(model: str) -> str:
    """The one normalization both classification and pricing must share.

    CHAOS-3552, adversarial review MEDIUM: the carve-out check stripped the
    model id and the price lookup did not, so ``LLM_MODEL="ask-dev-scripted-v1 "``
    (a trailing space in an env var -- entirely ordinary) classified as
    ``FIXTURE`` while ``_estimated_cost_microusd`` returned ``None``. The
    classifier said "carved out, proceed" and the pricer left the reservation
    standing on every call. Two paths that must agree, normalizing differently
    -- the same class of defect as the two price books this ticket is about.
    """

    return model.strip()


def _canonical_priced_model(model: str) -> str | None:
    """The price-book key ``model`` resolves to, honouring dated variants.

    Prefix matching exists for real models, which providers pin as dated
    snapshots (``gpt-5-nano-2026-01-01`` is the same model at the same rates).

    It is switched OFF for carve-out entries, and that exception is not
    cosmetic -- the anti-widening test caught this exact hole in the first
    revision of this change. The fixture is IN the price book, so prefix
    matching resolved ``ask-dev-scripted-v1-v2`` to it and reported the
    stranger as PRICED, widening the carve-out through the price book rather
    than through ``_CARVE_OUT_MODELS``. A test double has no dated variants to
    honour: an id that is not exactly the fixture is not the fixture.
    """

    model = _normalized_model_id(model)
    return next(
        (
            known
            for known in _PLATFORM_MODEL_PRICES
            if model == known
            or (known not in _CARVE_OUT_MODELS and model.startswith(f"{known}-"))
        ),
        None,
    )


def platform_cost_metering(
    *, provider: str, model: str, base_url: str | None
) -> PlatformCostMetering:
    """Classify how -- or whether -- this platform provider's cost can be metered.

    CHAOS-3552. This REPLACES the invariant the ticket proposed ("assert every
    certified model is priced"), which is not implementable:
    ``CERTIFIED_PLATFORM_AGENT_PROVIDERS`` lists PROVIDERS, not models; the
    model is operator-configured at runtime; and self-hosted providers are
    unpriced BY DESIGN (``budget.py``: "An absent pair is unavailable, never
    zero"). Requiring a price for every certified provider would break every
    self-hosted deployment.

    Enforced instead: *a platform provider whose cost cannot be priced must
    never silently book the reservation as its cost.* Only
    ``UNPRICED_CONFIGURATION_ERROR`` is a fault; the other three are answers.

    Order matters. The fixture is checked FIRST, before the endpoint test,
    because the acceptance stack serves it from its own scripted endpoint --
    classifying it as self-hosted would be true but useless, and would lose the
    near-zero price that keeps its reservation reconciled.
    """

    if _normalized_model_id(model) in _CARVE_OUT_MODELS:
        return PlatformCostMetering.FIXTURE
    if provider.strip().lower() in _SELF_HOSTED_PROVIDERS:
        return PlatformCostMetering.UNMETERED_SELF_HOSTED
    if not _official_openai_endpoint(base_url):
        return PlatformCostMetering.UNKNOWN_BILLABILITY
    if _canonical_priced_model(model) is None:
        return PlatformCostMetering.UNPRICED_CONFIGURATION_ERROR
    return PlatformCostMetering.PRICED


def _fingerprint(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()[:24]


def _estimated_cost_microusd(
    *,
    model: str,
    input_tokens: int,
    output_tokens: int,
    base_url: str | None = None,
    provider: str = "openai",
) -> int | None:
    """Cost for one call, or ``None`` when it genuinely cannot be known.

    CHAOS-3552. The two "no price" cases return DIFFERENT values, and that is
    the whole point -- ``ProviderBudget.add`` reconciles a numeric cost down
    from the US$1 admission reservation and leaves the reservation standing on
    ``None``, so returning ``None`` for both meant self-hosted deployments
    booked US$4/run for infrastructure the operator already owns.

    * Self-hosted BY PROVIDER (``local``/``ollama``/``lmstudio``) -> explicit
      **0**. Unmetered, and
      unmetered is the honest answer: the run costs the operator nothing on our
      platform key. Zero reconciles, so no reservation is booked.
    * OpenAI-official with an unpriced model -> ``None``. The reservation
      deliberately stands, because a platform run spends real operator dollars
      and unmetered openai spend bounded only by the request cap is not an
      acceptable posture. It is never SILENT: ``production_runtime._provider``
      warns and increments ``ASK_DEV_PLATFORM_MODEL_UNPRICED_TOTAL`` at
      construction, naming the model and the remedy.
    """

    if provider.strip().lower() in _SELF_HOSTED_PROVIDERS:
        return 0
    if not _official_openai_endpoint(base_url):
        # Billable-or-not is UNKNOWN here (Azure, OpenRouter, a paid gateway).
        # Unknown fails CLOSED: the reservation stands, loudly. Returning 0
        # would report real spend as free -- see UNKNOWN_BILLABILITY.
        return None
    canonical_model = _canonical_priced_model(model)
    if canonical_model is None:
        return None
    input_rate, output_rate = _PLATFORM_MODEL_PRICES[canonical_model]
    numerator = input_tokens * input_rate + output_tokens * output_rate
    return (numerator + 999_999) // 1_000_000


class OpenAICompatibleAgentProvider:
    """Normalize native and JSON tool decisions from OpenAI-compatible endpoints."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        base_url: str | None = None,
        client: Any | None = None,
        disclosure_key: str = "openai_compatible",
        context_window_tokens: int | None = None,
        cost_provider: str = "openai",
        organization: str = "",
        project: str = "",
        custom_headers: tuple[tuple[str, str], ...] = (),
    ) -> None:
        self.model = model
        self.base_url = base_url or ""
        #: CHAOS-3552: which PROVIDER this instance speaks for, so cost
        #: reporting can tell the operator's own hardware from a paid gateway.
        #: A URL cannot answer that; only the configured provider name can.
        self.cost_provider = cost_provider
        self.organization = organization
        self.project = project
        self.custom_headers = custom_headers
        self._http_client: Any | None = None
        if client is None:
            from openai import AsyncOpenAI

            # The OpenAI SDK's own import-time logging setup can reset the
            # content-carrying client loggers back to DEBUG if the operator
            # OPENAI_LOG env var is set, overriding configure_logging()'s
            # WARNING pin -- reassert it immediately after this (possibly
            # first-ever) import (CHAOS-3258).
            pin_content_carrying_client_loggers()

            self._http_client = make_hardened_async_httpx_client()
            # CHAOS-3285 round 6 (Codex HIGH): AsyncOpenAI reads
            # OPENAI_ORG_ID/OPENAI_PROJECT_ID/OPENAI_CUSTOM_HEADERS from the
            # process environment AMBIENTLY whenever organization/project
            # are left as Python None or default_headers is left unset --
            # a change to any of those env vars silently changes the wire
            # identity headers without ever touching this constructor's
            # inputs, and without changing certification.
            #
            # Empirically verified (openai==2.36.0 _client.py): the SDK's
            # env fallback is gated on `if organization is None:` /
            # `if project is None:` -- passing ANY non-None value (including
            # "") suppresses it. But passing "" also SENDS an empty
            # `OpenAI-Organization: ` header rather than omitting it, which
            # is not the same as "not configured". So: pass explicit
            # (possibly empty) strings to suppress the env read during
            # __init__, then reset back to None afterward when genuinely
            # not configured, so the header is correctly OMITTED
            # (`default_headers` treats `None` as "send Omit()", verified
            # against the same source) rather than sent empty.
            #
            # OPENAI_CUSTOM_HEADERS has no such `is None` gate at all -- the
            # SDK unconditionally merges it into `_custom_headers` whenever
            # the env var is set, regardless of what `default_headers` this
            # constructor passes. The only way to suppress it is to
            # overwrite `_custom_headers` post-construction with exactly
            # our own explicitly-resolved set (also empirically verified).
            client = AsyncOpenAI(
                api_key=api_key,
                base_url=base_url or None,
                organization=organization,
                project=project,
                default_headers=dict(custom_headers) if custom_headers else None,
                http_client=self._http_client,
            )
            if not organization:
                client.organization = None
            if not project:
                client.project = None
            client._custom_headers = dict(custom_headers)
        self._client = client
        self._capabilities = AgentProviderCapabilities(
            structured_output=StructuredOutputMode.JSON_SCHEMA,
            tool_decisions=ToolDecisionMode.NATIVE,
            streaming=StreamingMode.BUFFERED,
            supports_cancellation=True,
            context_window_tokens=context_window_tokens,
            max_output_tokens=None,
            readiness_version=READINESS_VERSION,
            disclosure_key=disclosure_key,
        )

    @property
    def capabilities(self) -> AgentProviderCapabilities:
        return self._capabilities

    @property
    def provider_fingerprint(self) -> str:
        return _fingerprint("openai-compatible", self.base_url, READINESS_VERSION)

    @property
    def model_fingerprint(self) -> str:
        return _fingerprint(self.provider_fingerprint, self.model)

    async def decide(
        self,
        messages: Sequence[AgentMessage],
        tools: Sequence[AgentToolDefinition],
        response_schema: Mapping[str, Any],
        timeout_seconds: float,
        max_output_tokens: int,
        signal: CancellationSignal | None = None,
    ) -> AgentDecisionResult:
        if signal is not None and signal.is_cancelled():
            raise AgentProviderError(
                AgentProviderErrorCode.CANCELLED,
                provider_dispatched=False,
            )
        # Built before any request is dispatched: a mapping collision is a
        # registry-construction bug (two distinct tool_ids sanitizing to the
        # same wire name), never a provider-caused failure, so it must
        # surface as a plain, uncaught error rather than an AgentProviderError
        # (CHAOS-3286). Callers that own a fixed registry (AskDevToolRegistry)
        # additionally assert this can't happen at registry build time.
        wire_tool_ids = build_wire_tool_name_map(item.tool_id for item in tools)
        started = time.monotonic()
        create_completion = cast(Any, self._client.chat.completions.create)
        # CHAOS-3285 round 5 (Codex HIGH): the ENTIRE request -- not just the
        # capability-gated controls round 4 extracted -- is assembled by
        # build_completion_request, the single producer the readiness
        # fingerprint's wire-request digest also consumes directly (see
        # production_runtime._wire_request_digest). Round 4's narrower
        # extraction still left max_completion_tokens, the response_format
        # wrapper's literal name/strict values, and the full generated
        # schema body assembled independently right here, invisible to the
        # fingerprint; nothing about the request is assembled anywhere else
        # now, so nothing here can drift from what gets fingerprinted again.
        completion_kwargs = build_completion_request(
            model=self.model,
            messages=messages,
            tools=tools,
            response_schema=response_schema,
            max_output_tokens=max_output_tokens,
        )
        provider_task = asyncio.create_task(create_completion(**completion_kwargs))
        cancel_task = asyncio.create_task(signal.wait()) if signal is not None else None
        try:
            waiters = {provider_task}
            if cancel_task is not None:
                waiters.add(cancel_task)
            done, _ = await asyncio.wait(
                waiters, timeout=timeout_seconds, return_when=asyncio.FIRST_COMPLETED
            )
            if cancel_task is not None and cancel_task in done:
                provider_task.cancel()
                await asyncio.gather(provider_task, return_exceptions=True)
                raise AgentProviderError(AgentProviderErrorCode.CANCELLED)
            if provider_task not in done:
                provider_task.cancel()
                await asyncio.gather(provider_task, return_exceptions=True)
                raise AgentProviderError(AgentProviderErrorCode.TIMEOUT, retryable=True)
            response = await provider_task
        except AgentProviderError:
            raise
        except Exception as exc:
            raise safe_agent_provider_error(exc) from None
        finally:
            if cancel_task is not None:
                cancel_task.cancel()
                await asyncio.gather(cancel_task, return_exceptions=True)

        # Normalized once the response is in hand, so it is available to
        # attach to every raise path below -- not only the success path. A
        # provider that reported output/reasoning exhaustion, a sequential-
        # tool-contract violation, or an otherwise malformed decision still
        # returned (and billed) a real response; discarding it before it is
        # read poisons BYO budget reconciliation into "usage_unavailable",
        # which disables enforcement and then rejects every later call
        # without dispatching it (CHAOS-3285).
        agent_usage = self._normalize_usage(response)
        # A response with no usage object at all normalizes to 0 input / 0
        # output above; a response that genuinely reports zero of both is
        # indistinguishable from that and, per the same invariant the
        # success path already relies on (a valid completion cannot consume
        # zero input AND zero output tokens), can't be real either. Only
        # attach usage to the raised error when it's actually informative --
        # otherwise a failure with unreported usage would reconcile as a
        # real $0 call (poisoning nothing, but silently making an unknown
        # cost look free) instead of the pre-existing conservative
        # "usage unavailable" handling for genuinely unreported usage
        # (CHAOS-3285).
        reported_usage = (
            agent_usage
            if agent_usage.input_tokens or agent_usage.output_tokens
            else None
        )

        try:
            # A model that ran out of its output/reasoning token budget
            # reports finish_reason="length" -- checked, and raised, before
            # any attempt to parse message content as JSON (below, inside
            # _normalize_response), so exhaustion never masquerades as
            # malformed output (INVALID_RESPONSE). This covers both an empty
            # ``content`` and a truncated-but-technically-parseable partial
            # JSON payload alike: the finish_reason alone is dispositive
            # (CHAOS-3285).
            if getattr(response.choices[0], "finish_reason", None) == "length":
                raise AgentProviderError(
                    AgentProviderErrorCode.OUTPUT_EXHAUSTED, usage=reported_usage
                )
            decision = self._normalize_response(
                response,
                allowed_tool_ids=frozenset(item.tool_id for item in tools),
                wire_tool_ids=wire_tool_ids,
            )
        except _SequentialToolContractViolation:
            raise AgentProviderError(
                AgentProviderErrorCode.PROVIDER_CONTRACT_VIOLATION,
                usage=reported_usage,
            ) from None
        except (
            AttributeError,
            IndexError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ):
            raise AgentProviderError(
                AgentProviderErrorCode.INVALID_RESPONSE, usage=reported_usage
            ) from None
        return AgentDecisionResult(
            decision=decision,
            usage=agent_usage,
            latency_ms=max(0, round((time.monotonic() - started) * 1000)),
            provider_fingerprint=self.provider_fingerprint,
            model_fingerprint=self.model_fingerprint,
        )

    def _normalize_usage(self, response: Any) -> AgentUsage:
        usage = getattr(response, "usage", None)
        input_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
        output_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
        prompt_details = getattr(usage, "prompt_tokens_details", None)
        cached_tokens = getattr(prompt_details, "cached_tokens", None)
        completion_details = getattr(usage, "completion_tokens_details", None)
        reasoning_tokens = getattr(completion_details, "reasoning_tokens", None)
        return AgentUsage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_input_tokens=(
                int(cached_tokens) if cached_tokens is not None else None
            ),
            reasoning_tokens=(
                int(reasoning_tokens) if reasoning_tokens is not None else None
            ),
            estimated_cost_microusd=_estimated_cost_microusd(
                model=self.model,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                base_url=self.base_url,
                provider=self.cost_provider,
            ),
        )

    @staticmethod
    def _message_payload(message: AgentMessage) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "role": message.role.value,
            "content": message.content,
        }
        if message.role is AgentMessageRole.TOOL:
            if not message.tool_call_id:
                raise ValueError("tool messages require tool_call_id")
            payload["tool_call_id"] = message.tool_call_id
        if message.tool_request is not None:
            if message.role is not AgentMessageRole.ASSISTANT:
                raise ValueError("tool requests require an assistant message")
            payload["tool_calls"] = [
                {
                    "id": message.tool_request.call_id,
                    "type": "function",
                    "function": {
                        # Replaying prior-round history: sanitize here too so
                        # every outbound function name is wire-legal,
                        # consistent with _tool_payload (CHAOS-3286).
                        "name": sanitize_tool_name(message.tool_request.tool_id),
                        "arguments": json.dumps(
                            dict(message.tool_request.arguments),
                            separators=(",", ":"),
                            sort_keys=True,
                        ),
                    },
                }
            ]
        return payload

    @staticmethod
    def _tool_payload(tool: AgentToolDefinition) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                # OpenAI's native tools[].function.name must match
                # ^[a-zA-Z0-9_-]+$; the canonical registry tool_id (e.g.
                # "query_metric.v1") is dotted and illegal on the wire.
                # Sanitized here at the boundary only -- reverse-mapped back
                # to the canonical tool_id in _normalize_response
                # (CHAOS-3286).
                "name": sanitize_tool_name(tool.tool_id),
                "description": tool.description,
                "parameters": OpenAICompatibleAgentProvider._structural_schema(
                    tool.input_schema
                ),
                "strict": True,
            },
        }

    @staticmethod
    def _decision_response_schema(
        response_schema: Mapping[str, Any],
        tools: Sequence[AgentToolDefinition],
        *,
        allow_final_answer: bool = True,
    ) -> dict[str, Any]:
        """Describe every decision in OpenAI's supported root-object subset."""

        final_value_schema = OpenAICompatibleAgentProvider._structural_schema(
            response_schema
        )
        definitions: dict[str, Any] = {}
        OpenAICompatibleAgentProvider._merge_definitions(
            definitions, final_value_schema.pop("$defs", None)
        )
        argument_schemas: list[dict[str, Any]] = []
        for tool in tools:
            arguments_schema = OpenAICompatibleAgentProvider._structural_schema(
                tool.input_schema
            )
            OpenAICompatibleAgentProvider._merge_definitions(
                definitions, arguments_schema.pop("$defs", None)
            )
            argument_schemas.append(arguments_schema)

        nullable_arguments: dict[str, Any]
        if argument_schemas:
            nullable_arguments = {"anyOf": [*argument_schemas, {"type": "null"}]}
        else:
            nullable_arguments = {"type": "null"}

        schema: dict[str, Any] = {
            "type": "object",
            "additionalProperties": False,
            "required": sorted(_DECISION_FIELDS),
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": [
                        *(["tool_request"] if tools else []),
                        *(["final_answer"] if allow_final_answer else []),
                        "disambiguation",
                        "refusal",
                    ],
                },
                "tool_id": {
                    "type": ["string", "null"],
                    "enum": [*(tool.tool_id for tool in tools), None],
                },
                "arguments": nullable_arguments,
                "call_id": {
                    "anyOf": [
                        {"type": "string", "minLength": 1, "maxLength": 256},
                        {"type": "null"},
                    ]
                },
                "value": (
                    {"anyOf": [final_value_schema, {"type": "null"}]}
                    if allow_final_answer
                    else {"type": "null"}
                ),
                "prompt": {"type": ["string", "null"]},
                "candidates": {
                    "anyOf": [
                        {"type": "array", "items": {"type": "string"}},
                        {"type": "null"},
                    ]
                },
                "code": {"type": ["string", "null"]},
                "message": {"type": ["string", "null"]},
            },
        }
        if definitions:
            schema["$defs"] = definitions
        return OpenAICompatibleAgentProvider._structural_schema(schema)

    @staticmethod
    def _structural_schema(schema: Mapping[str, Any]) -> dict[str, Any]:
        """Project runtime-validated JSON Schema into provider grammar syntax."""

        def project(node: Any) -> Any:
            if isinstance(node, list):
                return [project(item) for item in node]
            if not isinstance(node, Mapping):
                return node
            result: dict[str, Any] = {}
            for key, value in node.items():
                if key not in _STRUCTURAL_SCHEMA_KEYS:
                    continue
                if key in {"$defs", "properties"} and isinstance(value, Mapping):
                    result[key] = {
                        str(name): project(definition)
                        for name, definition in value.items()
                    }
                else:
                    result[key] = project(value)
            properties = result.get("properties")
            if isinstance(properties, Mapping):
                result["additionalProperties"] = False
                result["required"] = sorted(str(name) for name in properties)
            return result

        return cast(dict[str, Any], project(schema))

    @staticmethod
    def _answer_draft_schema(schema: Mapping[str, Any]) -> Mapping[str, Any]:
        """Remove server-owned/defaulted DevAnswer fields from provider grammar."""

        properties = schema.get("properties")
        if not isinstance(properties, Mapping):
            return schema
        draft_fields = {
            "status",
            "direct_summary",
        }
        if not draft_fields.issubset(properties):
            return schema

        selected_properties = {
            field: properties[field] for field in sorted(draft_fields)
        }
        definitions = schema.get("$defs")
        selected_definitions: dict[str, Any] = {}
        pending: list[Any] = list(selected_properties.values())
        while pending:
            node = pending.pop()
            if isinstance(node, Mapping):
                reference = node.get("$ref")
                if isinstance(reference, str) and reference.startswith("#/$defs/"):
                    name = reference.removeprefix("#/$defs/")
                    if (
                        name not in selected_definitions
                        and isinstance(definitions, Mapping)
                        and name in definitions
                    ):
                        definition = definitions[name]
                        selected_definitions[name] = definition
                        pending.append(definition)
                pending.extend(node.values())
            elif isinstance(node, list):
                pending.extend(node)

        draft: dict[str, Any] = {
            "type": "object",
            "additionalProperties": False,
            "properties": selected_properties,
            "required": sorted(draft_fields),
        }
        if selected_definitions:
            draft["$defs"] = selected_definitions
        return draft

    @staticmethod
    def _merge_definitions(target: dict[str, Any], incoming: object | None) -> None:
        if incoming is None:
            return
        if not isinstance(incoming, Mapping):
            raise ValueError("JSON schema definitions must be an object")
        for name, definition in incoming.items():
            existing = target.get(str(name))
            if existing is not None and existing != definition:
                raise ValueError("conflicting JSON schema definitions")
            target[str(name)] = definition

    @staticmethod
    def _normalize_response(
        response: Any,
        *,
        allowed_tool_ids: frozenset[str],
        wire_tool_ids: Mapping[str, str],
    ) -> Any:
        message = response.choices[0].message
        tool_calls = getattr(message, "tool_calls", None) or []
        if len(tool_calls) > 1:
            raise _SequentialToolContractViolation("only one tool decision is allowed")
        if tool_calls:
            call = tool_calls[0]
            # Native tool_calls[].function.name is the wire-sanitized name
            # (CHAOS-3286); reverse-map back to the canonical tool_id before
            # any further validation or persistence. The JSON decision-
            # envelope fallback path below is unaffected -- it's Ask Dev's
            # own structured-output schema, not a native tool definition, so
            # it is never subject to OpenAI's function-name wire constraint
            # and continues to use the canonical dotted tool_id directly.
            tool_id = wire_tool_ids.get(str(call.function.name))
            if tool_id is None:
                raise ValueError("tool decision is not registered")
            arguments = json.loads(call.function.arguments)
            if not isinstance(arguments, dict):
                raise ValueError("tool arguments must be an object")
            call_id = str(call.id)
            if not call_id:
                raise ValueError("tool decision requires a call ID")
            return AgentToolRequest(
                tool_id=tool_id,
                arguments=arguments,
                call_id=call_id,
            )
        payload = json.loads(str(message.content or ""))
        if not isinstance(payload, dict):
            raise ValueError("agent decision must be an object")
        kind = payload.get("kind")
        if kind == "tool_request":
            return OpenAICompatibleAgentProvider._json_tool_request(
                payload, allowed_tool_ids=allowed_tool_ids
            )
        if kind == "final_answer":
            OpenAICompatibleAgentProvider._validate_envelope_fields(
                payload,
                compact_fields={"kind", "value"},
            )
            value = payload.get("value")
            if not isinstance(value, dict):
                raise ValueError("final answer must be an object")
            legacy_tool = OpenAICompatibleAgentProvider._legacy_json_tool_request(
                value, allowed_tool_ids=allowed_tool_ids
            )
            if legacy_tool is not None:
                return legacy_tool
            return AgentFinalAnswer(value=value)
        if kind == "disambiguation":
            OpenAICompatibleAgentProvider._validate_envelope_fields(
                payload,
                compact_fields={"kind", "prompt", "candidates"},
            )
            candidates = payload.get("candidates")
            if not isinstance(payload.get("prompt"), str) or not isinstance(
                candidates, list
            ):
                raise ValueError("invalid disambiguation decision")
            if not all(isinstance(item, str) for item in candidates):
                raise ValueError("invalid disambiguation candidates")
            return AgentDisambiguation(
                prompt=str(payload["prompt"]),
                candidates=tuple(str(item) for item in candidates),
            )
        if kind == "refusal":
            OpenAICompatibleAgentProvider._validate_envelope_fields(
                payload,
                compact_fields={"kind", "code", "message"},
            )
            if not isinstance(payload.get("code"), str) or not isinstance(
                payload.get("message"), str
            ):
                raise ValueError("invalid refusal decision")
            return AgentRefusal(
                code=str(payload["code"]), message=str(payload["message"])
            )
        raise ValueError("unknown agent decision")

    @staticmethod
    def _json_tool_request(
        payload: Mapping[str, Any], *, allowed_tool_ids: frozenset[str]
    ) -> AgentToolRequest:
        OpenAICompatibleAgentProvider._validate_envelope_fields(
            payload,
            compact_fields={"kind", "tool_id", "arguments", "call_id"},
        )
        tool_id = payload.get("tool_id")
        arguments = payload.get("arguments")
        call_id = payload.get("call_id")
        if not isinstance(tool_id, str) or tool_id not in allowed_tool_ids:
            raise ValueError("tool decision is not registered")
        if not isinstance(arguments, dict):
            raise ValueError("tool arguments must be an object")
        if not isinstance(call_id, str) or not call_id:
            raise ValueError("tool decision requires a call ID")
        return AgentToolRequest(tool_id, arguments, call_id)

    @staticmethod
    def _validate_envelope_fields(
        payload: Mapping[str, Any],
        *,
        compact_fields: set[str],
    ) -> None:
        fields = set(payload)
        if fields == compact_fields:
            return
        if fields != _DECISION_FIELDS:
            raise ValueError("invalid agent decision fields")

    @staticmethod
    def _legacy_json_tool_request(
        value: Mapping[str, Any], *, allowed_tool_ids: frozenset[str]
    ) -> AgentToolRequest | None:
        """Normalize the exact LM Studio envelope produced by the old bad schema."""

        if set(value) != {"tool_call"}:
            return None
        tool_call = value.get("tool_call")
        if not isinstance(tool_call, dict) or set(tool_call) != {"name", "args"}:
            raise ValueError("invalid JSON tool decision")
        tool_id = tool_call.get("name")
        arguments = tool_call.get("args")
        if not isinstance(tool_id, str) or tool_id not in allowed_tool_ids:
            raise ValueError("tool decision is not registered")
        if not isinstance(arguments, dict):
            raise ValueError("tool arguments must be an object")
        canonical = json.dumps(
            {"tool_id": tool_id, "arguments": arguments},
            sort_keys=True,
            separators=(",", ":"),
        )
        call_id = "json-call-" + hashlib.sha256(canonical.encode()).hexdigest()[:16]
        return AgentToolRequest(tool_id, arguments, call_id)

    async def aclose(self) -> None:
        close = getattr(self._client, "close", None)
        if close is not None:
            await close()
        if self._http_client is not None:
            await self._http_client.aclose()


def build_completion_request(
    *,
    model: str,
    messages: Sequence[AgentMessage],
    tools: Sequence[AgentToolDefinition],
    response_schema: Mapping[str, Any],
    max_output_tokens: int,
) -> dict[str, Any]:
    """The COMPLETE request ``OpenAICompatibleAgentProvider.decide()`` sends
    to the wire for one call -- every keyword argument passed to
    ``client.chat.completions.create(**...)``. ``decide()`` calls this
    directly and dispatches its return value wholesale; nothing about the
    request is assembled anywhere else.

    This is the single producer the readiness fingerprint's wire-request
    digest also consumes (``production_runtime._wire_request_digest``), so
    every field assembled here -- the capability-gated wire controls
    (tool_choice, parallel_tool_calls, temperature, reasoning_effort), the
    model-resolved ``max_completion_tokens`` budget, the full
    ``response_format`` (both the wrapper's name/strict literals AND the
    generated schema body), the serialized tool definitions, and the
    message scaffold shape -- can never independently drift between what
    actually gets sent and what gets fingerprinted.

    CHAOS-3285 round 5 (Codex HIGH): round 4's narrower
    ``wire_policy_kwargs`` covered only the capability-gated controls, plus
    a HAND-DUPLICATED copy of the response_format wrapper's name/strict
    literals -- ``decide()`` itself still independently assembled
    ``max_completion_tokens`` (via ``model_family_budget``), the full
    generated schema body, and the serialized tool payloads, none of which
    the fingerprint ever hashed. Changing any of those (e.g. the wrapper's
    literal ``"name"``/``"strict"`` values) left the fingerprint unchanged.
    This function replaces that narrower extraction entirely.
    """

    allow_final_answer = not tools or any(
        message.role is AgentMessageRole.TOOL for message in messages
    )
    budget = model_family_budget(model)
    request: dict[str, Any] = {
        "model": model,
        "messages": [
            OpenAICompatibleAgentProvider._message_payload(item) for item in messages
        ],
        "tools": [OpenAICompatibleAgentProvider._tool_payload(item) for item in tools]
        or None,
        "tool_choice": (
            ("auto" if allow_final_answer else "required") if tools else None
        ),
        "max_completion_tokens": budget.request_max_completion_tokens(
            max_output_tokens
        ),
    }
    if tools and supports_parallel_tool_calls(model):
        # Ask Dev's runtime is a sequential one-decision-per-round state
        # machine (TRD 11-12, 20.4). Explicitly disabling native parallel
        # tool calls keeps a standards-compliant OpenAI-compatible model
        # from returning multiple tool calls in one response, which the
        # normalizer must otherwise reject (CHAOS-3254).
        request["parallel_tool_calls"] = False
    if supports_temperature(model):
        request["temperature"] = 0
    reasoning_effort = chat_completion_reasoning_effort(model)
    if reasoning_effort is not None:
        request["reasoning_effort"] = reasoning_effort
    if allow_final_answer:
        request["response_format"] = {
            "type": "json_schema",
            "json_schema": {
                "name": "ask_dev_decision",
                "strict": True,
                "schema": OpenAICompatibleAgentProvider._decision_response_schema(
                    OpenAICompatibleAgentProvider._answer_draft_schema(response_schema),
                    tools,
                    allow_final_answer=True,
                ),
            },
        }
    return request
