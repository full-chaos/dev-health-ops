"""OpenAI-compatible chat completion client, env-driven provider selection.

Deliberately thin and self-contained: this trial must not reach into the
product's own ``ops/.../llm/agent/policy.py`` machinery (budget guards,
provider policy, entitlement checks) -- that machinery is Ask-Dev-coupled by
construction (baseline-inventory.md §11) and this is a shadow trial with its
own isolation invariants, not a caller entitled to bypass product
governance. A candidate arm wired into the product later would go through
that seam properly; this client exists only to smoke-test extraction
plumbing directly against a local (or, later, cloud) model.

Env vars:

``LLM_PROVIDER``
    ``"local"`` (default) or ``"cloud"``.

    ``"cloud"`` talks to the real OpenAI API (``https://api.openai.com/v1``
    by default) using ``OPENAI_API_KEY``. Model defaults to ``gpt-5-mini``
    -- read from this repo's own production config
    (``DEFAULT_MODEL_BY_PROVIDER["openai"]``,
    ``src/dev_health_ops/llm/providers/base.py``) rather than picked by
    this trial, per CHAOS-3499 step 3 direction: the trial measures the
    production-class model, not a hand-picked one. ``gpt-5`` family models
    are routed through the Responses API (``client.responses.create``),
    matching ``OpenAIGPT5Provider``
    (``src/dev_health_ops/llm/providers/openai.py``) -- NOT Chat
    Completions, which that same production code reserves for pre-GPT-5
    models. They also do not accept a caller-selected ``temperature``
    (``openai_capabilities.supports_temperature`` returns ``False`` for
    any model starting with ``gpt-5``/``o1``/``o3``); this client omits the
    parameter for that family rather than sending one the API would
    reject, exactly like production does.
``LOCAL_LLM_BASE_URL``
    Overrides the local provider's base URL. Defaults to
    ``http://localhost:1234/v1`` -- the address a process running directly
    on the **host** reaches LM Studio at (pytest, scripts, the harness
    invoked from a worktree -- everything this trial actually runs as).
    From **inside a container**, override this to
    ``http://host.docker.internal:1234/v1``, or you will get a
    connection-refused that looks exactly like LM Studio being down when it
    is only the address that is wrong.

    Under ``LLM_PROVIDER=local`` the base URL's host is checked against an
    allowlist (``localhost``, ``127.0.0.1``, ``host.docker.internal``) --
    without this, pointing ``LOCAL_LLM_BASE_URL`` at a real paid endpoint
    would bill through a code path this trial's cost authorization never
    covered (``LLM_PROVIDER=local`` is meant to name "no spend", not "no
    provider name check"). A host outside the allowlist raises
    :class:`LLMUnavailable` naming the reason.
``LOCAL_LLM_MODEL``
    Defaults to ``"google/gemma-4-e4b"``.
``OPENAI_API_KEY``
    Required when ``LLM_PROVIDER=cloud``. No local-style dummy default --
    an empty/missing key is a real :class:`LLMUnavailable`, not a silent
    attempt against the real, billable API.
``OPENAI_MODEL``
    Overrides the cloud model. Defaults to ``gpt-5-mini`` (see above).
``OPENAI_BASE_URL``
    Overrides the cloud base URL. Defaults to
    ``https://api.openai.com/v1``.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from urllib.parse import urlparse

DEFAULT_LOCAL_BASE_URL = "http://localhost:1234/v1"
DEFAULT_LOCAL_MODEL = "google/gemma-4-e4b"

#: Per-provider request windows. These are deliberately NOT one shared
#: number: a locally-hosted model answering a ~16k-token extraction prompt
#: on developer hardware routinely takes minutes, where a cloud call that
#: has not answered in two minutes is genuinely wedged. Giving both the same
#: window is how a slow local model gets recorded as a failure it did not
#: commit -- the exact false-positive chris flagged when promoting the local
#: tier into the run-3 matrix. Overridable per config; see
#: :meth:`LLMConfig.for_local` / :meth:`LLMConfig.for_cloud`.
DEFAULT_CLOUD_TIMEOUT_SECONDS = 120.0
DEFAULT_LOCAL_TIMEOUT_SECONDS = 900.0

DEFAULT_CLOUD_BASE_URL = "https://api.openai.com/v1"
#: Matches src/dev_health_ops/llm/providers/base.py's
#: DEFAULT_MODEL_BY_PROVIDER["openai"] as of this trial's step-3 research --
#: a literal, not an import, per this module's own "must not reach into the
#: product's own machinery" principle (see module docstring); a plain data
#: constant would be a smaller coupling than the policy/budget/entitlement
#: machinery that principle is actually about, but pinning it here keeps
#: the trial fully self-contained and the resolved id auditable in one
#: place rather than silently following upstream drift.
DEFAULT_CLOUD_MODEL = "gpt-5-mini"

#: Model name prefixes production routes through the Responses API instead
#: of Chat Completions (src/dev_health_ops/llm/providers/openai.py's
#: _is_gpt5_family) -- and which reject a caller-selected temperature
#: (openai_capabilities.supports_temperature). Duplicated here rather than
#: imported for the same "self-contained trial client" reason as
#: DEFAULT_CLOUD_MODEL above.
_RESPONSES_API_MODEL_PREFIXES = ("gpt-5", "gpt-6", "openai/gpt-oss")

#: Model prefixes that reject a caller-selected ``temperature``, matching
#: production's ``openai_capabilities.supports_temperature`` (gpt-5/o1/o3).
#: A SUPERSET of ``_RESPONSES_API_MODEL_PREFIXES`` -- every Responses-API
#: model is also a no-temperature model, but o1/o3 are no-temperature
#: while still going through Chat Completions in production (routing and
#: temperature-support are two separate production policies; this trial
#: keeps them as two separate, single-source predicates rather than
#: conflating "which API" with "does it accept a caller-selected
#: temperature").
_NO_TEMPERATURE_MODEL_PREFIXES = _RESPONSES_API_MODEL_PREFIXES + ("o1", "o3")

#: Hosts LLM_PROVIDER=local is permitted to reach. Anything else is a real
#: (possibly billable) endpoint, which "local" must never silently become a
#: pass-through name for.
_LOCAL_ALLOWED_HOSTS = frozenset({"localhost", "127.0.0.1", "host.docker.internal"})


class LLMUnavailable(Exception):
    """The configured provider could not be reached, is misconfigured, or is
    not implemented.

    Callers must never catch this to fall back to a mock -- an unreachable
    provider is a real, reportable smoke finding ("LM Studio isn't up"), not
    something to paper over with fabricated output. See
    :mod:`harness.arms.extraction`'s handling: this exception propagates all
    the way to ``ArmResponse.not_run``, the same loud-NOT_RUN path every
    other arm uses when it cannot be measured.
    """


def _check_local_host(base_url: str) -> None:
    host = urlparse(base_url).hostname
    if host not in _LOCAL_ALLOWED_HOSTS:
        raise LLMUnavailable(
            f"LLM_PROVIDER=local but base URL {base_url!r} resolves to host "
            f"{host!r}, which is not in the local allowlist "
            f"{sorted(_LOCAL_ALLOWED_HOSTS)}. A real endpoint behind "
            "LLM_PROVIDER=local would bill through a path step 2's cost "
            "authorization never covered -- if this is intentional, that is "
            "exactly what the cloud provider is for."
        )


@dataclass(frozen=True)
class LLMConfig:
    provider: str
    base_url: str
    model: str
    api_key: str
    #: The request window for THIS model. Defaults to the cloud window;
    #: :meth:`for_local` supplies the longer local one. See
    #: ``DEFAULT_LOCAL_TIMEOUT_SECONDS``.
    timeout: float = DEFAULT_CLOUD_TIMEOUT_SECONDS

    @classmethod
    def for_cloud(
        cls,
        *,
        model: str,
        api_key: str,
        base_url: str = DEFAULT_CLOUD_BASE_URL,
        timeout: float = DEFAULT_CLOUD_TIMEOUT_SECONDS,
    ) -> LLMConfig:
        """A cloud config with the model named EXPLICITLY by the caller.

        The run-3 matrix measures a named tier per arm invocation, so the
        model must never come from the ambient environment: ``ops/.env``
        carries the deployed Ask Dev model (``LLM_MODEL``/``OPENAI_MODEL``),
        and an env-first resolution would silently redirect a run labelled
        "gpt-5-mini" at whatever the deployment happened to be configured
        with that day -- producing an artifact whose model column is a lie.
        The API KEY still comes from the environment, because a credential
        is not a measurement parameter.
        """
        if not (api_key or "").strip():
            raise LLMUnavailable(
                "a cloud tier needs a real OPENAI_API_KEY -- an empty key is "
                "not something to silently fall back to a local model for."
            )
        return cls(
            provider="cloud",
            base_url=base_url,
            model=model,
            api_key=api_key.strip(),
            timeout=timeout,
        )

    @classmethod
    def for_local(
        cls,
        *,
        model: str,
        base_url: str = DEFAULT_LOCAL_BASE_URL,
        api_key: str = "lm-studio-local",
        timeout: float = DEFAULT_LOCAL_TIMEOUT_SECONDS,
    ) -> LLMConfig:
        """A local config with the model named EXPLICITLY (see
        :meth:`for_cloud`) and the longer local request window by default.

        Enforces the same host allowlist ``from_env`` does -- an explicit
        constructor must not become the way around it.
        """
        _check_local_host(base_url)
        return cls(
            provider="local",
            base_url=base_url,
            model=model,
            api_key=api_key,
            timeout=timeout,
        )

    @classmethod
    def from_env(cls) -> LLMConfig:
        provider = os.environ.get("LLM_PROVIDER", "local").strip().lower()
        if provider == "local":
            base_url = os.environ.get("LOCAL_LLM_BASE_URL", DEFAULT_LOCAL_BASE_URL)
            _check_local_host(base_url)
            return cls(
                provider="local",
                base_url=base_url,
                model=os.environ.get("LOCAL_LLM_MODEL", DEFAULT_LOCAL_MODEL),
                # LM Studio does not check the key; the OpenAI SDK requires a
                # non-empty string regardless.
                api_key=os.environ.get("LOCAL_LLM_API_KEY", "lm-studio-local"),
                timeout=DEFAULT_LOCAL_TIMEOUT_SECONDS,
            )
        if provider == "cloud":
            api_key = os.environ.get("OPENAI_API_KEY", "").strip()
            if not api_key:
                raise LLMUnavailable(
                    "LLM_PROVIDER=cloud but OPENAI_API_KEY is unset/empty -- "
                    "a real, billable provider needs a real key; this is "
                    "not something to silently fall back to local for."
                )
            return cls(
                provider="cloud",
                base_url=os.environ.get("OPENAI_BASE_URL", DEFAULT_CLOUD_BASE_URL),
                model=os.environ.get("OPENAI_MODEL", DEFAULT_CLOUD_MODEL),
                api_key=api_key,
                timeout=DEFAULT_CLOUD_TIMEOUT_SECONDS,
            )
        raise LLMUnavailable(
            f"unknown LLM_PROVIDER={provider!r}; expected 'local' or 'cloud'"
        )


@dataclass(frozen=True)
class LLMResponse:
    content: str
    model: str
    #: Wall-clock seconds the provider call itself took. Recorded so the
    #: sweep artifact can show WHY a tier behaved as it did: the local tier
    #: answers an order of magnitude more slowly than the cloud tiers, and
    #: a reader comparing pass rates across tiers needs to see that the
    #: cheap tier was given the time it needed rather than guess.
    latency_seconds: float = 0.0


def _timeout_message(cfg: LLMConfig, api_label: str, exc: Exception) -> str:
    """The reason string for a request that exceeded its own window.

    Carries the ``could not reach`` infra marker deliberately: a timeout is
    an infra outcome, so the sweep must record it as a retryable NOT_RUN
    and NEVER as a scored miss (see ``run_measured_sweep.py``'s
    ``_INFRA_FAILURE_MARKER``) -- a slow model that ran out of clock has
    not answered wrongly, it has not answered at all. Names the configured
    window so a reader can tell "the window was too short for this tier"
    apart from "the provider was dead."
    """
    return (
        f"could not reach {cfg.provider} provider at {cfg.base_url} "
        f"(model={cfg.model}, {api_label}): request timed out after the "
        f"configured {cfg.timeout} second window "
        f"({type(exc).__name__}: {exc}). This is an INFRA outcome, never a "
        "model-quality result -- if this tier legitimately needs longer, "
        "raise its configured timeout and re-run; do not score it."
    )


def _is_responses_api_model(model: str) -> bool:
    return (model or "").strip().lower().startswith(_RESPONSES_API_MODEL_PREFIXES)


def _supports_temperature(model: str) -> bool:
    """Single source of truth for whether ``model`` accepts a
    caller-selected ``temperature`` -- shared by both branches of
    :func:`complete` rather than each branch hardcoding its own answer, so
    the two cannot silently drift (see ``_NO_TEMPERATURE_MODEL_PREFIXES``).
    """
    return not (model or "").strip().lower().startswith(_NO_TEMPERATURE_MODEL_PREFIXES)


def complete(
    system_prompt: str,
    user_prompt: str,
    *,
    config: LLMConfig | None = None,
) -> LLMResponse:
    """One completion, raising :class:`LLMUnavailable` on any failure to
    reach the provider, or on a response with nothing to read.

    Deliberately no retry loop: per step-2 direction (unchanged in step 3),
    a call that cannot reach its provider or parse is a reportable finding,
    not something to poll past inside this function -- see this module's
    docstring on ``max_retries=0``. A bounded, LOGGED retry policy for
    infra-level failures only belongs at the sweep-orchestration layer,
    which sees run-level context this function does not; folding it in
    here would blur "the provider was down" into "the provider was down
    and we tried again," a distinction the sweep discipline needs intact.

    Dispatches to the Responses API for ``gpt-5``-family models (matching
    production's ``OpenAIGPT5Provider`` -- see this module's docstring) and
    Chat Completions for everything else (local models via LM Studio, and
    any non-GPT-5 cloud model).
    """
    cfg = config or LLMConfig.from_env()
    try:
        from openai import (
            APIConnectionError,
            APIError,
            APIStatusError,
            APITimeoutError,
            OpenAI,
            omit,
        )
    except ImportError as exc:  # pragma: no cover - openai is a pinned dep
        raise LLMUnavailable(f"openai package not importable: {exc}") from exc

    client = OpenAI(
        base_url=cfg.base_url,
        api_key=cfg.api_key,
        timeout=cfg.timeout,
        max_retries=0,
    )
    started = time.monotonic()

    if _is_responses_api_model(cfg.model):
        try:
            response = client.responses.create(
                model=cfg.model,
                instructions=system_prompt,
                input=user_prompt,
                # gpt-5-family rejects a caller-selected temperature (see
                # module docstring) -- deliberately no "temperature" kwarg.
                # _supports_temperature(cfg.model) is always False for
                # every model routed here (_NO_TEMPERATURE_MODEL_PREFIXES
                # is a superset of _RESPONSES_API_MODEL_PREFIXES), so this
                # omission is never conditional in this branch.
            )
        except APITimeoutError as exc:
            # MUST precede the APIConnectionError branch -- APITimeoutError
            # is a SUBCLASS of it, so ordering these the other way round
            # would swallow every timeout into the generic unreachable
            # message and lose the configured-window detail a reader needs.
            raise LLMUnavailable(_timeout_message(cfg, "responses API", exc)) from exc
        except APIConnectionError as exc:
            # Genuine unreachability (network, DNS) -- shares the "could
            # not reach" marker the sweep's bounded-infra-retry policy
            # matches on. See run_measured_sweep.py.
            raise LLMUnavailable(
                f"could not reach {cfg.provider} provider at {cfg.base_url} "
                f"(model={cfg.model}, responses API): "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        except APIStatusError as exc:
            # A real response the provider actively returned -- auth,
            # rate-limit, bad-request, etc. Not a connectivity problem, so
            # deliberately NOT phrased "could not reach": the sweep's
            # infra-retry policy must never match this and retry a 401 or
            # a 400 that will fail identically every time.
            raise LLMUnavailable(
                f"{cfg.provider} provider at {cfg.base_url} (model={cfg.model}, "
                f"responses API) returned {exc.status_code} "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        except APIError as exc:  # pragma: no cover - catch-all, rare
            raise LLMUnavailable(
                f"{cfg.provider} provider at {cfg.base_url} (model={cfg.model}, "
                f"responses API) raised {type(exc).__name__}: {exc}"
            ) from exc
        content = getattr(response, "output_text", "") or ""
        if not content.strip():
            # Best-effort fallback extraction, matching production's own
            # OpenAIGPT5Provider -- some responses carry text only inside
            # output[].content[] rather than the output_text convenience
            # property.
            parts: list[str] = []
            for item in getattr(response, "output", []) or []:
                for c in getattr(item, "content", []) or []:
                    if getattr(c, "type", None) in ("output_text", "text"):
                        parts.append(getattr(c, "text", "") or "")
            content = "".join(parts)
        if not content.strip():
            raise LLMUnavailable(
                f"{cfg.provider} provider at {cfg.base_url} (model={cfg.model}) "
                "returned no output_text -- nothing to read as a completion"
            )
        return LLMResponse(
            content=content,
            model=cfg.model,
            latency_seconds=time.monotonic() - started,
        )

    # temperature=omit (the SDK's own "omit this parameter" sentinel, not
    # None/0) for a model that rejects a caller-selected value -- e.g. an
    # o1/o3-family model reached via OPENAI_MODEL override, just like
    # gpt-5-family (see _NO_TEMPERATURE_MODEL_PREFIXES) -- rather than
    # sending one the API would reject with a 400.
    try:
        chat_response = client.chat.completions.create(
            model=cfg.model,
            temperature=0 if _supports_temperature(cfg.model) else omit,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
    except APITimeoutError as exc:
        # Ordering matters -- see the responses-API branch's own note.
        raise LLMUnavailable(
            _timeout_message(cfg, "chat completions", exc)
            + " If this is LM Studio: a large local model on a loaded "
            "machine can legitimately exceed a cloud-sized window."
        ) from exc
    except APIConnectionError as exc:
        raise LLMUnavailable(
            f"could not reach {cfg.provider} provider at {cfg.base_url} "
            f"(model={cfg.model}): {type(exc).__name__}: {exc}. If this is "
            "LM Studio: confirm it is running and serving at that address "
            "-- host processes use localhost, containers use "
            "host.docker.internal, and swapping the two looks exactly like "
            "the provider being down."
        ) from exc
    except APIStatusError as exc:
        raise LLMUnavailable(
            f"{cfg.provider} provider at {cfg.base_url} (model={cfg.model}) "
            f"returned {exc.status_code} {type(exc).__name__}: {exc}"
        ) from exc
    except APIError as exc:  # pragma: no cover - catch-all, rare
        raise LLMUnavailable(
            f"{cfg.provider} provider at {cfg.base_url} (model={cfg.model}) "
            f"raised {type(exc).__name__}: {exc}"
        ) from exc
    if not chat_response.choices:
        raise LLMUnavailable(
            f"{cfg.provider} provider at {cfg.base_url} (model={cfg.model}) "
            "returned no choices -- nothing to read as a completion"
        )
    content = chat_response.choices[0].message.content or ""
    return LLMResponse(
        content=content,
        model=cfg.model,
        latency_seconds=time.monotonic() - started,
    )


def extract_json_array(text: str) -> str:
    """Find the first substring starting at a ``[`` that parses as valid
    JSON, ignoring everything else (prose, markdown fences, a stray ``[``
    inside unrelated text that is not itself valid JSON).

    Small local models often wrap structured output in explanation or
    ```json fences despite being told not to; a response can also contain a
    bracket character inside prose that has nothing to do with the intended
    array (e.g. "the answer is [1, 2] but also see [not json] for context").
    A greedy regex over the outermost brackets would grab past the real
    array into unrelated trailing content; scanning candidates with
    ``json.JSONDecoder.raw_decode`` and taking the first one that actually
    parses does not have that failure mode. This does not attempt to fix
    malformed JSON otherwise -- a response with no parseable array at all is
    a real extraction-quality finding, not something to paper over here.
    """
    decoder = json.JSONDecoder()
    start = 0
    while True:
        idx = text.find("[", start)
        if idx == -1:
            return text
        try:
            _, end = decoder.raw_decode(text, idx)
        except json.JSONDecodeError:
            start = idx + 1
            continue
        return text[idx:end]
