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
from dataclasses import dataclass
from urllib.parse import urlparse

DEFAULT_LOCAL_BASE_URL = "http://localhost:1234/v1"
DEFAULT_LOCAL_MODEL = "google/gemma-4-e4b"

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


@dataclass(frozen=True)
class LLMConfig:
    provider: str
    base_url: str
    model: str
    api_key: str

    @classmethod
    def from_env(cls) -> LLMConfig:
        provider = os.environ.get("LLM_PROVIDER", "local").strip().lower()
        if provider == "local":
            base_url = os.environ.get("LOCAL_LLM_BASE_URL", DEFAULT_LOCAL_BASE_URL)
            host = urlparse(base_url).hostname
            if host not in _LOCAL_ALLOWED_HOSTS:
                raise LLMUnavailable(
                    f"LLM_PROVIDER=local but LOCAL_LLM_BASE_URL={base_url!r} "
                    f"resolves to host {host!r}, which is not in the local "
                    f"allowlist {sorted(_LOCAL_ALLOWED_HOSTS)}. A real "
                    "endpoint behind LLM_PROVIDER=local would bill through a "
                    "path step 2's cost authorization never covered -- if "
                    "this is intentional, that is exactly what "
                    "LLM_PROVIDER=cloud is for (not wired yet)."
                )
            return cls(
                provider="local",
                base_url=base_url,
                model=os.environ.get("LOCAL_LLM_MODEL", DEFAULT_LOCAL_MODEL),
                # LM Studio does not check the key; the OpenAI SDK requires a
                # non-empty string regardless.
                api_key=os.environ.get("LOCAL_LLM_API_KEY", "lm-studio-local"),
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
            )
        raise LLMUnavailable(
            f"unknown LLM_PROVIDER={provider!r}; expected 'local' or 'cloud'"
        )


@dataclass(frozen=True)
class LLMResponse:
    content: str
    model: str


def _is_responses_api_model(model: str) -> bool:
    return (model or "").strip().lower().startswith(_RESPONSES_API_MODEL_PREFIXES)


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
        from openai import APIConnectionError, APIError, OpenAI
    except ImportError as exc:  # pragma: no cover - openai is a pinned dep
        raise LLMUnavailable(f"openai package not importable: {exc}") from exc

    client = OpenAI(
        base_url=cfg.base_url, api_key=cfg.api_key, timeout=120.0, max_retries=0
    )

    if _is_responses_api_model(cfg.model):
        try:
            response = client.responses.create(
                model=cfg.model,
                instructions=system_prompt,
                input=user_prompt,
                # gpt-5-family rejects a caller-selected temperature (see
                # module docstring) -- deliberately no "temperature" kwarg.
            )
        except (APIConnectionError, APIError) as exc:
            raise LLMUnavailable(
                f"could not reach {cfg.provider} provider at {cfg.base_url} "
                f"(model={cfg.model}, responses API): "
                f"{type(exc).__name__}: {exc}"
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
        return LLMResponse(content=content, model=cfg.model)

    try:
        chat_response = client.chat.completions.create(
            model=cfg.model,
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
    except (APIConnectionError, APIError) as exc:
        raise LLMUnavailable(
            f"could not reach {cfg.provider} provider at {cfg.base_url} "
            f"(model={cfg.model}): {type(exc).__name__}: {exc}. If this is "
            "LM Studio: confirm it is running and serving at that address "
            "-- host processes use localhost, containers use "
            "host.docker.internal, and swapping the two looks exactly like "
            "the provider being down."
        ) from exc
    if not chat_response.choices:
        raise LLMUnavailable(
            f"{cfg.provider} provider at {cfg.base_url} (model={cfg.model}) "
            "returned no choices -- nothing to read as a completion"
        )
    content = chat_response.choices[0].message.content or ""
    return LLMResponse(content=content, model=cfg.model)


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
