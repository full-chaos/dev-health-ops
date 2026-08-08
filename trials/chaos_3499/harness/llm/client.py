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
    ``"local"`` (default) or ``"cloud"``. ``"cloud"`` is accepted but not
    wired to a real provider yet -- raises :class:`LLMUnavailable` with a
    clear "not implemented" reason rather than silently falling back to
    local. Step 2 scope is local-model plumbing only, per orchestrator
    direction; cloud-vs-local per arm is a step-3 decision.
``LOCAL_LLM_BASE_URL``
    Overrides the local provider's base URL. Defaults to
    ``http://localhost:1234/v1`` -- the address a process running directly
    on the **host** reaches LM Studio at (pytest, scripts, the harness
    invoked from a worktree -- everything this trial actually runs as).
    From **inside a container**, override this to
    ``http://host.docker.internal:1234/v1``, or you will get a
    connection-refused that looks exactly like LM Studio being down when it
    is only the address that is wrong.
``LOCAL_LLM_MODEL``
    Defaults to ``"google/gemma-4-e4b"``.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

DEFAULT_LOCAL_BASE_URL = "http://localhost:1234/v1"
DEFAULT_LOCAL_MODEL = "google/gemma-4-e4b"


class LLMUnavailable(Exception):
    """The configured provider could not be reached, or is not implemented.

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
            return cls(
                provider="local",
                base_url=os.environ.get("LOCAL_LLM_BASE_URL", DEFAULT_LOCAL_BASE_URL),
                model=os.environ.get("LOCAL_LLM_MODEL", DEFAULT_LOCAL_MODEL),
                # LM Studio does not check the key; the OpenAI SDK requires a
                # non-empty string regardless.
                api_key=os.environ.get("LOCAL_LLM_API_KEY", "lm-studio-local"),
            )
        if provider == "cloud":
            raise LLMUnavailable(
                "LLM_PROVIDER=cloud is accepted but not wired to a real "
                "provider yet -- step 2 scope is local-model plumbing only. "
                "Set LLM_PROVIDER=local (or leave it unset)."
            )
        raise LLMUnavailable(
            f"unknown LLM_PROVIDER={provider!r}; expected 'local' or 'cloud'"
        )


@dataclass(frozen=True)
class LLMResponse:
    content: str
    model: str


def complete(
    system_prompt: str,
    user_prompt: str,
    *,
    config: LLMConfig | None = None,
) -> LLMResponse:
    """One chat completion, raising :class:`LLMUnavailable` on any failure
    to reach the provider.

    Deliberately no retry loop: per step-2 direction, a smoke that cannot
    reach its provider is a reportable finding, not something to poll past.
    """
    cfg = config or LLMConfig.from_env()
    try:
        from openai import APIConnectionError, APIError, OpenAI
    except ImportError as exc:  # pragma: no cover - openai is a pinned dep
        raise LLMUnavailable(f"openai package not importable: {exc}") from exc

    client = OpenAI(base_url=cfg.base_url, api_key=cfg.api_key, timeout=60.0)
    try:
        response = client.chat.completions.create(
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
    content = response.choices[0].message.content or ""
    return LLMResponse(content=content, model=cfg.model)


_JSON_ARRAY_RE = re.compile(r"\[.*\]", re.DOTALL)


def extract_json_array(text: str) -> str:
    """Best-effort strip of prose/markdown fences around a JSON array.

    Small local models often wrap structured output in explanation or
    ```json fences despite being told not to. This does not attempt to fix
    malformed JSON -- a response that still fails to parse after this is a
    real extraction-quality finding, not something to paper over here.
    """
    match = _JSON_ARRAY_RE.search(text)
    return match.group(0) if match else text
