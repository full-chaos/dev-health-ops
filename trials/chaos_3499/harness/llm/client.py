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

    Under ``LLM_PROVIDER=local`` the base URL's host is checked against an
    allowlist (``localhost``, ``127.0.0.1``, ``host.docker.internal``) --
    without this, pointing ``LOCAL_LLM_BASE_URL`` at a real paid endpoint
    would bill through a code path this trial's cost authorization never
    covered (``LLM_PROVIDER=local`` is meant to name "no spend", not "no
    provider name check"). A host outside the allowlist raises
    :class:`LLMUnavailable` naming the reason.
``LOCAL_LLM_MODEL``
    Defaults to ``"google/gemma-4-e4b"``.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from urllib.parse import urlparse

DEFAULT_LOCAL_BASE_URL = "http://localhost:1234/v1"
DEFAULT_LOCAL_MODEL = "google/gemma-4-e4b"

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
    to reach the provider, or on a response with nothing to read.

    Deliberately no retry loop: per step-2 direction, a smoke that cannot
    reach its provider is a reportable finding, not something to poll past.
    ``max_retries=0`` on the client makes that explicit rather than relying
    on the SDK default.
    """
    cfg = config or LLMConfig.from_env()
    try:
        from openai import APIConnectionError, APIError, OpenAI
    except ImportError as exc:  # pragma: no cover - openai is a pinned dep
        raise LLMUnavailable(f"openai package not importable: {exc}") from exc

    client = OpenAI(
        base_url=cfg.base_url, api_key=cfg.api_key, timeout=60.0, max_retries=0
    )
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
    if not response.choices:
        raise LLMUnavailable(
            f"{cfg.provider} provider at {cfg.base_url} (model={cfg.model}) "
            "returned no choices -- nothing to read as a completion"
        )
    content = response.choices[0].message.content or ""
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
