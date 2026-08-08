"""Unit tests for harness/llm/client.py that need no network at all.

Everything here is pure-Python: config resolution, the host allowlist, and
the JSON-array extraction heuristic. The end-to-end "does it actually talk
to a model" path is covered by test_extraction_smoke.py, which is env-gated
on a reachable provider; nothing here should be.
"""

from __future__ import annotations

import dataclasses

import httpx
import openai
import pytest

from ..harness.llm.client import LLMConfig, LLMUnavailable, complete, extract_json_array

# --------------------------------------------------------------------------
# Finding 2: LLM_PROVIDER=local must not become a name for "any base URL,
# including a real paid endpoint".
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "base_url",
    [
        "http://localhost:1234/v1",
        "http://127.0.0.1:1234/v1",
        "http://host.docker.internal:1234/v1",
    ],
)
def test_local_provider_accepts_allowlisted_hosts(
    monkeypatch: pytest.MonkeyPatch, base_url: str
) -> None:
    monkeypatch.setenv("LLM_PROVIDER", "local")
    monkeypatch.setenv("LOCAL_LLM_BASE_URL", base_url)
    cfg = LLMConfig.from_env()
    assert cfg.base_url == base_url


@pytest.mark.parametrize(
    "base_url",
    [
        "https://api.openai.com/v1",
        "https://my-billed-proxy.example.com/v1",
        "http://evil.localhost.attacker.example/v1",
    ],
)
def test_local_provider_rejects_a_real_endpoint(
    monkeypatch: pytest.MonkeyPatch, base_url: str
) -> None:
    """Regression: LOCAL_LLM_BASE_URL could point at a paid endpoint while
    LLM_PROVIDER=local, billing through a code path step 2's cost
    authorization never covered. The guard must be host-based, not a
    same-string check against the default -- "evil.localhost.attacker.example"
    contains "localhost" as a substring but is not the localhost host.
    """
    monkeypatch.setenv("LLM_PROVIDER", "local")
    monkeypatch.setenv("LOCAL_LLM_BASE_URL", base_url)
    with pytest.raises(LLMUnavailable, match="not in the local allowlist"):
        LLMConfig.from_env()


# --------------------------------------------------------------------------
# Finding 4: extract_json_array must not greedily span past the real array
# into unrelated trailing content that merely contains another bracket.
# --------------------------------------------------------------------------


def test_extract_json_array_stops_at_the_first_valid_array() -> None:
    """Regression: the old \\[.*\\] regex was greedy across the WHOLE text,
    so a response like this one had its match run all the way to the LAST
    ']' in the text, swallowing prose containing an unrelated, invalid
    bracket in between.
    """
    text = "Sure! The answer is [1, 2] but also note [not json at all] for context."
    assert extract_json_array(text) == "[1, 2]"


def test_extract_json_array_strips_markdown_fences() -> None:
    text = '```json\n[{"a": 1}]\n```'
    assert extract_json_array(text) == '[{"a": 1}]'


def test_extract_json_array_handles_nested_brackets() -> None:
    text = 'noise [1, [2, 3], {"k": [4]}] trailing prose with a ] stray bracket'
    assert extract_json_array(text) == '[1, [2, 3], {"k": [4]}]'


def test_extract_json_array_returns_original_text_when_nothing_parses() -> None:
    text = "no arrays here at all, just [ this is not json"
    assert extract_json_array(text) == text


# --------------------------------------------------------------------------
# Findings 8 and 9: complete() must construct its client with max_retries=0
# explicitly, and must never index into an empty choices list.
# --------------------------------------------------------------------------


class _FakeChoicesResponse:
    def __init__(self, choices: list) -> None:
        self.choices = choices


class _FakeResponsesResult:
    def __init__(self, output_text: str) -> None:
        self.output_text = output_text
        self.output: list = []


class _RaisingOrReturning:
    """A fake ``create()`` endpoint: records the kwargs it was called with,
    then either raises a pre-set exception (simulating a real SDK failure)
    or returns a pre-built response -- never both. One instance is a
    single, independent endpoint (chat completions XOR responses); the two
    branches of :func:`complete` each get their OWN instance below, so a
    test exercising one never accidentally observes or feeds the other.
    """

    def __init__(self, response=None, error: Exception | None = None) -> None:
        self._response = response
        self._error = error
        self.create_kwargs: dict | None = None

    def create(self, **kwargs):
        self.create_kwargs = kwargs
        if self._error is not None:
            raise self._error
        return self._response


class _FakeChat:
    def __init__(self, completions: _RaisingOrReturning) -> None:
        self.completions = completions


class _FakeOpenAIClient:
    """Records the kwargs it was constructed with; ``.chat.completions``
    and ``.responses`` are two INDEPENDENT fake endpoints (only one is
    exercised in the cloud client's mutually-exclusive dispatch, but they
    must never share state) -- stands in for openai.OpenAI without any
    network access at all.
    """

    last_instance: _FakeOpenAIClient | None = None

    def __init__(
        self,
        chat_response=None,
        chat_error: Exception | None = None,
        responses_response=None,
        responses_error: Exception | None = None,
        **init_kwargs,
    ) -> None:
        self.init_kwargs = init_kwargs
        self.chat = _FakeChat(_RaisingOrReturning(chat_response, chat_error))
        self.responses = _RaisingOrReturning(responses_response, responses_error)
        _FakeOpenAIClient.last_instance = self


def _install_fake_openai(
    monkeypatch: pytest.MonkeyPatch,
    *,
    chat_response: _FakeChoicesResponse | None = None,
    chat_error: Exception | None = None,
    responses_response: _FakeResponsesResult | None = None,
    responses_error: Exception | None = None,
) -> None:
    def _factory(**init_kwargs):
        return _FakeOpenAIClient(
            chat_response=chat_response,
            chat_error=chat_error,
            responses_response=responses_response,
            responses_error=responses_error,
            **init_kwargs,
        )

    monkeypatch.setattr(openai, "OpenAI", _factory)


def _connection_error(message: str = "boom") -> openai.APIConnectionError:
    request = httpx.Request("POST", "https://api.openai.com/v1/chat/completions")
    return openai.APIConnectionError(message=message, request=request)


def _status_error(status_code: int, message: str = "denied") -> openai.APIStatusError:
    request = httpx.Request("POST", "https://api.openai.com/v1/chat/completions")
    response = httpx.Response(status_code, request=request, text=message)
    return openai.APIStatusError(message=message, response=response, body=None)


def test_complete_constructs_client_with_max_retries_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No retry-until-it-works loop, made explicit at the SDK client level
    rather than relying on the library default (which is not zero).
    """
    fake_message = type("Msg", (), {"content": "[]"})()
    fake_choice = type("Choice", (), {"message": fake_message})()
    _install_fake_openai(
        monkeypatch, chat_response=_FakeChoicesResponse(choices=[fake_choice])
    )
    cfg = LLMConfig(
        provider="local", base_url="http://localhost:1234/v1", model="fake", api_key="x"
    )
    complete("system", "user", config=cfg)
    assert _FakeOpenAIClient.last_instance is not None
    assert _FakeOpenAIClient.last_instance.init_kwargs["max_retries"] == 0


def test_complete_raises_on_empty_choices(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: `response.choices[0]` would IndexError on an empty
    choices list instead of raising the same honest LLMUnavailable every
    other provider failure produces.
    """
    _install_fake_openai(monkeypatch, chat_response=_FakeChoicesResponse(choices=[]))
    cfg = LLMConfig(
        provider="local", base_url="http://localhost:1234/v1", model="fake", api_key="x"
    )
    with pytest.raises(LLMUnavailable, match="no choices"):
        complete("system", "user", config=cfg)


# --------------------------------------------------------------------------
# #1603 finding 3: APIConnectionError (infra, retryable by the sweep) and
# APIStatusError (a real response the provider returned -- auth, rate
# limit, bad request; never retried) must produce DIFFERENT LLMUnavailable
# messages. The sweep's bounded-infra-retry policy
# (run_measured_sweep.py's _INFRA_FAILURE_MARKER) matches on "could not
# reach" -- a status error's message must never contain that phrase, or a
# permanent config/quota error would get retried as if it were transient.
# --------------------------------------------------------------------------

_CLOUD_CFG = LLMConfig(
    provider="cloud",
    base_url="https://api.openai.com/v1",
    model="gpt-5-mini",
    api_key="x",
)


def test_connection_error_message_is_the_infra_marker_chat_completions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_openai(monkeypatch, chat_error=_connection_error())
    cfg = LLMConfig(
        provider="local", base_url="http://localhost:1234/v1", model="fake", api_key="x"
    )
    with pytest.raises(LLMUnavailable, match="could not reach"):
        complete("system", "user", config=cfg)


def test_status_error_message_is_not_the_infra_marker_chat_completions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 401/429/etc is a REAL answer from the provider, not a
    connectivity problem -- it must never be phrased "could not reach",
    or the sweep's bounded infra-retry would retry a permanent error.
    """
    _install_fake_openai(monkeypatch, chat_error=_status_error(401, "bad key"))
    cfg = LLMConfig(
        provider="local", base_url="http://localhost:1234/v1", model="fake", api_key="x"
    )
    with pytest.raises(LLMUnavailable) as exc_info:
        complete("system", "user", config=cfg)
    message = str(exc_info.value)
    assert "could not reach" not in message
    assert "401" in message
    assert "APIStatusError" in message or "AuthenticationError" in message


def test_connection_error_message_is_the_infra_marker_responses_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_openai(monkeypatch, responses_error=_connection_error())
    with pytest.raises(LLMUnavailable, match="could not reach"):
        complete("system", "user", config=_CLOUD_CFG)


def test_status_error_message_is_not_the_infra_marker_responses_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_openai(
        monkeypatch, responses_error=_status_error(429, "rate limited")
    )
    with pytest.raises(LLMUnavailable) as exc_info:
        complete("system", "user", config=_CLOUD_CFG)
    message = str(exc_info.value)
    assert "could not reach" not in message
    assert "429" in message


# --------------------------------------------------------------------------
# #1603 finding 4: temperature support must be derived from ONE shared
# predicate (_supports_temperature), not hardcoded per branch -- an
# o1/o3-family model reached via OPENAI_MODEL falls through to the Chat
# Completions branch (it is not gpt-5-family, so _is_responses_api_model
# is False for it) and must NOT get temperature=0, which the real API
# rejects for that family exactly like it does for gpt-5.
# --------------------------------------------------------------------------


def test_o3_mini_chat_completions_call_omits_temperature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_message = type("Msg", (), {"content": "[]"})()
    fake_choice = type("Choice", (), {"message": fake_message})()
    _install_fake_openai(
        monkeypatch, chat_response=_FakeChoicesResponse(choices=[fake_choice])
    )
    cfg = LLMConfig(
        provider="cloud",
        base_url="https://api.openai.com/v1",
        model="o3-mini",
        api_key="x",
    )
    complete("system", "user", config=cfg)
    assert _FakeOpenAIClient.last_instance is not None
    kwargs = _FakeOpenAIClient.last_instance.chat.completions.create_kwargs
    assert kwargs is not None
    assert kwargs["temperature"] is openai.omit, (
        "o3-mini rejects a caller-selected temperature -- the kwarg must "
        "be the SDK's omit sentinel, not 0 and not absent-but-unchecked"
    )


def test_temperature_supporting_model_still_gets_temperature_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control for the test above: the fix must not accidentally omit
    temperature for every model.
    """
    fake_message = type("Msg", (), {"content": "[]"})()
    fake_choice = type("Choice", (), {"message": fake_message})()
    _install_fake_openai(
        monkeypatch, chat_response=_FakeChoicesResponse(choices=[fake_choice])
    )
    cfg = LLMConfig(
        provider="local", base_url="http://localhost:1234/v1", model="fake", api_key="x"
    )
    complete("system", "user", config=cfg)
    assert _FakeOpenAIClient.last_instance is not None
    kwargs = _FakeOpenAIClient.last_instance.chat.completions.create_kwargs
    assert kwargs is not None
    assert kwargs["temperature"] == 0


def test_gpt5_responses_api_call_never_sends_temperature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_openai(
        monkeypatch, responses_response=_FakeResponsesResult(output_text="[]")
    )
    complete("system", "user", config=_CLOUD_CFG)
    assert _FakeOpenAIClient.last_instance is not None
    kwargs = _FakeOpenAIClient.last_instance.responses.create_kwargs
    assert kwargs is not None
    assert "temperature" not in kwargs


# --------------------------------------------------------------------------
# Run-3 amendment (chris, 2026-08-08): the local tier answers far more
# slowly than the cloud tiers, and a slow answer read as a failure would
# manufacture a false negative for the cheapest arm in the matrix. Three
# things have to hold, and none of them held before this round:
#
#   1. the timeout is CONFIGURED PER MODEL, not one hardcoded number;
#   2. a timeout is classified as INFRA (the sweep's retryable NOT_RUN
#      marker) and says so in words a reader of the artifact can act on;
#   3. a call's latency is measurable at all.
# --------------------------------------------------------------------------


def _timeout_error(message: str = "timed out") -> openai.APITimeoutError:
    request = httpx.Request("POST", "https://api.openai.com/v1/chat/completions")
    return openai.APITimeoutError(request=request)


def test_local_config_gets_a_longer_timeout_than_cloud() -> None:
    """The whole point of the per-model timeout: a locally-hosted model on
    a developer laptop needs a materially longer window than a cloud API
    call, and giving both the same window is what turns local slowness into
    a fabricated failure.
    """
    local = LLMConfig.for_local(model="google/gemma-4-e4b")
    cloud = LLMConfig.for_cloud(model="gpt-5-nano", api_key="x")
    assert local.timeout > cloud.timeout


def test_complete_passes_the_configured_timeout_to_the_sdk_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard: the timeout was hardcoded to 120.0 at the client
    construction site, so a config carrying a longer window had no effect
    whatsoever on the actual call.
    """
    fake_message = type("Msg", (), {"content": "[]"})()
    fake_choice = type("Choice", (), {"message": fake_message})()
    _install_fake_openai(
        monkeypatch, chat_response=_FakeChoicesResponse(choices=[fake_choice])
    )
    cfg = LLMConfig(
        provider="local",
        base_url="http://localhost:1234/v1",
        model="fake",
        api_key="x",
        timeout=987.0,
    )
    complete("system", "user", config=cfg)
    assert _FakeOpenAIClient.last_instance is not None
    assert _FakeOpenAIClient.last_instance.init_kwargs["timeout"] == 987.0


@pytest.mark.parametrize("branch", ["chat", "responses"])
def test_timeout_is_classified_as_infra_and_names_the_window(
    monkeypatch: pytest.MonkeyPatch, branch: str
) -> None:
    """A timeout must (a) carry the sweep's infra marker so it becomes a
    retryable NOT_RUN rather than a scored result, and (b) name the window
    it exceeded, so a reader of the artifact can tell "the model is slower
    than the window we gave it" from "the machine was unreachable" -- the
    exact false-positive risk the local tier introduces.
    """
    if branch == "chat":
        _install_fake_openai(monkeypatch, chat_error=_timeout_error())
        cfg = LLMConfig(
            provider="local",
            base_url="http://localhost:1234/v1",
            model="fake",
            api_key="x",
            timeout=42.0,
        )
    else:
        _install_fake_openai(monkeypatch, responses_error=_timeout_error())
        cfg = dataclasses.replace(_CLOUD_CFG, timeout=42.0)
    with pytest.raises(LLMUnavailable) as exc_info:
        complete("system", "user", config=cfg)
    message = str(exc_info.value)
    assert "could not reach" in message, (
        "a timeout is an infra failure -- it must match the sweep's "
        "_INFRA_FAILURE_MARKER so it becomes NOT_RUN, never a scored miss"
    )
    assert "timed out" in message
    assert "42.0" in message, (
        "the configured window must appear in the reason, or a reader "
        "cannot tell a too-short window from a dead provider"
    )


def test_complete_reports_call_latency(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_message = type("Msg", (), {"content": "[]"})()
    fake_choice = type("Choice", (), {"message": fake_message})()
    _install_fake_openai(
        monkeypatch, chat_response=_FakeChoicesResponse(choices=[fake_choice])
    )
    cfg = LLMConfig(
        provider="local", base_url="http://localhost:1234/v1", model="fake", api_key="x"
    )
    response = complete("system", "user", config=cfg)
    assert response.latency_seconds >= 0.0


def test_for_cloud_requires_a_real_key() -> None:
    """The explicit constructor must keep the same fail-closed behaviour
    ``from_env`` has: an empty key is a real LLMUnavailable, never a
    silent attempt against the billable API.
    """
    with pytest.raises(LLMUnavailable, match="key"):
        LLMConfig.for_cloud(model="gpt-5-nano", api_key="")


def test_for_local_rejects_a_non_allowlisted_host() -> None:
    """The explicit constructor must not become a way around the local
    host allowlist that ``from_env`` enforces.
    """
    with pytest.raises(LLMUnavailable, match="not in the local allowlist"):
        LLMConfig.for_local(
            model="google/gemma-4-e4b", base_url="https://api.openai.com/v1"
        )
