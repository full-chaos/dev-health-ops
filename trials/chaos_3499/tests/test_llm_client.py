"""Unit tests for harness/llm/client.py that need no network at all.

Everything here is pure-Python: config resolution, the host allowlist, and
the JSON-array extraction heuristic. The end-to-end "does it actually talk
to a model" path is covered by test_extraction_smoke.py, which is env-gated
on a reachable provider; nothing here should be.
"""

from __future__ import annotations

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


class _FakeCompletions:
    def __init__(self, response: _FakeChoicesResponse) -> None:
        self._response = response
        self.create_kwargs: dict | None = None

    def create(self, **kwargs) -> _FakeChoicesResponse:
        self.create_kwargs = kwargs
        return self._response


class _FakeChat:
    def __init__(self, completions: _FakeCompletions) -> None:
        self.completions = completions


class _FakeOpenAIClient:
    """Records the kwargs it was constructed with, and returns a
    pre-built response -- stands in for openai.OpenAI without any network
    access at all.
    """

    last_instance: _FakeOpenAIClient | None = None

    def __init__(self, response: _FakeChoicesResponse, **init_kwargs) -> None:
        self.init_kwargs = init_kwargs
        self.chat = _FakeChat(_FakeCompletions(response))
        _FakeOpenAIClient.last_instance = self


def _install_fake_openai(
    monkeypatch: pytest.MonkeyPatch, response: _FakeChoicesResponse
) -> None:
    def _factory(**init_kwargs):
        return _FakeOpenAIClient(response, **init_kwargs)

    monkeypatch.setattr(openai, "OpenAI", _factory)


def test_complete_constructs_client_with_max_retries_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No retry-until-it-works loop, made explicit at the SDK client level
    rather than relying on the library default (which is not zero).
    """
    fake_message = type("Msg", (), {"content": "[]"})()
    fake_choice = type("Choice", (), {"message": fake_message})()
    _install_fake_openai(monkeypatch, _FakeChoicesResponse(choices=[fake_choice]))
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
    _install_fake_openai(monkeypatch, _FakeChoicesResponse(choices=[]))
    cfg = LLMConfig(
        provider="local", base_url="http://localhost:1234/v1", model="fake", api_key="x"
    )
    with pytest.raises(LLMUnavailable, match="no choices"):
        complete("system", "user", config=cfg)
