"""Tests for LLM JSON utilities."""

from __future__ import annotations

from dev_health_ops.llm.json_utils import (
    extract_json_object,
    validate_json_or_empty,
)


class TestValidateJsonOrEmpty:
    def test_valid_json_roundtrips(self) -> None:
        assert validate_json_or_empty('{"a": 1}') == '{"a": 1}'

    def test_valid_json_compact(self) -> None:
        result = validate_json_or_empty('{\n  "a": 1\n}')
        assert result == '{"a": 1}'

    def test_invalid_returns_empty(self) -> None:
        assert validate_json_or_empty("not json") == ""

    def test_empty_returns_empty(self) -> None:
        assert validate_json_or_empty("") == ""
        assert validate_json_or_empty("   ") == ""

    def test_non_ascii_preserved(self) -> None:
        assert validate_json_or_empty('{"n": "café"}') == '{"n": "café"}'


class TestExtractJsonObject:
    def test_direct_object(self) -> None:
        assert extract_json_object('{"a": 1}') == {"a": 1}

    def test_object_within_text(self) -> None:
        text = 'Here is the result: {"a": 1, "b": 2}. Thanks!'
        assert extract_json_object(text) == {"a": 1, "b": 2}

    def test_empty_returns_none(self) -> None:
        assert extract_json_object("") is None
        assert extract_json_object("   \n") is None

    def test_no_braces_returns_none(self) -> None:
        assert extract_json_object("no braces here") is None

    def test_malformed_returns_none(self) -> None:
        assert extract_json_object("{not valid json}") is None

    def test_non_object_returns_none(self) -> None:
        # Arrays are not objects.
        assert extract_json_object("[1, 2, 3]") is None

    def test_nested_object(self) -> None:
        result = extract_json_object('prefix {"a": {"b": [1, 2]}} suffix')
        assert result == {"a": {"b": [1, 2]}}
