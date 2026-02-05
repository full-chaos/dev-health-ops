from dev_health_ops.api.utils.identity_aliases import (
    build_reverse_alias_map,
    normalize_alias,
)


def test_normalize_alias_strips_whitespace():
    assert normalize_alias("  john.doe@example.com  ") == "john.doe@example.com"


def test_normalize_alias_lowercases():
    assert normalize_alias("JOHN.DOE@EXAMPLE.COM") == "john.doe@example.com"


def test_normalize_alias_handles_empty_string():
    assert normalize_alias("") == ""


def test_normalize_alias_handles_none():
    assert normalize_alias(None) == ""


def test_normalize_alias_combined():
    assert normalize_alias("  John Doe  ") == "john doe"


def test_build_reverse_alias_map_basic():
    aliases = {
        "john.doe@example.com": ["jdoe", "John Doe"],
        "jane.smith@example.com": ["jsmith"],
    }
    reverse = build_reverse_alias_map(aliases)

    assert reverse["jdoe"] == "john.doe@example.com"
    assert reverse["john doe"] == "john.doe@example.com"
    assert reverse["jsmith"] == "jane.smith@example.com"


def test_build_reverse_alias_map_normalizes_keys():
    aliases = {
        "john.doe@example.com": ["  JDOE  ", "John Doe"],
    }
    reverse = build_reverse_alias_map(aliases)

    assert reverse["jdoe"] == "john.doe@example.com"
    assert reverse["john doe"] == "john.doe@example.com"


def test_build_reverse_alias_map_empty():
    aliases = {}
    reverse = build_reverse_alias_map(aliases)
    assert reverse == {}


def test_build_reverse_alias_map_skips_empty_aliases():
    aliases = {
        "john.doe@example.com": ["jdoe", "", "  ", None],
    }
    reverse = build_reverse_alias_map(aliases)

    assert reverse["jdoe"] == "john.doe@example.com"
    assert "" not in reverse
    assert None not in reverse


def test_build_reverse_alias_map_handles_duplicate_aliases():
    aliases = {
        "john.doe@example.com": ["jdoe"],
        "jane.doe@example.com": ["jdoe"],
    }
    reverse = build_reverse_alias_map(aliases)

    assert "jdoe" in reverse
    assert reverse["jdoe"] in ["john.doe@example.com", "jane.doe@example.com"]
