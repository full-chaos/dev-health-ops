"""Tests for shared normalize helpers."""

from __future__ import annotations

from dev_health_ops.providers.normalize_helpers import (
    as_dict,
    as_int,
    as_node_list,
    as_str,
    get_attr,
    get_nested,
    labels_from_nodes,
)


class TestAsStr:
    def test_none_returns_none(self) -> None:
        assert as_str(None) is None

    def test_string_passthrough(self) -> None:
        assert as_str("hello") == "hello"

    def test_int_coerced(self) -> None:
        assert as_str(42) == "42"

    def test_empty_string_kept(self) -> None:
        assert as_str("") == ""


class TestAsInt:
    def test_none_returns_none(self) -> None:
        assert as_int(None) is None

    def test_bool_coerced(self) -> None:
        assert as_int(True) == 1
        assert as_int(False) == 0

    def test_float_truncated(self) -> None:
        assert as_int(3.9) == 3

    def test_numeric_string(self) -> None:
        assert as_int("42") == 42

    def test_non_numeric_string_returns_none(self) -> None:
        assert as_int("abc") is None

    def test_dict_returns_none(self) -> None:
        assert as_int({"x": 1}) is None


class TestAsDict:
    def test_dict_passthrough(self) -> None:
        assert as_dict({"a": 1}) == {"a": 1}

    def test_non_dict_returns_empty(self) -> None:
        assert as_dict(None) == {}
        assert as_dict("x") == {}
        assert as_dict([1, 2]) == {}


class TestAsNodeList:
    def test_list_of_dicts(self) -> None:
        assert as_node_list([{"a": 1}, {"b": 2}]) == [{"a": 1}, {"b": 2}]

    def test_mixed_filtered(self) -> None:
        assert as_node_list([{"a": 1}, "x", None, {"b": 2}]) == [{"a": 1}, {"b": 2}]

    def test_non_list_returns_empty(self) -> None:
        assert as_node_list(None) == []
        assert as_node_list({"x": 1}) == []


class TestLabelsFromNodes:
    def test_dict_nodes(self) -> None:
        assert labels_from_nodes([{"name": "bug"}, {"name": "ui"}]) == ["bug", "ui"]

    def test_object_nodes(self) -> None:
        class N:
            def __init__(self, name: str) -> None:
                self.name = name

        assert labels_from_nodes([N("one"), N("two")]) == ["one", "two"]

    def test_none_returns_empty(self) -> None:
        assert labels_from_nodes(None) == []

    def test_missing_name_skipped(self) -> None:
        assert labels_from_nodes([{"x": 1}, {"name": "keep"}]) == ["keep"]


class TestGetAttr:
    def test_dict_lookup(self) -> None:
        assert get_attr({"a": 1}, "a") == 1

    def test_attribute_lookup(self) -> None:
        class Obj:
            x = "val"

        assert get_attr(Obj(), "x") == "val"

    def test_missing_returns_none(self) -> None:
        assert get_attr({}, "missing") is None
        assert get_attr(object(), "missing") is None


class TestGetNested:
    def test_single_key_dict(self) -> None:
        assert get_nested({"a": 1}, "a") == 1

    def test_chain_dict(self) -> None:
        assert get_nested({"a": {"b": {"c": 42}}}, "a", "b", "c") == 42

    def test_chain_mixed(self) -> None:
        class Leaf:
            def __init__(self, v: int) -> None:
                self.v = v

        assert get_nested({"node": Leaf(7)}, "node", "v") == 7

    def test_chain_none_short_circuits(self) -> None:
        assert get_nested({"a": None}, "a", "b") is None
