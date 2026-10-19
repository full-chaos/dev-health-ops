"""Python leg of the jwks.v1 property sweep.

contracts/auth/v1/sweeps/jwks.property-sweep.json is the closed enumeration of
where the schema differs from the real consumer (dev-health-go authverify).
The Go test drives the consumer and the schema; this file and the ajv runner
drive the schema alone, against the SAME literal verdicts. No expectation is
computed here.
"""

from __future__ import annotations

import copy
import json
import re
import unicodedata
from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.authclient.contracts import (
    contracts_dir,
    strictly_anchored,
    violations,
)

SURFACE = "jwks.v1"
SWEEP = json.loads(
    (contracts_dir() / "sweeps" / "jwks.property-sweep.json").read_text(
        encoding="utf-8"
    )
)
ROWS = SWEEP["rows"]


def _expand(value: Any) -> Any:
    if isinstance(value, dict) and "$repeat" in value:
        unit, count = value["$repeat"]
        return value.get("prefix", "") + unit * count + value.get("suffix", "")
    return value


def build(ops: list[dict[str, Any]]) -> Any:
    doc: Any = copy.deepcopy(SWEEP["base_document"])
    for op in ops:
        path = op["path"]
        if not path:
            doc = _expand(op["set"])
            continue
        parent = doc
        for step in path[:-1]:
            parent = parent[step]
        last = path[-1]
        if "rename" in op:
            parent[op["rename"]] = parent.pop(last)
        elif op.get("delete"):
            del parent[last]
        elif isinstance(parent, list) and last == len(parent):
            parent.append(_expand(op["set"]))
        else:
            parent[last] = _expand(op["set"])
    return doc


def verdict(doc: Any) -> str:
    return "refuse" if violations(SURFACE, doc) else "accept"


def _class_of(consumer: str, schema: str) -> str:
    if consumer == schema:
        return "agreement"
    return "narrowing" if consumer == "accept" else "client_enforced"


def test_the_sweep_is_not_empty_and_ids_are_unique() -> None:
    assert len(ROWS) > 100
    ids = [r["id"] for r in ROWS]
    assert len(ids) == len(set(ids))
    assert SWEEP["unicode_categories"]


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["id"])
def test_every_row_has_the_schema_verdict_its_literal_names(
    row: dict[str, Any],
) -> None:
    assert row["class"] == _class_of(row["consumer"], row["schema"])
    assert verdict(build(row["ops"])) == row["schema"]


@pytest.mark.parametrize(
    "entry", SWEEP["unicode_categories"], ids=lambda e: e["category"]
)
def test_every_unicode_category_representative_is_refused_as_a_kid(
    entry: dict[str, Any],
) -> None:
    rune = chr(int(entry["rune"].removeprefix("U+"), 16))
    doc = copy.deepcopy(SWEEP["base_document"])
    doc["keys"][0]["kid"] = "a" + rune + "b"
    assert verdict(doc) == entry["schema"]


def test_every_printable_ascii_character_is_an_accepted_kid() -> None:
    for code in range(0x21, 0x7F):
        doc = copy.deepcopy(SWEEP["base_document"])
        doc["keys"][0]["kid"] = chr(code)
        assert verdict(doc) == "accept", chr(code)


def test_python_dollar_would_accept_a_trailing_newline_and_the_loader_does_not() -> (
    None
):
    """P11. Bare ``re`` matches ``$`` before a final newline; the schema loader
    must not. Both halves are asserted so the guard cannot be removed unseen."""
    for pattern, text in (
        (r"^[!-~]{1,256}$", "a\n"),
        (r"^[A-Za-z0-9_-]{43}$", SWEEP["base_document"]["keys"][0]["x"] + "\n"),
    ):
        assert re.search(pattern, text), "the hazard this test guards is gone"
        assert not strictly_anchored(pattern).search(text)


def test_every_trailing_newline_row_exists_for_kid_and_x() -> None:
    ids = {r["id"] for r in ROWS}
    assert {"P4-trailing-lf", "P7-trailing-lf"} <= ids


def test_the_sweep_file_is_where_the_go_test_reads_it() -> None:
    assert Path(contracts_dir() / "sweeps" / "jwks.property-sweep.json").is_file()


def test_every_narrowing_is_in_a_declared_class_the_schema_names() -> None:
    classes = {c["id"]: 0 for c in SWEEP.get("declared_classes", [])}
    assert classes, "the sweep declares no narrowing classes"
    for row in ROWS + BYTE_ROWS:
        if row["class"] == "agreement":
            assert "declared_as" not in row, row["id"]
            continue
        assert row.get("declared_as") in classes, row["id"]
        classes[row["declared_as"]] += 1
    schema = json.loads(
        (contracts_dir() / "jsonschema" / "jwks.v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    for class_id, rows in classes.items():
        assert rows, f"class {class_id} has no row"
        assert f"[{class_id}]" in schema["description"], class_id


BYTE_ROWS = SWEEP["byte_layer_rows"]


def test_the_inventory_is_closed() -> None:
    inventory = SWEEP["inventory"]
    assert len(ROWS) == inventory["rows"]
    counts: dict[str, int] = {}
    for row in ROWS:
        counts[row["property"]] = counts.get(row["property"], 0) + 1
    assert counts == inventory["by_property"]
    assert len(SWEEP["unicode_categories"]) == inventory["unicode_categories"]
    assert len(BYTE_ROWS) == inventory["byte_layer_rows"]
    assert sorted(SWEEP["unicode_categories_excluded"]) == ["Cs", "LC"]
    assert inventory["unicode_categories_excluded"] == ["Cs", "LC"]


def test_every_unicode_category_python_knows_has_an_entry_or_is_cs() -> None:
    """Derived from unicodedata, independent of the sweep file's own list."""
    known = {
        unicodedata.category(chr(c))
        for c in range(0x110000)
        if not 0xD800 <= c <= 0xDFFF
    }
    listed = {e["category"] for e in SWEEP["unicode_categories"]}
    assert known <= listed, sorted(known - listed)


def test_every_code_point_as_x_and_as_kid_matches_the_literal_alphabets() -> None:
    alphabet = set(SWEEP["alphabets"]["x_characters"])
    assert len(alphabet) == 64
    lo, hi = (int(v.removeprefix("U+"), 16) for v in SWEEP["alphabets"]["kid_range"])
    prefix = SWEEP["base_document"]["keys"][0]["x"][:42]
    astral_probes = {
        base + offset
        for base in range(0x10000, 0x110000, 0x10000)
        for offset in (0, 1, 0xFFFF)
    }
    astral_probes |= {
        int(e["rune"].removeprefix("U+"), 16)
        for e in SWEEP["unicode_categories"]
        if int(e["rune"].removeprefix("U+"), 16) > 0xFFFF
    }
    for code in range(0x110000):
        if 0xD800 <= code <= 0xDFFF:
            continue
        # dense through the first 0x3000, then a stride; Go covers all of the BMP
        if code not in astral_probes and (
            (code >= 0x3000 and code % 7) or (code > 0xFFFF and code % 61)
        ):
            continue
        char = chr(code)
        doc = copy.deepcopy(SWEEP["base_document"])
        doc["keys"][0]["x"] = prefix + char
        assert verdict(doc) == ("accept" if char in alphabet else "refuse"), hex(code)
        doc = copy.deepcopy(SWEEP["base_document"])
        doc["keys"][0]["kid"] = char
        assert verdict(doc) == ("accept" if lo <= code <= hi else "refuse"), hex(code)


def _parse(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8"))


@pytest.mark.parametrize("row", BYTE_ROWS, ids=lambda r: r["id"])
def test_every_byte_layer_row_has_the_schema_path_verdict_its_literal_names(
    row: dict[str, Any],
) -> None:
    assert row["class"] == _class_of(row["consumer"], row["schema"])
    raw = row["raw"].encode("utf-8") if "raw" in row else bytes.fromhex(row["raw_hex"])
    try:
        got = verdict(_parse(raw))
    except ValueError:
        got = "refuse"
    assert got == row["schema"]
