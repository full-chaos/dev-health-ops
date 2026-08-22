#!/usr/bin/env python3
"""Shared compute-port parity comparator (CHAOS-3092 P0, CHAOS-3090 clause 4).

Two claims live in this tool and they are deliberately kept apart, because they
are proven by different evidence and neither implies the other:

``rows``
    **Algorithm row parity.** Two implementations of the same compute kind ran
    against two isolated scratch destinations that were seeded from the *same*
    producer-derived fixture. Every declared output table is compared by
    semantic primary key at four levels -- absolute count, key-set digest,
    canonical row digest, and sampled full-row differences -- and, with
    ``--repeat``, the declared update/deletion/tombstone/idempotency behaviour
    is re-checked after a second execution. The verdict says whether the two
    implementations write the same product rows.

``runtime``
    **Operational health.** One normalized Go runtime observation is compared
    against the pinned ``v0-celery-baseline`` capture using the
    ``v3-canary-release-proof`` thresholds. The verdict says whether the Go
    runtime behaves within the recorded Celery operational envelope. It says
    nothing about product rows.

A report never merges the two: every report carries an explicit ``claim`` field
and the CLI refuses to emit one report for both.

Numeric handling follows the ``UseNumber``/``big.Rat`` precedent established in
``internal/jobs/workgraph/publisher.go`` (see ``sameJSON`` there): JSON numbers
are decoded without going through float64 and compared by exact rational value,
so ``1``/``1.0``/``1e0`` compare equal while 9007199254740992 and
9007199254740993 stay distinct. Float tolerance is only ever available
per-field, must carry a written reason, and is recorded in the report as an
explicit reduction of what was proven. There is no global tolerance and the
manifest schema has no place to put one.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shlex
import subprocess
import sys
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_SCHEMA_VERSION = 1
REPORT_SCHEMA_VERSION = 1

CLAIM_ROWS = "algorithm_row_parity"
CLAIM_RUNTIME = "operational_health"

VERDICT_EQUAL = "EQUAL"
VERDICT_DIFFERENT = "DIFFERENT"
VERDICT_INDETERMINATE = "INDETERMINATE"
VERDICT_WITHIN_ENVELOPE = "WITHIN_ENVELOPE"
VERDICT_OUTSIDE_ENVELOPE = "OUTSIDE_ENVELOPE"
VERDICT_UNPROVEN = "UNPROVEN"

SUPPORTED_STORES = ("clickhouse", "postgres")
SUPPORTED_TYPES = (
    "string",
    "int",
    "float",
    "bool",
    "date",
    "datetime",
    "uuid",
    "json",
    "array",
)
VOLATILE_ACTIONS = ("drop", "placeholder", "utc_normalize", "ordinal")
REPEAT_POLICIES = ("idempotent", "append_duplicates", "replace_window", "tombstone")
NUMERIC_POLICIES = ("exact", "absolute_tolerance", "relative_tolerance")

# A parity destination is a scratch database by definition. `default` on the
# shared local ClickHouse container holds real dev data (ops/AGENTS.md safety
# contract), so it is refused as a side outright rather than trusted to be
# read-only: `--left-exec`/`--right-exec` write to whatever they are pointed at.
FORBIDDEN_DATABASES = ("default",)


class ComparisonError(RuntimeError):
    """A safe, non-sensitive reason a comparison cannot be made or trusted."""


# --------------------------------------------------------------------------
# Canonical value handling
# --------------------------------------------------------------------------


def exact_rational(value: Any) -> Fraction:
    """Return the exact rational value of a decoded JSON number.

    ``Fraction`` parses ``Decimal`` and ``int`` without rounding, which is the
    Python equivalent of ``big.Rat.SetString`` in ``publisher.go``. Going via
    ``float`` here would reopen exactly the large-integer collision that
    ``UseNumber`` exists to close.
    """
    if isinstance(value, bool):  # bool is an int subclass; keep it a bool
        raise ComparisonError("boolean is not a number")
    if isinstance(value, Decimal):
        return Fraction(value)
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, Fraction):
        return value
    raise ComparisonError(f"not an exactly-decoded number: {type(value).__name__}")


def as_exact(value: Any) -> Fraction | None:
    """Convert a persisted numeric value to its exact rational value.

    ``Fraction(float)`` is exact for the binary value the database returned --
    it does not re-round -- and ``Fraction(str(...))`` reads a decimal string
    at its written precision. ``None`` for anything arithmetic cannot be
    trusted on, so callers fail closed rather than compare garbage.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, Fraction):
        return value
    if isinstance(value, Decimal):
        return None if not value.is_finite() else Fraction(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return Fraction(value)
    if isinstance(value, str):
        try:
            return Fraction(Decimal(value.strip()))
        except (ValueError, ArithmeticError):
            return None
    return None


def decode_json_preserving_numbers(raw: str) -> Any:
    """Decode JSON text without converting numbers through float64.

    ``parse_float=Decimal`` is the ``UseNumber`` half of the precedent: the
    number keeps its exact value instead of being rounded into a float. The
    ``exact_rational`` comparison below is the ``big.Rat`` half: spelling
    (``1`` vs ``1.0`` vs ``1e0``) does not survive a jsonb/JSON round trip and
    must not be treated as a difference.
    """
    try:
        return json.loads(raw, parse_float=Decimal, parse_int=int)
    except (ValueError, TypeError) as error:
        raise ComparisonError("json_column_undecodable") from error


def canonical_json(value: Any) -> str:
    """Render a decoded JSON value in a key-order- and spelling-independent form."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, Decimal, Fraction)):
        rational = exact_rational(value)
        return f"n:{rational.numerator}/{rational.denominator}"
    if isinstance(value, str):
        return "s:" + json.dumps(value, ensure_ascii=False, sort_keys=True)
    if isinstance(value, Mapping):
        inner = ",".join(
            f"{json.dumps(str(k), ensure_ascii=False)}:{canonical_json(v)}"
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
        )
        return "{" + inner + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(canonical_json(item) for item in value) + "]"
    raise ComparisonError(f"unrepresentable json value: {type(value).__name__}")


def canonical_datetime(value: Any) -> str:
    """Normalize a timestamp to UTC, microsecond precision, explicit ``Z``.

    A naive value is read as UTC: every persisted timestamp in this platform is
    UTC by contract, and ClickHouse ``DateTime('UTC')`` hands back naive values.
    """
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        try:
            value = dt.datetime.fromisoformat(text)
        except ValueError as error:
            raise ComparisonError("timestamp_unparseable") from error
    if not isinstance(value, dt.datetime):
        raise ComparisonError(f"not a timestamp: {type(value).__name__}")
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def canonical_scalar(value: Any, kind: str) -> str:
    """Render one column value as a stable, type-aware canonical string."""
    if value is None:
        return "\x00null"
    if kind == "json":
        decoded = (
            value
            if not isinstance(value, str)
            else decode_json_preserving_numbers(value)
        )
        return "j:" + canonical_json(decoded)
    if kind == "datetime":
        return "t:" + canonical_datetime(value)
    if kind == "date":
        if isinstance(value, dt.datetime):
            value = value.date()
        if isinstance(value, dt.date):
            return "d:" + value.isoformat()
        return "d:" + str(value)
    if kind == "uuid":
        try:
            return "u:" + str(uuid.UUID(str(value)))
        except (ValueError, AttributeError, TypeError) as error:
            raise ComparisonError("uuid_unparseable") from error
    if kind == "bool":
        return "b:" + ("true" if bool(value) else "false")
    if kind == "int":
        return "i:" + str(int(value))
    if kind == "float":
        # repr() of a Python float round-trips exactly; Decimal keeps its own
        # exact value. Neither goes through a lossy string format.
        if isinstance(value, Decimal):
            return "f:" + str(Fraction(value))
        return "f:" + repr(float(value))
    if kind == "array":
        items = list(value) if isinstance(value, (list, tuple)) else [value]
        return (
            "a:[" + ",".join(canonical_scalar(item, "string") for item in items) + "]"
        )
    return "s:" + str(value)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _framed(part: str) -> str:
    """Length-prefix a component so a delimiter inside a value cannot forge one.

    Every joined structure below (a semantic key, a canonical row, a digest
    input) is built from database values. A value containing the delimiter byte
    could otherwise make two different rows serialize identically, which is a
    difference the comparator would silently not see.
    """
    return f"{len(part)}:{part}"


def digest_of_sorted(items: Iterable[str]) -> str:
    """Order-independent digest over a collection of canonical strings."""
    joined = "\x1e".join(_framed(item) for item in sorted(items))
    return sha256_text(joined)


# --------------------------------------------------------------------------
# Manifest model
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class NumericPolicy:
    policy: str
    tolerance: float | None
    reason: str | None

    @property
    def is_exact(self) -> bool:
        return self.policy == "exact"

    def within(self, left: Any, right: Any) -> bool:
        """Compare two persisted values under this field's declared policy.

        The arithmetic is exact rational, never binary float. Coercing both
        operands with ``float()`` first would make 9007199254740992 and
        9007199254740993 -- the pair this module's JSON handling exists to keep
        apart -- compare equal even at a tolerance of zero, so a declared
        numeric policy could be satisfied by values that genuinely differ.
        """
        if left is None or right is None:
            return left is None and right is None
        left_value = as_exact(left)
        right_value = as_exact(right)
        if left_value is None or right_value is None:
            # A value arithmetic cannot be trusted on (NaN, an infinity, an
            # undecodable string) is never "within" anything.
            return False
        tolerance = Fraction(str(self.tolerance if self.tolerance is not None else 0))
        difference = abs(left_value - right_value)
        if self.policy == "absolute_tolerance":
            return difference <= tolerance
        scale = max(abs(left_value), abs(right_value))
        if scale == 0:
            return left_value == right_value
        return difference / scale <= tolerance


@dataclass(frozen=True)
class FieldSpec:
    column: str
    type: str
    numeric: NumericPolicy | None
    volatile: str | None  # one of VOLATILE_ACTIONS

    @property
    def in_row_digest(self) -> bool:
        """Whether the field contributes to the canonical row digest.

        A tolerance-compared field cannot: a digest is exact by construction, so
        including it would report every within-tolerance value as a difference.
        Such fields are compared field-wise on matched keys instead, and the
        report names them under ``digest_excluded_fields`` so a reader can see
        precisely how much the digest proved.
        """
        if self.volatile == "drop":
            return False
        if self.numeric is not None and not self.numeric.is_exact:
            return False
        return True


@dataclass(frozen=True)
class OutputSpec:
    table: str
    store: str
    select: str
    semantic_key: tuple[str, ...]
    fields: tuple[FieldSpec, ...]
    repeat_policy: str
    tombstone_predicate: str | None
    allow_empty: bool

    def field(self, column: str) -> FieldSpec:
        for spec in self.fields:
            if spec.column == column:
                return spec
        raise ComparisonError(f"column not declared in manifest: {self.table}.{column}")

    @property
    def digest_excluded(self) -> tuple[str, ...]:
        return tuple(f.column for f in self.fields if not f.in_row_digest)


@dataclass(frozen=True)
class InputSpec:
    table: str
    store: str
    select: str


@dataclass(frozen=True)
class Manifest:
    path: Path
    sha256: str
    kind: str
    raw: Mapping[str, Any]
    inputs: tuple[InputSpec, ...]
    outputs: tuple[OutputSpec, ...]
    producers: Mapping[str, Any]
    determinism: Mapping[str, Any]
    fixture: Mapping[str, Any]
    migrations: Mapping[str, Any]

    def output(self, table: str) -> OutputSpec:
        for spec in self.outputs:
            if spec.table == table:
                return spec
        raise ComparisonError(f"table not declared in manifest: {table}")


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    if key not in mapping:
        raise ComparisonError(f"manifest_missing_field:{where}.{key}")
    return mapping[key]


def _numeric_policy(raw: Mapping[str, Any] | None, where: str) -> NumericPolicy | None:
    if raw is None:
        return None
    policy = str(_require(raw, "policy", where))
    if policy not in NUMERIC_POLICIES:
        raise ComparisonError(f"manifest_unknown_numeric_policy:{where}:{policy}")
    if policy == "exact":
        if "tolerance" in raw:
            raise ComparisonError(f"manifest_exact_policy_carries_tolerance:{where}")
        return NumericPolicy("exact", None, raw.get("reason"))
    tolerance = float(_require(raw, "tolerance", where))
    if tolerance < 0:
        raise ComparisonError(f"manifest_negative_tolerance:{where}")
    reason = str(raw.get("reason") or "").strip()
    if not reason:
        # A tolerance without a written reason is how a global tolerance gets
        # in by the back door. Refuse it at load time.
        raise ComparisonError(f"manifest_tolerance_without_reason:{where}")
    return NumericPolicy(policy, tolerance, reason)


def load_manifest(path: Path) -> Manifest:
    try:
        raw_bytes = path.read_bytes()
        document = json.loads(raw_bytes)
    except (OSError, ValueError) as error:
        raise ComparisonError("manifest_unreadable") from error
    if not isinstance(document, dict):
        raise ComparisonError("manifest_shape_invalid")
    version = document.get("schema_version")
    if version != MANIFEST_SCHEMA_VERSION:
        raise ComparisonError(f"manifest_schema_version_unsupported:{version!r}")
    if "tolerance" in document or "float_tolerance" in document:
        raise ComparisonError("manifest_global_tolerance_forbidden")

    kind = str(_require(document, "kind", "manifest"))

    inputs: list[InputSpec] = []
    for entry in _require(document, "inputs", "manifest"):
        store = str(_require(entry, "store", "inputs[]"))
        if store not in SUPPORTED_STORES:
            raise ComparisonError(f"manifest_unknown_store:{store}")
        inputs.append(
            InputSpec(
                table=str(_require(entry, "table", "inputs[]")),
                store=store,
                select=str(_require(entry, "select", "inputs[]")),
            )
        )

    outputs: list[OutputSpec] = []
    for entry in _require(document, "outputs", "manifest"):
        table = str(_require(entry, "table", "outputs[]"))
        store = str(_require(entry, "store", f"outputs[{table}]"))
        if store not in SUPPORTED_STORES:
            raise ComparisonError(f"manifest_unknown_store:{store}")
        repeat_policy = str(_require(entry, "repeat_policy", f"outputs[{table}]"))
        if repeat_policy not in REPEAT_POLICIES:
            raise ComparisonError(f"manifest_unknown_repeat_policy:{repeat_policy}")
        fields: list[FieldSpec] = []
        for column, spec in _require(entry, "fields", f"outputs[{table}]").items():
            column_type = str(_require(spec, "type", f"outputs[{table}].{column}"))
            if column_type not in SUPPORTED_TYPES:
                raise ComparisonError(f"manifest_unknown_type:{table}.{column}")
            volatile = spec.get("volatile")
            if volatile is not None and volatile not in VOLATILE_ACTIONS:
                raise ComparisonError(
                    f"manifest_unknown_volatile_action:{table}.{column}"
                )
            fields.append(
                FieldSpec(
                    column=column,
                    type=column_type,
                    numeric=_numeric_policy(
                        spec.get("numeric"), f"outputs[{table}].{column}.numeric"
                    ),
                    volatile=volatile,
                )
            )
        declared = {f.column for f in fields}
        semantic_key = tuple(_require(entry, "semantic_key", f"outputs[{table}]"))
        if not semantic_key:
            raise ComparisonError(f"manifest_empty_semantic_key:{table}")
        missing = [column for column in semantic_key if column not in declared]
        if missing:
            raise ComparisonError(
                f"manifest_key_column_undeclared:{table}:{','.join(missing)}"
            )
        volatile_key = [
            column
            for column in semantic_key
            if any(
                f.column == column and f.volatile in ("drop", "placeholder", "ordinal")
                for f in fields
            )
        ]
        if volatile_key:
            # A canonicalized-away key column would collapse distinct rows into
            # one key and hide both a missing row and an extra one.
            raise ComparisonError(
                f"manifest_volatile_semantic_key:{table}:{','.join(volatile_key)}"
            )
        tombstone_predicate = entry.get("tombstone_predicate")
        if repeat_policy == "tombstone" and not str(tombstone_predicate or "").strip():
            # Without a predicate there is nothing to check, and the policy
            # would be satisfied by a producer that deletes nothing and marks
            # nothing -- the exact behaviour it exists to catch.
            raise ComparisonError(f"manifest_tombstone_predicate_required:{table}")
        if tombstone_predicate and repeat_policy != "tombstone":
            raise ComparisonError(f"manifest_tombstone_predicate_unused:{table}")
        outputs.append(
            OutputSpec(
                table=table,
                store=store,
                select=str(_require(entry, "select", f"outputs[{table}]")),
                semantic_key=semantic_key,
                fields=tuple(fields),
                repeat_policy=repeat_policy,
                tombstone_predicate=tombstone_predicate,
                allow_empty=bool(entry.get("allow_empty", False)),
            )
        )
    if not outputs:
        raise ComparisonError("manifest_no_output_tables")

    return Manifest(
        path=path,
        sha256=hashlib.sha256(raw_bytes).hexdigest(),
        kind=kind,
        raw=document,
        inputs=tuple(inputs),
        outputs=tuple(outputs),
        producers=dict(_require(document, "producers", "manifest")),
        determinism=dict(_require(document, "determinism", "manifest")),
        fixture=dict(_require(document, "fixture", "manifest")),
        migrations=dict(_require(document, "migrations", "manifest")),
    )


# --------------------------------------------------------------------------
# Snapshots and comparison (pure -- no database access below this line)
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Snapshot:
    """Rows read from one destination for one declared table.

    ``columns`` comes from the driver's result metadata, not from the first
    row, so an empty result still carries the projection it was read with.
    """

    table: str
    columns: tuple[str, ...]
    rows: tuple[Mapping[str, Any], ...]


def row_key(spec: OutputSpec, row: Mapping[str, Any]) -> str:
    parts = []
    for column in spec.semantic_key:
        field_spec = spec.field(column)
        parts.append(canonical_scalar(row.get(column), field_spec.type))
    return "\x1f".join(_framed(part) for part in parts)


def _ordinal_maps(
    spec: OutputSpec, rows: Sequence[Mapping[str, Any]]
) -> dict[str, dict[str, int]]:
    """Rank each ``ordinal`` volatile column's distinct values.

    A generated identifier differs between two runs but its *ordering* among
    the run's own values is a real, comparable fact. Ranking replaces the value
    with that ordering so two runs that produced the same distinct-value
    structure compare equal without the identifiers themselves being compared.
    """
    maps: dict[str, dict[str, int]] = {}
    for field_spec in spec.fields:
        if field_spec.volatile != "ordinal":
            continue
        values = sorted(
            {
                canonical_scalar(row.get(field_spec.column), field_spec.type)
                for row in rows
            }
        )
        maps[field_spec.column] = {value: index for index, value in enumerate(values)}
    return maps


def canonical_row(
    spec: OutputSpec,
    row: Mapping[str, Any],
    ordinals: Mapping[str, Mapping[str, int]],
) -> str:
    parts: list[str] = []
    for field_spec in sorted(spec.fields, key=lambda f: f.column):
        if not field_spec.in_row_digest:
            continue
        value = row.get(field_spec.column)
        if field_spec.volatile == "placeholder":
            rendered = "\x00null" if value is None else "p:present"
        elif field_spec.volatile == "ordinal":
            rendered = "o:" + str(
                ordinals.get(field_spec.column, {}).get(
                    canonical_scalar(value, field_spec.type), -1
                )
            )
        else:
            rendered = canonical_scalar(value, field_spec.type)
        parts.append(f"{field_spec.column}={rendered}")
    return "\x1f".join(_framed(part) for part in parts)


@dataclass
class TableComparison:
    table: str
    left_count: int
    right_count: int
    left_key_digest: str
    right_key_digest: str
    left_row_digest: str
    right_row_digest: str
    digest_excluded_fields: tuple[str, ...]
    differences: list[dict[str, Any]] = field(default_factory=list)

    @property
    def equal(self) -> bool:
        return not self.differences

    def as_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "count": {
                "left": self.left_count,
                "right": self.right_count,
                "equal": self.left_count == self.right_count,
            },
            "key_set_digest": {
                "left": self.left_key_digest,
                "right": self.right_key_digest,
                "equal": self.left_key_digest == self.right_key_digest,
            },
            "canonical_row_digest": {
                "left": self.left_row_digest,
                "right": self.right_row_digest,
                "equal": self.left_row_digest == self.right_row_digest,
            },
            "digest_excluded_fields": list(self.digest_excluded_fields),
            "equal": self.equal,
            "differences": self.differences,
        }


def compare_snapshots(
    spec: OutputSpec,
    left: Snapshot,
    right: Snapshot,
    *,
    sample: int = 10,
) -> TableComparison:
    """Compare one table at all four declared levels.

    Levels are evaluated top-down but never short-circuit: a count difference
    does not suppress the key-set or row-level detail, because a lane needs to
    see *which* rows moved, not only that the totals disagree.
    """
    left_ordinals = _ordinal_maps(spec, left.rows)
    right_ordinals = _ordinal_maps(spec, right.rows)

    left_by_key: dict[str, list[Mapping[str, Any]]] = {}
    right_by_key: dict[str, list[Mapping[str, Any]]] = {}
    for row in left.rows:
        left_by_key.setdefault(row_key(spec, row), []).append(row)
    for row in right.rows:
        right_by_key.setdefault(row_key(spec, row), []).append(row)

    left_rows_canonical = [canonical_row(spec, r, left_ordinals) for r in left.rows]
    right_rows_canonical = [canonical_row(spec, r, right_ordinals) for r in right.rows]

    comparison = TableComparison(
        table=spec.table,
        left_count=len(left.rows),
        right_count=len(right.rows),
        left_key_digest=digest_of_sorted(left_by_key),
        right_key_digest=digest_of_sorted(right_by_key),
        left_row_digest=digest_of_sorted(left_rows_canonical),
        right_row_digest=digest_of_sorted(right_rows_canonical),
        digest_excluded_fields=spec.digest_excluded,
    )

    if comparison.left_count != comparison.right_count:
        comparison.differences.append(
            {
                "shape": "count_mismatch",
                "table": spec.table,
                "left": comparison.left_count,
                "right": comparison.right_count,
            }
        )

    missing = sorted(set(left_by_key) - set(right_by_key))
    extra = sorted(set(right_by_key) - set(left_by_key))
    for key in missing[:sample]:
        comparison.differences.append(
            {
                "shape": "row_missing_on_right",
                "table": spec.table,
                "semantic_key": _key_fields(spec, left_by_key[key][0]),
            }
        )
    if len(missing) > sample:
        comparison.differences.append(
            {
                "shape": "row_missing_on_right",
                "table": spec.table,
                "truncated": len(missing) - sample,
            }
        )
    for key in extra[:sample]:
        comparison.differences.append(
            {
                "shape": "row_extra_on_right",
                "table": spec.table,
                "semantic_key": _key_fields(spec, right_by_key[key][0]),
            }
        )
    if len(extra) > sample:
        comparison.differences.append(
            {
                "shape": "row_extra_on_right",
                "table": spec.table,
                "truncated": len(extra) - sample,
            }
        )

    shared = sorted(set(left_by_key) & set(right_by_key))
    reported = 0
    for key in shared:
        left_group = left_by_key[key]
        right_group = right_by_key[key]
        if len(left_group) != len(right_group):
            comparison.differences.append(
                {
                    "shape": "key_multiplicity_mismatch",
                    "table": spec.table,
                    "semantic_key": _key_fields(spec, left_group[0]),
                    "left_rows": len(left_group),
                    "right_rows": len(right_group),
                }
            )
            continue
        for left_row, right_row in zip(
            sorted(left_group, key=lambda r: canonical_row(spec, r, left_ordinals)),
            sorted(right_group, key=lambda r: canonical_row(spec, r, right_ordinals)),
        ):
            field_diffs = _field_differences(
                spec, left_row, right_row, left_ordinals, right_ordinals
            )
            if not field_diffs:
                continue
            if reported < sample:
                comparison.differences.append(
                    {
                        "shape": "row_mutated",
                        "table": spec.table,
                        "semantic_key": _key_fields(spec, left_row),
                        "fields": field_diffs,
                    }
                )
            reported += 1
    if reported > sample:
        comparison.differences.append(
            {
                "shape": "row_mutated",
                "table": spec.table,
                "truncated": reported - sample,
            }
        )
    return comparison


def _key_fields(spec: OutputSpec, row: Mapping[str, Any]) -> dict[str, str]:
    return {
        column: canonical_scalar(row.get(column), spec.field(column).type)
        for column in spec.semantic_key
    }


def _field_differences(
    spec: OutputSpec,
    left_row: Mapping[str, Any],
    right_row: Mapping[str, Any],
    left_ordinals: Mapping[str, Mapping[str, int]],
    right_ordinals: Mapping[str, Mapping[str, int]],
) -> list[dict[str, Any]]:
    diffs: list[dict[str, Any]] = []
    for field_spec in sorted(spec.fields, key=lambda f: f.column):
        if field_spec.volatile == "drop":
            continue
        left_value = left_row.get(field_spec.column)
        right_value = right_row.get(field_spec.column)
        if field_spec.numeric is not None and not field_spec.numeric.is_exact:
            if field_spec.numeric.within(left_value, right_value):
                continue
            diffs.append(
                {
                    "column": field_spec.column,
                    "shape": "numeric_out_of_policy",
                    "policy": field_spec.numeric.policy,
                    "tolerance": field_spec.numeric.tolerance,
                    "reason": field_spec.numeric.reason,
                    "left": _reportable(left_value),
                    "right": _reportable(right_value),
                }
            )
            continue
        if field_spec.volatile == "placeholder":
            left_rendered = "\x00null" if left_value is None else "p:present"
            right_rendered = "\x00null" if right_value is None else "p:present"
        elif field_spec.volatile == "ordinal":
            left_rendered = "o:" + str(
                left_ordinals.get(field_spec.column, {}).get(
                    canonical_scalar(left_value, field_spec.type), -1
                )
            )
            right_rendered = "o:" + str(
                right_ordinals.get(field_spec.column, {}).get(
                    canonical_scalar(right_value, field_spec.type), -1
                )
            )
        else:
            left_rendered = canonical_scalar(left_value, field_spec.type)
            right_rendered = canonical_scalar(right_value, field_spec.type)
        if left_rendered != right_rendered:
            diffs.append(
                {
                    "column": field_spec.column,
                    "shape": "value_mismatch",
                    "left": _reportable(left_value),
                    "right": _reportable(right_value),
                }
            )
    return diffs


def _reportable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return repr(value)
    return str(value)


# --------------------------------------------------------------------------
# Repeat-run policy
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class SnapshotStats:
    """The three digest levels of a single snapshot, independent of any side."""

    count: int
    key_digest: str
    row_digest: str


def snapshot_stats(spec: OutputSpec, snap: Snapshot) -> SnapshotStats:
    ordinals = _ordinal_maps(spec, snap.rows)
    return SnapshotStats(
        count=len(snap.rows),
        # A key-set digest is over the DISTINCT keys, matching
        # `compare_snapshots`: multiplicity is reported by the count level and
        # by `key_multiplicity_mismatch`, not by folding duplicates into this
        # digest. Without that, an append-on-replay producer would read as a
        # changed key set and hide the real behaviour.
        key_digest=digest_of_sorted({row_key(spec, row) for row in snap.rows}),
        row_digest=digest_of_sorted(
            canonical_row(spec, row, ordinals) for row in snap.rows
        ),
    )


def evaluate_repeat(
    spec: OutputSpec,
    side: str,
    first: SnapshotStats,
    second: SnapshotStats,
    *,
    tombstones_first: int | None = None,
    tombstones_second: int | None = None,
) -> dict[str, Any]:
    """Check one side's second execution against the declared repeat policy.

    This is a single-implementation behavioural claim -- does this producer
    update, append, tombstone, or no-op on replay -- evaluated separately for
    each side. A port that is row-equal on a first run but replays differently
    is not a correct port, and only a repeat run can see that.
    """
    observed: str
    keys_stable = first.key_digest == second.key_digest
    rows_stable = first.row_digest == second.row_digest
    grew = first.count > 0 and second.count > first.count

    if rows_stable and second.count == first.count:
        observed = "idempotent"
    elif keys_stable and grew:
        observed = "append_duplicates"
    elif keys_stable and second.count == first.count:
        observed = "replace_window"
    else:
        observed = "changed_key_set"

    entry: dict[str, Any] = {
        "table": spec.table,
        "side": side,
        "declared_policy": spec.repeat_policy,
        "observed": observed,
        "count_first": first.count,
        "count_second": second.count,
        "key_set_stable": keys_stable,
        "row_digest_stable": rows_stable,
    }

    if spec.repeat_policy == "tombstone":
        # A tombstone contract is a claim about MARKER ROWS, so it is checked
        # by counting rows matching the manifest's declared predicate -- not by
        # accepting any replay shape that happens to keep the key set stable.
        # A producer that touched nothing would satisfy that weaker reading
        # while writing no tombstone at all.
        entry["tombstones_first"] = tombstones_first
        entry["tombstones_second"] = tombstones_second
        if tombstones_first is None or tombstones_second is None:
            entry["matches_declared_policy"] = False
            entry["tombstone_status"] = "not_evaluated"
            return entry
        marked = tombstones_second > 0
        entry["tombstone_status"] = "present" if marked else "absent"
        entry["matches_declared_policy"] = bool(
            marked
            and keys_stable
            and observed in ("replace_window", "idempotent", "append_duplicates")
        )
        return entry

    entry["matches_declared_policy"] = observed == spec.repeat_policy
    return entry


# --------------------------------------------------------------------------
# Destination readers
# --------------------------------------------------------------------------


def _database_of(dsn: str) -> str:
    tail = dsn.rsplit("/", 1)[-1]
    return tail.split("?", 1)[0]


def guard_destination(dsn: str, label: str) -> None:
    database = _database_of(dsn)
    if database in FORBIDDEN_DATABASES:
        raise ComparisonError(
            f"destination_refused:{label}:database_{database}_holds_real_data"
        )
    if not database:
        raise ComparisonError(f"destination_refused:{label}:no_database_in_dsn")


class Reader:
    """Minimal read-only accessor for one destination."""

    def __init__(self, store: str, dsn: str) -> None:
        if store not in SUPPORTED_STORES:
            raise ComparisonError(f"unsupported_store:{store}")
        self.store = store
        self.dsn = dsn
        self._client: Any = None

    def __enter__(self) -> Reader:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _clickhouse(self) -> Any:
        if self._client is None:
            import clickhouse_connect  # imported lazily: the tool is stdlib-only otherwise

            self._client = clickhouse_connect.get_client(dsn=self.dsn)
        return self._client

    def _postgres(self) -> Any:
        """A synchronous SQLAlchemy engine.

        SQLAlchemy rather than a raw driver: it is already a first-class typed
        dependency here, and it keeps the DSN dialect the same one the rest of
        the repo's synchronous Postgres paths use
        (``postgresql+psycopg2://``; asyncpg is the runtime path).
        """
        if self._client is None:
            from sqlalchemy import create_engine

            self._client = create_engine(self.dsn)
        return self._client

    def read(self, select: str) -> tuple[list[str], list[dict[str, Any]]]:
        """Return the result's columns AND rows.

        The columns come from result metadata so they survive an empty result:
        a projection that selects the wrong columns while returning no rows
        must still be caught, and reading columns off the first row cannot do
        that.
        """
        if self.store == "clickhouse":
            result = self._clickhouse().query(select)
            columns = list(getattr(result, "column_names", []) or [])
            rows = [
                dict(zip(columns, row)) for row in getattr(result, "result_rows", [])
            ]
            if not columns:
                # clickhouse-connect returns an EMPTY column_names tuple for a
                # zero-row result, which would make the manifest-column check
                # unenforceable on exactly the case it most needs to cover.
                # DESCRIBE reports the projection's columns from the query
                # itself, independently of how many rows it matched.
                described = self._clickhouse().query(f"DESCRIBE ({select})")
                columns = [str(row[0]) for row in described.result_rows]
            return columns, rows
        from sqlalchemy import text

        with self._postgres().connect() as connection:
            result = connection.execute(text(select))
            columns = [str(key) for key in result.keys()]
            return columns, [dict(row) for row in result.mappings()]

    def scalar(self, select: str) -> Any:
        _, rows = self.read(select)
        if not rows:
            return None
        return next(iter(rows[0].values()))

    def close(self) -> None:
        if self._client is None:
            return
        try:
            closer = getattr(self._client, "dispose", None) or self._client.close
            closer()
        finally:
            self._client = None


def snapshot(reader: Reader, table: str, select: str) -> Snapshot:
    columns, rows = reader.read(select)
    return Snapshot(table=table, columns=tuple(columns), rows=tuple(rows))


def validate_columns(spec: OutputSpec, snap: Snapshot, side: str) -> None:
    """Refuse a snapshot whose columns disagree with the manifest.

    A ``select`` that drifts from the manifest's ``fields`` is the quietest way
    to weaken a parity claim: an undeclared column is simply never compared, and
    the run still reports EQUAL. Checking the actual result columns against the
    declaration turns that into a loud failure the first time either side moves.

    Validated from result metadata, so it holds for an empty result too: a
    query against the wrong projection that happens to return no rows would
    otherwise sail through as "equal zero rows".
    """
    observed = set(snap.columns)
    declared = {f.column for f in spec.fields}
    if observed != declared:
        missing = sorted(declared - observed)
        extra = sorted(observed - declared)
        raise ComparisonError(
            f"manifest_column_drift:{spec.table}:{side}:"
            f"undeclared={','.join(extra) or '-'}:unread={','.join(missing) or '-'}"
        )


# --------------------------------------------------------------------------
# Execution driver
# --------------------------------------------------------------------------


def run_producer(
    command: Sequence[str], dsn: str, side: str, run_index: int, as_of: str
) -> None:
    environment = dict(os.environ)
    environment.update(
        {
            "PARITY_DSN": dsn,
            "PARITY_SIDE": side,
            "PARITY_RUN_INDEX": str(run_index),
            "PARITY_AS_OF": as_of,
        }
    )
    completed = subprocess.run(  # noqa: S603 -- caller-declared command from a checked-in manifest
        list(command), env=environment, capture_output=True, text=True
    )
    if completed.returncode != 0:
        raise ComparisonError(
            f"producer_failed:{side}:run{run_index}:exit{completed.returncode}:"
            + (completed.stderr or "").strip().splitlines()[-1][:200]
        )


def normalize_command(command: Sequence[str]) -> list[str]:
    """Make a manifest-declared argv runnable from any working directory.

    Manifests name repo-relative scripts so they stay readable and reviewable.
    Resolving them against the repo root (and running a ``.py`` entry with the
    current interpreter) keeps the manifest free of absolute paths and of a
    hard-coded interpreter, either of which would break in a worktree.
    """
    parts = [str(part) for part in command]
    if not parts:
        raise ComparisonError("producer_command_empty")
    head = parts[0]
    if head.endswith(".py"):
        candidate = Path(head)
        if not candidate.is_absolute():
            candidate = REPO_ROOT / head
        return [sys.executable, str(candidate), *parts[1:]]
    return parts


def resolve_command(
    manifest: Manifest, label: str, override: str | None
) -> list[str] | None:
    if override:
        return normalize_command(shlex.split(override))
    declared = manifest.producers.get(label)
    if not isinstance(declared, Mapping):
        return None
    command = declared.get("command")
    if command is None:
        return None
    if isinstance(command, str):
        return normalize_command(shlex.split(command))
    return normalize_command(command)


# --------------------------------------------------------------------------
# rows mode
# --------------------------------------------------------------------------


def compare_rows(
    manifest: Manifest,
    *,
    left_dsn: str,
    right_dsn: str,
    left_label: str,
    right_label: str,
    left_command: Sequence[str] | None,
    right_command: Sequence[str] | None,
    repeat: int,
    sample: int,
    as_of: str,
) -> dict[str, Any]:
    guard_destination(left_dsn, left_label)
    guard_destination(right_dsn, right_label)
    if _database_of(left_dsn) == _database_of(right_dsn):
        raise ComparisonError("destinations_share_one_database")

    clock_policy = str((manifest.determinism.get("clock") or {}).get("policy", ""))
    if (
        "pinned" in clock_policy
        and not as_of.strip()
        and (left_command or right_command)
    ):
        # The manifest says the producer's window is pinned; running one without
        # PARITY_AS_OF silently hands it the host clock, and the two sides can
        # then land on different days. Refuse rather than compare.
        raise ComparisonError(f"as_of_required_for_clock_policy:{clock_policy}")

    stores = {spec.store for spec in manifest.outputs} | {
        spec.store for spec in manifest.inputs
    }
    if len(stores) != 1:
        # Multi-store kinds need one DSN pair per store; that is a manifest
        # extension, and silently reading only one store would understate the
        # comparison.
        raise ComparisonError(f"multi_store_manifest_unsupported:{sorted(stores)}")
    store = stores.pop()

    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "claim": CLAIM_ROWS,
        "kind": manifest.kind,
        "manifest": {
            "path": str(manifest.path.resolve().relative_to(REPO_ROOT))
            if str(manifest.path.resolve()).startswith(str(REPO_ROOT))
            else str(manifest.path),
            "sha256": manifest.sha256,
        },
        "determinism": dict(manifest.determinism),
        "migrations": dict(manifest.migrations),
        "fixture": {
            key: value for key, value in manifest.fixture.items() if key != "command"
        },
        "as_of": as_of,
        "sides": {
            "left": {"label": left_label, "database": _database_of(left_dsn)},
            "right": {"label": right_label, "database": _database_of(right_dsn)},
        },
        "runs": [],
        "inputs": {},
        "repeat": [],
        "differences": [],
    }

    with (
        Reader(store, left_dsn) as left_reader,
        Reader(store, right_dsn) as right_reader,
    ):
        # 1. Inputs first. A parity verdict over outputs is meaningless unless
        #    both sides actually consumed the same fixture, so an input
        #    mismatch is INDETERMINATE, never DIFFERENT.
        input_report: dict[str, Any] = {"verified": True, "tables": {}}
        for input_spec in manifest.inputs:
            left_rows = snapshot(left_reader, input_spec.table, input_spec.select)
            right_rows = snapshot(right_reader, input_spec.table, input_spec.select)
            left_digest = digest_of_sorted(
                "\x1f".join(f"{k}={_stable(v)}" for k, v in sorted(row.items()))
                for row in left_rows.rows
            )
            right_digest = digest_of_sorted(
                "\x1f".join(f"{k}={_stable(v)}" for k, v in sorted(row.items()))
                for row in right_rows.rows
            )
            equal = left_digest == right_digest
            input_report["tables"][input_spec.table] = {
                "left_digest": left_digest,
                "right_digest": right_digest,
                "left_count": len(left_rows.rows),
                "right_count": len(right_rows.rows),
                "equal": equal,
            }
            if not equal:
                input_report["verified"] = False
        report["inputs"] = input_report
        if not input_report["verified"]:
            report["verdict"] = VERDICT_INDETERMINATE
            report["reason"] = "input_fixture_mismatch"
            return report

        # Keyed by table: the PREVIOUS run's stats per side. Every replay is
        # checked against the run before it, not only run 2 against run 1 --
        # a producer can honour its declared policy on the first replay and
        # drift on the third, and `--repeat 4` must be able to see that.
        previous_stats: dict[str, dict[str, SnapshotStats]] = {}
        previous_tombstones: dict[str, dict[str, int | None]] = {}
        for run_index in range(1, max(1, repeat) + 1):
            if left_command:
                run_producer(left_command, left_dsn, left_label, run_index, as_of)
            if right_command:
                run_producer(right_command, right_dsn, right_label, run_index, as_of)

            run_entry: dict[str, Any] = {"index": run_index, "tables": {}}
            for output_spec in manifest.outputs:
                left_snapshot = snapshot(
                    left_reader, output_spec.table, output_spec.select
                )
                right_snapshot = snapshot(
                    right_reader, output_spec.table, output_spec.select
                )
                validate_columns(output_spec, left_snapshot, left_label)
                validate_columns(output_spec, right_snapshot, right_label)
                comparison = compare_snapshots(
                    output_spec, left_snapshot, right_snapshot, sample=sample
                )
                run_entry["tables"][output_spec.table] = comparison.as_dict()
                for difference in comparison.differences:
                    report["differences"].append({"run": run_index, **difference})

                if (
                    not output_spec.allow_empty
                    and not left_snapshot.rows
                    and not right_snapshot.rows
                ):
                    # Two empty tables have identical counts and identical
                    # digests. That is not parity, it is an absence of
                    # evidence -- most often a fixture that produced nothing
                    # or a projection that matched nothing on both sides.
                    report["runs"].append(run_entry)
                    report["verdict"] = VERDICT_INDETERMINATE
                    report["reason"] = f"output_empty_on_both_sides:{output_spec.table}"
                    return report

                tombstones = {
                    left_label: _tombstone_count(left_reader, output_spec),
                    right_label: _tombstone_count(right_reader, output_spec),
                }
                if output_spec.repeat_policy == "tombstone" and (
                    tombstones[left_label] != tombstones[right_label]
                ):
                    report["differences"].append(
                        {
                            "run": run_index,
                            "shape": "tombstone_count_mismatch",
                            "table": output_spec.table,
                            "left": tombstones[left_label],
                            "right": tombstones[right_label],
                        }
                    )
                run_entry["tables"][output_spec.table]["tombstones"] = tombstones

                stats = {
                    left_label: snapshot_stats(output_spec, left_snapshot),
                    right_label: snapshot_stats(output_spec, right_snapshot),
                }
                if run_index > 1:
                    for side, current in stats.items():
                        repeat_entry = evaluate_repeat(
                            output_spec,
                            side,
                            previous_stats[output_spec.table][side],
                            current,
                            tombstones_first=previous_tombstones.get(
                                output_spec.table, {}
                            ).get(side),
                            tombstones_second=tombstones[side],
                        )
                        repeat_entry["run"] = run_index
                        report["repeat"].append(repeat_entry)
                        if not repeat_entry["matches_declared_policy"]:
                            report["differences"].append(
                                {
                                    "run": run_index,
                                    "shape": "repeat_policy_violation",
                                    "table": output_spec.table,
                                    "side": side,
                                    "declared": repeat_entry["declared_policy"],
                                    "observed": repeat_entry["observed"],
                                }
                            )
                previous_stats[output_spec.table] = stats
                previous_tombstones[output_spec.table] = tombstones
            report["runs"].append(run_entry)

    report["verdict"] = (
        VERDICT_EQUAL if not report["differences"] else VERDICT_DIFFERENT
    )
    return report


def _tombstone_count(reader: Reader, spec: OutputSpec) -> int | None:
    """Count rows matching the manifest's declared tombstone predicate.

    ``None`` when the table declares no tombstone contract; a number otherwise,
    which is what makes the tombstone repeat policy checkable instead of
    assumed.
    """
    if not spec.tombstone_predicate:
        return None
    value = reader.scalar(
        f"SELECT count() FROM {spec.table} WHERE {spec.tombstone_predicate}"
        if reader.store == "clickhouse"
        else f"SELECT count(*) FROM {spec.table} WHERE {spec.tombstone_predicate}"
    )
    return None if value is None else int(value)


def _stable(value: Any) -> str:
    if isinstance(value, dt.datetime):
        return canonical_datetime(value)
    if isinstance(value, (dt.date, uuid.UUID)):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    return str(value)


# --------------------------------------------------------------------------
# runtime mode
# --------------------------------------------------------------------------


def _load_proof_module() -> Any:
    """Load ``canary_release_proof`` for its pinned-document and threshold logic.

    Reused rather than reimplemented: that module already owns how the v0
    baseline and the v3 thresholds are pinned (path *and* sha256), how a
    threshold set is validated, and the fail-closed ``thresholds_unapproved``
    rule. Re-deriving any of that here would create a second, divergent
    definition of the same operational claim.
    """
    import importlib.util

    path = Path(__file__).resolve().parent / "canary_release_proof.py"
    spec = importlib.util.spec_from_file_location("canary_release_proof", path)
    if spec is None or spec.loader is None:
        raise ComparisonError("canary_release_proof_unloadable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def compare_runtime(observation_path: Path) -> dict[str, Any]:
    proof = _load_proof_module()
    documents = proof.load_pinned_documents()
    proof.validate_baseline(documents["baseline"].value)
    proof.validate_thresholds(documents["thresholds"].value, documents["baseline"])

    try:
        observation = json.loads(observation_path.read_bytes())
    except (OSError, ValueError) as error:
        raise ComparisonError("observation_unreadable") from error
    if not isinstance(observation, dict):
        raise ComparisonError("observation_shape_invalid")
    if observation.get("runtime") != "go":
        raise ComparisonError("observation_runtime_not_go")
    proof.reject_sensitive_keys(observation)

    baseline = documents["baseline"].value
    thresholds = documents["thresholds"].value["thresholds"]
    measurements = baseline["measurements"]

    findings: list[dict[str, Any]] = []
    comparisons: list[dict[str, Any]] = []

    observed_families = observation.get("families") or {}
    observed_profiles = observation.get("profiles") or {}
    if not isinstance(observed_families, Mapping) or not observed_families:
        raise ComparisonError("observation_families_missing")
    if not isinstance(observed_profiles, Mapping) or not observed_profiles:
        raise ComparisonError("observation_profiles_missing")

    # Coverage, checked against the pinned baseline rather than against
    # whatever the observation happened to include. An operational-health claim
    # over an arbitrary subset is not the claim: a truncated capture would
    # otherwise produce no findings and read as compliant.
    baseline_families = {
        name
        for name, recorded in measurements["task_outcome_rates_by_family"].items()
        if isinstance(recorded, Mapping) and "counts" in recorded
    }
    # The union of every profile series the baseline actually recorded, not
    # just the CPU one: v0 carries queue-age profiles (external_ingest,
    # monitoring) that are not CPU profiles, and deriving coverage from CPU
    # alone let those disappear from an observation without a finding.
    baseline_profiles: set[str] = set()
    for series, scalar_key in (
        ("worker_cpu_cores_by_profile", "p50"),
        ("worker_memory_bytes_by_profile", "p50"),
        ("oldest_queue_age_seconds_by_profile", "p95"),
    ):
        baseline_profiles |= {
            name
            for name, recorded in measurements[series].items()
            if _baseline_scalar(recorded, scalar_key) is not None
        }
    for name in sorted(baseline_families - set(observed_families)):
        findings.append({"check": "baseline_family_not_observed", "family": name})
        comparisons.append({"scope": "family", "name": name, "status": "not_observed"})
    for name in sorted(baseline_profiles - set(observed_profiles)):
        findings.append({"check": "baseline_profile_not_observed", "profile": name})
        comparisons.append({"scope": "profile", "name": name, "status": "not_observed"})

    for family, observed in observed_families.items():
        recorded = measurements["task_outcome_rates_by_family"].get(family)
        if not isinstance(recorded, Mapping) or "counts" not in recorded:
            # Evidence rule: an absent series is `missing`, never a numeric
            # zero -- and a series with no baseline cannot be inside an
            # envelope the baseline never drew, so it is a finding too.
            comparisons.append(
                {"scope": "family", "name": family, "status": "missing_in_baseline"}
            )
            findings.append({"check": "series_missing_in_baseline", "family": family})
            continue
        _require_counts(observed, family)
        baseline_errors = int(recorded["counts"]["failure"]) + int(
            recorded["counts"]["discard"]
        )
        observed_errors = int(observed["counts"]["failure"]) + int(
            observed["counts"]["discard"]
        )
        error_delta = observed_errors - baseline_errors
        comparisons.append(
            {
                "scope": "family",
                "name": family,
                "status": "compared",
                "baseline_error_count": baseline_errors,
                "go_error_count": observed_errors,
                "error_count_delta": error_delta,
            }
        )
        if error_delta > thresholds["error_count_delta_max"]:
            findings.append({"check": "error_parity_failed", "family": family})
        if observed_errors > thresholds["go_error_count_max"]:
            findings.append(
                {"check": "go_error_count_ceiling_failed", "family": family}
            )

    for profile, observed in (observation.get("profiles") or {}).items():
        entry: dict[str, Any] = {
            "scope": "profile",
            "name": profile,
            "status": "compared",
        }
        for key, baseline_key, threshold_key in (
            ("cpu_cores", "worker_cpu_cores_by_profile", "cpu_cores_multiplier_max"),
            (
                "memory_bytes",
                "worker_memory_bytes_by_profile",
                "memory_bytes_multiplier_max",
            ),
        ):
            recorded = measurements[baseline_key].get(profile)
            baseline_value = _baseline_scalar(recorded)
            observed_value = observed.get(key)
            if observed_value is not None:
                observed_value = _require_measure(observed_value, f"{profile}.{key}")
            if baseline_value is None or observed_value is None:
                entry[key] = {"status": "missing"}
                # An unmeasurable budget is not a satisfied budget.
                findings.append({"check": f"{key}_not_measurable", "profile": profile})
                continue
            ratio = proof.ratio(float(observed_value), float(baseline_value))
            entry[key] = {
                "baseline": baseline_value,
                "go": observed_value,
                "ratio": ratio,
            }
            if ratio is None or ratio > thresholds[threshold_key]:
                findings.append(
                    {
                        "check": f"{key}_budget_failed",
                        "profile": profile,
                        "ratio": ratio,
                    }
                )
        lag_recorded = measurements["oldest_queue_age_seconds_by_profile"].get(profile)
        baseline_lag = _baseline_scalar(lag_recorded, "p95")
        observed_lag = observed.get("oldest_queue_age_seconds_p95")
        if observed_lag is not None:
            observed_lag = _require_measure(
                observed_lag, f"{profile}.oldest_queue_age_seconds_p95"
            )
        if baseline_lag is None or observed_lag is None:
            entry["lag_seconds"] = {"status": "missing"}
            # Same rule as the CPU and memory budgets: queue health that was
            # not measured has not been shown to be inside the envelope.
            findings.append({"check": "lag_seconds_not_measurable", "profile": profile})
        else:
            lag_delta = float(observed_lag) - float(baseline_lag)
            entry["lag_seconds"] = {
                "baseline": baseline_lag,
                "go": observed_lag,
                "delta": lag_delta,
            }
            if lag_delta > thresholds["lag_seconds_delta_max"]:
                findings.append({"check": "lag_parity_failed", "profile": profile})
        comparisons.append(entry)

    approved = bool(proof.threshold_review_approved(documents))
    authoritative = bool(baseline.get("authoritative_for_baseline"))

    if not approved or not authoritative:
        verdict = VERDICT_UNPROVEN
        reason = (
            "thresholds_unapproved" if not approved else "baseline_not_authoritative"
        )
    elif findings:
        verdict = VERDICT_OUTSIDE_ENVELOPE
        reason = None
    else:
        verdict = VERDICT_WITHIN_ENVELOPE
        reason = None

    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "claim": CLAIM_RUNTIME,
        "evidence": {
            "baseline": {
                "path": documents["baseline"].path,
                "sha256": documents["baseline"].sha256,
                "scope": baseline.get("scope"),
                "authoritative_for_baseline": authoritative,
            },
            "thresholds": {
                "path": documents["thresholds"].path,
                "sha256": documents["thresholds"].sha256,
                "review_approved": approved,
            },
        },
        "observation": {
            "build": observation.get("build"),
            "window": observation.get("window"),
        },
        "comparisons": comparisons,
        "findings": findings,
        "verdict": verdict,
        "reason": reason,
    }


def _require_measure(value: Any, name: str) -> float:
    """Refuse a runtime measurement that arithmetic cannot be trusted on.

    Python's JSON decoder accepts ``NaN`` and ``Infinity``, and nothing in the
    thresholds rejects a negative. Any of the three makes every ``>`` check
    below return False, so a malformed observation would produce no findings at
    all -- the shape of a pass. Reject before comparing, not after.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ComparisonError(f"observation_measure_not_a_number:{name}")
    number = float(value)
    if number != number or number in (float("inf"), float("-inf")):
        raise ComparisonError(f"observation_measure_not_finite:{name}")
    if number < 0:
        raise ComparisonError(f"observation_measure_negative:{name}")
    return number


def _require_counts(observed: Any, name: str) -> None:
    """Refuse an observation series whose counts are absent or not integers."""
    if not isinstance(observed, Mapping):
        raise ComparisonError(f"observation_series_shape_invalid:{name}")
    counts = observed.get("counts")
    if not isinstance(counts, Mapping):
        raise ComparisonError(f"observation_counts_missing:{name}")
    for key in ("success", "retry", "failure", "discard"):
        value = counts.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ComparisonError(f"observation_counts_invalid:{name}.{key}")


def _baseline_scalar(recorded: Any, key: str = "p50") -> float | None:
    if not isinstance(recorded, Mapping):
        return None
    if recorded.get("status") not in (None, "observed"):
        return None
    value = recorded.get(key, recorded.get("value"))
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    # A non-finite or negative baseline is unusable as a denominator or a
    # reference point; treat it as no baseline rather than compare against it.
    if number != number or number in (float("inf"), float("-inf")) or number < 0:
        return None
    return number


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _write_report(report: Mapping[str, Any], out: Path | None) -> None:
    text = json.dumps(report, indent=2, sort_keys=True, default=str)
    if out is None:
        print(text)
        return
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text + "\n", encoding="utf-8")
    print(json.dumps({"verdict": report.get("verdict"), "report": str(out)}))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)

    rows = modes.add_parser(
        "rows", help="Algorithm row parity between two destinations."
    )
    rows.add_argument("--manifest", type=Path, required=True)
    rows.add_argument("--left-dsn", required=True)
    rows.add_argument("--right-dsn", required=True)
    rows.add_argument("--left-label", default="python")
    rows.add_argument("--right-label", default="go")
    rows.add_argument(
        "--left-exec", help="Command producing the left side; overrides the manifest."
    )
    rows.add_argument(
        "--right-exec", help="Command producing the right side; overrides the manifest."
    )
    rows.add_argument(
        "--no-exec",
        action="store_true",
        help="Compare destinations exactly as they stand; run no producer on either side.",
    )
    rows.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Executions per side. 2 exercises the declared repeat policy.",
    )
    rows.add_argument("--sample", type=int, default=10)
    rows.add_argument(
        "--as-of",
        default="",
        help="Pinned UTC instant handed to producers as PARITY_AS_OF.",
    )
    rows.add_argument("--out", type=Path)

    runtime = modes.add_parser(
        "runtime",
        help="Operational health of a Go runtime observation vs the v0/v3 evidence.",
    )
    runtime.add_argument("--observation", type=Path, required=True)
    runtime.add_argument("--out", type=Path)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.mode == "rows":
            manifest = load_manifest(args.manifest)
            report = compare_rows(
                manifest,
                left_dsn=args.left_dsn,
                right_dsn=args.right_dsn,
                left_label=args.left_label,
                right_label=args.right_label,
                left_command=None
                if args.no_exec
                else resolve_command(manifest, args.left_label, args.left_exec),
                right_command=None
                if args.no_exec
                else resolve_command(manifest, args.right_label, args.right_exec),
                repeat=args.repeat,
                sample=args.sample,
                as_of=args.as_of,
            )
        else:
            report = compare_runtime(args.observation)
    except ComparisonError as error:
        print(json.dumps({"status": "error", "failure": str(error)}), file=sys.stderr)
        return 2
    _write_report(report, args.out)
    if report["verdict"] in (VERDICT_EQUAL, VERDICT_WITHIN_ENVELOPE):
        return 0
    if report["verdict"] in (VERDICT_DIFFERENT, VERDICT_OUTSIDE_ENVELOPE):
        return 1
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
