"""The Go/Python proof-gate comparator (CHAOS-4366 Wave 0, deliverable 5).

Implements CHAOS-4381 (chris ACCEPTED 2026-08-27 19:44 PT) sections 1-6
verbatim -- see ``.github/docs-legacy/plans/chaos-4381-parity-rules-proposal.md``
for the prose rules; this module is their executable form:

1. Error ordering -- errors compared as a SET keyed by (path, extensions.code);
   a message-only mismatch is a distinct, lower-severity finding
   (``error_message_drift``), never a ``mismatch`` terminal state on its own.
2. Null vs. omission -- exact key-presence semantics; a field present-and-null
   on one side but absent on the other is a ``mismatch``, except top-level
   transport-envelope keys on an explicit, written-reason allowlist.
3. Floating-point comparison -- Tier A (default) pass-through values compare
   exactly; Tier B computed/aggregated floats, declared per-field, get a
   1e-9 relative-or-absolute tolerance; NaN/Infinity is always a mismatch.
4. Watermark handling -- a watermark delta between baseline and candidate
   yields terminal state ``unsupported``, never ``mismatch`` -- this is not
   a Go-vs-Python defect, it is ClickHouse's eventual consistency.
5. List tie-ordering -- default strict (position-by-position); an operation
   may declare one list path ``relaxed``, comparing tie-blocks (grouped by
   the declared sort key) as sets by primary id, never the whole list as a
   set and never a cross-tie-block reorder as a match.
6. Falsification -- ``tests/api/graphql/test_go_api_comparator.py`` plants
   each defect class this module exists to catch (removed row, changed
   nullability, changed error path, reordered results in both ordering
   modes) and asserts the SPECIFIC terminal state, not a bare pass/fail --
   see plan §5's stranded-partition lesson (root AGENTS.md, CHAOS-3033):
   an unclassified "something differs" verdict is not an acceptable output.

This module is deliberately a pure comparison function over two already-
captured response snapshots -- it does not fetch, proxy, or persist
anything. Callers (a future stage-2 dual-run harness) capture
:class:`ResponseSnapshot`\\ s themselves and, when they want a durable
record, pass this module's :class:`ComparisonResult` into
``go_api_registry.record_proof_run`` (``terminal_state`` is drawn from the
same plan-§5 vocabulary that table's CHECK constraint enforces).
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Literal

from dev_health_ops.telemetry_metrics import (
    build_counter,
    load_otel_meter,
    load_prometheus,
)

__all__ = [
    "TERMINAL_STATE_MATCH",
    "TERMINAL_STATE_MISMATCH",
    "TERMINAL_STATE_UNSUPPORTED",
    "FloatTier",
    "TieOrdering",
    "ResponseSnapshot",
    "Finding",
    "ComparisonResult",
    "compare_responses",
]

TERMINAL_STATE_MATCH = "match"
TERMINAL_STATE_MISMATCH = "mismatch"
TERMINAL_STATE_UNSUPPORTED = "unsupported"

FloatTier = Literal["A", "B"]
TieOrdering = Literal["strict", "relaxed"]

_FLOAT_TOLERANCE = 1e-9

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)

#: New logic gets telemetry in the same PR (root AGENTS.md standing order):
#: every comparison this module runs is counted by its verdict, and every
#: individual finding by its kind -- independent of whether the caller goes
#: on to persist a ProofRun (go_api_registry_telemetry's counter only fires
#: when a caller chooses to record one; this one fires on every comparison).
GO_API_COMPARATOR_VERDICT_TOTAL = build_counter(
    "devhealth_go_api_comparator_verdict_total",
    "Go/Python proof-gate comparisons, by terminal state",
    ["terminal_state"],
    meter=_meter,
    prometheus=_prometheus,
)
GO_API_COMPARATOR_FINDING_TOTAL = build_counter(
    "devhealth_go_api_comparator_finding_total",
    "Go/Python proof-gate comparator findings, by kind",
    ["kind"],
    meter=_meter,
    prometheus=_prometheus,
)


@dataclass(frozen=True, slots=True)
class ResponseSnapshot:
    """One side's complete observable GraphQL response (plan §5 stage 2:
    "the complete observable response").

    ``data``/``errors`` follow the GraphQL response envelope shape exactly:
    ``data`` is the top-level ``data`` object; ``errors`` is the list of
    error objects, each with ``message``, ``path`` (list of str/int
    segments), and ``extensions`` (must carry ``code`` for this
    comparator's error-set rule to apply).

    ``data_present`` distinguishes ``"data": null`` (key present, value
    null -- e.g. a nullable root field that errored, per the GraphQL spec)
    from the ``data`` key being absent entirely (e.g. a request that never
    reached execution, such as a validation error) -- parity rule 2's
    null-vs-omission rule applies at the top level too, and both cases
    otherwise collapse to the same Python ``None`` in ``data``. Default
    ``True`` (the common case: a response that executed and has a value,
    possibly itself ``null``, for ``data``).

    ``watermark`` is parity rule 4's high-water column value observed by
    this side's query; ``None`` means the operation does not expose one
    (the caller is asserting watermark comparison does not apply here --
    see :func:`compare_responses`'s watermark handling).
    """

    data: Any
    data_present: bool = True
    errors: tuple[dict[str, Any], ...] = ()
    watermark: str | None = None


@dataclass(frozen=True, slots=True)
class Finding:
    """One comparator observation. ``kind`` distinguishes severity:
    ``"mismatch"`` blocks a MATCH verdict; every other kind (currently only
    ``"error_message_drift"``) is recorded but does not, by itself, block
    one -- see parity rule 1.
    """

    kind: str
    path: str
    detail: str


@dataclass(frozen=True, slots=True)
class ComparisonResult:
    terminal_state: str
    findings: tuple[Finding, ...] = field(default_factory=tuple)

    @property
    def is_match(self) -> bool:
        return self.terminal_state == TERMINAL_STATE_MATCH


def _record(result: ComparisonResult) -> ComparisonResult:
    GO_API_COMPARATOR_VERDICT_TOTAL.labels(terminal_state=result.terminal_state).inc()
    for finding in result.findings:
        GO_API_COMPARATOR_FINDING_TOTAL.labels(kind=finding.kind).inc()
    return result


def compare_responses(
    baseline: ResponseSnapshot,
    candidate: ResponseSnapshot,
    *,
    float_tier_fields: frozenset[str] = frozenset(),
    tie_ordering: TieOrdering = "strict",
    relaxed_list_path: str | None = None,
    tie_sort_key: Any = None,
    tie_block_id_field: str = "id",
    allowlisted_envelope_keys: frozenset[str] = frozenset(),
    require_watermark: bool = False,
) -> ComparisonResult:
    """Compare a baseline (Python) and candidate (Go) response.

    ``float_tier_fields`` names Tier-B fields by dotted path (e.g.
    ``"data.hotspots.edges.score"``, wildcarding list indices away) -- any
    field not named here is Tier A (exact) by default, per parity rule 3.

    ``tie_ordering="relaxed"`` applies ONLY to the list at
    ``relaxed_list_path`` (a dotted path to the list itself, e.g.
    ``"data.reviewEdges.edges"``) AND only when ``tie_ordering`` is
    literally ``"relaxed"`` -- passing a ``relaxed_list_path`` alongside
    ``tie_ordering="strict"`` (the default) must not silently relax that
    list; every other list in the response is compared strictly regardless.
    ``tie_sort_key`` is a callable ``(element) -> Hashable`` grouping
    elements into tie-blocks; ``tie_block_id_field`` names the primary-id
    key used to compare each tie-block as a set. Per parity rule 5 this is
    a documented, per-operation exception, never a default.

    ``allowlisted_envelope_keys`` excludes named keys from the null-vs-
    omission check ONLY at the top level of the GraphQL response envelope
    (parity rule 2's stated exception: "top-level transport envelope keys
    that one framework omits when empty") -- never inside ``data``, where a
    field that happens to be named e.g. ``extensions`` is ordinary business
    data, not a transport-envelope key.

    Watermark handling (parity rule 4) runs FIRST: if both sides report a
    watermark and they differ, the whole comparison short-circuits to
    ``unsupported`` -- a watermark delta is ClickHouse eventual consistency,
    never evidence of a Go-vs-Python defect, and must not be allowed to
    produce or hide a ``mismatch`` verdict. If ``require_watermark`` is
    True (the operation is registered as watermark-bearing), a MISSING
    watermark on either side is *also* ``unsupported`` -- parity rule 4:
    "every comparable operation must expose a watermark", so silently
    comparing data without one is not a valid ``match``. Operations that
    genuinely have no watermark concept (the comparator's default) simply
    never set ``require_watermark``.
    """
    if require_watermark and (
        baseline.watermark is None or candidate.watermark is None
    ):
        return _record(
            ComparisonResult(
                terminal_state=TERMINAL_STATE_UNSUPPORTED,
                findings=(
                    Finding(
                        kind="watermark_missing",
                        path="$.watermark",
                        detail=(
                            f"required watermark missing: baseline={baseline.watermark!r} "
                            f"candidate={candidate.watermark!r}"
                        ),
                    ),
                ),
            )
        )
    if (
        baseline.watermark is not None
        and candidate.watermark is not None
        and baseline.watermark != candidate.watermark
    ):
        return _record(
            ComparisonResult(
                terminal_state=TERMINAL_STATE_UNSUPPORTED,
                findings=(
                    Finding(
                        kind="watermark_drift",
                        path="$.watermark",
                        detail=(
                            f"baseline watermark {baseline.watermark!r} != "
                            f"candidate watermark {candidate.watermark!r}"
                        ),
                    ),
                ),
            )
        )

    findings: list[Finding] = []
    findings.extend(
        _compare_errors(baseline.errors, candidate.errors, path_prefix="$.errors")
    )

    if baseline.data_present != candidate.data_present:
        findings.append(
            Finding(
                kind="mismatch",
                path="$.data",
                detail=(
                    "present in baseline, absent in candidate"
                    if baseline.data_present
                    else "present in candidate, absent in baseline"
                ),
            )
        )
    elif baseline.data_present and candidate.data_present:
        # allowlisted_envelope_keys is passed only here, at the outermost
        # `data` dict -- _compare_dict/_compare_json/_compare_list all drop
        # it (pass frozenset()) before recursing into any nested value, so
        # a field named e.g. "extensions" NESTED inside `data` is never
        # excused, only a direct top-level key of `data` itself.
        findings.extend(
            _compare_json(
                baseline.data,
                candidate.data,
                path="$.data",
                float_tier_fields=float_tier_fields,
                relaxed_list_path=relaxed_list_path
                if tie_ordering == "relaxed"
                else None,
                tie_sort_key=tie_sort_key,
                tie_block_id_field=tie_block_id_field,
                allowlisted_envelope_keys=allowlisted_envelope_keys,
            )
        )
    # else: data absent on both sides -- nothing to compare, and absence on
    # both sides is not itself a finding (e.g. two validation-error responses
    # that both never reached execution).

    terminal_state = (
        TERMINAL_STATE_MISMATCH
        if any(f.kind == "mismatch" for f in findings)
        else TERMINAL_STATE_MATCH
    )
    return _record(
        ComparisonResult(terminal_state=terminal_state, findings=tuple(findings))
    )


# --- Error comparison (parity rule 1) ---------------------------------


def _error_sort_key(err: dict[str, Any]) -> tuple[str, str, str]:
    path = err.get("path") or []
    joined_path = ".".join(str(seg) for seg in path)
    code = str((err.get("extensions") or {}).get("code", ""))
    message = str(err.get("message", ""))
    return (joined_path, code, message)


def _error_identity(err: dict[str, Any]) -> tuple[str, str]:
    path = err.get("path") or []
    joined_path = ".".join(str(seg) for seg in path)
    code = str((err.get("extensions") or {}).get("code", ""))
    return (joined_path, code)


def _compare_errors(
    baseline_errors: tuple[dict[str, Any], ...],
    candidate_errors: tuple[dict[str, Any], ...],
    *,
    path_prefix: str,
) -> list[Finding]:
    baseline_sorted = sorted(baseline_errors, key=_error_sort_key)
    candidate_sorted = sorted(candidate_errors, key=_error_sort_key)

    baseline_by_identity = {_error_identity(e): e for e in baseline_sorted}
    candidate_by_identity = {_error_identity(e): e for e in candidate_sorted}

    findings: list[Finding] = []

    missing = baseline_by_identity.keys() - candidate_by_identity.keys()
    extra = candidate_by_identity.keys() - baseline_by_identity.keys()
    for identity in sorted(missing):
        findings.append(
            Finding(
                kind="mismatch",
                path=f"{path_prefix}[{identity[0]!r},{identity[1]!r}]",
                detail="error present in baseline, missing from candidate",
            )
        )
    for identity in sorted(extra):
        findings.append(
            Finding(
                kind="mismatch",
                path=f"{path_prefix}[{identity[0]!r},{identity[1]!r}]",
                detail="error present in candidate, missing from baseline",
            )
        )

    for identity in baseline_by_identity.keys() & candidate_by_identity.keys():
        base_err = baseline_by_identity[identity]
        cand_err = candidate_by_identity[identity]
        base_message = base_err.get("message")
        cand_message = cand_err.get("message")
        if base_message != cand_message:
            findings.append(
                Finding(
                    kind="error_message_drift",
                    path=f"{path_prefix}[{identity[0]!r},{identity[1]!r}].message",
                    detail=f"{base_message!r} != {cand_message!r}",
                )
            )

    return findings


# --- Structural JSON comparison (parity rules 2, 3, 5) -----------------

_LIST_INDEX_SUFFIX_RE = re.compile(r"\[\d+\]")


def _tiered_path(path: str) -> str:
    """Normalize a concrete comparator path (e.g. "$.data.edges[0].score")
    to the dotted, index-free form a caller declares a Tier-B field with
    (e.g. "data.edges.score"): drop the leading "$" segment and strip any
    "[N]" list-index suffix from each remaining segment."""
    segments = path.split(".")
    if segments and segments[0] == "$":
        segments = segments[1:]
    normalized = [_LIST_INDEX_SUFFIX_RE.sub("", seg) for seg in segments]
    return ".".join(normalized)


def _compare_json(
    baseline: Any,
    candidate: Any,
    *,
    path: str,
    float_tier_fields: frozenset[str],
    relaxed_list_path: str | None,
    tie_sort_key: Any,
    tie_block_id_field: str,
    allowlisted_envelope_keys: frozenset[str],
) -> list[Finding]:
    if isinstance(baseline, dict) and isinstance(candidate, dict):
        return _compare_dict(
            baseline,
            candidate,
            path=path,
            float_tier_fields=float_tier_fields,
            relaxed_list_path=relaxed_list_path,
            tie_sort_key=tie_sort_key,
            tie_block_id_field=tie_block_id_field,
            allowlisted_envelope_keys=allowlisted_envelope_keys,
        )
    if isinstance(baseline, list) and isinstance(candidate, list):
        return _compare_list(
            baseline,
            candidate,
            path=path,
            float_tier_fields=float_tier_fields,
            relaxed_list_path=relaxed_list_path,
            tie_sort_key=tie_sort_key,
            tie_block_id_field=tie_block_id_field,
            allowlisted_envelope_keys=allowlisted_envelope_keys,
        )
    if isinstance(baseline, bool) or isinstance(candidate, bool):
        if baseline is not candidate:
            return [
                Finding(
                    kind="mismatch", path=path, detail=f"{baseline!r} != {candidate!r}"
                )
            ]
        return []
    if isinstance(baseline, (int, float)) and isinstance(candidate, (int, float)):
        return _compare_number(
            float(baseline),
            float(candidate),
            path=path,
            float_tier_fields=float_tier_fields,
        )
    if baseline != candidate:
        return [
            Finding(kind="mismatch", path=path, detail=f"{baseline!r} != {candidate!r}")
        ]
    return []


def _compare_number(
    baseline: float,
    candidate: float,
    *,
    path: str,
    float_tier_fields: frozenset[str],
) -> list[Finding]:
    if (
        math.isnan(baseline)
        or math.isnan(candidate)
        or math.isinf(baseline)
        or math.isinf(candidate)
    ):
        # Parity rule 3: NaN/Infinity is ALWAYS a mismatch, never
        # tolerance-compared -- including when both sides agree (inf==inf
        # is true in IEEE 754, but a non-finite value reaching the client
        # at all is itself the defect this rule exists to catch).
        return [
            Finding(
                kind="mismatch",
                path=path,
                detail=f"non-finite value: {baseline!r} vs {candidate!r} (NaN/Infinity always mismatches)",
            )
        ]

    tier: FloatTier = "B" if _tiered_path(path) in float_tier_fields else "A"
    if tier == "A":
        if baseline != candidate:
            return [
                Finding(
                    kind="mismatch",
                    path=path,
                    detail=f"{baseline!r} != {candidate!r} (Tier A, exact)",
                )
            ]
        return []

    tolerance = max(
        _FLOAT_TOLERANCE, _FLOAT_TOLERANCE * max(abs(baseline), abs(candidate))
    )
    if abs(baseline - candidate) > tolerance:
        return [
            Finding(
                kind="mismatch",
                path=path,
                detail=f"{baseline!r} != {candidate!r} (Tier B, tolerance {tolerance!r})",
            )
        ]
    return []


def _compare_dict(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    path: str,
    float_tier_fields: frozenset[str],
    relaxed_list_path: str | None,
    tie_sort_key: Any,
    tie_block_id_field: str,
    allowlisted_envelope_keys: frozenset[str],
) -> list[Finding]:
    """``allowlisted_envelope_keys`` is used for THIS dict's own key-presence
    check only, then dropped (recursive calls always pass ``frozenset()``)
    -- parity rule 2's allowlist exception is for top-level transport-
    envelope keys, not for a same-named field nested anywhere inside
    ``data``. compare_responses only ever calls this with a non-empty
    allowlist for the outermost ``data`` dict; every nested dict, at any
    depth, compares with no exclusions.
    """
    findings: list[Finding] = []
    all_keys = set(baseline.keys()) | set(candidate.keys())
    for key in sorted(all_keys, key=str):
        child_path = f"{path}.{key}"
        in_baseline = key in baseline
        in_candidate = key in candidate
        if in_baseline != in_candidate:
            if key in allowlisted_envelope_keys:
                continue
            findings.append(
                Finding(
                    kind="mismatch",
                    path=child_path,
                    detail=(
                        "present in baseline, absent in candidate"
                        if in_baseline
                        else "present in candidate, absent in baseline"
                    ),
                )
            )
            continue
        findings.extend(
            _compare_json(
                baseline[key],
                candidate[key],
                path=child_path,
                float_tier_fields=float_tier_fields,
                relaxed_list_path=relaxed_list_path,
                tie_sort_key=tie_sort_key,
                tie_block_id_field=tie_block_id_field,
                allowlisted_envelope_keys=frozenset(),
            )
        )
    return findings


def _compare_list(
    baseline: list[Any],
    candidate: list[Any],
    *,
    path: str,
    float_tier_fields: frozenset[str],
    relaxed_list_path: str | None,
    tie_sort_key: Any,
    tie_block_id_field: str,
    allowlisted_envelope_keys: frozenset[str],
) -> list[Finding]:
    if len(baseline) != len(candidate):
        return [
            Finding(
                kind="mismatch",
                path=path,
                detail=f"length {len(baseline)} != {len(candidate)}",
            )
        ]

    use_relaxed = (
        relaxed_list_path is not None
        and path == relaxed_list_path
        and tie_sort_key is not None
    )
    if use_relaxed:
        return _compare_list_relaxed(
            baseline,
            candidate,
            path=path,
            tie_sort_key=tie_sort_key,
            tie_block_id_field=tie_block_id_field,
            float_tier_fields=float_tier_fields,
            allowlisted_envelope_keys=allowlisted_envelope_keys,
        )

    findings: list[Finding] = []
    for index, (base_item, cand_item) in enumerate(zip(baseline, candidate)):
        findings.extend(
            _compare_json(
                base_item,
                cand_item,
                path=f"{path}[{index}]",
                float_tier_fields=float_tier_fields,
                relaxed_list_path=relaxed_list_path,
                tie_sort_key=tie_sort_key,
                tie_block_id_field=tie_block_id_field,
                # Dropped, not forwarded -- see _compare_dict's doc comment;
                # the allowlist applies only at the exact top-level `data`
                # dict compare_responses first calls, never any element
                # reached by descending into a list.
                allowlisted_envelope_keys=frozenset(),
            )
        )
    return findings


def _compare_list_relaxed(
    baseline: list[Any],
    candidate: list[Any],
    *,
    path: str,
    tie_sort_key: Any,
    tie_block_id_field: str,
    float_tier_fields: frozenset[str],
    allowlisted_envelope_keys: frozenset[str],
) -> list[Finding]:
    """Parity rule 5's `relaxed` tie-ordering: group each side into
    tie-blocks by ``tie_sort_key``, in first-seen block order (block ORDER
    is still part of the contract -- only within-block order is relaxed),
    then compare each block pairwise as a set keyed by
    ``tie_block_id_field``. A cross-tie-block reorder still surfaces as a
    block-order or block-membership mismatch; only an in-tie-block reorder
    is absorbed.
    """

    def _blocks(items: list[Any]) -> list[tuple[Any, list[Any]]]:
        blocks: list[tuple[Any, list[Any]]] = []
        for item in items:
            key = tie_sort_key(item)
            if blocks and blocks[-1][0] == key:
                blocks[-1][1].append(item)
            else:
                blocks.append((key, [item]))
        return blocks

    baseline_blocks = _blocks(baseline)
    candidate_blocks = _blocks(candidate)

    findings: list[Finding] = []
    if len(baseline_blocks) != len(candidate_blocks) or any(
        b[0] != c[0] for b, c in zip(baseline_blocks, candidate_blocks)
    ):
        findings.append(
            Finding(
                kind="mismatch",
                path=path,
                detail=(
                    f"tie-block sequence differs: "
                    f"{[b[0] for b in baseline_blocks]!r} != "
                    f"{[c[0] for c in candidate_blocks]!r}"
                ),
            )
        )
        return findings

    for block_index, (base_block, cand_block) in enumerate(
        zip(baseline_blocks, candidate_blocks)
    ):
        base_by_id = {item[tie_block_id_field]: item for item in base_block[1]}
        cand_by_id = {item[tie_block_id_field]: item for item in cand_block[1]}
        block_path = f"{path}<tie-block {block_index}>"
        if base_by_id.keys() != cand_by_id.keys():
            findings.append(
                Finding(
                    kind="mismatch",
                    path=block_path,
                    detail=(
                        f"tie-block membership differs: "
                        f"{sorted(base_by_id.keys())!r} != {sorted(cand_by_id.keys())!r}"
                    ),
                )
            )
            continue
        for item_id in base_by_id:
            findings.extend(
                _compare_json(
                    base_by_id[item_id],
                    cand_by_id[item_id],
                    path=f"{block_path}[id={item_id!r}]",
                    float_tier_fields=float_tier_fields,
                    relaxed_list_path=None,
                    tie_sort_key=None,
                    tie_block_id_field=tie_block_id_field,
                    allowlisted_envelope_keys=frozenset(),
                )
            )
    return findings
