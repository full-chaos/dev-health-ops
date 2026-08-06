"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: the ``wave4_case_result.v1`` receipt
writer, and a session-level closed-registry summary.

Ad-hoc schema, like ``scripts/acceptance/acceptance_artifact.py``'s
``ask_dev_acceptance_artifact.v1`` (confirmed via recon: acceptance-runner
receipts live entirely outside ``export_contracts.py``/
``export_contracts_v2.py``, which are reserved for wire contracts served to
real clients with fixture-coverage totality enforcement) -- a plain dict
with a hand-picked ``schema_version`` string, written as JSON under
``tests/acceptance/``.

This receipt doubles as the CHAOS-3389 QUA shadow-replay harness (fold-in
design, 2026-08-05): its ``resolution_path`` field is exactly what a future
shadow-mode replay diffs case-by-case against the deterministic baseline,
so its shape is fixed now rather than treated as an implementation detail
free to change later.

False-green prevention (team-lead mandate): ``write`` refuses to produce a
receipt with zero assertions (a case that asserted nothing proves nothing,
regardless of whether every one of its zero checks "passed"), and
:func:`write_session_summary` refuses to claim a session covered anything
if it collected zero case receipts, and reports an expected-vs-received set
difference against the corpus registry rather than silently aggregating
"whatever showed up".
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acceptance.acceptance_artifact import redact_secrets

__all__ = [
    "DECLARED_BLOCKED_RECEIPT_STATUS",
    "RECEIPT_SCHEMA_VERSION",
    "RESOLUTION_PATHS",
    "ReceiptValidationError",
    "SessionSummaryError",
    "Wave4Assertion",
    "Wave4CaseRecorder",
    "write_declared_blocked_receipt",
    "write_session_summary",
]

RECEIPT_SCHEMA_VERSION = "wave4_case_result.v1"

#: A THIRD status value, distinct from "passed"/"failed" -- world-manifest
#: discipline (team-lead direction, 2026-08-06): a declared-blocked case
#: never ran, so it must never look like a pass OR a same-shape failure; it
#: gets its own explicit, loud, ticket-linked status.
DECLARED_BLOCKED_RECEIPT_STATUS = "declared-blocked"

#: Codex round-3 finding (MEDIUM, confirmed) -- mirrors
#: ``case_schema.py``'s identical pattern (this module stays deliberately
#: un-import-coupled to that one, so the check is duplicated rather than
#: shared, matching this module's existing "backend-agnostic" philosophy).
_BLOCKED_BY_TICKET_PATTERN = re.compile(r"^CHAOS-\d+\b")

#: Team-lead decree, binding, kebab-case. ``qua-shadow``/``qua-committed``
#: are reserved for the future QUA shadow-replay mode and are never set by
#: this repo's own resolution_path derivation today -- they exist in this
#: set so a future caller CAN set them without this module rejecting a
#: valid value it merely doesn't produce itself yet.
RESOLUTION_PATHS = frozenset(
    {
        "deterministic-exact",
        "deterministic-alias",
        "miss-clarification",
        "qua-shadow",
        "qua-committed",
    }
)


class ReceiptValidationError(Exception):
    """A receipt would be written in a state that reads as false coverage."""


#: Every key ``write()`` itself computes and guards. Codex round-1 finding
#: (HIGH, confirmed): ``set_extra`` had no reserved-key check and its
#: ``**self._extra`` was merged LAST into the artifact dict, so
#: ``set_extra("status", "passed")`` (or ``resolution_path``/``world_digest``/
#: ``assertion_count``/...) silently overwrote the guarded value -- a
#: caller could record a failed assertion, then paper over it with one
#: extra-field call. Closed two ways, redundantly: :meth:`set_extra` now
#: refuses any of these keys outright, AND ``write()`` spreads ``_extra``
#: FIRST so even a hypothetical future bypass of the first guard could only
#: ever be overwritten by the real computed value, never the reverse.
_RESERVED_RECEIPT_KEYS = frozenset(
    {
        "schema_version",
        "case_id",
        "question",
        "subject_class",
        "resolution_profile_ref",
        "resolution_path",
        "world_digest",
        "started_at",
        "finished_at",
        "status",
        "assertion_count",
        "assertions",
    }
)


@dataclass(frozen=True, slots=True)
class Wave4Assertion:
    category: str
    name: str
    passed: bool
    detail: str


@dataclass(slots=True)
class Wave4CaseRecorder:
    """Collects one corpus case's assertions and writes its
    ``wave4_case_result.v1`` receipt.

    ``resolution_path`` and ``world_digest`` are tracked as explicitly-set
    fields (via :meth:`set_resolution_path`/:meth:`set_world_digest`), not
    constructor defaults, so a caller that forgets to set either gets a
    loud error at :meth:`write` time rather than a receipt silently
    claiming ``resolution_path: null`` for a case that actually had one, or
    an un-pinned world digest.
    """

    case_id: str
    question: str
    subject_class: str
    resolution_profile_ref: str | None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    assertions: list[Wave4Assertion] = field(default_factory=list)
    _resolution_path: str | None = field(default=None, repr=False)
    _resolution_path_set: bool = field(default=False, repr=False)
    _world_digest: str | None = field(default=None, repr=False)
    _world_digest_set: bool = field(default=False, repr=False)
    _extra: dict[str, Any] = field(default_factory=dict, repr=False)

    def check(self, *, category: str, name: str, condition: bool, detail: str) -> None:
        """Record one assertion. Never raises on a failed condition --
        unlike ``ScenarioRecorder.check`` (acceptance_artifact.py), a
        corpus case's remaining invariants still matter for diagnosis even
        after one fails, so this records every assertion for the case and
        lets :meth:`write` compute the overall verdict from the full list.
        """

        self.assertions.append(
            Wave4Assertion(
                category=category,
                name=name,
                passed=bool(condition),
                detail=redact_secrets(detail),
            )
        )

    def set_resolution_path(self, value: str | None) -> None:
        if value is not None and value not in RESOLUTION_PATHS:
            raise ReceiptValidationError(
                f"resolution_path {value!r} is not one of {sorted(RESOLUTION_PATHS)!r}"
            )
        self._resolution_path = value
        self._resolution_path_set = True

    def set_world_digest(self, value: str) -> None:
        if not value:
            raise ReceiptValidationError("world_digest must be a non-empty string")
        self._world_digest = value
        self._world_digest_set = True

    def set_extra(self, key: str, value: Any) -> None:
        """Escape hatch for fields specific to one case/family (e.g. a
        quota snapshot, a provider-script id used) that do not warrant a
        dedicated constructor parameter every other case must also thread
        through.

        Raises on any of :data:`_RESERVED_RECEIPT_KEYS` -- see that
        constant's docstring for why (Codex round-1, HIGH).
        """

        if key in _RESERVED_RECEIPT_KEYS:
            raise ReceiptValidationError(
                f"{key!r} is a reserved receipt field computed by write() -- "
                "set_extra cannot be used to set or override it"
            )
        self._extra[key] = value

    def write(self, path: Path) -> dict[str, Any]:
        if not self.assertions:
            raise ReceiptValidationError(
                f"case {self.case_id!r}: refusing to write a receipt with "
                "zero assertions -- a case that asserted nothing proves "
                "nothing about its own claim, regardless of the case's "
                "'invariants' declaration (see case_schema.py's own "
                "non-empty-invariants guard, which should make this "
                "unreachable in practice; this is the defense-in-depth "
                "backstop at write time)"
            )
        if not self._resolution_path_set:
            raise ReceiptValidationError(
                f"case {self.case_id!r}: resolution_path was never set -- "
                "call set_resolution_path(None) explicitly for a "
                "non-subject-shaped case, so the receipt records an honest, "
                "deliberate absence rather than a value that was simply "
                "never assigned"
            )
        if not self._world_digest_set:
            raise ReceiptValidationError(
                f"case {self.case_id!r}: world_digest was never set -- "
                "every receipt must be pinned to the WORLD_DIGEST it ran "
                "against (ruling D2), even a matched one"
            )

        finished_at = datetime.now(UTC)
        all_passed = all(a.passed for a in self.assertions)
        # `_extra` spreads FIRST -- defense in depth alongside set_extra's own
        # reserved-key rejection (Codex round-1, HIGH): even if a reserved
        # key somehow reached `_extra`, the computed fields below always win.
        artifact: dict[str, Any] = {
            **self._extra,
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "case_id": self.case_id,
            "question": self.question,
            "subject_class": self.subject_class,
            "resolution_profile_ref": self.resolution_profile_ref,
            "resolution_path": self._resolution_path,
            "world_digest": self._world_digest,
            "started_at": self.started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "status": "passed" if all_passed else "failed",
            "assertion_count": len(self.assertions),
            "assertions": [
                {
                    "category": a.category,
                    "name": a.name,
                    "passed": a.passed,
                    "detail": a.detail,
                }
                for a in self.assertions
            ],
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(artifact, indent=2, sort_keys=False) + "\n", encoding="utf-8"
        )
        return artifact


def write_declared_blocked_receipt(
    *,
    case_id: str,
    question: str,
    subject_class: str,
    resolution_profile_ref: str | None,
    blocked_by: str,
    path: Path,
) -> dict[str, Any]:
    """Write a ``wave4_case_result.v1`` receipt for a case that never ran.

    Takes plain values rather than a ``CorpusCase`` object (case_schema.py's
    type) so this module stays decoupled from that one -- the same
    "backend-agnostic" philosophy ``resolution_path.py`` already applies to
    its own inputs. ``blocked_by`` is REQUIRED and non-empty: a
    declared-blocked receipt with no ticket reference would be exactly the
    silent, untraceable "blocked" this whole mechanism exists to prevent
    (mirrors ``case_schema.py``'s own load-time requirement).
    """

    if not blocked_by.strip():
        raise ReceiptValidationError(
            f"case {case_id!r}: declared-blocked receipt requires a "
            "non-empty 'blocked_by' ticket reference"
        )
    if not _BLOCKED_BY_TICKET_PATTERN.match(blocked_by.strip()):
        raise ReceiptValidationError(
            f"case {case_id!r}: declared-blocked receipt 'blocked_by' "
            f"{blocked_by!r} does not start with a real ticket reference "
            "(e.g. 'CHAOS-3393')"
        )
    now = datetime.now(UTC).isoformat()
    artifact: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "case_id": case_id,
        "question": question,
        "subject_class": subject_class,
        "resolution_profile_ref": resolution_profile_ref,
        "resolution_path": None,
        "world_digest": None,
        "started_at": now,
        "finished_at": now,
        "status": DECLARED_BLOCKED_RECEIPT_STATUS,
        "blocked_by": blocked_by,
        "assertion_count": 0,
        "assertions": [],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(artifact, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    return artifact


class SessionSummaryError(Exception):
    """A corpus-run session's receipts do not support a trustworthy summary."""


def write_session_summary(
    receipts: list[dict[str, Any]],
    path: Path,
    *,
    expected_case_ids: frozenset[str],
) -> dict[str, Any]:
    """Aggregate a corpus run's per-case receipts into one session summary.

    False-green guards, per the team-lead mandate:

    * ``case_count > 0`` -- a session that collected zero receipts raises
      rather than writing a summary that would otherwise report "0/0 cases
      passed", which reads as vacuously green.
    * expected-vs-received set difference against ``expected_case_ids`` --
      reported, never silently absorbed. A case in ``expected_case_ids``
      with no receipt is ``missing``; a receipt whose case id is not in
      ``expected_case_ids`` is ``unexpected`` (e.g. a misrouted or stale
      artifact). Neither condition raises here -- Phase 5a's aggregator
      owns the launch-threshold gating decision over this summary; this
      function's job is to make both conditions visible in the artifact,
      not to itself decide pass/fail policy on them.
    """

    if not receipts:
        raise SessionSummaryError(
            "refusing to write a session summary for zero case receipts -- "
            "a corpus run that collected no cases at all must fail loud, "
            "never report a vacuous, technically-zero-failures green"
        )
    received_ids = {receipt["case_id"] for receipt in receipts}
    missing = sorted(expected_case_ids - received_ids)
    unexpected = sorted(received_ids - expected_case_ids)
    passed = sum(1 for receipt in receipts if receipt.get("status") == "passed")
    declared_blocked = sorted(
        receipt["case_id"]
        for receipt in receipts
        if receipt.get("status") == DECLARED_BLOCKED_RECEIPT_STATUS
    )
    summary = {
        "schema_version": "wave4_session_summary.v1",
        "case_count": len(receipts),
        "passed_count": passed,
        "failed_count": len(receipts) - passed - len(declared_blocked),
        "declared_blocked_count": len(declared_blocked),
        "declared_blocked_case_ids": declared_blocked,
        "expected_case_count": len(expected_case_ids),
        "missing_case_ids": missing,
        "unexpected_case_ids": unexpected,
        "generated_at": datetime.now(UTC).isoformat(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(summary, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    return summary
