"""The raw machine-readable trial result. This file IS the source of truth.

CHAOS-3619: "preserve raw machine-readable results as the source of truth and
render reports from them". Everything in the rendered report is derived from
a :class:`TrialRecordSet` written here, and ``report.py`` reads the file
rather than a live run so the document cannot quietly describe a different
sweep than the one it claims.

**The durable-record decision, recorded here because it is the artifact's
own provenance.** CHAOS-3618 left the comparison rows as log lines and left
CHAOS-3619 to choose between landing a durable table and parsing the
versioned ``investigation_shadow_record.v1`` log shape. This trial does
neither, and the third option is better than both:

* a durable ClickHouse table is a production schema migration for a
  shadow-only trial that must leave no production trace, and it would put
  trial rows in a tenant's real analytics store;
* parsing log lines re-derives structure that existed one function call
  earlier, and it adds a capture failure mode where a dropped line is
  indistinguishable from a case that never ran -- which is precisely the
  "a measurement that did not happen must fail loudly" hazard.

``shadow_record_payload()`` already derives a versioned, field-complete
mapping from the seam's own dataclass, by iterating its ``fields()``. The
runner calls it and embeds the result verbatim. The versioned shape stays
the contract, field-completeness comes for free (a field added to the record
appears here without an edit), and an absent record becomes impossible
rather than merely detectable.

**Two independent verdicts per row, and neither substitutes for the other.**
``shadow`` is what the seam decided -- did the packet validate, did it cite
only canonical evidence, is it attributable to an arm. ``evaluation`` is
what the frozen oracles decided -- is the packet *right*. A packet can be
seam-rejected and still be scored for subject, cohort and lineage quality,
and collapsing the two would lose exactly the finding that distinguishes "the
arm answered well but bypassed canonical authority" from "the arm answered
badly".
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .binding import TRIAL_ARTIFACT_SCHEMA_VERSION, RunClass, TrialBinding
from .dispositions import CaseDisposition, is_measured

__all__ = [
    "VOID_FILENAME_MARKER",
    "ArmResult",
    "CaseRecord",
    "DimensionOutcome",
    "TrialRecordSet",
    "load_records",
    "write_records",
]

#: The filename infix a voided run must carry.
#:
#: The run class is already inside the file, but a reader holding two
#: artifacts sees filenames first, and "which of these was the real sweep" is
#: exactly the question a smoke run makes ambiguous. Enforced rather than
#: conventional: a convention is what fails the one time it matters.
VOID_FILENAME_MARKER = "SMOKE-VOID"


@dataclass(frozen=True, slots=True)
class DimensionOutcome:
    """One evaluation dimension's verdict for one (case, arm) pair."""

    dimension_id: str
    verdict: str
    detail: str


@dataclass(frozen=True, slots=True)
class ArmResult:
    """One arm's outcome on one case.

    ``dimension_outcomes`` is empty for every disposition except ``SCORED``,
    and :meth:`__post_init__` enforces that rather than trusting callers. An
    unscored row carrying dimension verdicts is the shape in which a
    timed-out or refused case quietly becomes a column of failures in the
    report -- a measurement that never happened, rendered as one that did.
    """

    arm_id: str
    disposition: str
    detail: str
    latency_ms: int
    packet_emitted: bool
    #: The seam's own versioned record, embedded verbatim from
    #: ``shadow_record_payload``. ``None`` when no packet reached the seam.
    shadow: dict[str, Any] | None = None
    contract_valid: bool | None = None
    contract_error: str = ""
    outcome_permitted: bool | None = None
    outcome_detail: str = ""
    is_clean: bool | None = None
    authorization_summary: str = ""
    dimension_outcomes: tuple[DimensionOutcome, ...] = ()
    #: The named debt or ticket that owns an EXPECTED_LIMITATION or
    #: NOT_COMPARABLE row. Required for those dispositions: an unattributed
    #: limitation is indistinguishable from an untested one.
    limitation_owner: str = ""

    def __post_init__(self) -> None:
        disposition = CaseDisposition(self.disposition)
        if self.dimension_outcomes and not is_measured(disposition):
            raise ValueError(
                f"arm {self.arm_id!r} carries {len(self.dimension_outcomes)} "
                f"dimension verdicts under disposition {disposition.value!r}, "
                "which is not a measured disposition. Only a SCORED row may "
                "carry verdicts; anything else would render an absent "
                "measurement as a column of results"
            )
        if (
            disposition
            in {
                CaseDisposition.EXPECTED_LIMITATION,
                CaseDisposition.NOT_COMPARABLE,
            }
            and not self.limitation_owner
        ):
            raise ValueError(
                f"arm {self.arm_id!r} is {disposition.value!r} with no named "
                "owner. A limitation nobody owns reads as a mystery, and the "
                "issue requires expected limitations to be attributable"
            )


@dataclass(frozen=True, slots=True)
class CaseRecord:
    """One corpus case, both arms, plus what the case itself declares."""

    case_id: str
    question: str
    question_family: str
    corpus_family: str
    comparison_shape: str
    variant_kind: str
    expected_answer: str
    principal_id: str
    organization_id: str
    declared_dimension_ids: tuple[str, ...]
    arms: tuple[ArmResult, ...] = ()

    def by_arm(self) -> dict[str, ArmResult]:
        return {result.arm_id: result for result in self.arms}


@dataclass(frozen=True, slots=True)
class TrialRecordSet:
    """Every case, every arm, and what produced them.

    There is deliberately **no** summary, total, percentage or score field.
    The frozen scoring registry types ``aggregate_prohibited`` as
    ``Literal[True]`` on all 28 dimensions; a total here would be the one
    number every reader quoted, and it would hide an arm that improved
    ambiguity while harming driver precision. ``report.py`` derives per-family
    and per-dimension tables from ``cases`` and never a headline.
    """

    schema_version: str
    binding: TrialBinding
    cases: tuple[CaseRecord, ...]
    #: Cases the corpus itself declares unmeasurable or not authorable, kept
    #: with their stated reason. Carried rather than dropped so the artifact
    #: accounts for every registered case: a missing row and a skipped row
    #: look identical once the file is the only evidence.
    non_authored: tuple[dict[str, str], ...] = field(default_factory=tuple)

    def to_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["schema_version"] = TRIAL_ARTIFACT_SCHEMA_VERSION
        return payload


def write_records(records: TrialRecordSet, path: Path) -> None:
    """Write the record set, sorted and newline-terminated.

    ``sort_keys`` and a fixed indent make the file byte-reproducible from
    the same sweep, so a diff between two runs shows what the runs did and
    not how a dict happened to iterate.

    A voided run must say so in its own filename. Putting that only in a JSON
    field would leave two identically-named artifacts whose difference is one
    line deep, and the interim graph-arm packets a smoke run captures today
    carry known-defective vocabulary and withdrawn evidence
    (CHAOS-3627/3628). Those must never survive into anything a reader could
    mistake for the measured sweep. The check runs in both directions,
    because a real measurement filed under a void name is as unusable as the
    reverse.
    """

    run_class = getattr(records.binding, "run_class", None)
    if run_class == RunClass.SMOKE_VOID.value and VOID_FILENAME_MARKER not in path.name:
        raise ValueError(
            f"refusing to write a {RunClass.SMOKE_VOID.value} run to "
            f"{path.name!r}: a voided run's filename must contain "
            f"{VOID_FILENAME_MARKER!r}. A smoke run has the same shape as a "
            "measurement and its packets carry known-defective arm output; a "
            "filename that does not say so is the one thing standing between "
            "it and being quoted as a result"
        )
    if run_class == RunClass.MEASURED.value and VOID_FILENAME_MARKER in path.name:
        raise ValueError(
            f"refusing to write a {RunClass.MEASURED.value} run to "
            f"{path.name!r}: the filename claims {VOID_FILENAME_MARKER!r}, so "
            "a real measurement would be filed where nobody may cite it"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(records.to_json(), indent=2, sort_keys=True, default=str) + "\n"
    )


def load_records(path: Path) -> dict[str, Any]:
    """Read a record set as plain data.

    Returns the mapping rather than rehydrating the dataclasses on purpose:
    the report and its tests are consumers of the *file*, and rehydrating
    would let a change to these classes silently change what the report
    reads out of an artifact written by an older revision.
    """

    if not path.exists():
        raise FileNotFoundError(
            f"no trial records at {path}. The report is rendered from the raw "
            "records and there is nothing to render: run the sweep first. "
            "This is deliberately an error rather than an empty report, "
            "because an empty report reads as a trial that measured nothing "
            "and passed"
        )
    payload: dict[str, Any] = json.loads(path.read_text())
    found = payload.get("schema_version")
    if found != TRIAL_ARTIFACT_SCHEMA_VERSION:
        raise ValueError(
            f"trial records at {path} declare schema {found!r}, but this "
            f"revision reads {TRIAL_ARTIFACT_SCHEMA_VERSION!r}. Refused rather "
            "than parsed on a best-effort basis: a report rendered from a "
            "shape it does not understand is a report that quietly omits "
            "whatever moved"
        )
    return payload
