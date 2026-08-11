"""CHAOS-3646: the measured sweep. Admission off vs admission on.

Answers exactly one question the CHAOS-3619 trial left open, and refuses to
answer any other. The trial proved the graph arm builds valid investigation
packets and could not measure whether an Ask Dev **answer** is better for
them, because the frame boundary rejects graph-discovered evidence outright.
So every case here is run twice over the same traversal:

* **admission off** -- the arm mints its own handles, exactly as the merged
  trial measured. The packet meets ``InvestigationShadow`` and the seam
  decides.
* **admission on** -- the arm's pointers go to the canonical evidence service,
  the admitted refs enter the frame, the packet cites them verbatim, and the
  same seam decides again.

Then the frozen oracles score the packet on the judgment dimensions. No new
oracle, no modified oracle, no aggregate: the frozen registry types
``aggregate_prohibited`` as ``Literal[True]`` on all 28 dimensions, and a
headline number here would hide exactly the per-case variation the correction
addendum requires to stay visible.

FAIRNESS. Seeds are derived from the question by production
``extract_mentions`` plus resolution against the arm's own projection. No
oracle, no expected subject, no corpus case attribute other than the question
text and the principal reaches the arm.

Run: ``python -m trials.chaos_3646.sweep``
"""

from __future__ import annotations

import asyncio
import json
import re
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dev_health_ops.api.dev.contracts_v2.embedded import DevEvidenceRefV2
from dev_health_ops.api.dev.evidence_service import (
    MAX_ADMISSION_CANDIDATES,
    EvidenceAdmission,
    EvidenceAdmissionResult,
    ScopeResolveRequest,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.investigation_corpus.cases import CASE_REGISTRY
from dev_health_ops.api.dev.investigation_corpus.evaluate import evaluate_payload
from dev_health_ops.api.dev.investigation_shadow import (
    InvestigationShadow,
    InvestigationShadowStatus,
)
from dev_health_ops.api.dev.question_interpreter import extract_mentions
from dev_health_ops.context_fabric.graph_arm.admission import candidates_from_readout
from dev_health_ops.context_fabric.graph_arm.discovery import search_candidates

from .canonical import corpus_evidence_service
from .spine import grant_for

RESULTS = Path(__file__).resolve().parent / "results" / "admission-records.json"

SIGNING_SECRET = "chaos-3646-admission-trial-secret-not-a-real-key"

#: The dimensions this lane was asked to measure. Named individually rather
#: than swept, because "score the judgment dimensions" is a choice a reader
#: must be able to check against the registry.
MEASURED_DIMENSIONS = (
    "answer_usefulness_beyond_dashboard",
    "principal_driver_precision",
    "principal_driver_recall",
    "symptom_versus_driver_distinction",
    "comparative_judgment_support",
)


@dataclass
class LegRecord:
    """One (case, admission setting) row."""

    admission: str
    seam_status: str
    seam_detail: str
    candidates_submitted: int = 0
    candidates_admitted: int = 0
    refusals: dict[str, int] = field(default_factory=dict)
    evidence_cited: int = 0
    #: ``None`` when the packet was never put to the validator (no oracle,
    #: or the arm faulted). False is a VERDICT and must not stand in for one.
    contract_valid: bool | None = None
    contract_error: str | None = None
    dimensions: dict[str, str] = field(default_factory=dict)
    dimension_details: dict[str, str] = field(default_factory=dict)


@dataclass
class CaseRecord:
    case_id: str
    question: str
    principal_id: str
    seeds: list[str]
    outcome: str
    legs: list[LegRecord] = field(default_factory=list)
    note: str = ""


#: The untyped backstop. Production ``extract_mentions`` only recognises a
#: name ADJACENT TO A KIND NOUN ("project Lattice Search"), so "do we have
#: enough people on Lattice Search?" extracts nothing -- a ceiling BOTH arms
#: share and the largest single cause of the CHAOS-3619 trial's refusals (21
#: of 26). This backstop is NOT production code and is not claimed as any
#: arm's capability: it exists so admission can be measured at all, and every
#: case it unlocks is a case the deployed extraction would not reach. Stated
#: here, and again in the artifact, because a reader who took these as
#: deployed results would be reading a capability nothing ships.
_CAPITALIZED_SPAN = re.compile(
    r"\b(?:[A-Z][A-Za-z0-9]*)(?:[ -](?:[A-Z][A-Za-z0-9]*))*\b"
)
_SENTENCE_WORDS = frozenset(
    {
        "What",
        "Which",
        "Who",
        "Why",
        "How",
        "Where",
        "When",
        "Is",
        "Are",
        "Do",
        "Does",
        "Did",
        "Can",
        "Should",
        "I",
        "We",
        "The",
        "A",
        "An",
        "Compare",
        "Any",
        "Show",
    }
)


def _mention_texts(question: str) -> tuple[str, ...]:
    """Subject phrases, production extraction first, backstop second."""

    phrases = [
        mention.normalized_lookup_text
        for mention in extract_mentions(question)
        if mention.normalized_lookup_text
    ]
    for match in _CAPITALIZED_SPAN.finditer(question):
        words = [word for word in match.group(0).split() if word not in _SENTENCE_WORDS]
        if words:
            phrases.append(" ".join(words))
    return tuple(dict.fromkeys(phrases))


def _seeds_for(question: str, projection) -> tuple[str, ...]:
    """Subject ids the arm can reach from the question alone.

    Nothing from the oracle, nothing from the case beyond its question text
    and its principal. The arm's OWN discovery does the resolution, against
    the principal's true grant, so a subject the grant withholds resolves to
    nothing -- which is the arm behaving correctly, not failing.
    """

    grant = sorted(grant_for())
    seeds: list[str] = []
    for phrase in _mention_texts(question):
        matches, _filtered = search_candidates(phrase, projection.nodes, grant)
        seeds.extend(match.canonical_id for match in matches)
    return tuple(dict.fromkeys(seeds))[:1]


def _admit(readout, principal_id: str) -> tuple[EvidenceAdmissionResult, dict]:
    service = corpus_evidence_service(
        org_id=readout.org_id, principal_id=principal_id, secret=SIGNING_SECRET
    )
    candidates = candidates_from_readout(readout)
    request = ScopeResolveRequest(
        explicit_refs=(),
        team_filter_refs=(),
        allow_organization_fallback=False,
    )

    # BATCHED, never truncated. ``MAX_ADMISSION_CANDIDATES`` bounds one round,
    # not a run, and slicing to it would silently drop every record past the
    # 25th -- which the first version did, and which surfaced as a driver
    # citing evidence "this packet never indexed". A cap that removes evidence
    # without saying so reads as a clean packet.
    admissions: list[EvidenceAdmission] = []
    for start in range(0, len(candidates), MAX_ADMISSION_CANDIDATES):
        batch = candidates[start : start + MAX_ADMISSION_CANDIDATES]
        round_result = asyncio.run(
            service.admit(
                org_id=readout.org_id,
                permission_fingerprint="chaos-3646-sweep",
                scope_request=request,
                candidates=batch,
            )
        )
        admissions.extend(round_result.admissions)
    result = EvidenceAdmissionResult(tuple(admissions))
    admitted = {
        item.candidate.locator: DevEvidenceRefV2.model_validate(
            item.evidence.model_dump()
        )
        for item in result.admissions
        if item.evidence is not None
    }
    return result, admitted


def _seam_verdict(packet, canonical_evidence) -> tuple[str, str]:
    shadow = InvestigationShadow(enabled=True)
    record = shadow.evaluate(
        payload=json.loads(packet.model_dump_json()),
        run_id=packet.versions.trial.run_id,
        organization_id=packet.organization_id,
        canonical_evidence=tuple(canonical_evidence),
    )
    return record.status.value, record.detail or ""


def _score(case_id: str, packet) -> tuple[bool | None, str | None, dict, dict]:
    try:
        evaluation = evaluate_payload(case_id, json.loads(packet.model_dump_json()))
    except KeyError:
        # The frozen corpus registers cases the frozen oracle set does not
        # cover. Reported as "no oracle" rather than skipped: an unscored case
        # that vanished from the artifact would shrink the denominator without
        # saying so.
        return (
            None,
            None,
            dict.fromkeys(MEASURED_DIMENSIONS, "no_oracle"),
            dict.fromkeys(
                MEASURED_DIMENSIONS,
                "the frozen oracle set holds no oracle for this case, so "
                "nothing here was scored in either direction",
            ),
        )
    verdicts: dict[str, str] = {}
    details: dict[str, str] = {}
    for result in evaluation.results:
        name = str(result.dimension_id)
        if name in MEASURED_DIMENSIONS:
            verdicts[name] = str(result.verdict.value)
            details[name] = result.detail
    return evaluation.contract_valid, evaluation.contract_error, verdicts, details


def _refusal_counts(result: EvidenceAdmissionResult) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in result.admissions:
        if item.evidence is not None:
            continue
        key = f"{item.state.value}:{item.warning}"
        counts[key] = counts.get(key, 0) + 1
    return counts


def run() -> list[CaseRecord]:
    from . import spine

    projection = spine.projection()
    records: list[CaseRecord] = []
    for case in CASE_REGISTRY.values():
        if case.principal_id != world.PRINCIPAL_ANALYST:
            continue
        seeds = _seeds_for(case.question, projection)
        record = CaseRecord(
            case_id=case.case_id,
            question=case.question,
            principal_id=case.principal_id,
            seeds=list(seeds),
            outcome="not_reached",
        )
        if not seeds:
            record.note = (
                "no authorized subject resolved from the question; the arm "
                "never reached evidence, so admission is not exercised. Same "
                "shared-layer ceiling CHAOS-3619 measured on 21 of 26 "
                "refusals -- not an admission result in either direction"
            )
            records.append(record)
            continue
        readout = spine.readout(seeds)
        for admission_on in (False, True):
            leg = _run_leg(case, readout, seeds, admission_on)
            record.legs.append(leg)
        record.outcome = "measured"
        records.append(record)
    return records


def _run_leg(case, readout, seeds, admission_on: bool) -> LegRecord:
    from . import spine

    if admission_on:
        result, admitted = _admit(readout, case.principal_id)
        canonical = list(admitted.values())
        leg = LegRecord(
            admission="on",
            seam_status="",
            seam_detail="",
            candidates_submitted=len(result.admissions),
            candidates_admitted=len(admitted),
            refusals=_refusal_counts(result),
        )
    else:
        admitted = None
        # Admission off: the frame's canonical set is what the native run
        # produced, which for a graph-discovered record is nothing. Empty is
        # the honest value and is the whole reason the trial recorded a
        # canonical bypass.
        canonical = []
        leg = LegRecord(admission="off", seam_status="", seam_detail="")
    try:
        packet = spine.packet(
            readout, case, seeds, admitted_evidence=admitted, drivers=True
        )
    except Exception as fault:
        # ARM_FAULT, recorded rather than skipped or worked around. The same
        # disposition the CHAOS-3619 trial gave A05: a case the arm cannot
        # emit is a measured outcome, and a sweep that dropped it would report
        # a smaller, cleaner corpus than it actually ran.
        leg.seam_status = "arm_fault"
        leg.seam_detail = f"{type(fault).__name__}: {fault}"
        leg.dimensions = dict.fromkeys(MEASURED_DIMENSIONS, "arm_fault")
        leg.dimension_details = dict.fromkeys(
            MEASURED_DIMENSIONS,
            "the arm could not emit a packet for this case, so nothing was "
            "scored; see seam_detail",
        )
        return leg
    leg.seam_status, leg.seam_detail = _seam_verdict(packet, canonical)
    leg.evidence_cited = len(packet.evidence_coverage.evidence_index)
    (
        leg.contract_valid,
        leg.contract_error,
        leg.dimensions,
        leg.dimension_details,
    ) = _score(case.case_id, packet)
    if leg.seam_status != InvestigationShadowStatus.RECORDED.value:
        # The seam refused the packet, so there IS no final answer and the
        # dimension verdicts below describe a packet that never reached one.
        # Blanked rather than reported: a scored row beside a rejected packet
        # is precisely the "reads as coverage" failure this lane exists to
        # avoid.
        leg.dimension_details = {
            name: "not measured through the frame: the seam refused the packet"
            for name in leg.dimensions
        }
        leg.dimensions = {name: "not_measured" for name in leg.dimensions}
    return leg


def _provenance() -> dict[str, Any]:
    root = Path(__file__).resolve().parents[2]

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()

    return {
        "lane_commit": git("rev-parse", "HEAD"),
        "tree_clean": git("status", "--porcelain") == "",
        "corpus_version": world.CORPUS_VERSION,
    }


def main() -> None:
    records = run()
    payload = {
        "schema_version": "chaos_3646_admission_records.v1",
        "provenance": _provenance(),
        "measured_dimensions": list(MEASURED_DIMENSIONS),
        "aggregate_prohibited": True,
        "cases": [asdict(record) for record in records],
    }
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(f"wrote {RESULTS}")


if __name__ == "__main__":
    main()
