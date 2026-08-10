"""CHAOS-3678: ``build_production_packet``, the packet_builder extraction.

``build_packet`` (CHAOS-3617) and its ``JobContext``/``TrialContext`` are the
trial's byte-stable constructor and stay untouched -- the whole existing
``tests/context_fabric/`` suite is the byte-stability proof for that path,
since any output drift there fails those tests. This module covers only what
is new: ``build_production_packet``, which emits the same frozen
``ask_dev_investigation_packet.v1`` shape for CHAOS-3678's production caller,
with no ``QuestionFamilyID`` and no trial/fixture/corpus concept at all.

Two things this module holds the line on, because both are silent-drift
risks a type checker cannot catch:

* the family/comparison-shape permission check
  (``QUESTION_FAMILY_REGISTRY[...].permitted_comparison_shapes``) must fire
  for the trial path and must NOT fire for production -- proven side by side
  with the same comparison shape, not asserted from reading the code;
* the emitted packet must validate against the real, generated JSON Schema
  on disk, not just construct without a Python exception.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import jsonschema
import pytest

from dev_health_ops.api.dev.contracts_v2.base import QuestionIntentID
from dev_health_ops.api.dev.investigation_contract import (
    ComparisonShape,
    QuestionFamilyID,
)
from dev_health_ops.api.dev.investigation_contract.export import ARTIFACT_ROOT
from dev_health_ops.context_fabric.graph_arm import fixtures
from dev_health_ops.context_fabric.graph_arm.packet_builder import (
    JobContext,
    ProductionJobContext,
    TrialContext,
    build_packet,
    build_production_packet,
)
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

_PRODUCED_AT = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
_TRIAL_RUN_ID = "4f9a2c1e-1111-4222-8333-444455556666"
_PRODUCTION_RUN_ID = "7c3e9a10-2222-4333-9444-555566667777"


def _readout(projection):
    return asyncio.run(
        ProjectionGraphReader(projection).neighbourhood(
            org_id=projection.org_id,
            seed_canonical_ids=["proj_nightfall_migration"],
            authorized_entity_ids=fixtures.alpha_authorized_ids(),
            max_hops=3,
        )
    )


def _watermark() -> IndexWatermark:
    return IndexWatermark(
        indexed_through=fixtures.WINDOW_END,
        projected_at=fixtures.WINDOW_END,
        records_indexed=42,
    )


@pytest.fixture
def readout(alpha_projection):
    return _readout(alpha_projection)


def _production_packet(
    readout, signer, *, comparison_shape=ComparisonShape.SINGULAR_SUBJECT
):
    return build_production_packet(
        readout=readout,
        job=ProductionJobContext(
            job_id="prod_job_status",
            intent_id=QuestionIntentID.ENTITY_STATUS,
            run_id=_PRODUCTION_RUN_ID,
            job_statement="Status of the Nightfall Migration project.",
            comparison_shape=comparison_shape,
            window_start=fixtures.WINDOW_START,
            window_end=fixtures.WINDOW_END,
        ),
        watermark=_watermark(),
        signer=signer,
        produced_at=_PRODUCED_AT,
    )


def test_build_production_packet_declares_production_provenance(readout, signer):
    packet = _production_packet(readout, signer)
    job = packet.analytical_job
    assert job.schema_version == "ask_dev_analytical_job.v2"
    assert job.question_family is None
    assert job.production_job is not None
    assert job.production_job.intent_id == QuestionIntentID.ENTITY_STATUS.value
    assert job.production_job.run_id == _PRODUCTION_RUN_ID


def test_build_production_packet_omits_trial_metadata(readout, signer):
    packet = _production_packet(readout, signer)
    assert packet.versions.trial is None
    assert packet.versions.corpus_version is None


def test_build_production_packet_validates_against_the_generated_schema(
    readout, signer
):
    """Real jsonschema.validate against the artifact on disk, not just a
    successful Pydantic construction -- the two implementations can diverge
    (CHAOS-3615), so this exercises the actual generated schema.
    """

    packet = _production_packet(readout, signer)
    schema = json.loads(
        (
            ARTIFACT_ROOT / "schemas" / "ask_dev_investigation_packet.v1.schema.json"
        ).read_text(encoding="utf-8")
    )
    jsonschema.validate(packet.model_dump(mode="json"), schema)


def test_the_trial_family_check_still_fires_for_a_shape_it_forbids(readout, signer):
    """RED anchor for the next test. STRUGGLING_TEAMS permits only
    DISCOVERED_COHORT/PORTFOLIO_WIDE, so a SINGULAR_SUBJECT job under that
    family must still be refused on the trial path -- proving the check this
    extraction gates out for production is still load-bearing for
    ``build_packet``.
    """

    with pytest.raises(ValueError, match="does not permit"):
        build_packet(
            readout=readout,
            job=JobContext(
                job_id="job_status",
                question_family=QuestionFamilyID("struggling_teams"),
                job_statement="Status of the Nightfall Migration project.",
                comparison_shape=ComparisonShape.SINGULAR_SUBJECT,
                window_start=fixtures.WINDOW_START,
                window_end=fixtures.WINDOW_END,
            ),
            watermark=_watermark(),
            signer=signer,
            trial=TrialContext(run_id=_TRIAL_RUN_ID),
            produced_at=_PRODUCED_AT,
        )


def test_build_production_packet_skips_the_family_permission_check(readout, signer):
    """The same comparison shape the test above proves is family-forbidden
    for a trial job must NOT be refused for a production job, because
    production has no question family and classifies its own job/shape
    before ever calling this arm (CHAOS-3678 GO ruling) -- there is no
    family here for a permission check to consult.
    """

    packet = _production_packet(
        readout, signer, comparison_shape=ComparisonShape.SINGULAR_SUBJECT
    )
    assert packet.analytical_job.comparison_shape is ComparisonShape.SINGULAR_SUBJECT


def test_build_production_packet_still_enforces_arm_capability_rules(readout, signer):
    """The family check is gated out, but the arm's own cohort-capability
    rule (independent of any question family) still applies to production:
    a cohort-shaped job with no cohort proposal is still refused.
    """

    from dev_health_ops.context_fabric.graph_arm.packet_builder import (
        UnsupportedComparisonShapeError,
    )

    with pytest.raises(UnsupportedComparisonShapeError):
        _production_packet(
            readout, signer, comparison_shape=ComparisonShape.PORTFOLIO_WIDE
        )
