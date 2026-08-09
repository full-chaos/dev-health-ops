"""CHAOS-3620: semantic safety, and what the trial is allowed to observe.

Two halves of one question — what may the graph *say*, and what may the
system *record about what it said*.

The semantic half is governed by one rule from the corrective plan: "the
graph determines what is relevant; canonical services determine what is
measurable". Everything here tests the second clause, because the first is
the capability under trial and the second is the thing that must not bend if
the capability turns out to be good.

The observability half is scored against CHAOS-3218's own semantics rather
than a parallel invented one. The trial's single durable emission is the
``investigation_shadow_record.v1`` line
(``orchestrator_persistence.py:287-323``), so that record is what gets
audited for content safety — not the packet, which never leaves the process
as telemetry.

Two named gaps are pinned here rather than described: the shadow record
carries no authorization-filtered count at all, and the trial has no durable
record table.
"""

from __future__ import annotations

import json
from functools import cache

import pytest

from dev_health_ops.api.dev import investigation_shadow as shadow
from dev_health_ops.api.dev.investigation_contract import (
    DriverCategory,
    DriverRole,
    DriverStanding,
)
from dev_health_ops.api.dev.investigation_contract.vocabulary import (
    ASSERTED_DRIVER_STANDINGS,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.investigation_corpus.authorization import (
    entity_sightings,
)
from dev_health_ops.context_fabric.graph_arm.drivers import (
    MEASUREMENT_ONLY_CATEGORIES,
)
from dev_health_ops.context_fabric.graph_arm.identity import partition_for_org
from tests.context_fabric import chaos_3620_spine as spine

#: Backend vocabulary that must not reach any consumer surface. A superset
#: of the corpus scorer's list (``evaluate.py:1451-1459``) plus the arm's own
#: partition prefix, which is a storage location and would be the most
#: literal possible leak.
BANNED_BACKEND_TOKENS = (
    "graphiti",
    "neo4j",
    "falkordb",
    "kuzu",
    "neptune",
    "cypher",
    "match (",
    "gremlin",
    "group_id",
    "cf_trial_",
    "name_embedding",
    "fact_embedding",
)


#: Same denial-of-packet exemption the authorization sweep carries, and for
#: the same reason: ``team_atlas`` cannot produce a driver-bearing packet at
#: all (CHAOS-3634). Driver DISCOVERY still runs for it — only packet
#: emission is refused — so the finding sweep below loses nothing, which is
#: why it uses ``discover_drivers`` output rather than packets.
PACKET_UNCONSTRUCTIBLE_WITH_DRIVERS = {"team_atlas"}


def _every_authorized_subject() -> tuple[str, ...]:
    """**Every** entity the analyst may see, not just the projects.

    Adversarial review found this narrowed to ``proj_*`` — 13 of 47 — while
    the class docstrings claimed sweeps over "the whole authorized world".
    Teams, repositories, services, work units and the rest all produce
    findings, and a "the arm never does X" claim that skipped three quarters
    of its subjects was supporting a much smaller statement than it made.
    """

    return tuple(sorted(world.PRINCIPALS[world.PRINCIPAL_ANALYST].visible_entity_ids))


@cache
def _all_findings():
    """Every finding the arm produces anywhere in the authorized world.

    Swept rather than sampled because the claims below are all of the form
    "the arm never does X", and a sample can only ever support "the arm did
    not do X here".

    Built from ``discover_drivers`` rather than from packets so the one seed
    whose PACKET cannot be constructed still contributes its findings — the
    semantic claims are about what the arm is willing to assert, and a
    contract refusal downstream does not unmake an assertion.
    """

    return tuple(
        (seed, finding)
        for seed in _every_authorized_subject()
        for finding in spine.findings_for(seed)
    )


# --------------------------------------------------------------------------
# The graph may not create canonical truth
# --------------------------------------------------------------------------


class TestNoCanonicalTruthIsCreated:
    def test_the_sweep_actually_produces_findings(self) -> None:
        """Anti-vacuity for every "never" below."""

        findings = _all_findings()
        assert len(findings) >= 5, (
            f"the whole authorized world produced only {len(findings)} "
            "findings, so the negative claims in this class are close to "
            "vacuous"
        )

    def test_no_staffing_or_capacity_claim_is_ever_ASSERTED(self) -> None:
        """The ban, stated as what is actually true.

        The first version of this test claimed the arm produces *no*
        capacity/staffing finding at all, in any standing. That was false and
        only looked true because the sweep covered 13 of 47 subjects.
        Widening it found ``drv_metric_atlas_load`` on ``team_atlas``
        immediately.

        The safety property survives — and via a better mechanism than the
        one originally described. The finding exists, is derived from a cited
        canonical measurement, and is capped at ``CONTEXTUAL_CORRELATE`` /
        ``CANDIDATE_ONLY``: it can be shown to a reader as context and can
        never be attributed as a cause. What must never happen is a capacity
        claim reaching *asserted* standing, and that is what is checked.
        """

        capacity = [
            (seed, finding)
            for seed, finding in _all_findings()
            if finding.category is DriverCategory.CAPACITY_OR_STAFFING
        ]
        assert capacity, (
            "the corpus no longer produces any capacity/staffing finding, so "
            "this ban is vacuous — check the sweep before believing it"
        )
        offenders = [
            (seed, finding.driver_id, str(finding.standing))
            for seed, finding in capacity
            if finding.standing in ASSERTED_DRIVER_STANDINGS
        ]
        assert not offenders, (
            f"a capacity/staffing finding reached asserted standing: {offenders}"
        )

    def test_an_unqualified_capacity_finding_cannot_be_EMITTED_at_all(self) -> None:
        """The second, independent refusal — and a denial-of-packet.

        Beyond the standing cap, the frozen contract refuses a
        capacity/staffing driver carrying no ``staffing_qualification``: "a
        staffing claim that says nothing about its denominator is an
        unsupported claim". On ``team_atlas`` that refusal aborts packet
        construction entirely.

        Recorded as its own disposition rather than absorbed as a skip. It is
        a real safety refusal working, *and* a denial of service for that
        subject — CHAOS-3634 owns making the arm qualify the finding instead
        of losing the packet. Both halves are true and a reader needs both.
        """

        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="staffing_qualification"):
            spine.investigate("team_atlas", with_drivers=True)

    def test_no_measurement_only_category_reaches_asserted_standing(self) -> None:
        """The general rule the staffing ban is one instance of."""

        offenders = [
            (seed, finding.driver_id, str(finding.category))
            for seed, finding in _all_findings()
            if finding.category in MEASUREMENT_ONLY_CATEGORIES
            and finding.standing in ASSERTED_DRIVER_STANDINGS
        ]
        assert not offenders, (
            f"a measurement-only category was asserted as a driver: {offenders}"
        )

    def test_the_measurement_only_set_is_not_empty(self) -> None:
        """Without this the two bans above pass if the set is emptied."""

        assert MEASUREMENT_ONLY_CATEGORIES, (
            "the measurement-only category set is empty, so the refusal that "
            "reads it can never fire"
        )
        assert DriverCategory.CAPACITY_OR_STAFFING in MEASUREMENT_ONLY_CATEGORIES, (
            "capacity/staffing left the measurement-only set, so a structural "
            "rule may now assert a staffing claim"
        )

    def test_a_measured_value_is_cited_verbatim_and_never_recomputed(self) -> None:
        """ "Cited, never computed", checked against the world's own number.

        The comparison is against ``WORLD_MEASUREMENTS``, which is the
        canonical service's value. A rounded, scaled or re-derived number
        would be the arm measuring rather than citing — and would be
        invisible in the packet, because both render as a number.
        """

        projection = spine.helio_projection()
        by_key = {
            measurement.measurement_key: measurement
            for measurement in world.WORLD_MEASUREMENTS
            if measurement.tenant_id == world.ORG_HELIO
        }
        checked = 0
        for node in projection.observation_nodes():
            measurement = by_key.get(node.canonical_id)
            if measurement is None:
                continue
            checked += 1
            assert node.attributes["measurement_value"] == measurement.value, (
                f"{node.canonical_id} carries {node.attributes['measurement_value']!r} "
                f"where the canonical service said {measurement.value!r}"
            )
        assert checked, (
            "no canonical measurement reached the projection, so verbatim "
            "carriage was never checked"
        )

    def test_a_declared_status_is_reported_as_a_symptom_not_a_completion_verdict(
        self,
    ) -> None:
        """Declared-complete is a claim someone made, not a fact about the world.

        The arm reads ``declared_status`` and may surface the mismatch. It
        must not turn that into a completion judgment of its own, so any
        finding derived from a declared status carries the SYMPTOM role and
        never asserted standing.
        """

        declared = [
            (seed, finding)
            for seed, finding in _all_findings()
            if finding.driver_id.startswith("drv_symptom_sc_")
        ]
        assert declared, (
            "no declared-status symptom was produced anywhere, so this claim "
            "is untested"
        )
        for seed, finding in declared:
            assert finding.role is DriverRole.SYMPTOM, (
                f"{seed}/{finding.driver_id} treats a declared status as a "
                f"{finding.role}"
            )
            assert finding.standing not in ASSERTED_DRIVER_STANDINGS, (
                f"{seed}/{finding.driver_id} asserts a completion judgment"
            )


# --------------------------------------------------------------------------
# Missing denominators disclose; they do not refuse
# --------------------------------------------------------------------------


class TestAMissingDenominatorDisclosesRatherThanRefuses:
    """The corrective plan is explicit in both directions: a missing
    allocation or headcount denominator must reduce confidence and require
    qualification, and must **not** make the capacity question unsupported.
    A wholesale refusal is as much a failure as an unqualified claim.
    """

    def test_the_corpus_plants_a_project_with_no_allocation_feed(self) -> None:
        record = world.EVIDENCE_BY_SLUG.get("sh_solstice_no_allocation")
        assert record is not None, (
            "the corpus no longer plants a project with no allocation feed, "
            "so the disclose-don't-refuse behaviour is untested"
        )

    def test_that_project_still_receives_an_investigation(self) -> None:
        investigation = spine.investigate("proj_solstice", with_drivers=True)
        assert investigation.packet.related_context.entities, (
            "a project with no allocation feed received an empty related "
            "context: the question was refused wholesale"
        )
        assert investigation.findings, (
            "a project with no allocation feed produced no findings at all"
        )

    def test_the_absence_is_surfaced_rather_than_swallowed(self) -> None:
        investigation = spine.investigate("proj_solstice", with_drivers=True)
        surfaced = [
            finding
            for finding in investigation.findings
            if "no_allocation" in finding.driver_id
        ]
        assert surfaced, (
            "the missing allocation feed is not surfaced anywhere in the "
            "findings, so a consumer cannot tell the denominator is absent"
        )

    def test_an_uncomparable_measurement_says_why_rather_than_disappearing(
        self,
    ) -> None:
        """The other half of disclosure.

        A measured value with no cohort median cannot say whether it is
        unusual. Dropping it silently would make the packet look complete;
        the arm keeps it, excludes it, and states the reason in its own
        summary.
        """

        investigation = spine.investigate("proj_solstice", with_drivers=True)
        excluded = [
            finding
            for finding in investigation.findings
            if finding.standing is DriverStanding.EXCLUDED
            and finding.driver_id.startswith("drv_metric_")
        ]
        assert excluded, "no uncomparable measurement was retained and excluded"
        assert any(
            "cohort comparison" in finding.summary_detail for finding in excluded
        ), (
            "an uncomparable measurement was excluded without saying why: "
            f"{[f.summary_detail for f in excluded]}"
        )


# --------------------------------------------------------------------------
# Symptoms are not promoted
# --------------------------------------------------------------------------


class TestSymptomsAreNotPromotedWithoutLineage:
    def test_no_symptom_anywhere_reaches_asserted_standing(self) -> None:
        offenders = [
            (seed, finding.driver_id, str(finding.standing))
            for seed, finding in _all_findings()
            if finding.role is DriverRole.SYMPTOM
            and finding.standing in ASSERTED_DRIVER_STANDINGS
        ]
        assert not offenders, f"a symptom was promoted to a driver: {offenders}"

    def test_symptoms_are_actually_produced(self) -> None:
        symptoms = [
            finding
            for _seed, finding in _all_findings()
            if finding.role is DriverRole.SYMPTOM
        ]
        assert symptoms, (
            "the arm produces no symptoms at all, so refusing to promote one "
            "measures nothing"
        )

    def test_at_most_one_principal_driver_per_investigation(self) -> None:
        for seed in _every_authorized_subject():
            if seed in PACKET_UNCONSTRUCTIBLE_WITH_DRIVERS:
                # Not a gap in this claim: the promotion rule is checked on
                # the FINDINGS for this seed below, and its packet is refused
                # for an unrelated reason (CHAOS-3634).
                continue
            packet = spine.investigate(seed, with_drivers=True).packet
            assert len(packet.driver_analysis.principal_driver_ids) <= 1, (
                f"{seed} names {len(packet.driver_analysis.principal_driver_ids)} "
                "principal drivers; the frozen contract admits at most one"
            )

    def test_at_most_one_principal_driver_even_where_no_packet_can_be_built(
        self,
    ) -> None:
        """The promotion rule read off the findings, so no subject is skipped.

        Covers the seed whose packet cannot be constructed — otherwise the
        contract refusal would silently exempt it from a rule that is about
        driver discovery, not about emission.
        """

        for seed in _every_authorized_subject():
            principals = [
                finding.driver_id
                for finding in spine.findings_for(seed)
                if finding.standing is DriverStanding.PRINCIPAL_DRIVER
            ]
            assert len(principals) <= 1, (
                f"{seed} produced {len(principals)} principal drivers: {principals}"
            )

    def test_a_principal_driver_is_produced_somewhere(self) -> None:
        """Anti-vacuity: "at most one" is satisfied by never producing any."""

        principals = [
            (seed, finding.driver_id)
            for seed, finding in _all_findings()
            if finding.standing is DriverStanding.PRINCIPAL_DRIVER
        ]
        assert principals, (
            "no investigation in the whole authorized world produced a "
            "principal driver, so the promotion rules are untested"
        )


# --------------------------------------------------------------------------
# No graph-native surface reaches a consumer
# --------------------------------------------------------------------------


class TestNoGraphNativeSurfaceLeaves:
    @pytest.mark.parametrize("seed", ("proj_acr", "team_cinder", "proj_pulse"))
    def test_no_backend_vocabulary_appears_in_the_packet(self, seed: str) -> None:
        blob = spine.investigate(seed, with_drivers=True).packet.model_dump_json()
        hits = [token for token in BANNED_BACKEND_TOKENS if token in blob.casefold()]
        assert not hits, f"backend vocabulary in the {seed} packet: {hits}"

    def test_the_storage_partition_name_never_appears_in_the_packet(self) -> None:
        """The most literal leak available, checked by name.

        The readout carries the partition (``readback.py:200``) and the
        emitter must not forward it: a partition is a keyspace, which is a
        storage location, which is exactly what the corrective plan forbids
        reaching a client.
        """

        partition = partition_for_org(world.ORG_HELIO)
        investigation = spine.investigate("proj_acr", with_drivers=True)
        assert investigation.readout.partition == partition, (
            "the readout no longer carries the partition, so this test is "
            "checking for something that could not leak"
        )
        assert partition not in investigation.packet.model_dump_json(), (
            f"the storage partition {partition!r} reached the packet"
        )

    def test_the_shadow_record_carries_no_backend_vocabulary_either(self) -> None:
        """The packet is not what leaves the process. The record is."""

        payload = json.dumps(shadow.shadow_record_payload(_recorded()))
        hits = [token for token in BANNED_BACKEND_TOKENS if token in payload.casefold()]
        assert not hits, f"backend vocabulary in the shadow record: {hits}"

    def test_the_seam_imports_no_arm_and_reads_arm_identity_off_the_packet(
        self,
    ) -> None:
        """Why no arm can smuggle a surface through the seam.

        ``investigation_shadow`` names no arm module. Arm identity arrives as
        a string on the packet it was handed, so the seam has nothing
        backend-shaped to forward even if an arm wanted it to.
        """

        source = shadow.__file__
        with open(source, encoding="utf-8") as handle:
            text = handle.read()
        assert "graph_arm" not in text, (
            "the shadow seam now names the graph arm, so the arm is reachable "
            "from the product path"
        )
        assert _recorded().arm_id == "graph_assisted_shadow_arm", (
            "arm identity is no longer read off the packet"
        )


# --------------------------------------------------------------------------
# Observability, on CHAOS-3218 semantics
# --------------------------------------------------------------------------


def _recorded() -> shadow.InvestigationShadowRecord:
    """One successful shadow evaluation of a real graph-arm packet.

    The canonical evidence is the packet's own indexed evidence, which is
    what the orchestrator supplies from the frame
    (``orchestrator.py:3877``). Supplying nothing produces a
    ``CANONICAL_BYPASS_REJECTED`` record instead — a real refusal, exercised
    separately below rather than worked around here.
    """

    investigation = spine.investigate("proj_identity_rewrite", with_drivers=True)
    packet = investigation.packet
    return shadow.InvestigationShadow(enabled=True).evaluate(
        payload=json.loads(packet.model_dump_json()),
        run_id=spine.RUN_ID,
        organization_id=world.ORG_HELIO,
        canonical_evidence=tuple(
            entry.evidence for entry in packet.evidence_coverage.evidence_index
        ),
    )


class TestTheTelemetryIsContentSafe:
    def test_a_real_graph_arm_packet_is_recorded_at_all(self) -> None:
        record = _recorded()
        assert record.status is shadow.InvestigationShadowStatus.RECORDED, (
            f"the seam refused a well-formed graph-arm packet: "
            f"{record.status} / {record.detail}"
        )

    def test_no_entity_display_label_reaches_the_record(self) -> None:
        """Unbounded entity labels are excluded by CHAOS-3218 by name.

        Checked against every label the world contains rather than a sample,
        because a label reaching telemetry is a disclosure whichever entity
        it belongs to.
        """

        payload = json.dumps(shadow.shadow_record_payload(_recorded()))
        leaked = sorted(
            {
                entity.display_label
                for entity in world.WORLD_ENTITIES
                if entity.display_label and entity.display_label in payload
            }
        )
        assert not leaked, f"entity labels reached telemetry: {leaked}"

    def test_no_question_text_or_source_prose_reaches_the_record(self) -> None:
        payload = json.dumps(shadow.shadow_record_payload(_recorded()))
        for document in world.WORLD_DOCUMENTS:
            assert document.body[:40] not in payload, (
                f"source prose from {document.document_id} reached telemetry"
            )
        assert "What is the current status" not in payload, (
            "the question text reached telemetry"
        )

    def test_the_record_carries_latency_versions_outcome_and_counts(self) -> None:
        """What CHAOS-3218 requires be observable, present and bounded."""

        record = _recorded()
        assert record.latency_ms >= 0, "no latency was observed"
        assert record.projection_version, "no projection version was recorded"
        assert record.packet_schema_version, "no contract version was recorded"
        assert record.outcome, "no public outcome was recorded"
        facts = dict(fact.split(":", 1) for fact in record.frame_facts if ":" in fact)
        for required in (
            "cohort_members",
            "lineage_paths",
            "principal_drivers",
            "missing_sources",
        ):
            assert required in facts, (
                f"the record carries no {required} count: {record.frame_facts}"
            )
            assert facts[required].isdigit(), (
                f"{required} is not a bounded count: {facts[required]!r}"
            )

    def test_every_fallback_has_a_named_status_rather_than_a_silent_drop(
        self,
    ) -> None:
        statuses = {status.value for status in shadow.InvestigationShadowStatus}
        assert {
            "producer_gap",
            "packet_invalid",
            "canonical_bypass_rejected",
            "seam_fault",
        } <= statuses, f"a named fallback reason disappeared from the seam: {statuses}"

    def test_a_packet_whose_evidence_did_not_come_from_a_canonical_service_is_refused(
        self,
    ) -> None:
        """The positive control for the whole seam, and a safety property in
        its own right: an arm cannot introduce evidence of its own."""

        investigation = spine.investigate("proj_identity_rewrite", with_drivers=True)
        record = shadow.InvestigationShadow(enabled=True).evaluate(
            payload=json.loads(investigation.packet.model_dump_json()),
            run_id=spine.RUN_ID,
            organization_id=world.ORG_HELIO,
            canonical_evidence=(),
        )
        assert (
            record.status is shadow.InvestigationShadowStatus.CANONICAL_BYPASS_REJECTED
        ), (
            "a packet citing evidence no canonical service minted was "
            f"accepted: {record.status}"
        )

    def test_a_packet_claiming_another_organization_is_refused(self) -> None:
        investigation = spine.investigate("proj_identity_rewrite", with_drivers=True)
        packet = investigation.packet
        record = shadow.InvestigationShadow(enabled=True).evaluate(
            payload=json.loads(packet.model_dump_json()),
            run_id=spine.RUN_ID,
            organization_id=world.ORG_LUMEN,
            canonical_evidence=tuple(
                entry.evidence for entry in packet.evidence_coverage.evidence_index
            ),
        )
        assert (
            record.status is shadow.InvestigationShadowStatus.CANONICAL_BYPASS_REJECTED
        ), "a Helio packet was recorded against a Lumen run"


class TestSeamAuthorityIsNotAuthorization:
    """Two different dimensions that look identical in a red column.

    The seam refuses a packet whose cited evidence is not in the native
    frame's canonical set. That refusal is about **authority** — whether a
    canonical service admitted the evidence — not about **authorization** —
    whether the caller may see it. The two produce the same visible outcome
    (a rejected packet) and mean opposite things about safety.

    The distinction is load-bearing rather than pedantic. Once the graph arm
    cites source-issued world handles, the seam can reject a packet citing
    *authentic* evidence the graph legitimately discovered and canonical
    services never admitted to the frame. That is a measured architectural
    fact — the seam contract has no admission path for graph-discovered
    evidence — and counting it toward the zero-unauthorized-leakage gate
    would report an architecture boundary as a security failure, which is
    how a real leak later gets dismissed as "one of those".

    Asserted here so nobody folds the columns together, and so the trial's
    seam verdict and oracle evaluation stay two independent readings.
    """

    def test_a_bypass_rejection_says_nothing_about_the_callers_grant(self) -> None:
        """The same packet: refused by the seam, clean to the oracle.

        Both readings are taken on one packet, which is the only way to show
        they are independent. If a bypass rejection implied an authorization
        problem, the entity-sighting audit would have something to report —
        and it has nothing.
        """

        investigation = spine.investigate("proj_identity_rewrite", with_drivers=True)
        record = shadow.InvestigationShadow(enabled=True).evaluate(
            payload=json.loads(investigation.packet.model_dump_json()),
            run_id=spine.RUN_ID,
            organization_id=world.ORG_HELIO,
            canonical_evidence=(),
        )
        assert (
            record.status is shadow.InvestigationShadowStatus.CANONICAL_BYPASS_REJECTED
        ), f"the seam did not reject on authority: {record.status}"

        visible = world.PRINCIPALS[world.PRINCIPAL_ANALYST].visible_entity_ids
        disclosed = sorted(
            entity_id
            for entity_id in entity_sightings(investigation.packet)
            if entity_id in world.ENTITIES_BY_ID and entity_id not in visible
        )
        assert not disclosed, (
            "the seam-rejected packet ALSO discloses unauthorized entities: "
            f"{disclosed}. The two dimensions have stopped being independent "
            "and this test can no longer separate them"
        )

    def test_the_seam_reports_authority_under_its_own_status_name(self) -> None:
        """The status is not called anything authorization-shaped.

        A reader scanning trial output should not be able to mistake the
        column. ``canonical_bypass_rejected`` names the admission boundary;
        no seam status names authorization at all, because the seam does not
        evaluate it.
        """

        statuses = {status.value for status in shadow.InvestigationShadowStatus}
        assert "canonical_bypass_rejected" in statuses
        assert not [
            status
            for status in statuses
            if "authoriz" in status or "permission" in status
        ], (
            "a seam status now names authorization, so a seam verdict can be "
            f"read as an authorization verdict: {sorted(statuses)}"
        )

    def test_the_seam_record_carries_no_authorization_verdict_field(self) -> None:
        """Nothing in the emitted record invites the conflation either."""

        payload = shadow.shadow_record_payload(_recorded())
        assert not [
            field for field in payload if "authoriz" in field or "permission" in field
        ], (
            "the shadow record now carries an authorization-shaped field; "
            "the trial's seam column and oracle column must stay separate "
            f"readings: {sorted(payload)}"
        )


class TestTheObservabilityGapsAreNamed:
    """Two things CHAOS-3620 asks to be observable that are not. Asserted so
    they cannot read as coverage, and so closing either turns this red."""

    def test_the_shadow_record_carries_no_authorization_filtered_count(self) -> None:
        """The count exists on the packet and does not survive into telemetry.

        An operator watching the trial cannot currently see that any answer
        was narrowed by authorization, which is one of the six signals the
        issue names.
        """

        investigation = spine.investigate("team_cinder")
        assert (
            investigation.packet.subject_discovery.authorization_filtered_count == 1
        ), "this pin is only meaningful on a run that filtered something"
        fields = set(shadow.shadow_record_payload(_recorded()))
        assert not {field for field in fields if "authorization" in field}, (
            "the shadow record now carries an authorization signal -- the "
            "CHAOS-3620 observability gap record must be updated"
        )

    def test_there_is_no_durable_record_table(self) -> None:
        """The versioned log line is the whole contract today.

        Recorded because "we have telemetry" and "we can query telemetry
        after the fact" are different claims, and the trial's evidence
        requirements depend on the second.
        """

        from dev_health_ops.api.dev import orchestrator_persistence

        with open(orchestrator_persistence.__file__, encoding="utf-8") as handle:
            source = handle.read()
        marker = "def record_investigation_shadow"
        body = source[source.index(marker) :].split("\ndef ", 1)[0]
        assert "INSERT" not in body.upper(), (
            "the shadow recorder now writes to a table -- the CHAOS-3620 "
            "durable-record gap record must be updated"
        )
        assert "logger.info" in body, (
            "the shadow recorder no longer emits the versioned log line, so "
            "the trial has no observation channel at all"
        )
