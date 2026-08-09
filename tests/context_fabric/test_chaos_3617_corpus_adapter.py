"""CHAOS-3617 PR2: the corpus adapter, and authorization by grant not tenancy.

The corpus plants a **same-tenant restricted project** (``proj_quarry``)
specifically so that the obvious authorization implementation looks right and
is not. An arm that derives the caller's visible set from the tenant returns
Quarry to the analyst principal, and no tenant-level check anywhere — not the
partition, not the cross-tenant negative tests, not the packet's own
authorized-set validator — catches it. Only the true per-principal grant
does.

So the headline test here is not "the adapter loads the world". It is that
the tenant-derived set and the grant-derived set **differ**, that the
difference is exactly Quarry, and that the arm uses the second.
"""

from __future__ import annotations

import ast
import asyncio
from pathlib import Path

import pytest

from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import build_projection
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader

_ADAPTER_SOURCE = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "dev_health_ops"
    / "context_fabric"
    / "graph_arm"
    / "corpus_adapter.py"
)


@pytest.fixture(scope="module")
def helio_projection():
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


class TestAuthorizationIsByGrantNotTenancy:
    def test_the_two_derivations_disagree_by_exactly_the_restricted_project(
        self,
    ) -> None:
        """The whole reason this capability is not a one-liner.

        If tenancy and grant agreed, an arm could authorize the easy way and
        pass every test. They disagree by exactly one entity, and that entity
        is same-tenant, so no partition or cross-tenant check can see it.
        """

        by_tenant = set(adapter.seed_ids_for_tenant(world.ORG_HELIO))
        by_grant = set(adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST))

        assert by_tenant != by_grant
        assert by_tenant - by_grant == {world.PROJ_QUARRY}
        assert not by_grant - by_tenant, (
            "the grant should never exceed the tenant; if it does, the corpus "
            "or this adapter has crossed a boundary"
        )

    def test_the_analyst_cannot_see_the_restricted_project(self) -> None:
        assert world.PROJ_QUARRY not in adapter.authorized_entity_ids_for(
            world.PRINCIPAL_ANALYST
        )

    def test_someone_can_see_it(self) -> None:
        """The negative control.

        An entity nobody could see would make the exclusion above pass for a
        trivial reason and the authorization oracle unfalsifiable in the
        other direction — which is why the corpus ships a compliance
        principal.
        """

        assert world.PROJ_QUARRY in adapter.authorized_entity_ids_for(
            world.PRINCIPAL_COMPLIANCE
        )

    def test_an_unknown_principal_raises_rather_than_seeing_everything(self) -> None:
        """The tempting default turns the boundary into a no-op."""

        with pytest.raises(KeyError):
            adapter.authorized_entity_ids_for("principal_does_not_exist")

    def test_a_traversal_under_the_analyst_grant_never_reaches_the_restricted_project(
        self, helio_projection
    ) -> None:
        """End to end, through the real traversal rather than the set alone."""

        authorized = adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
        readout = asyncio.run(
            ProjectionGraphReader(helio_projection).neighbourhood(
                org_id=world.ORG_HELIO,
                seed_canonical_ids=sorted(authorized)[:5],
                authorized_entity_ids=sorted(authorized),
                max_hops=3,
            )
        )
        reached = {entity.canonical_id for entity in readout.entities}
        assert world.PROJ_QUARRY not in reached
        touched: set[str] = set()
        for path in readout.paths:
            touched |= path.touched_ids()
        assert world.PROJ_QUARRY not in touched, (
            "a path routed through the restricted project, which discloses it "
            "exists even though its own record was withheld"
        )

    def test_the_same_traversal_under_the_compliance_grant_can_reach_it(
        self, helio_projection
    ) -> None:
        """Without this, "not reached" could mean the traversal was broken."""

        authorized = adapter.authorized_entity_ids_for(world.PRINCIPAL_COMPLIANCE)
        readout = asyncio.run(
            ProjectionGraphReader(helio_projection).neighbourhood(
                org_id=world.ORG_HELIO,
                seed_canonical_ids=[world.PROJ_QUARRY],
                authorized_entity_ids=sorted(authorized),
                max_hops=2,
            )
        )
        assert world.PROJ_QUARRY in {entity.canonical_id for entity in readout.entities}


class TestTenantIsolationThroughTheAdapter:
    def test_a_helio_batch_carries_no_lumen_entity(self) -> None:
        batch = adapter.corpus_batch(world.ORG_HELIO)
        lumen = {
            entity_id
            for entity_id, entity in world.ENTITIES_BY_ID.items()
            if entity.tenant_id == world.ORG_LUMEN
        }
        assert lumen, "the corpus must have a second tenant or this is vacuous"
        assert not {record.canonical_id for record in batch.entities} & lumen

    def test_each_tenant_projects_into_its_own_partition(self) -> None:
        helio = build_projection(adapter.corpus_batch(world.ORG_HELIO))
        lumen = build_projection(adapter.corpus_batch(world.ORG_LUMEN))
        assert helio.partition != lumen.partition

    def test_the_near_duplicate_project_exists_in_both_and_stays_distinct(
        self,
    ) -> None:
        """The corpus's cross-tenant bait, addressed arithmetically.

        Node addresses include the org, so even identically-named records in
        two tenants land on different nodes.
        """

        helio = build_projection(adapter.corpus_batch(world.ORG_HELIO))
        lumen = build_projection(adapter.corpus_batch(world.ORG_LUMEN))
        helio_ids = {node.canonical_id for node in helio.nodes}
        lumen_ids = {node.canonical_id for node in lumen.nodes}
        shared = helio_ids & lumen_ids
        for canonical_id in shared:
            helio_node = next(
                node for node in helio.nodes if node.canonical_id == canonical_id
            )
            lumen_node = next(
                node for node in lumen.nodes if node.canonical_id == canonical_id
            )
            assert helio_node.uuid != lumen_node.uuid


class TestTheAdapterCannotSeeWhatItIsScoredAgainst:
    def test_it_imports_world_and_nothing_else_from_the_corpus(self) -> None:
        """Absence, not discipline.

        An arm that can read ``oracles``/``cases``/``evaluate`` can be tuned
        to them, and no amount of care makes that unfalsifiable. The import
        simply is not there, and this fails the moment one appears.
        """

        tree = ast.parse(_ADAPTER_SOURCE.read_text())
        corpus_imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if "investigation_corpus" in node.module:
                    corpus_imports.add(node.module)
                    corpus_imports.update(
                        f"{node.module}.{alias.name}" for alias in node.names
                    )
            elif isinstance(node, ast.Import):
                corpus_imports.update(
                    alias.name
                    for alias in node.names
                    if "investigation_corpus" in alias.name
                )
        forbidden = {"oracles", "cases", "evaluate", "reference", "coverage"}
        leaked = {
            name
            for name in corpus_imports
            if any(part in name.split(".") for part in forbidden)
        }
        assert not leaked, leaked
        assert corpus_imports, "the adapter reads the corpus; this cannot be empty"

    def test_no_arm_module_reads_the_scoring_surfaces(self) -> None:
        arm = _ADAPTER_SOURCE.parent
        offenders: dict[str, list[str]] = {}
        for path in sorted(arm.glob("*.py")):
            text = path.read_text()
            hits = [
                name
                for name in (
                    "investigation_corpus.oracles",
                    "investigation_corpus.cases",
                )
                if name in text
            ]
            if hits:
                offenders[path.name] = hits
        assert not offenders, offenders


class TestEvidenceIdentityComesFromTheWorld:
    def test_every_ingested_observation_is_a_real_corpus_evidence_slug(
        self, helio_projection
    ) -> None:
        """The arm must not invent evidence identity for corpus records.

        A handle the adapter built itself is a handle no oracle can match,
        and the failure would read as "the arm cited nothing" rather than
        "the arm cited something nobody can resolve".
        """

        observations = {
            node.canonical_id for node in helio_projection.observation_nodes()
        }
        assert observations
        assert observations <= set(world.EVIDENCE_BY_SLUG)

    def test_the_world_mint_is_the_only_source_of_handles(self) -> None:
        for slug in sorted(world.EVIDENCE_BY_SLUG)[:5]:
            assert world.EVIDENCE_BY_SLUG[slug].handle == world.evidence_handle(slug)


class TestAdversarialMaterialIsIngestedNotFiltered:
    def test_adversarial_evidence_reaches_the_graph(self, helio_projection) -> None:
        """Filtering at the door would make every poisoning test vacuous.

        The world plants adversarial records so a correct arm can be seen
        *not citing* them. An adapter that dropped them would satisfy every
        exclusion expectation while the arm did nothing at all.
        """

        adversarial = {
            slug
            for slug, evidence in world.EVIDENCE_BY_SLUG.items()
            if evidence.is_adversarial and evidence.tenant_id == world.ORG_HELIO
        }
        assert adversarial, "the corpus must plant adversarial evidence"
        ingested = {node.canonical_id for node in helio_projection.observation_nodes()}
        assert adversarial & ingested, (
            "adversarial evidence was filtered at ingestion, which would make "
            "every poisoned-linkage expectation pass for free"
        )

    def test_injected_documents_are_ingested_but_never_approved(self) -> None:
        """Present for extraction tests to work on; never handed to a model."""

        injected = [
            document
            for document in world.WORLD_DOCUMENTS
            if document.contains_injection
        ]
        assert injected, "the corpus must plant an injected document"
        projection = build_projection(adapter.corpus_batch(world.ORG_HELIO))
        for document in injected:
            if document.tenant_id != world.ORG_HELIO:
                continue
            assert document.document_id in projection.rejected_document_ids
            assert document.document_id not in {
                approved.canonical_id for approved in projection.approved_documents
            }

    def test_document_approval_is_decided_explicitly_not_by_a_missing_attribute(
        self,
    ) -> None:
        """Regression for a silent default I nearly shipped.

        The first draft read ``getattr(document, "is_approved", False)``.
        ``WorldDocument`` has no such field — it models the idea as ``trust``
        plus ``contains_injection`` — so every document would have been
        marked unapproved *by accident*, and every extraction-path test would
        have passed without extraction ever being attempted.
        """

        from dev_health_ops.context_fabric.graph_arm.corpus_adapter import (
            _document_is_approved,
        )

        # AST, not a text search: the adapter's own docstring quotes the
        # mistake in order to explain it, and a grep would match the
        # explanation. Third time this trap has bitten in this lane, so it is
        # worth naming: a guard that cannot tell prose from code fails for
        # reasons unrelated to what it claims to measure.
        tree = ast.parse(_ADAPTER_SOURCE.read_text())
        getattr_defaults = [
            ast.unparse(node)
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) == 3
        ]
        assert not getattr_defaults, (
            "a three-argument getattr in the adapter silently absorbs a "
            f"corpus modelling mismatch: {getattr_defaults}"
        )

        # A NON-injected document, because injection short-circuits to False
        # before the trust mapping is consulted -- picking an injected one
        # would make the "unknown trust raises" assertion below unreachable
        # and the test would pass while proving the wrong branch.
        sample = next(
            document
            for document in world.WORLD_DOCUMENTS
            if not document.contains_injection
        )
        assert _document_is_approved(sample) is False

        broken = type(sample)(
            **{
                **{
                    field: getattr(sample, field)
                    for field in sample.__dataclass_fields__
                },
                "trust": "not_a_real_trust_level",
            }
        )
        with pytest.raises(ValueError, match="no approval rule"):
            _document_is_approved(broken)

    def test_injection_short_circuits_before_the_trust_mapping(self) -> None:
        """Approval is what points a model at text, so injection wins outright.

        Asserted separately because the test above deliberately avoids this
        path, and an unasserted short circuit is one a refactor can drop.
        """

        from dev_health_ops.context_fabric.graph_arm.corpus_adapter import (
            _document_is_approved,
        )

        injected = next(
            document
            for document in world.WORLD_DOCUMENTS
            if document.contains_injection
        )
        promoted = type(injected)(
            **{
                **{
                    field: getattr(injected, field)
                    for field in injected.__dataclass_fields__
                },
                "trust": world.TrustLevel.CANONICAL,
            }
        )
        assert _document_is_approved(promoted) is False, (
            "an injected document was approved because something relabelled its trust"
        )


class TestTheBatchIsProjectable:
    def test_the_helio_world_projects_without_refusal(self, helio_projection) -> None:
        """The adapter's output must satisfy every PR1 ingestion guard.

        Orientation, dangling endpoints, separator bytes, control characters,
        prose bounds — the corpus is real data written by another lane, so
        this is the first time those guards meet something they did not
        author.
        """

        assert len(helio_projection.entity_nodes()) >= 40
        assert len(helio_projection.edges) >= 60
        assert helio_projection.observation_nodes()

    def test_every_projected_edge_keeps_the_corpus_orientation(
        self, helio_projection
    ) -> None:
        planted = {
            (edge.source_entity_id, str(edge.relationship), edge.target_entity_id)
            for edge in world.RELATIONSHIPS_BY_KEY.values()
            if edge.tenant_id == world.ORG_HELIO
        }
        projected = {
            (
                edge.source_canonical_id,
                str(edge.relationship),
                edge.target_canonical_id,
            )
            for edge in helio_projection.edges
        }
        assert projected <= planted, sorted(projected - planted)
