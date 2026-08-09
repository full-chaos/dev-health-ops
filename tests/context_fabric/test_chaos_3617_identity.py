"""CHAOS-3617: canonical IDs are the identity; Graphiti mints nothing.

The issue's rule is one sentence — "canonical IDs remain entity identity;
Graphiti does not mint competing product identities" — and it has three
testable consequences, which are the three groups below: addresses are
derived (not minted), addresses are org-scoped (so two tenants cannot
collide), and the partition is server-derived (so a caller cannot supply one
as authorization).
"""

from __future__ import annotations

import uuid

import pytest

from dev_health_ops.context_fabric.graph_arm import identity
from dev_health_ops.context_fabric.graph_arm.vocabulary import (
    GraphEntityKind,
    GraphObservationKind,
)


class TestDeterministicAddressing:
    def test_the_same_record_always_gets_the_same_address(self) -> None:
        first = identity.node_uuid("org_a", GraphEntityKind.PROJECT, "proj_x")
        second = identity.node_uuid("org_a", GraphEntityKind.PROJECT, "proj_x")
        assert first == second

    def test_the_address_is_a_function_of_the_canonical_id_alone(self) -> None:
        """No randomness, no clock, no insertion order.

        If this were a mint, re-projecting the same world would produce a
        second copy of every node and the store would double on every run.
        """

        assert identity.node_uuid("org_a", GraphEntityKind.PROJECT, "proj_x") == str(
            uuid.uuid5(identity.GRAPH_ARM_NAMESPACE, "org_a\0entity\0project\0proj_x")
        )

    def test_kind_participates_in_the_address(self) -> None:
        project = identity.node_uuid("org_a", GraphEntityKind.PROJECT, "shared_id")
        team = identity.node_uuid("org_a", GraphEntityKind.TEAM, "shared_id")
        assert project != team

    def test_entities_and_observations_never_share_an_address(self) -> None:
        """A node that is both an entity and evidence could cite itself."""

        entity = identity.node_uuid("org_a", GraphEntityKind.PROJECT, "x")
        observation = identity.observation_uuid(
            "org_a", GraphObservationKind.INCIDENT, "x"
        )
        assert entity != observation

    def test_relationship_addresses_include_both_endpoints(self) -> None:
        """Otherwise a second owner would overwrite the first edge."""

        first = identity.relationship_uuid(
            "org_a",
            "owned_by_team",
            GraphEntityKind.PROJECT,
            "proj_x",
            GraphEntityKind.TEAM,
            "team_1",
        )
        second = identity.relationship_uuid(
            "org_a",
            "owned_by_team",
            GraphEntityKind.PROJECT,
            "proj_x",
            GraphEntityKind.TEAM,
            "team_2",
        )
        assert first != second

    def test_separator_prevents_boundary_collisions(self) -> None:
        """``("a", "b:c")`` and ``("a:b", "c")`` must not hash alike.

        Canonical ids routinely contain ``:`` and ``/``. Concatenating the
        hash inputs without a separator that cannot appear in an identifier
        would let two different records share one node.
        """

        left = identity.node_uuid("org_a", GraphEntityKind.PROJECT, "b:c")
        right = identity.node_uuid("org_a:b", GraphEntityKind.PROJECT, "c")
        assert left != right

    def test_namespace_is_frozen(self) -> None:
        """Changing it re-addresses every node and invalidates recorded runs."""

        assert identity.GRAPH_ARM_NAMESPACE == uuid.UUID(
            "6f0f1d4e-9f2a-5c6b-8d13-2c7a4b5e9f01"
        )

    def test_an_empty_canonical_id_is_refused(self) -> None:
        with pytest.raises(ValueError, match="canonical_id must not be empty"):
            identity.node_uuid("org_a", GraphEntityKind.PROJECT, "")


class TestTenantScopedAddressing:
    def test_identical_canonical_ids_in_two_orgs_get_different_addresses(
        self,
    ) -> None:
        """The cross-tenant near-duplicate case, at the identity layer.

        ``team_platform`` exists byte-identically in both fixture orgs. If
        the org id were not inside the hash input, one org's write would
        land on the other's node -- a leak no downstream filter could undo,
        because by then the data would already be shared.
        """

        alpha = identity.node_uuid("org_alpha", GraphEntityKind.TEAM, "team_platform")
        beta = identity.node_uuid("org_beta", GraphEntityKind.TEAM, "team_platform")
        assert alpha != beta

    def test_relationship_addresses_are_org_scoped_too(self) -> None:
        args = (
            "owned_by_team",
            GraphEntityKind.PROJECT,
            "p",
            GraphEntityKind.TEAM,
            "t",
        )
        assert identity.relationship_uuid(
            "org_alpha", *args
        ) != identity.relationship_uuid("org_beta", *args)


class TestServerDerivedPartition:
    def test_partition_is_derived_from_the_org_id(self) -> None:
        assert identity.partition_for_org("org_alpha") == "cf_trial_org_alpha"

    def test_partition_round_trips(self) -> None:
        assert identity.org_from_partition("cf_trial_org_alpha") == "org_alpha"

    @pytest.mark.parametrize(
        "org_id",
        ["org/alpha", "org alpha", "", "org:alpha", "-leading", "Org_A", "ORG_A"],
    )
    def test_an_org_id_that_would_need_escaping_is_refused_not_sanitised(
        self, org_id: str
    ) -> None:
        """Normalising ``a/b`` to ``a_b`` would collide two organizations.

        This is the worst failure the module could have, so the response is
        refusal rather than best effort.
        """

        with pytest.raises(ValueError, match="not a lowercase plain identifier"):
            identity.partition_for_org(org_id)

    def test_partition_derivation_is_injective_over_accepted_ids(self) -> None:
        """Two accepted org ids can never share a partition.

        The defect this pins, found by adversarial review: the validator used
        to accept mixed case while derivation lowercased, so ``Org_A`` and
        ``org_a`` were BOTH accepted and BOTH derived ``cf_trial_org_a``.
        Partition is keyspace, so those two organizations shared one store --
        one org's purge would drop the other's data and a read would see
        both. The fix narrows what is accepted rather than normalising it,
        because normalising is precisely what collides them.
        """

        accepted = [
            "org_alpha",
            "org_beta",
            "orga",
            "org-a",
            "org1",
            "a",
            "org_alpha_2",
        ]
        partitions = [identity.partition_for_org(org) for org in accepted]
        assert len(set(partitions)) == len(accepted), (
            f"partition derivation is not injective: {sorted(partitions)}"
        )

    @pytest.mark.parametrize("org_id", ["Org_A", "ORG_A", "orgAlpha"])
    def test_a_mixed_case_org_id_is_refused_rather_than_normalised(
        self, org_id: str
    ) -> None:
        """The specific regression. Refusal, not lowercasing.

        A test asserting only that ``Org_A`` derives *something* would have
        passed while the collision was live, so this asserts the refusal.
        """

        with pytest.raises(ValueError, match="lowercase plain identifier"):
            identity.partition_for_org(org_id)

    def test_a_case_variant_cannot_reach_another_orgs_keyspace(self) -> None:
        """The isolation statement of the same property.

        If ``Org_A`` were accepted, it would derive ``org_a``'s partition and
        pass ``assert_partition_matches_org`` for a tenant it is not.
        """

        with pytest.raises(ValueError):
            identity.assert_partition_matches_org("cf_trial_org_a", "Org_A")

    def test_a_partition_from_another_org_is_rejected(self) -> None:
        """A supplied partition is never an authorization claim.

        The assertion re-derives the expected partition from the
        server-known org id rather than trusting the one travelling with the
        results, which is what makes a caller-supplied ``group_id`` useless.
        """

        with pytest.raises(PermissionError, match="never an authorization grant"):
            identity.assert_partition_matches_org("cf_trial_org_beta", "org_alpha")

    def test_a_matching_partition_is_accepted(self) -> None:
        identity.assert_partition_matches_org("cf_trial_org_alpha", "org_alpha")

    def test_a_non_trial_partition_is_not_recognised(self) -> None:
        with pytest.raises(ValueError, match="not a trial graph partition"):
            identity.org_from_partition("default_db")
