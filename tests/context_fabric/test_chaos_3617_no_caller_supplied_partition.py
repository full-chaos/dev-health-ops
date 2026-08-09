"""CHAOS-3617: a caller cannot supply a graph partition. Not "must not" — cannot.

The issue's rule is that the server derives the organization/partition and
"callers never supply Graphiti ``group_id`` as authorization". The arm's
other tests check that a *mismatched* partition is rejected. This module
checks the stronger and more durable property: **there is no parameter to
supply one through.**

The distinction matters. A validated parameter is one refactor away from
being validated in only some paths; an absent parameter cannot be passed at
all, in any path, and a future edit that adds one fails here rather than
quietly widening the surface. So this asserts against the *signatures*, and
backs that with a runtime check that the public entry points actually reject
the keyword.

Two helpers in :mod:`identity` do take a ``partition``, and both are
exempted **by name with a stated reason** rather than by a pattern that
would absorb a third one silently:

* ``assert_partition_matches_org`` — the checker. It takes the partition in
  order to refuse it; that is the opposite of accepting one.
* ``org_from_partition`` — a parser used for diagnostics, and explicitly
  documented as not an authorization decision.

The exemption list is asserted to be exactly those two, so a third
``partition``-taking helper is a test failure and not a judgement call.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

from dev_health_ops.context_fabric.graph_arm import (
    build_projection,
    identity,
    packet_builder,
    readback,
    store,
)

_ARM_ROOT = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "dev_health_ops"
    / "context_fabric"
    / "graph_arm"
)

#: Parameter names through which a caller could name a graph PARTITION —
#: a storage location. Every spelling the backends use, not just Graphiti's:
#: the rule is about the capability, and renaming the field must not be a way
#: around it.
_PARTITION_PARAMETERS = frozenset(
    {
        "group_id",
        "group_ids",
        "partition",
        "partitions",
        "database",
        "db",
        "graph",
        "graph_id",
        "graph_name",
        "keyspace",
        "namespace",
    }
)

#: Names for the ORGANIZATION, which is a different thing and is *supposed*
#: to be a parameter.
#:
#: This list started out inside ``_PARTITION_PARAMETERS`` — I put ``tenant``
#: and ``tenant_id`` there — and the guard duly fired on the corpus adapter's
#: ``corpus_batch(tenant_id)``. That was the guard being wrong, not the code:
#: ``org_id`` was never in the list, because the arm's entire design is that
#: the server supplies the organization and the arm *derives* the partition
#: from it. ``tenant_id`` is the corpus's word for ``org_id``, so treating
#: them differently was an inconsistency, not a safeguard.
#:
#: Relaxing a guard to make new code pass is exactly the move that deserves
#: suspicion, so the compensating assertion is below: every one of these
#: identifiers must still reach ``partition_for_org``, i.e. the derivation is
#: not bypassed. What must never exist is a parameter naming the storage
#: location directly.
_ORGANIZATION_PARAMETERS = frozenset(
    {"org_id", "organization_id", "tenant", "tenant_id"}
)

#: ``module.qualname`` -> why this one is allowed to take a partition.
#: Checked for exactness below.
_EXEMPT: dict[str, str] = {
    "identity.assert_partition_matches_org": (
        "the checker -- it takes a partition in order to refuse it, which is "
        "the opposite of accepting one"
    ),
    "identity.org_from_partition": (
        "a parser for diagnostics, documented as not an authorization decision"
    ),
}


def _public_callables() -> list[tuple[str, ast.arguments]]:
    """Every public function and method in the arm, with its parameters.

    Read from the AST rather than by importing and introspecting, so that a
    parameter on a method of a class that is expensive or conditional to
    construct is still seen.
    """

    found: list[tuple[str, ast.arguments]] = []
    for path in sorted(_ARM_ROOT.glob("*.py")):
        module = path.stem
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                if node.name.startswith("_"):
                    continue
                owner = next(
                    (
                        parent.name
                        for parent in ast.walk(tree)
                        if isinstance(parent, ast.ClassDef) and node in parent.body
                    ),
                    None,
                )
                if owner is not None and owner.startswith("_"):
                    continue
                qualname = (
                    f"{module}.{owner}.{node.name}"
                    if owner
                    else f"{module}.{node.name}"
                )
                found.append((qualname, node.args))
    return found


def _parameter_names(args: ast.arguments) -> set[str]:
    names = {arg.arg for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs)}
    for extra in (args.vararg, args.kwarg):
        if extra is not None:
            names.add(extra.arg)
    return names


class TestNoPartitionParameterExists:
    def test_the_arm_has_public_callables_to_check(self) -> None:
        """Anti-vacuity. An empty scan would make every assertion below pass."""

        callables = _public_callables()
        assert len(callables) > 20, len(callables)
        names = {name for name, _ in callables}
        assert "store.GraphArmStore.for_org" in names
        assert "readback.LiveGraphReader.neighbourhood" in names
        assert "packet_builder.build_packet" in names

    def test_no_public_callable_accepts_a_partition(self) -> None:
        found: dict[str, list[str]] = {}
        for qualname, args in _public_callables():
            hits = sorted(_parameter_names(args) & _PARTITION_PARAMETERS)
            if not hits:
                continue
            key = qualname.rsplit(".", 2)
            simple = f"{key[0]}.{key[-1]}"
            if simple in _EXEMPT:
                continue
            found[qualname] = hits
        assert not found, (
            "these public callables accept a caller-supplied graph partition: "
            f"{found}. The server derives the partition; a caller must not be "
            "able to name one, because a validated parameter is one refactor "
            "away from being validated in only some paths"
        )

    def test_the_exemption_list_is_exact(self) -> None:
        """A third partition-taking helper must fail, not be waved through."""

        taking: set[str] = set()
        for qualname, args in _public_callables():
            if _parameter_names(args) & _PARTITION_PARAMETERS:
                key = qualname.rsplit(".", 2)
                taking.add(f"{key[0]}.{key[-1]}")
        assert taking == set(_EXEMPT), (
            f"exemption list drifted: takes={sorted(taking)}, exempt={sorted(_EXEMPT)}"
        )

    def test_every_exemption_states_a_reason(self) -> None:
        for name, reason in _EXEMPT.items():
            assert reason.strip(), name

    def test_the_two_parameter_families_are_disjoint(self) -> None:
        """An organization is not a partition, and neither list may absorb
        the other."""

        assert not (_PARTITION_PARAMETERS & _ORGANIZATION_PARAMETERS)

    def test_an_organization_identifier_still_reaches_partition_derivation(
        self,
    ) -> None:
        """The compensating assertion for relaxing the list above.

        Accepting ``tenant_id``/``org_id`` is only safe while the partition
        is *derived* from it. If some path ever took an organization and
        reached the store without going through ``partition_for_org``, the
        relaxation would have opened the hole it was argued not to.
        """

        from dev_health_ops.context_fabric.graph_arm import corpus_adapter, identity

        batch = corpus_adapter.corpus_batch("org_helio")
        projection = build_projection(batch)
        assert projection.partition == identity.partition_for_org("org_helio")
        assert projection.org_id == "org_helio"

    def test_no_public_callable_names_a_storage_location(self) -> None:
        """Restated positively: the ban is on partitions, not organizations."""

        offenders: dict[str, list[str]] = {}
        for qualname, args in _public_callables():
            hits = sorted(_parameter_names(args) & _PARTITION_PARAMETERS)
            if not hits:
                continue
            key = qualname.rsplit(".", 2)
            if f"{key[0]}.{key[-1]}" in _EXEMPT:
                continue
            offenders[qualname] = hits
        assert not offenders, offenders


class TestPublicEntryPointsRejectTheKeyword:
    """The runtime half. Signatures can be read; this is what a caller hits."""

    def test_opening_a_store_rejects_a_supplied_partition(self) -> None:
        with pytest.raises(TypeError, match="group_id"):
            store.GraphArmStore.for_org("org_alpha", group_id="cf_trial_org_beta")  # type: ignore[call-arg]

    def test_the_deletion_visit_rejects_a_supplied_partition(self) -> None:
        # The TypeError is raised at call time, before a coroutine exists, so
        # there is nothing to await -- the `del` keeps mypy's unused-coroutine
        # check honest without pretending the call returned something.
        with pytest.raises(TypeError, match="partition"):
            coro = store.org_deletion_visit(  # type: ignore[call-arg]
                "org_alpha", False, partition="cf_trial_x"
            )
            del coro

    def test_projection_rejects_a_supplied_partition(self, alpha_projection) -> None:
        from dev_health_ops.context_fabric.graph_arm import fixtures

        with pytest.raises(TypeError, match="group_id"):
            build_projection(fixtures.alpha_batch(), group_id="cf_trial_x")  # type: ignore[call-arg]

    def test_traversal_rejects_a_supplied_partition(self, alpha_projection) -> None:
        reader = readback.ProjectionGraphReader(alpha_projection)
        with pytest.raises(TypeError, match="group_id"):
            coro = reader.neighbourhood(  # type: ignore[call-arg]
                org_id="org_alpha",
                seed_canonical_ids=[],
                authorized_entity_ids=[],
                group_id="cf_trial_org_beta",
            )
            del coro

    def test_the_partition_a_traversal_uses_comes_from_the_org_id(
        self, alpha_projection
    ) -> None:
        """The positive statement of the same property.

        The readout reports the partition it searched, and that value is the
        one :func:`identity.partition_for_org` derives -- not something the
        caller handed in, because there is nowhere to hand one in.
        """

        import asyncio

        from dev_health_ops.context_fabric.graph_arm import fixtures

        readout = asyncio.run(
            readback.ProjectionGraphReader(alpha_projection).neighbourhood(
                org_id=fixtures.ALPHA_ORG,
                seed_canonical_ids=["proj_nightfall_migration"],
                authorized_entity_ids=fixtures.alpha_authorized_ids(),
            )
        )
        assert readout.partition == identity.partition_for_org(fixtures.ALPHA_ORG)


class TestNoGraphNativeIdentifierIsAcceptedEither:
    def test_the_packet_builder_takes_no_backend_identifier(self) -> None:
        """``build_packet`` reads the org from the readout, not from a caller."""

        parameters = set(inspect.signature(packet_builder.build_packet).parameters)
        assert not (parameters & _PARTITION_PARAMETERS)
        assert "organization_id" not in parameters, (
            "the packet's organization must come from the readout that was "
            "actually authorized, never from a separate caller-supplied value "
            "that could disagree with it"
        )
