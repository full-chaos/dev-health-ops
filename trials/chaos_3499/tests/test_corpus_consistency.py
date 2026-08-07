"""Independent re-derivation of the as-of oracles from ground truth.

The oracles in :mod:`corpus.oracles` are hand-authored. Hand-authored is the
right call -- it forces someone to state what a correct answer contains -- but
it means a typo in an expectation would silently become the definition of
correct, and every arm would then be scored against the typo.

So the expectations are derived a second time, by a different route:
:func:`corpus.ground_truth.select` walks the planted facts and applies the
axis semantics directly. Where the two derivations disagree, one of them is
wrong and the suite says so rather than picking a winner.

Only as-of oracles are cross-checked here. History-shaped modes (supersession,
prior attempts, conflicts) are not a filter over ground truth -- they are a
retrieval question -- so there is no second derivation to compare against, and
pretending otherwise would just be re-running the same list twice.
"""

from __future__ import annotations

import pytest

from ..corpus import ground_truth as gt
from ..corpus.oracles import ALL_ORACLES
from ..harness.contracts import QueryMode

_AS_OF_ORACLES = [o for o in ALL_ORACLES if o.query.query_mode is QueryMode.AS_OF]

#: Oracle -> the visibility context its scenario runs under. Kept here rather
#: than imported from the golden builder so the two derivations do not share a
#: source of truth for anything except the ground-truth facts themselves.
_VISIBILITY = {
    "O2_blocking_valid": gt.ALPHA_FULL_VISIBILITY,
    "O2_blocking_observed": gt.ALPHA_FULL_VISIBILITY,
    "O7_valid": gt.ALPHA_FULL_VISIBILITY,
    "O7_null_valid_from": gt.ALPHA_FULL_VISIBILITY,
}


@pytest.mark.parametrize("oracle", _AS_OF_ORACLES, ids=lambda o: o.oracle_id)
def test_must_include_is_reproduced_by_independent_derivation(oracle) -> None:
    """Everything the oracle requires must be derivable from ground truth."""
    visibility = _VISIBILITY[oracle.oracle_id]
    assert oracle.query.axis is not None
    derived = gt.select(
        as_of=oracle.query.as_of,
        axis=oracle.query.axis.value,
        visibility=visibility,
        predicates=frozenset(oracle.query.allowed_relation_types) or None,
    )
    derived_identities = {(f.subject, f.predicate, f.object) for f in derived}

    for expectation in oracle.must_include:
        identity = (expectation.subject, expectation.predicate, expectation.object)
        assert identity in derived_identities, (
            f"{oracle.oracle_id} requires {expectation.describe()}, but an "
            f"independent walk of ground truth on axis "
            f"{oracle.query.axis.value} at {oracle.query.as_of.isoformat()} "
            f"does not produce it. One of the two is wrong."
        )


@pytest.mark.parametrize("oracle", _AS_OF_ORACLES, ids=lambda o: o.oracle_id)
def test_must_exclude_is_absent_from_independent_derivation(oracle) -> None:
    """Nothing the oracle forbids may be derivable either.

    Catches the subtler slip: an exclusion that ground truth says *should* be
    present, which would make the oracle demand a wrong answer.
    """
    visibility = _VISIBILITY[oracle.oracle_id]
    derived = gt.select(
        as_of=oracle.query.as_of,
        axis=oracle.query.axis.value,
        visibility=visibility,
        predicates=frozenset(oracle.query.allowed_relation_types) or None,
    )
    derived_identities = {(f.subject, f.predicate, f.object) for f in derived}

    for expectation in oracle.must_exclude:
        identity = (expectation.subject, expectation.predicate, expectation.object)
        assert identity not in derived_identities, (
            f"{oracle.oracle_id} forbids {expectation.describe()}, but ground "
            f"truth says it holds on axis {oracle.query.axis.value} at "
            f"{oracle.query.as_of.isoformat()}. The oracle is demanding a "
            f"wrong answer."
        )


def test_the_axis_pair_genuinely_diverges_in_ground_truth() -> None:
    """The corpus, not the oracle, is what makes the axis pair discriminating.

    If ground truth produced the same set on both axes, the pair would be two
    spellings of one question and an axis-blind arm would pass both.
    """
    as_of = gt.AS_OF_JUL_15
    common = {
        "visibility": gt.ALPHA_FULL_VISIBILITY,
        "predicates": frozenset({"blocks"}),
    }
    valid = {f.fact_key for f in gt.select(as_of=as_of, axis="valid_time", **common)}
    observed = {
        f.fact_key for f in gt.select(as_of=as_of, axis="observed_time", **common)
    }
    assert valid != observed, (
        "valid-time and observed-time produce identical sets at the pinned "
        "instant; the axis-pair case is not testing anything"
    )
    assert "gt_blocks_105_110_backfilled" in valid
    assert "gt_blocks_105_110_backfilled" not in observed


def test_closed_window_is_open_on_the_observed_axis_before_it_was_known() -> None:
    """The asymmetry the whole axis distinction rests on.

    ATL-101's block ended 07-18 and that ending was ingested 07-18. Asked on
    the observed axis as of 07-15, the window must still read as open --
    reporting it closed would be answering the valid-time question.
    """
    fact = gt.GROUND_TRUTH_BY_KEY["gt_blocks_101_110"]
    assert fact.true_at(gt.AS_OF_JUL_15)
    assert fact.known_at(gt.AS_OF_JUL_15)
    later = gt.TRIAL_NOW
    assert not fact.true_at(later)
    assert not fact.known_at(later)


def test_null_interval_start_holds_at_every_instant_in_ground_truth() -> None:
    """Ground truth's position on the nullable-valid_from defect.

    An interval with no start began before recorded time, so it is true at
    every as_of. ClickHouse disagrees -- `NULL <= as_of` is false -- which is
    exactly the defect O7_null_valid_from measures. This test pins ground
    truth's side of that disagreement so the oracle cannot drift toward the
    buggy behaviour.
    """
    fact = gt.GROUND_TRUTH_BY_KEY["gt_svc_repo_null_start"]
    assert fact.valid_from is None
    for instant in (gt.CORPUS_START, gt.AS_OF_JUL_15, gt.AS_OF_JUL_25, gt.TRIAL_NOW):
        assert fact.true_at(instant)


def test_cross_tenant_material_is_never_derivable_for_the_alpha_org() -> None:
    """The leak gate, asserted against ground truth rather than an arm."""
    for axis in ("valid_time", "observed_time"):
        derived = gt.select(
            as_of=gt.TRIAL_NOW,
            axis=axis,
            visibility=gt.ALPHA_FULL_VISIBILITY,
            include_adversarial=True,
        )
        assert all(f.org_id == gt.ORG_ALPHA for f in derived), (
            "ground-truth selection leaked another tenant's facts"
        )


def test_revocation_removes_exactly_the_revoked_repository() -> None:
    common = {"as_of": gt.TRIAL_NOW, "axis": "valid_time"}
    full = {
        f.fact_key for f in gt.select(visibility=gt.ALPHA_FULL_VISIBILITY, **common)
    }
    revoked = {f.fact_key for f in gt.select(visibility=gt.ALPHA_WEB_REVOKED, **common)}
    removed = full - revoked
    assert removed == {"gt_ep5_web_repo"}, (
        f"revoking repo_atlas_web removed {removed}; expected exactly the "
        "web-repo episode -- over-removal is as wrong as under-removal"
    )
