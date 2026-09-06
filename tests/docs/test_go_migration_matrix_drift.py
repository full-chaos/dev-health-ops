"""Go migration matrix drift guard.

``docs/go-migration-matrix.md`` answers "who computes/writes this today, Go
or Python" for every provider-sync dataset (rendered directly from
``contracts/provider-matrix/v1/matrix.json``, the frozen CUT-08 parity
contract) plus every daily-metrics family, remaining-metrics family, and
workgraph/investment kind. This test guards the same class of drift
``tests/docs/test_python_go_ledger_drift.py`` guards for the sync/kind/route
surface: a family/dataset can be added, removed, or have its executor change
without the doc being regenerated, and this test must fail loudly when that
happens instead of the page quietly going stale.
"""

from __future__ import annotations

import ast
import importlib.util
import subprocess
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CLI_PY = ROOT / "src" / "dev_health_ops" / "cli.py"
DRIFT_SCRIPT = ROOT / "scripts" / "check_go_migration_matrix_docs_drift.py"
GEN_SCRIPT = ROOT / "scripts" / "gen_go_migration_matrix_docs.py"
CANONICAL_DOC = ROOT / "docs" / "go-migration-matrix.md"

BLOCK_MARKERS = (
    (
        "<!-- BEGIN GENERATED PROVIDER SYNC MATRIX -->",
        "<!-- END GENERATED PROVIDER SYNC MATRIX -->",
        "render_provider_sync_block",
    ),
    (
        "<!-- BEGIN GENERATED DAILY METRICS MATRIX -->",
        "<!-- END GENERATED DAILY METRICS MATRIX -->",
        "render_daily_metrics_block",
    ),
    (
        "<!-- BEGIN GENERATED REMAINING METRICS MATRIX -->",
        "<!-- END GENERATED REMAINING METRICS MATRIX -->",
        "render_remaining_metrics_block",
    ),
    (
        "<!-- BEGIN GENERATED WORKGRAPH INVESTMENT MATRIX -->",
        "<!-- END GENERATED WORKGRAPH INVESTMENT MATRIX -->",
        "render_workgraph_investment_block",
    ),
)


def _load_gen_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(
        "gen_go_migration_matrix_docs", GEN_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_matrix_drift_check_exits_clean() -> None:
    """check_go_migration_matrix_docs_drift.py must exit 0 and emit no ERROR lines."""
    assert DRIFT_SCRIPT.is_file(), f"missing drift script: {DRIFT_SCRIPT}"
    result = subprocess.run(
        [sys.executable, str(DRIFT_SCRIPT)],
        check=False,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Go migration matrix drift check failed:\n{result.stdout}\n{result.stderr}"
    )
    assert "ERROR:" not in result.stdout, (
        f"drift check reported errors:\n{result.stdout}"
    )


def test_matrix_generated_blocks_match_producers() -> None:
    """The four published blocks must match matrix.json / families.json.

    Read-only verification: proves the published page is in sync with its
    producers without writing to disk.
    """
    assert GEN_SCRIPT.is_file(), f"missing gen script: {GEN_SCRIPT}"
    assert CANONICAL_DOC.is_file(), f"missing canonical page: {CANONICAL_DOC}"

    gen = _load_gen_module()
    doc = CANONICAL_DOC.read_text(encoding="utf-8")

    for begin, end, render_fn_name in BLOCK_MARKERS:
        expected_block = getattr(gen, render_fn_name)()
        start = doc.find(begin)
        stop = doc.find(end)
        assert start != -1 and stop > start, (
            f"generated markers {begin}/{end} missing in {CANONICAL_DOC}"
        )
        actual_block = doc[start : stop + len(end)]
        assert actual_block == expected_block, (
            f"Generated block {begin} in docs/go-migration-matrix.md is stale. Run "
            "'python scripts/gen_go_migration_matrix_docs.py' and commit the result."
        )


def test_every_daily_and_remaining_family_has_a_curated_row() -> None:
    """Falsification control: the generator must refuse to render on drift.

    Proves the guard can fail, not just pass -- root AGENTS.md's rule that an
    unexercised guard is not evidence it works. Mutates a live producer set in
    memory (never touches disk) and asserts the generator raises.
    """
    gen = _load_gen_module()

    mutated_daily = {f["name"] for f in gen.load_daily_families()} | {
        "a_brand_new_daily_family"
    }
    try:
        gen._consistency_guard(
            "daily metrics family(ies)",
            mutated_daily,
            set(gen.DAILY_CITATION_LEDGER),
            "",
        )
        raised = False
    except SystemExit:
        raised = True
    assert raised, "consistency guard did not fail on an untracked new daily family"

    mutated_remaining = {f["name"] for f in gen.load_remaining_families()} | {
        "a_brand_new_remaining_family"
    }
    try:
        gen._consistency_guard(
            "remaining metrics family(ies)",
            mutated_remaining,
            set(gen.REMAINING_EXECUTOR_LEDGER),
            "",
        )
        raised = False
    except SystemExit:
        raised = True
    assert raised, "consistency guard did not fail on an untracked new remaining family"


def test_provider_sync_rejects_an_unmapped_go_executor_value() -> None:
    """Falsification control for §1: an unmapped go_executor value must refuse to render.

    matrix.json currently has only ``native_go`` -- if it ever gains a
    genuinely-Python-bridged pair, this generator must be taught the new
    value explicitly rather than silently mis-rendering it as NATIVE.
    """
    gen = _load_gen_module()
    original = getattr(gen, "load_matrix_pairs")
    try:
        setattr(
            gen,
            "load_matrix_pairs",
            lambda: [
                *original(),
                {
                    "provider": "github",
                    "dataset": "a-brand-new-dataset",
                    "go_executor": "python_bridge_nobody_mapped_yet",
                    "route_destinations": [],
                    "route_ready": False,
                    "plannable": False,
                },
            ],
        )
        try:
            gen.render_provider_sync_block()
            raised = False
        except SystemExit:
            raised = True
        assert raised, (
            "render_provider_sync_block did not refuse an unmapped go_executor value"
        )
    finally:
        setattr(gen, "load_matrix_pairs", original)


def test_remaining_families_executor_matches_the_daily_go_worker_wiring() -> None:
    """Pin the 6-native/1-compat split this page exists to correct (chris, 09-04:

    "we haven't finished the port as I was led to believe again" -- but in
    the OTHER direction here: the 09-01 snapshot undercounted native
    coverage. Regression-guards the exact split so a future edit cannot
    silently flip a row without a reviewer noticing in the diff. Reads
    contracts/native-families/v1/native-families.json -- the Go-AST-derived
    artifact, not a curated Python dict (REMAINING_EXECUTOR_LEDGER no longer
    carries an executor value at all, only citation/route/ticket prose).

    CHAOS-4296 moved release_impact from compat to native.
    """
    gen = _load_gen_module()
    artifact_remaining = gen.load_native_families_artifact()["remaining"]
    natives = {
        name for name, executor in artifact_remaining.items() if executor == "native"
    }
    compats = {
        name for name, executor in artifact_remaining.items() if executor == "compat"
    }
    assert natives == {
        "dora",
        "capacity",
        "recommendations",
        "membership_backfill",
        "work_item_attribution",
        "release_impact",
    }
    assert compats == {"complexity"}


def test_daily_finalize_compat_families_matches_known_calls() -> None:
    """Pin today's known set of finalize-scope Python remainders (CHAOS-5118
    class): every family with a REAL call inside run_daily_metrics_finalize's
    body, proven by the AST walk, not asserted by citation prose. A future
    change to which calls exist there must show up as a diff to this pin,
    the same discipline test_remaining_families_executor_matches_the_daily_go_worker_wiring
    uses for the artifact's native/compat split.
    """
    gen = _load_gen_module()
    daily_names = {f["name"] for f in gen.load_daily_families()}
    remaining_names = {f["name"] for f in gen.load_remaining_families()}
    compat_families = gen.load_daily_finalize_compat_families(
        daily_names, remaining_names
    )
    # ("daily", "team_cognitive_load") is DELIBERATELY absent (CHAOS-5141):
    # its Python compute was DELETED outright (job_daily.py no longer has a
    # `_write_team_cognitive_load_for_day` call at all, so this generator's
    # AST walk simply never sees it) once reachability analysis proved a
    # fallback to it was never actually reachable in production. Historical
    # note: before the deletion, this same absence was achieved by forcing
    # the call dormant via FINALIZE_CALL_DORMANT_SKIP_GATED instead (the call
    # matched the naming convention exactly, the same shape ic_finalize's
    # irregular-named calls once had before their ledger entries were
    # removed) -- the mechanism still exists, generically, for the next
    # family this happens to.
    #
    # ("daily", "compounding_risk") is ALSO gone as of CHAOS-5084/no-straddle
    # (#2275 v2), the same way: its finalize-scope Python remainder
    # (_write_compounding_risk_team_rows_for_day) is deleted outright --
    # CompoundingRiskTeamExecutor (Go) is the sole writer for team-scope
    # compounding_risk_daily now, with no Python compute anywhere, so the AST
    # walk over run_daily_metrics_finalize's body no longer finds a real call
    # naming this family at all. The two split-status tests that used to
    # exercise this transition with compounding_risk as the vehicle
    # (test_split_status_still_counts_as_compat_once_repo_scope_goes_native,
    # test_split_render_fires_through_the_actual_count_helpers_and_reverses)
    # are deleted along with it -- the "port genuinely finishing" state they
    # simulated via mocking is now the real, permanent state.
    #
    # ("remaining", "complexity") is ALSO now absent (CHAOS-5051): it was
    # never really about remaining/complexity's own compute -- it was
    # _write_team_complexity_for_day's call misattributed via
    # FINALIZE_CALL_IRREGULAR_FAMILY, the only mechanical evidence available
    # before team_complexity had its own native "daily" registration to
    # classify under. CHAOS-5051 deletes that call (and its Python compute)
    # entirely, so there is no finalize-scope Python call left for the AST
    # walk to find under either name.
    #
    # With BOTH of the remaining two compat-family entries gone, the set this
    # test pins is now EMPTY -- every finalize-scope family that once had a
    # live Python remainder now has none.
    assert compat_families == set()


def test_finalize_completeness_guard_rejects_an_unmapped_write_call() -> None:
    """Falsification control: an in-scope finalize call (matches the
    `_write_<family>_..._for_day` naming convention) that names no live
    family must refuse to render -- the exact "new Python half hides by
    omission" defect this whole mechanism exists to close. Mutates the
    AST-derived call set in memory (never touches job_daily.py on disk).
    """
    gen = _load_gen_module()
    original = getattr(gen, "load_finalize_write_calls")
    try:
        setattr(
            gen,
            "load_finalize_write_calls",
            lambda: original() | {"_write_a_brand_new_family_for_day"},
        )
        try:
            gen.load_daily_finalize_compat_families(
                {f["name"] for f in gen.load_daily_families()},
                {f["name"] for f in gen.load_remaining_families()},
            )
            raised = False
        except SystemExit:
            raised = True
        assert raised, (
            "load_daily_finalize_compat_families did not refuse an unmapped "
            "_write_*_for_day call"
        )
    finally:
        setattr(gen, "load_finalize_write_calls", original)


def test_finalize_ledger_rejects_a_stale_irregular_entry() -> None:
    """Falsification control, the OTHER completeness direction: an
    irregular-name ledger entry naming a call that no longer exists in
    run_daily_metrics_finalize's body must refuse to render, rather than
    just silently stop contributing its family (which would look identical
    to that family never having had a Python finalize remainder at all)."""
    gen = _load_gen_module()
    original = dict(gen.FINALIZE_CALL_IRREGULAR_FAMILY)
    try:
        gen.FINALIZE_CALL_IRREGULAR_FAMILY["a_call_that_does_not_exist"] = (
            "remaining",
            "complexity",
        )
        try:
            gen._assert_no_stale_finalize_ledger_entries(
                {f["name"] for f in gen.load_daily_families()},
                {f["name"] for f in gen.load_remaining_families()},
            )
            raised = False
        except SystemExit:
            raised = True
        assert raised, (
            "_assert_no_stale_finalize_ledger_entries did not refuse a stale "
            "irregular-name entry"
        )
    finally:
        gen.FINALIZE_CALL_IRREGULAR_FAMILY.clear()
        gen.FINALIZE_CALL_IRREGULAR_FAMILY.update(original)


def test_finalize_ledger_rejects_a_stale_dormant_entry() -> None:
    """Same completeness direction as
    test_finalize_ledger_rejects_a_stale_irregular_entry, applied to
    FINALIZE_CALL_DORMANT_SKIP_GATED (CHAOS-5141): a forced-dormant entry
    naming a call that no longer exists in run_daily_metrics_finalize's body
    must refuse to render, not silently stop mattering."""
    gen = _load_gen_module()
    original = dict(gen.FINALIZE_CALL_DORMANT_SKIP_GATED)
    try:
        gen.FINALIZE_CALL_DORMANT_SKIP_GATED[
            "_write_a_call_that_does_not_exist_for_day"
        ] = ("daily", "compounding_risk")
        try:
            gen._assert_no_stale_finalize_ledger_entries(
                {f["name"] for f in gen.load_daily_families()},
                {f["name"] for f in gen.load_remaining_families()},
            )
            raised = False
        except SystemExit:
            raised = True
        assert raised, (
            "_assert_no_stale_finalize_ledger_entries did not refuse a stale "
            "dormant-skip-gated entry"
        )
    finally:
        gen.FINALIZE_CALL_DORMANT_SKIP_GATED.clear()
        gen.FINALIZE_CALL_DORMANT_SKIP_GATED.update(original)


def test_dormant_skip_gated_call_never_enters_the_compat_set() -> None:
    """CHAOS-5141, #2255 r3's own base-branch counterpart: proves
    FINALIZE_CALL_DORMANT_SKIP_GATED is actually consulted BEFORE the
    naming-convention branch, not merely defined. A call shaped exactly
    like the naming convention expects (`_write_<family>_..._for_day`,
    every family token present) would otherwise resolve to that family --
    forcing it dormant must make `_finalize_call_family` return None
    regardless.

    Uses a SYNTHETIC family/call name rather than a real one: the original
    fixture for this test, `_write_team_cognitive_load_for_day`, was itself
    deleted from job_daily.py (CHAOS-5141, its Python compute proved
    unreachable and was removed outright rather than left dormant) -- the
    mechanism this test guards is generic infrastructure for the NEXT family
    this happens to, so the fixture no longer needs a real, currently-live
    call to exercise it.
    """
    gen = _load_gen_module()
    daily_names = {f["name"] for f in gen.load_daily_families()} | {
        "a_synthetic_dormant_family"
    }
    remaining_names = {f["name"] for f in gen.load_remaining_families()}
    call_name = "_write_a_synthetic_dormant_family_for_day"
    original = dict(gen.FINALIZE_CALL_DORMANT_SKIP_GATED)
    try:
        # Sanity/positive control: WITHOUT the force-dormant entry, this call
        # genuinely does resolve via the naming convention (proves the test
        # would fail loudly if the dormant check were removed or bypassed).
        gen.FINALIZE_CALL_DORMANT_SKIP_GATED.clear()
        resolved_without_gate = gen._finalize_call_family(
            call_name, daily_names, remaining_names
        )
        assert resolved_without_gate == ("daily", "a_synthetic_dormant_family"), (
            f"positive control failed: {call_name!r} does not match the naming "
            "convention on its own -- this fixture's premise is wrong"
        )

        gen.FINALIZE_CALL_DORMANT_SKIP_GATED[call_name] = (
            "daily",
            "a_synthetic_dormant_family",
        )
        resolved_with_gate = gen._finalize_call_family(
            call_name, daily_names, remaining_names
        )
        assert resolved_with_gate is None, (
            f"_finalize_call_family returned {resolved_with_gate!r} for a "
            "call named in FINALIZE_CALL_DORMANT_SKIP_GATED -- the force-"
            "dormant check is not actually short-circuiting the naming-"
            "convention branch"
        )
    finally:
        gen.FINALIZE_CALL_DORMANT_SKIP_GATED.clear()
        gen.FINALIZE_CALL_DORMANT_SKIP_GATED.update(original)


def test_dormant_skip_gated_entry_rejects_a_family_no_longer_live() -> None:
    """#2255 confirmation-pass finding (P2, CHAOS-5141): the reviewer's repro
    turned into a red/green test. FINALIZE_CALL_DORMANT_SKIP_GATED forces a
    call dormant by NAME, but the (namespace, family) it names must still be
    checked against the live family set -- exactly like
    FINALIZE_CALL_IRREGULAR_FAMILY already is. Without that check, a
    renamed/removed family while a job_daily.py call kept its old name would
    silently keep resolving to None (looks correct, for the wrong reason)
    instead of raising. Both call sites -- _finalize_call_family (the
    per-call resolver) and _assert_no_stale_finalize_ledger_entries (the
    completeness sweep) -- must refuse when the mapped family is not live in
    its namespace.

    Uses a synthetic call name (the original fixture, team_cognitive_load's
    real call, was deleted along with its Python compute -- see
    test_dormant_skip_gated_call_never_enters_the_compat_set's docstring).
    """
    gen = _load_gen_module()
    daily_names = {f["name"] for f in gen.load_daily_families()}
    remaining_names = {f["name"] for f in gen.load_remaining_families()}
    call_name = "_write_a_synthetic_dormant_family_for_day"
    original = dict(gen.FINALIZE_CALL_DORMANT_SKIP_GATED)
    try:
        gen.FINALIZE_CALL_DORMANT_SKIP_GATED[call_name] = (
            "daily",
            "a_family_that_does_not_exist",
        )

        try:
            gen._finalize_call_family(call_name, daily_names, remaining_names)
            raised_in_resolver = False
        except SystemExit:
            raised_in_resolver = True
        assert raised_in_resolver, (
            "_finalize_call_family did not refuse a FINALIZE_CALL_DORMANT_SKIP_GATED "
            "entry mapping to a family that is not live -- a renamed/removed family "
            "would silently stay dormant instead of raising"
        )

        # _assert_no_stale_finalize_ledger_entries's FIRST check is whether the
        # call name is even still present in job_daily.py's AST at all -- for
        # a synthetic name that is never true, and that check would raise
        # first, exercising the WRONG guard (staleness, not liveness). Make
        # the call name "present" via the same load_finalize_write_calls
        # monkeypatch test_finalize_completeness_guard_rejects_an_unmapped_write_call
        # uses, so this assertion isolates the liveness check specifically.
        original_load_calls = getattr(gen, "load_finalize_write_calls")
        setattr(
            gen,
            "load_finalize_write_calls",
            lambda: original_load_calls() | {call_name},
        )
        try:
            try:
                gen._assert_no_stale_finalize_ledger_entries(
                    daily_names, remaining_names
                )
                raised_in_sweep = False
            except SystemExit:
                raised_in_sweep = True
            assert raised_in_sweep, (
                "_assert_no_stale_finalize_ledger_entries did not refuse a "
                "FINALIZE_CALL_DORMANT_SKIP_GATED entry mapping to a family that is "
                "not live"
            )
        finally:
            setattr(gen, "load_finalize_write_calls", original_load_calls)
    finally:
        gen.FINALIZE_CALL_DORMANT_SKIP_GATED.clear()
        gen.FINALIZE_CALL_DORMANT_SKIP_GATED.update(original)


def test_is_compat_executor_counts_a_split_row_but_not_a_bare_native_one() -> None:
    """Count-semantics contract (team-lead's 2026-09-05 ruling): a split
    "NATIVE (repo) / COMPAT-Python (finalize)" row is Python-compat and must
    count as one; a bare "NATIVE" row, or work_item_state's "NATIVE,
    post_bridge" (a Python-WRITTEN INPUT dependency, not a Python SCOPE of
    the family -- left as its own state per team-lead's ruling) must not.
    """
    gen = _load_gen_module()
    assert gen.is_compat_executor("COMPAT-Python") is True
    assert gen.is_compat_executor("NATIVE (repo) / COMPAT-Python (finalize)") is True
    assert gen.is_compat_executor("NATIVE") is False
    assert gen.is_compat_executor("NATIVE, post_bridge") is False


# CHAOS-5084/no-straddle (#2275 v2) and CHAOS-5051 (#2299):
# test_split_status_still_counts_as_compat_once_repo_scope_goes_native and
# test_split_render_fires_through_the_actual_count_helpers_and_reverses used
# to live here, using compounding_risk (daily namespace) and complexity
# (remaining namespace) as the vehicles for proving the split-status
# render/count logic handles a family whose repo scope went native while its
# finalize scope was still real Python. Both vehicles' finalize-scope Python
# is deleted in/around this change -- compounding_risk_team's by this PR
# (CompoundingRiskTeamExecutor is the sole writer for that scope now) and
# complexity's (really team_complexity's _write_team_complexity_for_day) by
# #2299 -- so the transitional state those tests exercised no longer exists
# to simulate in either namespace; the "port genuinely finishing" end-state
# each test's own docstring anticipated via mocking is now the real,
# permanent state, pinned instead by
# test_daily_finalize_compat_families_matches_known_calls' updated
# (now-empty) set above.


# ---------------------------------------------------------------------------
# Round-2 findings (codex, CHAOS-5118). F3: the 4 tests below are
# falsification controls for guard branches round-1's own tests never
# exercised (F1/F2/F4 fixes above; the two existing tests above only reach
# the unmapped-write-call and stale-irregular-entry branches). Each proves
# reachability explicitly: the mutation/input is the MINIMAL one that drives
# execution into the specific branch under test, not just "reddens somewhere".
# ---------------------------------------------------------------------------


def test_finalize_call_family_rejects_multiple_matches_in_one_namespace() -> None:
    """Falsification control for the same-namespace multi-match guard
    (`_finalize_call_family`'s `len(daily_matches) > 1` branch): a call
    whose `_write_<...>_for_day` middle prefix-matches MORE THAN ONE daily
    family name must refuse, not silently pick the longest.

    Reachability: `work_item` and `work_item_state` both legitimately
    prefix-match the constructed middle `work_item_state_details`
    (`middle == family or middle.startswith(family + "_")` for each), so
    both land in `daily_matches` and the length-2 check at the top of the
    ambiguity block is what raises -- not any other guard in the function.
    """
    gen = _load_gen_module()
    try:
        gen._finalize_call_family(
            "_write_work_item_state_details_for_day",
            {"work_item", "work_item_state"},
            set(),
        )
        raised = False
    except SystemExit:
        raised = True
    assert raised, (
        "_finalize_call_family did not refuse a call matching multiple "
        "daily families via the naming convention"
    )


def test_finalize_call_family_rejects_an_implausible_irregular_mapping() -> None:
    """Falsification control for the irregular-mapping plausibility guard
    inside `_finalize_call_family` (distinct from
    test_finalize_ledger_rejects_a_stale_irregular_entry, which tests
    `_assert_no_stale_finalize_ledger_entries` -- a different function):
    an irregular-ledger entry that fails `_irregular_mapping_plausible`
    must refuse to resolve, not silently return the implausible family.

    Reachability: `_write_team_complexity_for_day` is a real key already in
    FINALIZE_CALL_IRREGULAR_FAMILY (restored in `finally`); retargeting its
    value to `("daily", "team_wellbeing")` sends `_finalize_call_family`
    down the irregular-entry branch (the call name is a real key), past the
    live-family-membership check (`team_wellbeing` is live), and into the
    plausibility check specifically -- which must reject this pairing
    (round-2 F2's own reported false-positive case).
    """
    gen = _load_gen_module()
    original = dict(gen.FINALIZE_CALL_IRREGULAR_FAMILY)
    try:
        gen.FINALIZE_CALL_IRREGULAR_FAMILY["_write_team_complexity_for_day"] = (
            "daily",
            "team_wellbeing",
        )
        try:
            gen._finalize_call_family(
                "_write_team_complexity_for_day", {"team_wellbeing"}, {"complexity"}
            )
            raised = False
        except SystemExit:
            raised = True
        assert raised, (
            "_finalize_call_family did not refuse an implausible irregular mapping"
        )
    finally:
        gen.FINALIZE_CALL_IRREGULAR_FAMILY.clear()
        gen.FINALIZE_CALL_IRREGULAR_FAMILY.update(original)


def test_finalize_call_family_rejects_a_name_valid_in_both_namespaces() -> None:
    """Falsification control for the daily/remaining cross-namespace
    ambiguity guard (`_finalize_call_family`'s `if daily_match and
    remaining_match` branch) -- the exact CHAOS-5118 round-1 F4 shape
    (`work_item_attribution` names both a full daily compute and a
    narrower remaining staleness backstop).

    Reachability: passing the SAME family name in both `daily_names` and
    `remaining_names` makes the call's middle prefix-match exactly one
    family in EACH namespace individually (no multi-match guard fires
    first), so only the cross-namespace check can be what raises.
    """
    gen = _load_gen_module()
    try:
        gen._finalize_call_family(
            "_write_work_item_attribution_for_day",
            {"work_item_attribution"},
            {"work_item_attribution"},
        )
        raised = False
    except SystemExit:
        raised = True
    assert raised, (
        "_finalize_call_family did not refuse a name valid in both the daily "
        "and remaining namespaces"
    )


def test_load_finalize_write_calls_rejects_an_opaque_call_target(tmp_path) -> None:
    """Falsification control for the opaque-call guard in
    load_finalize_write_calls (round-1 F1) -- proves it still fires on a
    fresh input, not just that today's real job_daily.py happens to have
    zero opaque calls (which the real-file run alone cannot distinguish
    from a guard that never fires at all).

    Reachability: a synthetic job_daily.py, pointed to via a monkeypatched
    `JOB_DAILY_PY`, whose `run_daily_metrics_finalize` calls through a
    subscript (`dispatch["x"]()`) -- a shape that is neither `ast.Name` nor
    `ast.Attribute`, landing in the `opaque` branch specifically.
    """
    gen = _load_gen_module()
    synthetic = tmp_path / "job_daily.py"
    synthetic.write_text(
        "def run_daily_metrics_finalize():\n    dispatch = {}\n    dispatch['x']()\n"
    )
    original = getattr(gen, "JOB_DAILY_PY")
    try:
        setattr(gen, "JOB_DAILY_PY", synthetic)
        try:
            gen.load_finalize_write_calls()
            raised = False
        except SystemExit:
            raised = True
        assert raised, "load_finalize_write_calls did not refuse an opaque call target"
    finally:
        setattr(gen, "JOB_DAILY_PY", original)


def test_load_finalize_write_calls_resolves_a_plain_name_alias(tmp_path) -> None:
    """Positive control for round-2 finding F1's fix: a call reached
    through an unambiguous local alias (`writer = _write_x_for_day;
    writer(...)`) must resolve to the ALIASED function's name, not the
    bare alias identifier -- proving the fix actually closes the gap
    codex demonstrated, not just that it refuses somewhere new.
    """
    gen = _load_gen_module()
    synthetic = tmp_path / "job_daily.py"
    synthetic.write_text(
        "def run_daily_metrics_finalize():\n"
        "    writer = _write_compounding_risk_team_rows_for_day\n"
        "    writer()\n"
    )
    original = getattr(gen, "JOB_DAILY_PY")
    try:
        setattr(gen, "JOB_DAILY_PY", synthetic)
        calls = gen.load_finalize_write_calls()
        assert calls == {"_write_compounding_risk_team_rows_for_day"}, (
            "load_finalize_write_calls did not resolve the alias to the "
            f"assigned function's name: {calls!r}"
        )
    finally:
        setattr(gen, "JOB_DAILY_PY", original)


def test_load_finalize_write_calls_rejects_an_unresolvable_alias(tmp_path) -> None:
    """Negative control alongside the positive one above: an alias
    assigned MORE THAN ONCE to different targets cannot be resolved to a
    single function name, and must refuse rather than silently falling
    back to treating the bare alias identifier as an ordinary (and
    therefore presumed out-of-scope) call name.
    """
    gen = _load_gen_module()
    synthetic = tmp_path / "job_daily.py"
    synthetic.write_text(
        "def run_daily_metrics_finalize():\n"
        "    if True:\n"
        "        writer = _write_compounding_risk_team_rows_for_day\n"
        "    else:\n"
        "        writer = _write_team_complexity_for_day\n"
        "    writer()\n"
    )
    original = getattr(gen, "JOB_DAILY_PY")
    try:
        setattr(gen, "JOB_DAILY_PY", synthetic)
        try:
            gen.load_finalize_write_calls()
            raised = False
        except SystemExit:
            raised = True
        assert raised, (
            "load_finalize_write_calls did not refuse an alias assigned to "
            "two different targets"
        )
    finally:
        setattr(gen, "JOB_DAILY_PY", original)


def test_load_finalize_write_calls_resolves_a_for_loop_alias(tmp_path) -> None:
    """Positive control for round-3 finding F1: a call reached through a
    `for` loop binding over a single-element literal container
    (`for writer in (real_fn,): writer(...)`) must resolve to the real
    function's name, the exact shape codex's r3 round demonstrated the
    round-2 fix missed (it only collected `ast.Assign` bindings).
    """
    gen = _load_gen_module()
    synthetic = tmp_path / "job_daily.py"
    synthetic.write_text(
        "def run_daily_metrics_finalize():\n"
        "    for writer in (_write_compounding_risk_team_rows_for_day,):\n"
        "        writer()\n"
    )
    original = getattr(gen, "JOB_DAILY_PY")
    try:
        setattr(gen, "JOB_DAILY_PY", synthetic)
        calls = gen.load_finalize_write_calls()
        assert calls == {"_write_compounding_risk_team_rows_for_day"}, (
            "load_finalize_write_calls did not resolve the for-loop alias to "
            f"the assigned function's name: {calls!r}"
        )
    finally:
        setattr(gen, "JOB_DAILY_PY", original)


def test_load_finalize_write_calls_resolves_a_chained_alias(tmp_path) -> None:
    """Positive control for round-3 finding F1's other shape: an
    alias-of-an-alias (`first = real_fn; second = first; second()`) must
    resolve all the way to the real function's name, not stop at the
    first hop (which would record the call as the intermediate alias
    name `"first"`, an ordinary-looking identifier that silently reads
    as out-of-scope infrastructure).
    """
    gen = _load_gen_module()
    synthetic = tmp_path / "job_daily.py"
    synthetic.write_text(
        "def run_daily_metrics_finalize():\n"
        "    first = _write_compounding_risk_team_rows_for_day\n"
        "    second = first\n"
        "    second()\n"
    )
    original = getattr(gen, "JOB_DAILY_PY")
    try:
        setattr(gen, "JOB_DAILY_PY", synthetic)
        calls = gen.load_finalize_write_calls()
        assert calls == {"_write_compounding_risk_team_rows_for_day"}, (
            "load_finalize_write_calls did not resolve the alias chain to "
            f"the real function's name: {calls!r}"
        )
    finally:
        setattr(gen, "JOB_DAILY_PY", original)


def test_load_finalize_write_calls_rejects_an_alias_cycle(tmp_path) -> None:
    """Negative control: a cyclic alias chain (`a = b` then, elsewhere,
    `b = a`) can never resolve to a real function no matter how far it is
    followed -- must refuse rather than loop forever or silently pick
    one side of the cycle.
    """
    gen = _load_gen_module()
    synthetic = tmp_path / "job_daily.py"
    synthetic.write_text(
        "def run_daily_metrics_finalize():\n    a = b\n    b = a\n    a()\n"
    )
    original = getattr(gen, "JOB_DAILY_PY")
    try:
        setattr(gen, "JOB_DAILY_PY", synthetic)
        try:
            gen.load_finalize_write_calls()
            raised = False
        except SystemExit:
            raised = True
        assert raised, "load_finalize_write_calls did not refuse a cyclic alias chain"
    finally:
        setattr(gen, "JOB_DAILY_PY", original)


def test_load_finalize_write_calls_rejects_a_with_statement_alias(tmp_path) -> None:
    """Negative control: `with x() as y:` binds y to whatever
    `x().__enter__()` returns, which cannot be determined from source --
    a call through such a binding must refuse, never be silently treated
    as a direct reference to a module-level function named `y`.
    """
    gen = _load_gen_module()
    synthetic = tmp_path / "job_daily.py"
    synthetic.write_text(
        "def run_daily_metrics_finalize():\n"
        "    with make_writer() as writer:\n"
        "        writer()\n"
    )
    original = getattr(gen, "JOB_DAILY_PY")
    try:
        setattr(gen, "JOB_DAILY_PY", synthetic)
        try:
            gen.load_finalize_write_calls()
            raised = False
        except SystemExit:
            raised = True
        assert raised, (
            "load_finalize_write_calls did not refuse a call through a "
            "with-statement binding"
        )
    finally:
        setattr(gen, "JOB_DAILY_PY", original)


def test_load_finalize_write_calls_rejects_a_r3_destructured_for_alias(
    tmp_path,
) -> None:
    """Negative control for confirmation-pass finding F1: a `for` loop whose
    target is DESTRUCTURED (`for (writer,) in get_writers(): writer()`)
    matched none of the r3 fix's per-shape branches (only a bare `Name`
    target was handled) and so was never added to raw_next/aliases at all --
    the call fell through as an ordinary, presumably-irrelevant call name
    instead of refusing. Must now refuse outright.
    """
    gen = _load_gen_module()
    synthetic = tmp_path / "job_daily.py"
    synthetic.write_text(
        "def run_daily_metrics_finalize():\n"
        "    for (writer,) in get_writers():\n"
        "        writer()\n"
    )
    original = getattr(gen, "JOB_DAILY_PY")
    try:
        setattr(gen, "JOB_DAILY_PY", synthetic)
        try:
            gen.load_finalize_write_calls()
            raised = False
        except SystemExit:
            raised = True
        assert raised, (
            "load_finalize_write_calls did not refuse a call through a "
            "destructured for-loop target"
        )
    finally:
        setattr(gen, "JOB_DAILY_PY", original)


def test_load_finalize_write_calls_rejects_an_annotated_alias(tmp_path) -> None:
    """Negative control for confirmation-pass finding F3: an `AnnAssign`
    (`writer: object = get_writer()`) originally matched no branch in the
    r3 fix at all (only `ast.Assign` was modeled), so `writer` was never
    added to raw_next/aliases -- the later call fell through as an
    ordinary, presumably-irrelevant call name instead of refusing.
    `AnnAssign` to a `Name` target is now resolved the same way `Assign`
    is (a call's value, like here, is unresolvable via `_one_hop_value`),
    so the later bare call still refuses -- just via the same path
    `writer = get_writer(); writer()` already took, not a hardcoded
    AnnAssign-specific refusal.
    """
    gen = _load_gen_module()
    synthetic = tmp_path / "job_daily.py"
    synthetic.write_text(
        "def run_daily_metrics_finalize():\n"
        "    writer: object = get_writer()\n"
        "    writer()\n"
    )
    original = getattr(gen, "JOB_DAILY_PY")
    try:
        setattr(gen, "JOB_DAILY_PY", synthetic)
        try:
            gen.load_finalize_write_calls()
            raised = False
        except SystemExit:
            raised = True
        assert raised, (
            "load_finalize_write_calls did not refuse a call through an "
            "annotated-assignment alias"
        )
    finally:
        setattr(gen, "JOB_DAILY_PY", original)


def test_load_finalize_write_calls_allows_an_annotated_literal_declaration(
    tmp_path,
) -> None:
    """Positive control: an `AnnAssign` to a `Name` target whose value is an
    ordinary literal (`ch_client: Any = None`) is real, common code --
    job_daily.py itself declares `ch_client: Any = None` and
    `git_metrics: list[Any] = []` this way -- and must NOT be refused just
    because it has a value. It resolves to an unresolvable alias exactly
    like a plain `Assign` to a literal would, which is harmless unless
    something later calls it bare (see the sibling
    rejects_an_annotated_alias test for that case).
    """
    gen = _load_gen_module()
    synthetic = tmp_path / "job_daily.py"
    synthetic.write_text(
        "def run_daily_metrics_finalize():\n"
        "    ch_client: object = None\n"
        "    _write_compounding_risk_team_rows_for_day()\n"
    )
    original = getattr(gen, "JOB_DAILY_PY")
    try:
        setattr(gen, "JOB_DAILY_PY", synthetic)
        calls = gen.load_finalize_write_calls()
        assert calls == {"_write_compounding_risk_team_rows_for_day"}, (
            "load_finalize_write_calls wrongly refused/misbehaved on an "
            f"annotated literal declaration that is never called: {calls!r}"
        )
    finally:
        setattr(gen, "JOB_DAILY_PY", original)


def test_load_finalize_write_calls_allows_a_subscript_or_attribute_target(
    tmp_path,
) -> None:
    """Positive control: `d[k] = v` / `obj.attr = v` mutate something that
    already exists and introduce NO new bare local name -- unlike a
    destructured Tuple/List/Starred target, they can never be the thing a
    later bare `name()` call resolves through. Real code does this
    routinely (job_daily.py: `team_metrics_params["org_id"] = org_id`), so
    this must be skipped, not refused as an unmodeled destructuring shape.
    """
    gen = _load_gen_module()
    synthetic = tmp_path / "job_daily.py"
    synthetic.write_text(
        "def run_daily_metrics_finalize():\n"
        "    params['org_id'] = org_id\n"
        "    obj.attr = org_id\n"
        "    _write_compounding_risk_team_rows_for_day()\n"
    )
    original = getattr(gen, "JOB_DAILY_PY")
    try:
        setattr(gen, "JOB_DAILY_PY", synthetic)
        calls = gen.load_finalize_write_calls()
        assert calls == {"_write_compounding_risk_team_rows_for_day"}, (
            "load_finalize_write_calls wrongly refused a Subscript/Attribute "
            f"assignment target: {calls!r}"
        )
    finally:
        setattr(gen, "JOB_DAILY_PY", original)


def test_load_finalize_write_calls_allows_a_destructured_target_never_called(
    tmp_path,
) -> None:
    """Positive control for the final F1 design: a destructured `for k, v
    in ...:` loop target is real, common code (job_daily.py:
    `for k, v in ... if k in team_metrics_field_names`) that never calls
    `k` or `v` bare. Refusing the moment a destructuring pattern EXISTS,
    regardless of whether any of its names are ever called, was tried
    first and broke on exactly this shape -- it must instead be tracked
    as unresolvable and only refuse if a later bare call actually reaches
    it (see the sibling rejects_a_r3_destructured_for_alias test for that
    case).
    """
    gen = _load_gen_module()
    synthetic = tmp_path / "job_daily.py"
    synthetic.write_text(
        "def run_daily_metrics_finalize():\n"
        "    pairs = {}\n"
        "    pair_source = []\n"
        "    for k, v in pair_source:\n"
        "        pairs[k] = v\n"
        "    _write_compounding_risk_team_rows_for_day()\n"
    )
    original = getattr(gen, "JOB_DAILY_PY")
    try:
        setattr(gen, "JOB_DAILY_PY", synthetic)
        calls = gen.load_finalize_write_calls()
        assert calls == {"_write_compounding_risk_team_rows_for_day"}, (
            "load_finalize_write_calls wrongly refused a destructured "
            f"for-loop target that is never called bare: {calls!r}"
        )
    finally:
        setattr(gen, "JOB_DAILY_PY", original)


def test_workgraph_ledger_rejects_a_row_whose_kind_left_the_artifact() -> None:
    """Falsification control for CHAOS-5153's reverse §4 guard.

    A WORKGRAPH_INVESTMENT_LEDGER row surviving after its kind's
    addWorkgraphWorker case is deleted (the CHAOS-4438 shape: dead code
    removed from Go, but the hand-maintained doc row never follows) must
    fail generation, not keep rendering as a live table row forever.
    Mutates the artifact in memory (never touches
    contracts/native-families/v1/native-families.json on disk) to drop a
    real workgraph kind and asserts the guard refuses.
    """
    gen = _load_gen_module()
    original = getattr(gen, "load_native_families_artifact")

    def artifact_missing_materialize() -> dict:
        real = original()
        workgraph = dict(real["workgraph"])
        del workgraph["investment.materialize"]
        return {**real, "workgraph": workgraph}

    try:
        setattr(gen, "load_native_families_artifact", artifact_missing_materialize)
        try:
            gen.render_workgraph_investment_block()
            raised = False
        except SystemExit:
            raised = True
        assert raised, (
            "render_workgraph_investment_block did not refuse a ledger row "
            "whose kind is no longer in the native-families artifact"
        )
    finally:
        setattr(gen, "load_native_families_artifact", original)


def test_stale_direct_job_modules_are_not_wired_into_cli() -> None:
    """Cheap regression guard for CHAOS-5153's CLI-verb citation fix
    (team-lead, 2026-09-05): none of the five `job_*.py` modules the
    matrix's OLD (now-corrected) prose cited as the writer for
    `daily`/`rebuild`/`complexity`/`dora`/`capacity`/`release-impact`
    are actually imported by `cli.py` -- CHAOS-5055 moved all six verbs
    to dispatch through `workerctl_dispatch.py` instead (the same
    coordinator the automatic post-sync/fixed-schedule pipeline uses).

    UPDATE (CHAOS-5307): four of these five modules' own `register_commands`
    functions (the actual dead code this docstring used to flag as "not this
    ticket's concern to remove") have now been deleted -- `job_daily.py`,
    `job_complexity_db.py`, `job_dora.py`, `job_capacity.py` no longer define
    `register_commands`/their `_cmd_metrics_*` CLI wrappers at all (their
    underlying compute functions are untouched; only the unreachable argparse
    wiring was removed). `job_release_impact.py` no longer exists as a file.
    This guard still holds regardless: checking `cli.py`'s own import list is
    what proves reachability, independent of whether the dead functions still
    exist in their source files or have since been deleted.

    This does not mechanize the whole CLI-verb section -- it only prevents
    the SPECIFIC regression this fix corrects from silently recurring:
    if one of these five modules is ever wired back into `cli.py`'s
    dispatch tree, this guard fails loudly, rather than the doc quietly
    going stale a second time with nobody noticing until the next
    manual audit.
    """
    assert CLI_PY.is_file(), f"missing cli.py: {CLI_PY}"
    tree = ast.parse(CLI_PY.read_text(encoding="utf-8"))
    imported_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module.rsplit(".", 1)[-1])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imported_modules.add(alias.name.rsplit(".", 1)[-1])

    stale_writer_modules = {
        "job_daily",
        "job_complexity_db",
        "job_dora",
        "job_capacity",
        "job_release_impact",
    }
    collision = stale_writer_modules & imported_modules
    assert not collision, (
        f"cli.py now imports {sorted(collision)} -- CHAOS-5153 assumed these "
        "modules are NOT reachable from cli.py's dispatch tree (CHAOS-5055 moved "
        "daily/rebuild/complexity/dora/capacity/release-impact to "
        "workerctl_dispatch.py). If one of these is wired back in, "
        "docs/go-migration-matrix.md's CLI-verb rows need re-checking, not just "
        "this guard silenced."
    )
