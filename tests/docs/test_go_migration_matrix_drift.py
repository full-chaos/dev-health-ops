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

import importlib.util
import subprocess
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
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
    """Pin the 5-native/2-compat split this page exists to correct (chris, 09-04:

    "we haven't finished the port as I was led to believe again" -- but in
    the OTHER direction here: the 09-01 snapshot undercounted native
    coverage. Regression-guards the exact split so a future edit cannot
    silently flip a row without a reviewer noticing in the diff. Reads
    contracts/native-families/v1/native-families.json -- the Go-AST-derived
    artifact, not a curated Python dict (REMAINING_EXECUTOR_LEDGER no longer
    carries an executor value at all, only citation/route/ticket prose).
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
    }
    assert compats == {"complexity", "release_impact"}


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
    assert compat_families == {
        ("daily", "compounding_risk"),
        ("daily", "team_cognitive_load"),
        ("daily", "ic_finalize"),
        ("remaining", "complexity"),
    }


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


def test_split_status_still_counts_as_compat_once_repo_scope_goes_native() -> None:
    """End-to-end proof of the defect fix: simulate compounding_risk's (§2)
    and complexity's (§3) repo scope going native -- as review-bench's #2230
    is about to do for compounding_risk -- and assert the rendered row is
    STILL is_compat_executor-true, not bare NATIVE. This is what makes
    CHAOS-3092's "zero COMPAT rows" close condition correct across that
    transition instead of satisfiable by omission.
    """
    gen = _load_gen_module()
    daily_names = {f["name"] for f in gen.load_daily_families()}
    remaining_names = {f["name"] for f in gen.load_remaining_families()}
    finalize_compat = gen.load_daily_finalize_compat_families(
        daily_names, remaining_names
    )

    daily_status = gen.daily_family_executor(
        "compounding_risk", {"compounding_risk": "native"}, finalize_compat
    )
    assert daily_status == "NATIVE (repo) / COMPAT-Python (finalize)"
    assert gen.is_compat_executor(daily_status) is True

    remaining_status = gen.remaining_family_executor(
        "complexity", {"complexity": "native"}, finalize_compat
    )
    assert remaining_status == "NATIVE (repo) / COMPAT-Python (finalize)"
    assert gen.is_compat_executor(remaining_status) is True


def test_split_render_fires_through_the_actual_count_helpers_and_reverses() -> None:
    """Required before merge (team-lead, 2026-09-05): prove the split render
    and count_compat_daily_families FIRE TOGETHER through the real counting
    function (not just the underlying predicate, which the previous test
    already covers) -- AND that the effect reverses once the Python
    remainder is genuinely gone, so this isn't a one-way ratchet that keeps
    counting a family as compat forever just because it once had a finalize
    call.

    Two states compared against the SAME baseline count:
    1. compounding_risk's repo scope goes native, finalize call untouched
       (still real) -- count_compat_daily_families must NOT drop: the split
       row still counts.
    2. ALSO remove `_write_compounding_risk_team_rows_for_day` from the
       AST-walked call set (simulating the Python port genuinely
       finishing) -- NOW the family must drop out of the compat count, and
       its rendered status must degrade to bare "NATIVE".
    """
    gen = _load_gen_module()
    baseline = gen.count_compat_daily_families()

    original_artifact = getattr(gen, "load_native_families_artifact")

    def native_compounding_risk() -> dict:
        real = original_artifact()
        return {**real, "daily": {**real["daily"], "compounding_risk": "native"}}

    try:
        setattr(gen, "load_native_families_artifact", native_compounding_risk)

        after_native = gen.count_compat_daily_families()
        assert after_native == baseline, (
            "a family whose repo scope went native but still has a real finalize "
            "call must still count as compat (split row) -- count must not drop"
        )

        daily_names = {f["name"] for f in gen.load_daily_families()}
        remaining_names = {f["name"] for f in gen.load_remaining_families()}
        finalize_compat = gen.load_daily_finalize_compat_families(
            daily_names, remaining_names
        )
        artifact_daily = gen.load_native_families_artifact()["daily"]
        assert (
            gen.daily_family_executor(
                "compounding_risk", artifact_daily, finalize_compat
            )
            == "NATIVE (repo) / COMPAT-Python (finalize)"
        )

        original_calls = getattr(gen, "load_finalize_write_calls")
        try:
            setattr(
                gen,
                "load_finalize_write_calls",
                lambda: (
                    original_calls() - {"_write_compounding_risk_team_rows_for_day"}
                ),
            )

            after_mutation = gen.count_compat_daily_families()
            assert after_mutation == baseline - 1, (
                "once the finalize call is actually gone, the family must drop "
                "OUT of the compat count -- otherwise this is a one-way ratchet"
            )

            finalize_compat_after = gen.load_daily_finalize_compat_families(
                daily_names, remaining_names
            )
            assert "compounding_risk" not in finalize_compat_after
            status_after = gen.daily_family_executor(
                "compounding_risk", artifact_daily, finalize_compat_after
            )
            assert status_after == "NATIVE"
            assert gen.is_compat_executor(status_after) is False
        finally:
            setattr(gen, "load_finalize_write_calls", original_calls)
    finally:
        setattr(gen, "load_native_families_artifact", original_artifact)


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
