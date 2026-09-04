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
