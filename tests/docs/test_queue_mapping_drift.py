"""Queue-mapping docs drift guard (CHAOS-4044).

The Celery-to-River queue mapping is published at
``docs/contribute/architecture/go-worker-runtime.md``, inside the generated
``BEGIN/END GENERATED QUEUE MAP`` block written by
``scripts/gen_queue_mapping_docs.py`` from ``deploy/go-workers/deployment.json``,
``contracts/jobs/v1/registry.json``, ``contracts/jobs/v1/migration-state.json``,
``compose.yml``, and ``deploy/docker-compose/compose.production.yml``.
``scripts/check_queue_mapping_docs_drift.py`` fails when the published page
and those producers disagree, mirroring ``tests/docs/test_investment_drift.py``.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DRIFT_SCRIPT = ROOT / "scripts" / "check_queue_mapping_docs_drift.py"
GEN_SCRIPT = ROOT / "scripts" / "gen_queue_mapping_docs.py"
CANONICAL_DOC = ROOT / "docs" / "contribute" / "architecture" / "go-worker-runtime.md"

BEGIN = "<!-- BEGIN GENERATED QUEUE MAP -->"
END = "<!-- END GENERATED QUEUE MAP -->"


def _load_gen_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location("gen_queue_mapping_docs", GEN_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_queue_mapping_drift_check_exits_clean() -> None:
    """check_queue_mapping_docs_drift.py must exit 0 and emit no ERROR lines."""
    assert DRIFT_SCRIPT.is_file(), f"missing drift script: {DRIFT_SCRIPT}"
    result = subprocess.run(
        [sys.executable, str(DRIFT_SCRIPT)],
        check=False,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Queue mapping docs drift check failed:\n{result.stdout}\n{result.stderr}"
    )
    assert "ERROR:" not in result.stdout, (
        f"drift check reported errors:\n{result.stdout}"
    )


def test_go_worker_runtime_generated_block_matches_producers() -> None:
    """The published block must match its producers (deployment.json, registry.json,
    migration-state.json, compose.yml, compose.production.yml).

    Read-only verification: proves the published page is in sync with its
    producers without writing to disk.
    """
    assert GEN_SCRIPT.is_file(), f"missing gen script: {GEN_SCRIPT}"
    assert CANONICAL_DOC.is_file(), f"missing canonical page: {CANONICAL_DOC}"

    gen = _load_gen_module()
    expected_block = gen.render_block()

    doc = CANONICAL_DOC.read_text(encoding="utf-8")
    start = doc.find(BEGIN)
    stop = doc.find(END)
    assert start != -1 and stop > start, (
        f"generated queue-map markers missing in {CANONICAL_DOC}"
    )
    actual_block = doc[start : stop + len(END)]

    assert actual_block == expected_block, (
        "Generated block in docs/contribute/architecture/go-worker-runtime.md is "
        "stale. Run 'python scripts/gen_queue_mapping_docs.py' and commit the result."
    )


def test_go_worker_runtime_does_not_enshrine_worker_enabled_switches() -> None:
    """CHAOS-4054 deleted WORKER_*_ENABLED; the page must say so, in past tense.

    This guard predates the deletion, when the only thing that could be
    asserted was that the page called the switch surface dying rather than
    current. Steps 1-3 shipped, so "being retired" is now itself the wrong
    framing -- a reader arriving at an in-flight description would still go
    looking for a switch to flip. The assertion is tightened accordingly: the
    page must state the surface is deleted, and must name the two planes that
    replaced it.

    Tightened, not relaxed: "being retired" no longer satisfies this test.
    """
    doc = CANONICAL_DOC.read_text(encoding="utf-8")
    assert "CHAOS-4054" in doc, (
        "go-worker-runtime.md must reference the CHAOS-4054 two-plane decision "
        "record when discussing WORKER_*_ENABLED route switches"
    )
    assert "are **deleted**" in doc or "surface is deleted" in doc, (
        "go-worker-runtime.md must state that the WORKER_*_ENABLED surface is "
        "deleted -- not that it is being retired, and never as the current "
        "enablement model"
    )
    for phrase in ("being retired", "is dying", "on its way out"):
        assert phrase not in doc, (
            f"go-worker-runtime.md still describes the route-switch surface as "
            f"in-flight ({phrase!r}). CHAOS-4054 steps 1-3 deleted it; the page "
            "must describe the finished state."
        )
    assert "IntegrationDataset.is_enabled" in doc and "-Q" in doc, (
        "go-worker-runtime.md must name both replacement planes: intent "
        "(IntegrationDataset.is_enabled) and serving (-Q topology)"
    )
