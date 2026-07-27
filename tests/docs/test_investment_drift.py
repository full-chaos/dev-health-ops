"""Investment-docs drift guard.

The canonical Investment vocabulary lives in ``src/dev_health_ops/investment_taxonomy.py``
and is published at ``docs/reference/taxonomies/investment.md``. That page carries the
generated ``BEGIN/END GENERATED TAXONOMY`` block written by
``scripts/gen_taxonomy_docs.py``; ``scripts/check_investment_docs_drift.py`` fails when
the page and the registry disagree, or when any published page shows a taxonomy example
using a key the registry does not define.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DRIFT_SCRIPT = ROOT / "scripts" / "check_investment_docs_drift.py"
GEN_SCRIPT = ROOT / "scripts" / "gen_taxonomy_docs.py"
# Canonical public taxonomy reference page: both the generated source of truth and the
# human-readable vocabulary now live here.
CANONICAL_TAXONOMY_DOC = ROOT / "docs" / "reference" / "taxonomies" / "investment.md"
TAXONOMY_SRC = ROOT / "src" / "dev_health_ops" / "investment_taxonomy.py"
MATERIALIZE_MODULE = (
    ROOT / "src" / "dev_health_ops" / "work_graph" / "investment" / "materialize.py"
)
STALE_TAXONOMY_FIXTURE = (
    ROOT
    / "tests"
    / "docs"
    / "fixtures"
    / "investment_taxonomy"
    / "operational-external.md"
)

BEGIN = "<!-- BEGIN GENERATED TAXONOMY -->"
END = "<!-- END GENERATED TAXONOMY -->"


def _load_gen_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location("gen_taxonomy_docs", GEN_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_investment_taxonomy_drift_check_exits_clean() -> None:
    """check_investment_docs_drift.py must exit 0 and emit no ERROR lines."""
    assert DRIFT_SCRIPT.is_file(), f"missing drift script: {DRIFT_SCRIPT}"
    result = subprocess.run(
        [sys.executable, str(DRIFT_SCRIPT)],
        check=False,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Investment docs drift check failed:\n{result.stdout}\n{result.stderr}"
    )
    assert "ERROR:" not in result.stdout, (
        f"drift check reported errors:\n{result.stdout}"
    )


def test_canonical_taxonomy_generated_block_matches_registry() -> None:
    """The generated block on the canonical page must match the Python registry.

    Read-only verification: proves the published page is in sync with
    ``investment_taxonomy.py`` without writing to disk.
    """
    assert GEN_SCRIPT.is_file(), f"missing gen script: {GEN_SCRIPT}"
    assert CANONICAL_TAXONOMY_DOC.is_file(), (
        f"missing canonical taxonomy page: {CANONICAL_TAXONOMY_DOC}"
    )

    gen = _load_gen_module()
    themes, subcategories, mapping = gen.load_taxonomy()
    expected_block = gen.render_block(themes, subcategories, mapping)

    doc = CANONICAL_TAXONOMY_DOC.read_text(encoding="utf-8")
    start = doc.find(BEGIN)
    stop = doc.find(END)
    assert start != -1 and stop > start, (
        f"generated taxonomy markers missing in {CANONICAL_TAXONOMY_DOC}"
    )
    actual_block = doc[start : stop + len(END)]

    assert actual_block == expected_block, (
        "Generated block in docs/reference/taxonomies/investment.md is stale. "
        "Run 'python scripts/gen_taxonomy_docs.py' and commit the result."
    )


def test_canonical_public_taxonomy_documents_current_vocabulary() -> None:
    """The public taxonomy reference must document the canonical, fixed vocabulary."""
    assert CANONICAL_TAXONOMY_DOC.is_file(), (
        f"missing canonical taxonomy page: {CANONICAL_TAXONOMY_DOC}"
    )
    content = CANONICAL_TAXONOMY_DOC.read_text(encoding="utf-8")

    # Declares the vocabulary canonical and not workspace-configurable.
    assert "canonical" in content.casefold(), (
        "canonical taxonomy page must declare the vocabulary canonical"
    )
    assert "not workspace-configurable" in content.casefold(), (
        "canonical taxonomy page must state the labels are not workspace-configurable"
    )
    # No competing vocabulary may be defined elsewhere.
    assert "must not define a competing vocabulary" in content.casefold(), (
        "canonical taxonomy page must forbid a competing vocabulary"
    )
    # Names the canonical Python registry as its source of truth.
    assert "investment_taxonomy.py" in content, (
        "canonical taxonomy page must reference the canonical Python source"
    )
    # All 5 canonical theme keys must appear on the public page.
    gen = _load_gen_module()
    themes, _, _ = gen.load_taxonomy()
    for theme in themes:
        assert f"`{theme}`" in content, (
            f"theme key '{theme}' is not documented in the canonical taxonomy page"
        )


def test_period_filtering_follows_component_construction() -> None:
    """Investment components are built before period filtering (ADR-002, Option A).

    Cross-period components must be constructed before the period window is applied,
    otherwise components that straddle the window boundary are dropped.
    """
    assert MATERIALIZE_MODULE.is_file(), f"missing materializer: {MATERIALIZE_MODULE}"
    materialize = MATERIALIZE_MODULE.read_text(encoding="utf-8")
    build_components_index = materialize.index("components = _build_components(")
    period_filter_index = materialize.index(
        "if bounds.end < config.from_ts or bounds.start >= config.to_ts:"
    )
    assert build_components_index < period_filter_index, (
        "period filtering must remain after component construction for ADR-002 Option A"
    )


def test_investment_taxonomy_fixture_reports_operational_external() -> None:
    drift = _load_drift_module()
    errors = drift._unknown_taxonomy_example_keys(
        STALE_TAXONOMY_FIXTURE.read_text(encoding="utf-8"),
        STALE_TAXONOMY_FIXTURE.name,
    )

    assert errors == [
        "operational-external.md JSON example #1 unknown subcategories keys: "
        "['operational.external']"
    ]


def _load_drift_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(
        "check_investment_docs_drift", DRIFT_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
