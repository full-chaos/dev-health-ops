"""Investment-docs drift guard.

Extends docs test coverage for CHAOS-2316 (investment taxonomy as shared semantic
source) and CHAOS-2326 (ADR-002 investment-period-components alignment). Both issues
have 'Reconcile and close' disposition in the coverage matrix; these tests are the
automated proof required before reconciliation.

Canonical/legacy split: the generated taxonomy source of truth (BEGIN/END markers,
shared-semantic-source declaration) and the ADR history are preserved under
``.github/docs-legacy/``; the generation and drift tooling operate on that archived
source. The public reference page under ``docs/reference/taxonomies/`` is validated
separately for the current canonical vocabulary.
"""

from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LEGACY_DOCS = ROOT / ".github" / "docs-legacy"
DRIFT_SCRIPT = ROOT / "scripts" / "check_investment_docs_drift.py"
GEN_SCRIPT = ROOT / "scripts" / "gen_taxonomy_docs.py"
# Archived generated source of truth: the shared-semantic-source taxonomy document
# that the drift and generation tooling read and write.
ARCHIVED_TAXONOMY_DOC = LEGACY_DOCS / "product" / "investment-taxonomy.md"
# Canonical public taxonomy reference page.
CANONICAL_TAXONOMY_DOC = ROOT / "docs" / "reference" / "taxonomies" / "investment.md"
TAXONOMY_SRC = ROOT / "src" / "dev_health_ops" / "investment_taxonomy.py"
# Preserved ADR source history.
ADR_002 = LEGACY_DOCS / "architecture" / "adr" / "002-investment-period-components.md"
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


def test_archived_taxonomy_generated_block_matches_registry() -> None:
    """The generated block in the archived taxonomy source must match the registry.

    Proves that the archived shared-semantic-source document is in sync with the
    canonical Python registry without writing to disk (read-only verification).
    """
    assert GEN_SCRIPT.is_file(), f"missing gen script: {GEN_SCRIPT}"
    assert ARCHIVED_TAXONOMY_DOC.is_file(), (
        f"missing archived taxonomy source: {ARCHIVED_TAXONOMY_DOC}"
    )

    gen = _load_gen_module()
    themes, subcategories, mapping = gen.load_taxonomy()
    expected_block = gen.render_block(themes, subcategories, mapping)

    doc = ARCHIVED_TAXONOMY_DOC.read_text(encoding="utf-8")
    start = doc.find(BEGIN)
    stop = doc.find(END)
    assert start != -1 and stop > start, (
        f"generated taxonomy markers missing in {ARCHIVED_TAXONOMY_DOC}"
    )
    actual_block = doc[start : stop + len(END)]

    assert actual_block == expected_block, (
        "Generated block in the archived investment-taxonomy.md is stale. "
        "Run 'python scripts/gen_taxonomy_docs.py' and commit the result."
    )


def test_archived_taxonomy_doc_is_shared_semantic_source() -> None:
    """The archived taxonomy source must remain the single shared semantic source."""
    assert ARCHIVED_TAXONOMY_DOC.is_file(), (
        f"missing archived taxonomy source: {ARCHIVED_TAXONOMY_DOC}"
    )
    content = ARCHIVED_TAXONOMY_DOC.read_text(encoding="utf-8")

    # Declares itself the shared semantic source
    assert "shared" in content and "semantic source" in content, (
        "archived investment-taxonomy.md must declare itself the shared semantic source"
    )
    # References the canonical Python module
    assert "investment_taxonomy.py" in content, (
        "archived investment-taxonomy.md must reference the canonical Python source"
    )
    # Taxonomy is fixed — no synonyms, no overrides, no per-team config
    assert "fixed" in content, (
        "archived investment-taxonomy.md must state the taxonomy is fixed"
    )
    # Categorization never returns unknown
    assert "never" in content, (
        "archived investment-taxonomy.md must state categorization never returns unknown"
    )
    # Generated block present
    assert BEGIN in content and END in content, (
        "archived investment-taxonomy.md is missing generated taxonomy markers"
    )
    # All 5 canonical theme keys must appear in the doc
    gen = _load_gen_module()
    themes, _, _ = gen.load_taxonomy()
    for theme in themes:
        assert f"`{theme}`" in content, (
            f"theme key '{theme}' is not documented in the archived investment-taxonomy.md"
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
    # All 5 canonical theme keys must appear on the public page.
    gen = _load_gen_module()
    themes, _, _ = gen.load_taxonomy()
    for theme in themes:
        assert f"`{theme}`" in content, (
            f"theme key '{theme}' is not documented in the canonical taxonomy page"
        )


def test_adr_002_is_accepted_option_a_with_no_code_changes() -> None:
    """ADR-002 must be ACCEPTED, document Option A, and confirm no materializer changes."""
    assert ADR_002.is_file(), f"missing ADR-002: {ADR_002}"
    content = ADR_002.read_text(encoding="utf-8")

    # Status must be ACCEPTED
    assert "ACCEPTED" in content, "ADR-002 is not marked ACCEPTED"
    # Decision must be Option A (cross-period components)
    assert "Option A" in content, (
        "ADR-002 must document the Option A decision (cross-period components)"
    )
    # Must confirm no materializer code changes
    assert "No materializer code changes" in content, (
        "ADR-002 must state 'No materializer code changes are included with this ADR'"
    )
    # Must reference parent CHAOS-2326
    assert "CHAOS-2326" in content, "ADR-002 must reference parent issue CHAOS-2326"
    assert MATERIALIZE_MODULE.is_file(), f"missing materializer: {MATERIALIZE_MODULE}"
    materialize = MATERIALIZE_MODULE.read_text(encoding="utf-8")
    build_components_index = materialize.index("components = _build_components(")
    period_filter_index = materialize.index(
        "if bounds.end < config.from_ts or bounds.start >= config.to_ts:"
    )
    assert build_components_index < period_filter_index, (
        "period filtering must remain after component construction for ADR-002 Option A"
    )


def test_archived_taxonomy_all_linked_docs_exist() -> None:
    """All relative links in the archived taxonomy source must resolve to files."""
    assert ARCHIVED_TAXONOMY_DOC.is_file(), (
        f"missing archived taxonomy source: {ARCHIVED_TAXONOMY_DOC}"
    )
    taxonomy_dir = ARCHIVED_TAXONOMY_DOC.parent

    link_re = re.compile(r"\[.*?\]\((\.\./[^\)]+\.md)\)")
    content = ARCHIVED_TAXONOMY_DOC.read_text(encoding="utf-8")
    links = link_re.findall(content)

    assert links, "no relative links found in the archived investment-taxonomy.md"
    for rel_link in links:
        target = (taxonomy_dir / rel_link).resolve()
        assert target.is_file(), (
            f"archived investment-taxonomy.md has a broken link: {rel_link}"
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
