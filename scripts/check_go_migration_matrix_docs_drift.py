#!/usr/bin/env python3
"""Fail when the published Go migration matrix drifts from its producers.

Same shape as ``scripts/check_python_go_ledger_docs_drift.py`` (CHAOS-4433):
this script recomputes the four generated blocks from the current producers
(``contracts/provider-matrix/v1/matrix.json``,
``internal/jobs/metrics/daily/families.json``,
``internal/jobs/metrics/remaining/families.json``, plus the entirely
hand-maintained workgraph/investment ledger) via
``scripts/gen_go_migration_matrix_docs.py`` and fails if the published blocks
in ``docs/go-migration-matrix.md`` disagree -- or if the generator itself
refuses to render because a family/dataset has no curated row.
"""

from __future__ import annotations

import importlib.util
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GEN_SCRIPT = ROOT / "scripts" / "gen_go_migration_matrix_docs.py"
DOC_PATH = ROOT / "docs" / "go-migration-matrix.md"

BLOCKS = (
    (
        "provider sync",
        "render_provider_sync_block",
        "<!-- BEGIN GENERATED PROVIDER SYNC MATRIX -->",
        "<!-- END GENERATED PROVIDER SYNC MATRIX -->",
    ),
    (
        "daily metrics",
        "render_daily_metrics_block",
        "<!-- BEGIN GENERATED DAILY METRICS MATRIX -->",
        "<!-- END GENERATED DAILY METRICS MATRIX -->",
    ),
    (
        "remaining metrics",
        "render_remaining_metrics_block",
        "<!-- BEGIN GENERATED REMAINING METRICS MATRIX -->",
        "<!-- END GENERATED REMAINING METRICS MATRIX -->",
    ),
    (
        "workgraph/investment",
        "render_workgraph_investment_block",
        "<!-- BEGIN GENERATED WORKGRAPH INVESTMENT MATRIX -->",
        "<!-- END GENERATED WORKGRAPH INVESTMENT MATRIX -->",
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


def check_matrix_doc() -> list[str]:
    errors: list[str] = []
    if not DOC_PATH.is_file():
        errors.append(f"matrix page missing: {DOC_PATH}")
        return errors

    try:
        gen = _load_gen_module()
    except SystemExit as exc:
        errors.append(f"generator failed to load: {exc}")
        return errors

    doc = DOC_PATH.read_text(encoding="utf-8")
    for label, render_fn_name, begin, end in BLOCKS:
        try:
            expected_block = getattr(gen, render_fn_name)()
        except SystemExit as exc:
            errors.append(f"{label} matrix out of sync with its producer: {exc}")
            continue
        start = doc.find(begin)
        stop = doc.find(end)
        if start == -1 or stop == -1 or stop < start:
            errors.append(f"generated {label} matrix markers missing in {DOC_PATH}")
            continue
        actual_block = doc[start : stop + len(end)]
        if actual_block != expected_block:
            errors.append(
                f"generated {label} matrix block in {DOC_PATH.relative_to(ROOT)} is stale. "
                "Run 'python scripts/gen_go_migration_matrix_docs.py' and commit the result."
            )
    return errors


def main() -> int:
    errors = check_matrix_doc()
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Go migration matrix drift check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
