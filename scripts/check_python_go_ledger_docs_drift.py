#!/usr/bin/env python3
"""Fail when the published Python<->Go live-path ledger drifts from its producers.

Same shape as ``scripts/check_queue_mapping_docs_drift.py`` (CHAOS-4044): this
script recomputes the four generated blocks from the current producers
(``contracts/jobs/v1/registry.json``, ``contracts/jobs/v1/migration-state.json``,
``internal/syncdispatchruntime/bridge.go``, ``src/dev_health_ops/workers/*.py``,
``internal/scheduler/sync/source_discovery.go``)
via ``scripts/gen_python_go_ledger_docs.py`` and fails if the published blocks in
``docs/reference/runtime/python-go-live-path-ledger.md`` disagree -- or if the
generator itself refuses to render because a kind/route/file/source-discovery
provider has no curated ledger row. See CHAOS-4433 (and CHAOS-4602 for the
fourth block).
"""

from __future__ import annotations

import importlib.util
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GEN_SCRIPT = ROOT / "scripts" / "gen_python_go_ledger_docs.py"
DOC_PATH = ROOT / "docs" / "reference" / "runtime" / "python-go-live-path-ledger.md"

BLOCKS = (
    (
        "kind",
        "render_kind_block",
        "<!-- BEGIN GENERATED KIND LEDGER -->",
        "<!-- END GENERATED KIND LEDGER -->",
    ),
    (
        "bridge route",
        "render_route_block",
        "<!-- BEGIN GENERATED BRIDGE ROUTE LEDGER -->",
        "<!-- END GENERATED BRIDGE ROUTE LEDGER -->",
    ),
    (
        "worker file",
        "render_worker_block",
        "<!-- BEGIN GENERATED WORKER FILE LEDGER -->",
        "<!-- END GENERATED WORKER FILE LEDGER -->",
    ),
    (
        "source-discovery provider",
        "render_source_discovery_block",
        "<!-- BEGIN GENERATED SOURCE DISCOVERY LEDGER -->",
        "<!-- END GENERATED SOURCE DISCOVERY LEDGER -->",
    ),
)


def _load_gen_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(
        "gen_python_go_ledger_docs", GEN_SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_ledger_doc() -> list[str]:
    errors: list[str] = []
    if not DOC_PATH.is_file():
        errors.append(f"ledger page missing: {DOC_PATH}")
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
            errors.append(
                f"{label} ledger out of sync with its producer (registry.json / "
                f"bridge.go / workers dir): {exc}"
            )
            continue
        start = doc.find(begin)
        stop = doc.find(end)
        if start == -1 or stop == -1 or stop < start:
            errors.append(f"generated {label} ledger markers missing in {DOC_PATH}")
            continue
        actual_block = doc[start : stop + len(end)]
        if actual_block != expected_block:
            errors.append(
                f"generated {label} ledger block in "
                f"{DOC_PATH.relative_to(ROOT)} is stale. Run "
                "'python scripts/gen_python_go_ledger_docs.py' and commit the result."
            )
    return errors


def main() -> int:
    errors = check_ledger_doc()
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Python<->Go live-path ledger drift check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
