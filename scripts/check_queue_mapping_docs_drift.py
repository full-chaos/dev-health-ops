#!/usr/bin/env python3
"""Fail when the published queue-mapping table drifts from its producers.

Companion to ``scripts/check_investment_docs_drift.py``, same shape: this
script recomputes the generated block from the current producers
(``deploy/go-workers/deployment.json``, ``contracts/jobs/v1/registry.json``,
``internal/jobs/metrics/remaining/families.json``, ``compose.yml``) via
``scripts/gen_queue_mapping_docs.py`` and fails if the published block in
``docs/contribute/architecture/go-worker-runtime.md`` disagrees. See
CHAOS-4044.
"""

from __future__ import annotations

import importlib.util
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GEN_SCRIPT = ROOT / "scripts" / "gen_queue_mapping_docs.py"
DOC_PATH = ROOT / "docs" / "contribute" / "architecture" / "go-worker-runtime.md"

BEGIN = "<!-- BEGIN GENERATED QUEUE MAP -->"
END = "<!-- END GENERATED QUEUE MAP -->"


def _load_gen_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location("gen_queue_mapping_docs", GEN_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_queue_mapping_doc() -> list[str]:
    errors: list[str] = []
    gen = _load_gen_module()
    try:
        expected_block = gen.render_block()
    except SystemExit as exc:
        errors.append(f"generator failed to render (producers out of sync?): {exc}")
        return errors

    doc = DOC_PATH.read_text(encoding="utf-8")
    start = doc.find(BEGIN)
    stop = doc.find(END)
    if start == -1 or stop == -1 or stop < start:
        errors.append(f"generated queue-map markers missing in {DOC_PATH}")
        return errors
    actual_block = doc[start : stop + len(END)]
    if actual_block != expected_block:
        errors.append(
            f"generated queue-map block in {DOC_PATH.relative_to(ROOT)} is stale. "
            "Run 'python scripts/gen_queue_mapping_docs.py' and commit the result."
        )
    return errors


def main() -> int:
    errors = check_queue_mapping_doc()
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Queue mapping docs drift check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
