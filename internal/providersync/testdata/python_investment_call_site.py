#!/usr/bin/env python3
"""Reflect the two premises the investment reachability tripwire stands on.

The tripwire claims four of the real config's 44 rules cannot fire from the
work-item production-helper call site. That claim rests entirely on two facts
about PRODUCTION code, neither of which lives in the config file:

  1. the call site builds its artifact as an inline dict literal whose keys are
     exactly ``labels``, ``component``, ``title`` and ``provider`` -- so
     ``paths`` is absent, and the matcher's path_prefix arm rejects every
     artifact reaching it from there;
  2. ``WorkItem`` declares no ``component`` field, so the call site's
     ``getattr(item, "component", "")`` cannot return anything but ``""``.

Both were previously hand-transcribed into the Go test as
``InvestmentArtifact{Component: "", Paths: nil}``, with no derivation. Adding a
``component`` field to ``WorkItem`` would have made the dead rules live while
the test went on passing, because the test was asserting against its own copy
of the premise rather than against the premise. Emitting both from the real
sources makes that model change fail the tripwire loudly.

Usage: python_investment_call_site.py <call_site.py> <work_items.py>
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import sys


def _load_field_reflection():
    module_path = pathlib.Path(__file__).resolve().parent / "field_reflection.py"
    spec = importlib.util.spec_from_file_location(
        "dev_health_field_reflection", module_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    reflection = _load_field_reflection()
    call_site = pathlib.Path(sys.argv[1]).resolve()
    work_items = pathlib.Path(sys.argv[2]).resolve()
    payload = {
        "call_site_artifact_keys": sorted(
            reflection.call_dict_literal_keys(call_site.read_text(), "classify")
        ),
        "work_item_fields": sorted(
            reflection.dataclass_field_names(work_items.read_text(), "WorkItem")
        ),
    }
    json.dump(payload, sys.stdout, sort_keys=True, separators=(",", ":"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
