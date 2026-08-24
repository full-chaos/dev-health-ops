#!/usr/bin/env python3
"""Live-Python oracle for FeatureDecisionReason's closed vocabulary.

Emits {member.name: member.value} for every value the REAL
``dev_health_ops.licensing.feature_policy.FeatureDecisionReason`` StrEnum
declares, as stable JSON on stdout. The Go side
(canonical_incident_reason_oracle_test.go) diffs its own
scheduler/sync.FeatureDecisionReason constant set against this -- per
AGENTS.md's live-python-oracle mandate, a differential check against the
actual producer, not a hand-copied string list that could silently drift if
Python ever adds, renames, or removes a reason value.

feature_policy.py is deliberately import-light (registry.py's own docstring:
"Lives in `licensing/` (not `models/`) so it can be imported without pulling
in SQLAlchemy") -- no stdout-redirection dance is needed here, unlike the
finalize_sync_run zero-unit oracle, which imports a module with a heavy
Celery-instrumented import graph.
"""

from __future__ import annotations

import json
import sys


def main() -> int:
    from dev_health_ops.licensing.feature_policy import FeatureDecisionReason

    reasons = {member.name: member.value for member in FeatureDecisionReason}
    json.dump(reasons, sys.stdout, sort_keys=True, separators=(",", ":"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
