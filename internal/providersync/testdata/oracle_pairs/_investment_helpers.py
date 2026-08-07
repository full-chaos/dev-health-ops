"""Shared case plumbing for the two investment classifier oracle pairs.

Both pairs -- ``analytics/investment/classify`` (the config shapes both engines
CLASSIFY) and ``analytics/investment/refusal`` (the config shapes both engines
REFUSE) -- resolve the same config names to the same files and build the same
artifact from the same case keys. Keeping that in one module is not tidiness:
if the two pairs resolved names independently, a refusal case could be pointed
at a different file from the classify case that is supposed to be its
counterpart, and the "same file, different artifact" pairs (path_prefix_null,
bare_component) would stop proving what their names claim.

The leading underscore keeps this out of ci/check_go.sh's pair enumeration --
it is a helper, not a registered pair, and must not be expected to produce an
execution proof of its own.
"""

from __future__ import annotations

import pathlib
from typing import Any

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]

# The file production actually loads (job_work_items.py:448).
REAL_CONFIG = REPO_ROOT / "src/dev_health_ops/config/investment_areas.yaml"

# Everything else resolves by NAME under one directory, rather than through a
# hand-maintained dict on each side. A per-name table drifts: a case pointed at
# a name the Go side spells differently would silently compare two different
# files. The Go side (investmentConfigPath) applies exactly this rule.
CONFIG_DIR = REPO_ROOT / "internal/providersync/testdata/investment_configs"


def config_path(name: str) -> pathlib.Path:
    """Resolve a case's config NAME to a path.

    "missing" is not special-cased: it resolves like every other name, to a
    file that is deliberately not checked in. Python logs a warning and
    classifies with an EMPTY rule list rather than raising, which is the only
    way to observe the 0.0/legacy_default fallback at all -- the real config's
    catch-all otherwise makes it unreachable.
    """
    if name == "real":
        return REAL_CONFIG
    return CONFIG_DIR / f"{name}.yaml"


def artifact(case: dict[str, Any]) -> dict[str, Any]:
    """Build the artifact dict for one case.

    ``labels``, ``title`` and ``provider`` are exactly what the production call
    site supplies (job_work_items.py:1377-1383).

    ``component`` and ``paths`` are supplied ONLY when the case sets them, and
    that is the point rather than an economy. The call site always passes
    ``component`` (always ""), and never passes ``paths`` -- but the matcher
    reads both with ``artifact.get(...)``, and an ABSENT key yields None, which
    is a different value from "" for the membership test and a different
    control flow for the path arm. Defaulting them here would make those two
    states unreachable from any case and leave the Go pointer fields with
    nothing measuring them.
    """
    case_artifact = case.get("Artifact") or {}
    built: dict[str, Any] = {
        "labels": list(case_artifact.get("Labels") or []),
        "title": case_artifact.get("Title", ""),
        "provider": case_artifact.get("Provider", "github"),
    }
    if "Component" in case_artifact:
        built["component"] = case_artifact["Component"]
    if case_artifact.get("Paths") is not None:
        built["paths"] = list(case_artifact["Paths"])
    return built
