"""CHAOS-6950: the unsupported deploy alternates stay deleted.

The supported production path is the Helm chart (deploy/helm/dev-health). The
production Docker Compose file, the Swarm stacks, the raw Kubernetes manifests
and the split Compose overlays duplicated its contract, still ran the Python
`dev-hops-api` image family, and drifted from it, so they were deleted. This
pins that no deployable artefact under `deploy/` other than the chart names the
Python api/runner image again, and that the deleted trees do not come back.
"""

from __future__ import annotations

import re
from pathlib import Path

_DEPLOY = Path(__file__).resolve().parents[1] / "deploy"

_DELETED_TREES = ("docker-compose", "docker-swarm", "kubernetes")

# The Python image family: dev-hops-api / dev-hops-runner, and the placeholder
# names the alternates used for it.
_PYTHON_IMAGE = re.compile(
    r"dev-hops-(?:api|runner)|your-org/dev-health-ops|dev-health-ops:(?:latest|local)"
)


def test_the_deleted_deploy_trees_do_not_come_back() -> None:
    present = [name for name in _DELETED_TREES if (_DEPLOY / name).exists()]
    assert not present, (
        f"deploy/{present} were deleted (CHAOS-6950): Helm is the one supported "
        "production artefact; a new alternate needs its own supported statement"
    )


def test_no_deploy_artefact_outside_the_chart_names_the_python_image() -> None:
    offenders: list[str] = []
    for path in sorted(_DEPLOY.rglob("*")):
        if not path.is_file() or "helm" in path.relative_to(_DEPLOY).parts[:1]:
            continue
        if path.suffix in {".json", ".png"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if _PYTHON_IMAGE.search(text):
            offenders.append(str(path.relative_to(_DEPLOY)))
    assert not offenders, (
        f"deploy artefacts outside the chart reference the Python api image: {offenders}"
    )
