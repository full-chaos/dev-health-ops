"""Every Go image a deployment renderer names must be one CI publishes.

CHAOS-3923: the renderers referenced ``ghcr.io/<owner>/dev-health-go-*`` while
no workflow published those targets -- ``go.yml`` builds them with a plain
``docker build`` for Trivy and SBOM only. A host with just Docker installed
could not start the Go topology, and the pull failure read as a registry
permission problem rather than a missing publish job. Nothing failed in CI,
because no test connected the names renderers use to the names CI pushes.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "docker-images.yml"
CONTAINER_GATE = ROOT / "ci" / "check_go_containers.sh"
DEPLOY = ROOT / "deploy"

IMAGE_PREFIX = "dev-health-go-"
_ALL_TARGETS = re.compile(r"^readonly ALL_TARGETS=\((?P<targets>[^)]*)\)", re.MULTILINE)
_GHCR_IMAGE = re.compile(
    r"ghcr\.io/[^/\s]+/" + IMAGE_PREFIX + r"(?P<target>[a-z0-9-]+)"
)


def _gate_targets() -> set[str]:
    """The Go container targets ci/check_go_containers.sh knows about."""
    match = _ALL_TARGETS.search(CONTAINER_GATE.read_text(encoding="utf-8"))
    assert match is not None, "ALL_TARGETS is no longer declared as one literal array"
    return set(match.group("targets").split())


def _published_targets(job: str) -> set[str]:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return set(workflow["jobs"][job]["strategy"]["matrix"]["target"])


def _renderer_image_targets() -> dict[str, set[Path]]:
    """Map each referenced Go image target to the renderers that name it."""
    referenced: dict[str, set[Path]] = {}
    for path in sorted(DEPLOY.rglob("*")):
        if path.suffix not in {".yml", ".yaml"} or "vendor" in path.parts:
            continue
        for match in _GHCR_IMAGE.finditer(path.read_text(encoding="utf-8")):
            referenced.setdefault(match.group("target"), set()).add(path)
    return referenced


def test_publish_matrix_covers_every_container_gate_target() -> None:
    # One list, not two. A second, shorter publish list is exactly how the next
    # target ships unpublished.
    assert _published_targets("go-build") == _gate_targets()


def test_merge_matrix_matches_the_build_matrix() -> None:
    # A target built but never merged pushes digests that no tag ever points
    # at, which looks published until something tries to pull it.
    assert _published_targets("go-merge") == _published_targets("go-build")


def test_every_referenced_go_image_is_published() -> None:
    published = _published_targets("go-merge")
    referenced = _renderer_image_targets()
    assert referenced, (
        "no renderer references a Go image; the guard would pass vacuously"
    )
    unpublished = {
        target: sorted(str(path.relative_to(ROOT)) for path in paths)
        for target, paths in referenced.items()
        if target not in published
    }
    assert not unpublished, (
        f"deployment renderers name Go images that CI never publishes: {unpublished}"
    )
