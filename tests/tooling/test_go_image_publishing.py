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
_CI_SCRIPT_REF = re.compile(r"\bci/[A-Za-z0-9_.-]+\.(?:sh|py)\b")


def _build_job_ci_script_references() -> set[str]:
    """Every `ci/*.sh`/`ci/*.py` path the `build` job's own `run:` steps
    invoke -- read from the parsed YAML `run:` text (what actually
    executes), not a raw grep over the file, same discipline as
    `_latest_tag_step_script()` in test_docker_images_fanin_gate.py."""
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    refs: set[str] = set()
    for step in workflow["jobs"]["build"]["steps"]:
        refs.update(_CI_SCRIPT_REF.findall(step.get("run", "")))
    return refs


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


def test_the_publish_workflow_rebuilds_when_it_changes() -> None:
    """A change to the publish pipeline must republish.

    The Go publish jobs landed on main and every build job skipped: the
    `changes` filter matches src/cmd/internal/docker/deploy paths, none of
    which a workflow edit touches. `changes` went green, the run reported
    success, and the registry stayed empty -- the measurement said fine
    because it never looked at the thing that changed.
    """
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    filters = yaml.safe_load(workflow["jobs"]["changes"]["steps"][1]["with"]["filters"])
    assert ".github/workflows/docker-images.yml" in filters["code"]


def test_build_job_ci_script_references_are_covered_by_the_changes_filter() -> None:
    """CHAOS-4949 (#2162), codex round 5 P1: the `build` job's own base-guard
    step invokes `ci/python_base_ref.sh` directly, but the `changes` filter
    (which decides whether `build` even RUNS on a PR) had no entry for it --
    a PR touching ONLY that script set changes.code=false and skipped
    `build`, and the guard the script belongs to, entirely. Same failure
    shape as test_the_publish_workflow_rebuilds_when_it_changes above, one
    filter section over.

    Recurrence guard, not a one-off fix: every `ci/*.sh`/`ci/*.py` the
    build job's `run:` steps reference must appear verbatim in the
    filter's `code` list. A future SECOND script added to the build job
    without a matching filter update fails this test immediately, rather
    than silently skipping `build` the same way -- this is what actually
    prevents recurrence, independent of whether the filter entry chosen
    for today's single script is broad or narrow."""
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    filters = yaml.safe_load(workflow["jobs"]["changes"]["steps"][1]["with"]["filters"])
    code_patterns = set(filters["code"])
    referenced = _build_job_ci_script_references()
    assert referenced, (
        "no ci/*.sh or ci/*.py reference found in the build job's run: "
        "steps -- this guard would pass vacuously; if ci/python_base_ref.sh's "
        "invocation moved or was renamed, update _build_job_ci_script_references, "
        "don't just delete this assert"
    )
    uncovered = sorted(ref for ref in referenced if ref not in code_patterns)
    assert not uncovered, (
        f"the build job invokes {uncovered} but the `changes` filter's code "
        "list does not name them verbatim -- a PR touching only these "
        "scripts sets changes.code=false and skips the build job (and "
        "whatever guard the script belongs to) entirely"
    )
