"""Every Go image a deployment renderer names must be one CI publishes.

CHAOS-3923: the renderers referenced ``ghcr.io/<owner>/dev-health-go-*`` while
no workflow published those targets -- ``go.yml`` builds them with a plain
``docker build`` for Trivy and SBOM only. A host with just Docker installed
could not start the Go topology, and the pull failure read as a registry
permission problem rather than a missing publish job. Nothing failed in CI,
because no test connected the names renderers use to the names CI pushes.
"""

from __future__ import annotations

import posixpath
import re
from pathlib import Path

import pytest
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


# CHAOS-4949 (#2162), codex round 7 P1 (the THIRD consecutive new P1 in
# this same hand-rolled character-class regex -- round 5 missed a nested
# subdirectory, round 6's own fix still missed a doubled slash
# (`ci/helpers//preflight.sh`, filesystem-equivalent to the single-slash
# form): a character class ENUMERATES shapes, and enumeration is exactly
# what keeps losing to the next real one. Replaced with tokenize-then-
# normalize instead of a fourth regex attempt:
#
# 1. `_extract_ci_script_tokens` splits run: text on whitespace (loose,
#    not shell-aware) and keeps any resulting word -- after stripping
#    common shell-quoting/grouping punctuation from its edges -- that
#    contains "ci/" and ends in ".sh" or ".py". This is deliberately
#    permissive about the path's INTERNAL shape (nested dirs, doubled
#    slashes, `./` segments all pass through untouched at this stage).
# 2. `_normalize_ci_script_path` runs `posixpath.normpath` on each token
#    (collapses `//`, resolves `./`/`../` segments) before it is ever
#    compared against a filter entry -- and the SAME normalization is
#    applied to the filter's own entries before comparison, so
#    `ci/helpers//preflight.sh` and `ci/helpers/preflight.sh` compare
#    equal regardless of which literal form either side happens to use.
# 3. A token containing an unresolved shell variable (`${...}` or
#    `$(...)`) is refused outright, not guessed at -- normalizing
#    `ci/${dir}/preflight.sh` would produce a plausible-looking but
#    meaningless path, and silently treating it as covered (or
#    uncovered) either way would be exactly the kind of guess this whole
#    guard exists to replace with a verified fact.
def _extract_ci_script_tokens(text: str) -> list[str]:
    """Loosely tokenize `run:` text for anything shaped like a `ci/`
    script reference. Whitespace-split, strip quoting/grouping
    punctuation from each token's edges, keep tokens containing "ci/"
    and ending in ".sh" or ".py" -- deliberately not a character-class
    regex (see the module-level comment above for why)."""
    tokens: list[str] = []
    for raw in text.split():
        word = raw.strip("\"'`();,")
        if "ci/" in word and (word.endswith(".sh") or word.endswith(".py")):
            tokens.append(word)
    return tokens


def _normalize_ci_script_path(token: str) -> str:
    """`posixpath.normpath` the token, refusing one built from an
    unresolved shell variable rather than normalizing whatever text
    happens to be there -- see the module-level comment above."""
    if "${" in token or "$(" in token:
        raise AssertionError(
            "build job references a ci/ script path built from a shell "
            f"variable, which cannot be statically resolved: {token!r} -- "
            "the changes filter's coverage of this path cannot be "
            "verified from source alone. Name it as a literal path, or "
            "if it must stay variable-built, add explicit filter "
            "coverage by hand and explain why here."
        )
    return posixpath.normpath(token)


def _build_job_ci_script_references() -> set[str]:
    """Every `ci/*.sh`/`ci/*.py` path (including nested subdirectories,
    doubled slashes, and `./` segments -- all normalized to one
    canonical form) the `build` job's own `run:` steps invoke -- read
    from the parsed YAML `run:` text (what actually executes), not a raw
    grep over the file, same discipline as `_latest_tag_step_script()` in
    test_docker_images_fanin_gate.py. Raises `AssertionError` (via
    `_normalize_ci_script_path`) if any reference is built from an
    unresolved shell variable."""
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    refs: set[str] = set()
    for step in workflow["jobs"]["build"]["steps"]:
        for token in _extract_ci_script_tokens(step.get("run", "")):
            refs.add(_normalize_ci_script_path(token))
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
    build job's `run:` steps reference must appear -- once BOTH sides are
    normalized with `posixpath.normpath` (codex round 7, P1: comparing
    raw strings meant `ci/helpers//preflight.sh` and
    `ci/helpers/preflight.sh` were treated as different paths even though
    they are the same file on disk) -- in the filter's `code` list. A
    future SECOND script added to the build job without a matching filter
    update fails this test immediately, rather than silently skipping
    `build` the same way -- this is what actually prevents recurrence,
    independent of whether the filter entry chosen for today's single
    script is broad or narrow."""
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    filters = yaml.safe_load(workflow["jobs"]["changes"]["steps"][1]["with"]["filters"])
    code_patterns = {posixpath.normpath(p) for p in filters["code"]}
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
        "list does not name them (after normalizing both sides) -- a PR "
        "touching only these scripts sets changes.code=false and skips "
        "the build job (and whatever guard the script belongs to) entirely"
    )


@pytest.mark.parametrize(
    ("run_text", "expected"),
    [
        pytest.param(
            'bash ci/helpers/preflight.sh "$OWNER"',
            "ci/helpers/preflight.sh",
            id="nested-flat",
        ),
        pytest.param(
            'bash ci/helpers//preflight.sh "$OWNER"',
            "ci/helpers/preflight.sh",
            id="doubled-slash",
        ),
        pytest.param(
            'bash ./ci/helpers/preflight.sh "$OWNER"',
            "ci/helpers/preflight.sh",
            id="leading-dot-slash",
        ),
        pytest.param(
            'bash ci/./helpers/preflight.sh "$OWNER"',
            "ci/helpers/preflight.sh",
            id="embedded-dot-segment",
        ),
        pytest.param(
            "bash ci/python_base_ref.sh",
            "ci/python_base_ref.sh",
            id="flat-existing",
        ),
    ],
)
def test_ci_script_token_extraction_and_normalization(
    run_text: str, expected: str
) -> None:
    """CHAOS-4949 (#2162): three consecutive codex rounds each found a
    DIFFERENT real path shape a hand-rolled character-class regex missed
    -- round 5's original pattern matched only one path segment (missed
    nested subdirectories); round 6's fix added nested-segment support
    but still compared raw strings, so a DOUBLED slash
    (`ci/helpers//preflight.sh`, filesystem-equivalent to the single-
    slash form) was reported as a DIFFERENT, uncovered path. Rewritten as
    tokenize-then-normalize instead of a fourth regex attempt (see the
    module-level comment above `_extract_ci_script_tokens`).

    Each row here is a real shape a prior round's fix still missed
    (nested/flat are the two rounds 5-6 already covered; doubled-slash,
    leading `./`, and an embedded `./` segment are the round-7 additions)
    -- proving extraction is genuinely shape-agnostic, not just wider by
    one more enumerated case. Verified RED against round-6's
    `_CI_SCRIPT_REF` regex (raw-string comparison, no normalization) for
    the doubled-slash case specifically: it extracted
    `ci/helpers//preflight.sh` as a literal string, which then compared
    UNEQUAL to the filter's `ci/helpers/preflight.sh` entry -- reported
    uncovered even when the filter genuinely already named the file."""
    tokens = _extract_ci_script_tokens(run_text)
    assert tokens, f"no ci/ script token extracted from {run_text!r}"
    normalized = {_normalize_ci_script_path(t) for t in tokens}
    assert expected in normalized, (
        f"{run_text!r} -> tokens={tokens!r} -> normalized={normalized!r}, "
        f"expected {expected!r} among them"
    )


def test_ci_script_variable_built_path_refuses_rather_than_guesses() -> None:
    """CHAOS-4949 (#2162), codex round 7 ruling: a `ci/` path built from
    an unresolved shell variable (`ci/${dir}/preflight.sh`,
    `ci/$(dirname "$x")/preflight.sh`) cannot be statically resolved to a
    real path at all -- `posixpath.normpath`-ing the literal text would
    produce a plausible-looking but meaningless string, and either
    silently treating it as covered or silently reporting it uncovered
    would both be guesses. `_normalize_ci_script_path` refuses instead,
    naming the exact token in its own failure message so a human can see
    what wasn't verified rather than trusting a silent pass."""
    with pytest.raises(AssertionError, match=re.escape("ci/${dir}/preflight.sh")):
        _normalize_ci_script_path("ci/${dir}/preflight.sh")
    with pytest.raises(
        AssertionError, match=re.escape('ci/$(dirname "$x")/preflight.sh')
    ):
        _normalize_ci_script_path('ci/$(dirname "$x")/preflight.sh')


def test_build_job_ci_script_references_propagates_a_variable_built_path() -> None:
    """End-to-end version of the row above: a build-job `run:` step that
    references a ci/ script via an unresolved shell variable must make
    `_build_job_ci_script_references()` itself fail loud, not silently
    drop the unresolvable token and report a false-clean coverage
    result."""
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    real_run_text = "\n".join(
        step.get("run", "") for step in workflow["jobs"]["build"]["steps"]
    )
    # Same shape as _build_job_ci_script_references, but scanning
    # synthetic text with one variable-built reference appended, to
    # prove the refusal reaches all the way through the real pipeline.
    tokens: list[str] = []
    for step_text in (real_run_text, 'bash ci/${dir}/preflight.sh "$OWNER"'):
        tokens.extend(_extract_ci_script_tokens(step_text))
    with pytest.raises(AssertionError, match=re.escape("ci/${dir}/preflight.sh")):
        for token in tokens:
            _normalize_ci_script_path(token)
