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
import shlex
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
#
# CHAOS-4949 (#2162), codex round 8 P1/P2 (bigboy, THREE MORE real gaps
# in the tokenizer itself, found the round after "stop enumerating
# shapes" -- ruling this time: stop trying to PARSE SHELL, not "parse it
# better"):
#
# (a) A backslash-newline shell line continuation split one real
#     reference across two whitespace-split words with neither half
#     matching -- `_join_line_continuations` joins those back into one
#     logical line, mirroring exactly what bash itself does, BEFORE any
#     tokenizing happens.
# (b) A `$(...)`/`${...}` substitution's own INTERNAL whitespace (e.g.
#     `ci/$(dirname "$x")/preflight.sh`) can split what should be one
#     path across several words -- no single word then contains both
#     "ci/" and ".sh", so the OLD per-token `${`/`$(` refusal in
#     `_normalize_ci_script_path` never even got a token to refuse.
#     Genuinely fixing this by finding the true boundaries of a shell
#     word is parsing shell, which is exactly what round 7 already
#     ruled out. Instead: `_ci_reference_windows` finds every `ci/`
#     occurrence on the (joined) logical line and looks ONLY as far as
#     THAT occurrence's own next `.sh`/`.py` -- if `$(`, `${`, or a
#     backtick appears anywhere in that window, the path itself is
#     built from a substitution and the whole extraction refuses,
#     naming the exact window, before any word-splitting happens at
#     all. This is scoped to each reference's own span on purpose, not
#     "does this line contain a substitution anywhere": the real,
#     existing `eval "$(bash ci/python_base_ref.sh ... | awk ...)"` has
#     a `$(` on the same line, but it wraps the whole command for
#     OUTPUT CAPTURE -- the path between `ci/` and `.sh` is a static
#     literal with nothing dynamic in it, and a line-wide check would
#     have wrongly refused this real, already-covered invocation.
# (c) `text.split()` alone cannot tell a real invocation
#     (`bash ci/x.sh`) apart from the SAME text appearing as a quoted
#     diagnostic argument (`echo "... ci/x.sh"`) or inside a `#`
#     comment -- both contain "ci/" and end in ".sh". A candidate word
#     now only counts as an invocation when it is in COMMAND POSITION:
#     the first word on its logical line, immediately after one of
#     `;`, `&&`, `||`, `|`, immediately after an interpreter keyword
#     (`bash`, `sh`, `python`, `python3`, or the two-word `uv run`), or
#     it starts with `./`. Anything else -- an `echo` argument, `#`
#     comment prose, a `-f` file-existence test's operand -- is ignored.
#
# CHAOS-4949 (#2162), codex round 9 (peer read, lane-runner-fallback):
# THREE more real gaps in round 8's raw-text-window design, all in the
# same shape round 8 itself warned about ("any other shell quoting/
# substitution shape is explicitly out of scope") -- but these three are
# common enough to close, not exotic:
#
# (d) `text.split()` never treated `;` as its own boundary, so
#     `echo hi;bash ci/real.sh` (no space around the `;`) left "hi;bash"
#     as one glued word -- "bash" was never its own token, so the real
#     invocation right after it was invisible to the command-position
#     check.
# (e) A quoted diagnostic argument whose LAST word happened to be
#     `ci/foo.sh` (e.g. `echo "please run bash ci/foo.sh now"`) put
#     "bash" and "ci/foo.sh" as ADJACENT raw-split words even though
#     they are both inside the SAME quoted string -- round 8's
#     command-position check has no concept of quoting, so it read this
#     as a real `bash`-invokes-`ci/foo.sh` shape.
# (f) `_ci_reference_windows`' "scan to the next `.sh`/`.py` anywhere
#     later on the line" could overshoot PAST an unrelated, non-script
#     `ci/` mention (`ci/notes`, no `.sh` suffix of its own) all the way
#     to a SEPARATE, later, genuinely static `ci/real.sh` invocation --
#     if anything dynamic (even something unrelated, like a `$(date)`
#     substitution) sat between the two, the window spanned both and
#     refused the real invocation for no reason.
#
# Round 7 already ruled out fixing (d)/(e)/(f) by trying to parse shell.
# The fix here still isn't a shell parser -- it's `shlex`, configured to
# do exactly two things: split on real statement separators (`;`, `&`,
# `&&`, `|`, `||`) as their own tokens, and treat a quoted string as ONE
# opaque token with the quote characters stripped (posix mode). That
# alone closes (d) and (e) structurally: a real `;` is never glued to an
# adjacent word again, and a quoted argument can never be split apart
# into "the interpreter word" and "the path word" the way naive
# whitespace-splitting could.
#
# `shlex` alone does NOT solve `$(...)`'s own internal whitespace (that
# was round 8's actual, still-real motivation for windowing) -- feeding
# `ci/$(dirname "$x")/preflight.sh` straight to `shlex` still splits it
# into multiple tokens at the space inside the substitution, same as a
# bare `text.split()` would. So `$(...)` regions are found and MASKED
# first (`_mask_dollar_paren_regions`, the same balanced-paren tracking
# round 8 used, now applied BEFORE tokenizing, not as a raw-text scan
# after): the region's own content is pulled out and tokenized
# separately (so a REAL invocation living inside a `$(...)` capture,
# like the production `eval "$(bash ci/python_base_ref.sh ... | awk
# ...)"` line, is still found), and the OUTER text gets the whole region
# replaced with a fixed, whitespace-free placeholder (`$()`) -- so
# `ci/$(dirname "$x")/preflight.sh` becomes the outer token
# `ci/$()/preflight.sh`, which (a) never gets split by the internal
# whitespace that used to defeat detection, and (b) still visibly
# contains a `$(` marker, so the per-token dynamic-substitution check
# below still catches it. This is what closes (f) too: because the
# check is now PER SHLEX TOKEN (never "scan raw text to the next `.sh`
# anywhere"), a static `ci/notes` mention and a separate static
# `ci/real.sh` invocation are simply two different tokens, never one
# window spanning both.
#
# Nested `$(...)` (a substitution inside a substitution, or inside
# `$((...))` arithmetic) is masked up to 2 levels deep -- deeper nesting
# is the same "explicitly out of scope" residual round 8 already
# accepted, not a new limit.
_CI_SCRIPT_LINE_CONTINUATION = re.compile(r"\\[ \t]*\n")
_CI_SCRIPT_INTERPRETERS = frozenset({"bash", "sh", "python", "python3"})
_CI_SCRIPT_OPERATORS = frozenset({";", "&", "&&", "|", "||"})
_CI_SCRIPT_DYNAMIC_MARKERS = ("$(", "${", "`")
_CI_SCRIPT_MASK_MAX_DEPTH = 2


def _join_line_continuations(text: str) -> str:
    """Bash joins a backslash immediately followed by a newline into the
    PREVIOUS line -- mirror that here, structurally, before any
    tokenizing (unchanged from round 8)."""
    return _CI_SCRIPT_LINE_CONTINUATION.sub("", text)


def _mask_dollar_paren_regions(line: str, _depth: int = 0) -> tuple[str, list[str]]:
    """Find every top-level `$(...)` on `line` by balanced-paren
    tracking (any `(` increases depth, not just another `$(`, so nested
    arithmetic like `$(( $(date -u +%s) + N ))` doesn't close early on
    the first inner `)`) -- same mechanism round 8 used, applied BEFORE
    tokenizing this time. Returns `(masked_line, region_bodies)`: in
    `masked_line`, each region's CONTENT is replaced with the fixed,
    whitespace-free placeholder `$()` (the `$(`/`)` markers survive, so
    a dynamic-marker check on the outer text still fires; nothing inside
    the parens can reintroduce whitespace into an otherwise-clean outer
    word). `region_bodies` holds each region's own original content, so
    it can be tokenized separately -- a real invocation can live INSIDE
    a `$(...)` capture (the production `eval "$(bash
    ci/python_base_ref.sh ...)"` line), not just outside one. Recurses
    into each region's own body up to `_CI_SCRIPT_MASK_MAX_DEPTH` levels,
    so a nested substitution's own `ci/` references (if any) are found
    too."""
    regions: list[str] = []
    out: list[str] = []
    i, n = 0, len(line)
    while i < n:
        if line[i : i + 2] == "$(":
            depth = 1
            start = i + 2
            j = start
            while j < n and depth > 0:
                if line[j] == "(":
                    depth += 1
                elif line[j] == ")":
                    depth -= 1
                j += 1
            if depth == 0:
                regions.append(line[start : j - 1])
                out.append("$()")
                i = j
                continue
        out.append(line[i])
        i += 1
    masked = "".join(out)
    all_regions = list(regions)
    if _depth < _CI_SCRIPT_MASK_MAX_DEPTH:
        for region in regions:
            _, nested = _mask_dollar_paren_regions(region, _depth + 1)
            all_regions.extend(nested)
    return masked, all_regions


def _shell_tokenize(text: str) -> list[str]:
    """Quote-aware, statement-separator-aware tokenization: a quoted
    string becomes ONE token with its quote characters stripped (posix
    mode), and `;`/`&`/`&&`/`|`/`||` are each their own token instead of
    being glued to an adjacent word. Not a shell parser -- `$(...)`
    handling is the caller's job (`_mask_dollar_paren_regions`), and
    unbalanced/unparseable quoting returns an empty token list rather
    than raising, since a `run:` script can contain lines this guard
    doesn't need to understand (comments, unrelated shell) alongside the
    ones it does."""
    lexer = shlex.shlex(text, posix=True, punctuation_chars=";&|")
    lexer.whitespace_split = True
    try:
        return list(lexer)
    except ValueError:
        return []


def _ci_script_tokens_from_words(words: list[str]) -> list[str]:
    """Given an already-tokenized word list (from `_shell_tokenize`),
    find every `ci/` reference in COMMAND POSITION: the first word, or
    immediately after one of `;`/`&`/`&&`/`|`/`||`, immediately after an
    interpreter keyword (`bash`/`sh`/`python`/`python3`, or the
    two-word `uv run`), or a word starting with `./`. Anything else --
    a quoted `echo` argument, `#` comment prose (never its own token
    once split off from `#`, so its `#`-prefixed first word never
    matches an interpreter/operator), a `-f` file-existence test's
    operand -- is ignored, same design as round 8's command-position
    filter, just fed clean tokens instead of raw-split ones. Raises
    `AssertionError` on any word containing "ci/" AND a dynamic
    substitution marker (`$(`, `${`, or a backtick), regardless of
    command position -- a path built from a shell variable cannot be
    statically resolved no matter where it appears."""
    tokens: list[str] = []
    for i, word in enumerate(words):
        if word in _CI_SCRIPT_OPERATORS:
            continue
        if "ci/" not in word:
            continue
        # Candidate SHAPE first (a static ".sh"/".py" suffix visible in
        # the word), dynamic-marker check second -- checking the marker
        # first would flag ANY word merely containing both "ci/" and an
        # unrelated "${...}"/"$(...)" elsewhere in the same word, e.g. a
        # multi-clause echo string that mentions "ci/python_base_ref.sh"
        # as prose and separately references "${MIRROR_LANDED}" later in
        # the SAME quoted argument -- a real false positive this exact
        # ordering produced against this repo's own real workflow text
        # before being caught. A dynamic path built with no static
        # ".sh"/".py" suffix visible anywhere (the whole filename itself
        # computed at runtime) is a categorically different, effectively
        # unguardable case from source alone -- out of scope, same as
        # round 8's other documented residuals, not something this
        # ordering is meant to catch.
        if not (word.endswith(".sh") or word.endswith(".py")):
            continue
        if any(marker in word for marker in _CI_SCRIPT_DYNAMIC_MARKERS):
            raise AssertionError(
                "build job references a ci/ script path built from a "
                "shell substitution, which cannot be statically "
                f"resolved: {word!r} -- the changes filter's coverage of "
                "this path cannot be verified from source alone. Name "
                "it as a literal path, or if it must stay "
                "variable-built, add explicit filter coverage by hand "
                "and explain why here."
            )
        prev = words[i - 1] if i > 0 else ""
        prev2 = words[i - 2] if i > 1 else ""
        in_command_position = (
            i == 0
            or prev in _CI_SCRIPT_OPERATORS
            or prev in _CI_SCRIPT_INTERPRETERS
            or (prev == "run" and prev2 == "uv")
            or word.startswith("./")
        )
        if in_command_position:
            tokens.append(word)
    return tokens


def _extract_ci_script_tokens(text: str) -> list[str]:
    """Every `ci/*.sh`/`ci/*.py` script INVOCATION in `run:` text -- see
    the module-level comment above for the full design (join line
    continuations, mask `$(...)` regions, shlex-tokenize the masked
    outer text and each region's own content separately, filter to
    command position). Raises `AssertionError` if any resulting word
    contains "ci/" alongside a shell substitution marker -- re-raised
    with the ORIGINAL (unmasked) logical line appended, since the raised
    word itself may show the `$()` placeholder rather than the real
    dynamic text a human needs to see to fix it."""
    text = _join_line_continuations(text)
    tokens: list[str] = []
    for line in text.splitlines():
        if "ci/" not in line:
            continue
        masked, regions = _mask_dollar_paren_regions(line)
        for candidate_text in (masked, *regions):
            if "ci/" not in candidate_text:
                continue
            try:
                tokens.extend(
                    _ci_script_tokens_from_words(_shell_tokenize(candidate_text))
                )
            except AssertionError as exc:
                raise AssertionError(f"{exc} (line: {line!r})") from exc
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
        pytest.param(
            'bash ci/helpers/\\\npreflight.sh "$OWNER"',
            "ci/helpers/preflight.sh",
            id="line-continuation",
        ),
        pytest.param(
            # Verbatim from .github/workflows/docker-images.yml:480-481 --
            # the real, existing, currently-covered invocation. It has a
            # `$(` on the same logical line (wrapping the whole command
            # for OUTPUT CAPTURE), which is exactly why the windowed
            # refusal below is scoped to each `ci/` reference's own span
            # rather than "does this line contain a substitution
            # anywhere" -- a line-wide check would wrongly refuse this.
            'eval "$(bash ci/python_base_ref.sh "${{ github.repository_owner }}" \\\n'
            '                  | awk \'$1 == "ghcr" { print "ref=" $2 } '
            '$1 == "upstream" { print "upstream=" $2 }\')"',
            "ci/python_base_ref.sh",
            id="real-eval-capture-not-refused",
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
    leading `./`, and an embedded `./` segment are the round-7 additions;
    line-continuation and the real eval-capture line are the round-8
    additions) -- proving extraction is genuinely shape-agnostic, not
    just wider by one more enumerated case.

    CORRECTED (codex round 8, P3): an earlier version of this docstring
    claimed round-6's `_CI_SCRIPT_REF` regex extracted
    `ci/helpers//preflight.sh` as a literal string for the doubled-slash
    case, which then compared unequal to the filter's normalized entry
    and was reported uncovered. That claim was never actually run and is
    wrong -- the regex returns NO MATCH AT ALL against
    `ci/helpers//preflight.sh` (verified:
    ``re.compile(r"\\bci/(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+\\.(?:sh|py)\\b").findall(...)``
    → `[]`). The regex's actual round-6-era failure mode for this input
    was silently extracting NOTHING, not a false "uncovered" report --
    the test's coverage of this shape is still real, only its stated
    red-first mechanism was misdescribed."""
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
    the pipeline fail loud, not silently drop the unresolvable reference
    and report a false-clean coverage result.

    UPDATED (codex round 8): the refusal now fires at EXTRACTION time
    (the windowed scan in `_extract_ci_script_tokens` itself, item (b)
    in the module comment), not only later inside
    `_normalize_ci_script_path` -- a `${`/`$(` substitution INSIDE the
    path can defeat word-splitting entirely (round 8 P1), so waiting
    until normalize-time to catch it would be too late: there might be
    no matching token to normalize at all. First asserts the real
    workflow's own run text extracts cleanly with no refusal (the
    positive control this test needs to be meaningful), then that
    appending one variable-built reference makes extraction itself
    raise."""
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    real_run_text = "\n".join(
        step.get("run", "") for step in workflow["jobs"]["build"]["steps"]
    )
    _extract_ci_script_tokens(real_run_text)  # positive control: no refusal today
    with pytest.raises(AssertionError, match=re.escape("ci/${dir}/preflight.sh")):
        _extract_ci_script_tokens(
            real_run_text + '\nbash ci/${dir}/preflight.sh "$OWNER"'
        )


def test_ci_script_dynamic_path_refuses_via_windowed_scan() -> None:
    """CHAOS-4949 (#2162), codex round 8 (bigboy) P1, sub-case (b),
    reproduced: `_extract_ci_script_tokens` splits `run:` text on
    whitespace, so `ci/$(dirname "$x")/preflight.sh` -- a real,
    executable shell form -- lands the substitution's own internal
    whitespace across TWO separate words, and neither one alone
    contains both "ci/" and ".sh". Before the windowed scan, extraction
    silently returned zero tokens for this input -- a real script
    reference invisible to the guard entirely, not even reaching the
    old per-token `${`/`$(` refusal in `_normalize_ci_script_path`
    (nothing to refuse if no token was ever produced). Verified this
    row goes RED (returns `[]` instead of raising) against a version of
    `_extract_ci_script_tokens` with the windowed-refusal loop removed,
    before trusting it."""
    with pytest.raises(AssertionError, match=re.escape('$(dirname "$x")')):
        _extract_ci_script_tokens('bash ci/$(dirname "$x")/preflight.sh')


def test_ci_script_two_references_one_dynamic_still_refuses() -> None:
    """CHAOS-4949 (#2162), codex round 8 ruling: the windowed scan judges
    EACH `ci/` occurrence on a logical line by its own span, not the
    line as a whole -- so a line naming one static, already-covered
    script and one dynamically-built script must still refuse on the
    second, rather than the first occurrence's clean window somehow
    vouching for the whole line."""
    with pytest.raises(AssertionError, match=re.escape('$(dirname "$x")')):
        _extract_ci_script_tokens(
            'bash ci/python_base_ref.sh; bash ci/$(dirname "$x")/preflight.sh'
        )


def test_ci_script_ignores_comment_and_echo_references() -> None:
    """CHAOS-4949 (#2162), codex round 8 (bigboy) P2, reproduced: a
    quoted diagnostic argument to `echo`, or a `#` comment, can contain
    text shaped exactly like a real `ci/` script reference (contains
    "ci/", ends in ".sh"/".py") without the build job ever invoking it.
    Before the command-position filter, `_extract_ci_script_tokens`
    could not tell these apart from a real invocation and reported a
    never-run script as uncovered -- a false positive that would block
    the workflow on a file nothing actually calls. Verified this row
    goes RED (both non-invocation references extracted) against a
    version of `_extract_ci_script_tokens` with the command-position
    check removed (`in_command_position` hardcoded `True`), before
    trusting it."""
    text = (
        "bash ci/python_base_ref.sh\n"
        "# see ci/helpers/notes.sh for background\n"
        'echo "diagnostic ci/helpers/not-invoked.sh"'
    )
    tokens = set(_extract_ci_script_tokens(text))
    assert tokens == {"ci/python_base_ref.sh"}, (
        f"expected only the real invocation to be extracted, got {tokens!r} "
        "-- a comment or echo argument shaped like a ci/ script reference "
        "must be ignored, not reported as an uncovered invocation"
    )


def test_ci_script_semicolon_boundary_is_a_real_command_position() -> None:
    """CHAOS-4949 (#2162), codex round 9 (peer read, lane-runner-fallback)
    P1, reproduced: `echo hi;bash ci/real-invocation.sh` (no space around
    the `;`) left "hi;bash" as one glued word under raw `text.split()`
    -- "bash" was never its own token, so the real invocation right
    after it never satisfied the old command-position check and was
    silently invisible. `shlex` (configured with `;` as a punctuation
    character) splits a real statement separator into its own token
    regardless of surrounding whitespace, closing this structurally.
    Verified this row goes RED (empty token list) against a version of
    `_shell_tokenize` reverted to plain `text.split()`, before trusting
    it."""
    tokens = _extract_ci_script_tokens("echo hi;bash ci/real-invocation.sh")
    assert tokens == ["ci/real-invocation.sh"], (
        f"expected the real invocation right after the ';' to be "
        f"extracted, got {tokens!r}"
    )


def test_ci_script_ignores_a_quoted_argument_ending_in_dot_sh() -> None:
    """CHAOS-4949 (#2162), codex round 9 (peer read) P2, reproduced: a
    quoted `echo` argument whose text happens to contain "bash" directly
    followed by a `ci/...sh` reference (`echo "please run bash
    ci/foo.sh now"`) put "bash" and "ci/foo.sh" as ADJACENT raw-split
    words under the round-8 design, even though both are inside the SAME
    quoted string -- round 8's command-position check has no concept of
    quoting, so this satisfied it as if `bash` were really invoking
    `ci/foo.sh`. `shlex` (posix mode) keeps the whole quoted string as
    ONE token, so "bash" is never adjacent to "ci/foo.sh" as separate
    words when they're both inside the same quotes. Verified this row
    goes RED (the decoy reference wrongly extracted) against a version
    of `_shell_tokenize` reverted to plain `text.split()`, before
    trusting it."""
    tokens = _extract_ci_script_tokens(
        'bash ci/python_base_ref.sh\necho "please run bash ci/foo.sh now"'
    )
    assert tokens == ["ci/python_base_ref.sh"], (
        f"expected only the real invocation to be extracted, got {tokens!r} "
        "-- a ci/...sh-shaped reference inside a quoted echo argument "
        "must be ignored even when it directly follows the word 'bash' "
        "inside the same quotes"
    )


def test_ci_script_windowed_refusal_does_not_overreach_past_an_operator() -> None:
    """CHAOS-4949 (#2162), codex round 9 (peer read) P3, reproduced:
    round 8's raw-text "scan to the next .sh/.py anywhere later on the
    line" could overshoot PAST an unrelated, non-script `ci/` mention
    (`ci/notes`, no `.sh` suffix of its own) all the way to a SEPARATE,
    later, genuinely static `ci/real.sh` invocation -- with an unrelated
    `$(date)` substitution sitting between the two, the old window
    spanned both and refused the real, static invocation for no reason.
    Masking `$(...)` regions and checking per SHLEX TOKEN (never
    "raw-scan to the next suffix anywhere") means `ci/notes` and
    `ci/real.sh` are simply two different tokens, never one window.
    Verified this row goes RED (wrongly raises) against a version of
    `_ci_script_tokens_from_words` that reverts to round 8's
    `_ci_reference_windows` raw-text scan, before trusting it."""
    tokens = _extract_ci_script_tokens(
        'echo "see ci/notes" && x=$(date) && bash ci/real.sh'
    )
    assert tokens == ["ci/real.sh"], (
        f"expected the real, static invocation after the unrelated "
        f"$(date) substitution to be extracted without a false refusal, "
        f"got {tokens!r}"
    )
