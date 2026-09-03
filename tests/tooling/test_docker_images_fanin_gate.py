"""CHAOS-4947: the main-head decision for moving tags is made ONCE, not
once per `merge`/`go-merge` matrix leg -- and it is a descendant check, not
an equals-head check.

Measured on run 33717963309: the per-leg `mainhead` step let eight legs
independently ask "is my commit still main's head", up to 34 minutes apart
in one run. A commit landing mid-window let early legs tag `latest` and
late legs decline, splitting `latest` across a combination of families that
never existed as a real commit -- and equals-head alone (even fanned in)
still strands `latest` behind a complete, newer image set whenever a build
is overtaken but still ahead of what's currently tagged.

This file asserts the STRUCTURE that closes both failure modes, not the
runtime behaviour (no registry, no git history, no Docker here) -- parsed
YAML plus a couple of text-level greps, same spirit as the other
tests/tooling files that assert about *shape* because the alternative is
asserting nothing until the next incident.
"""

from __future__ import annotations

import os
import stat
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "docker-images.yml"
WORKFLOW_TEXT = WORKFLOW_PATH.read_text(encoding="utf-8")


def _workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW_TEXT)


def _jobs() -> dict[str, Any]:
    return _workflow()["jobs"]


def _steps(job: dict[str, Any]) -> list[dict[str, Any]]:
    return list(job.get("steps") or [])


def _step_names(job: dict[str, Any]) -> list[str]:
    return [str(step.get("name", "")) for step in _steps(job)]


def test_fan_in_job_exists_and_needs_both_merge_matrices() -> None:
    """The decision has to see every family before it decides for any of
    them -- a fan-in job that only needs one of `merge`/`go-merge` would
    apply moving tags to the Python images before the Go images (or the
    matrices) have even finished publishing their immutable tags,
    reopening exactly the window this ticket closes."""
    jobs = _jobs()
    fan_in_names = [name for name in jobs if "fan-in" in name or "fan_in" in name]
    assert fan_in_names, (
        "no job with 'fan-in' in its name found in docker-images.yml -- "
        "CHAOS-4947's fan-in job appears to have been removed or renamed"
    )
    for name in fan_in_names:
        needs = jobs[name].get("needs")
        needs_list = [needs] if isinstance(needs, str) else list(needs or [])
        assert "merge" in needs_list and "go-merge" in needs_list, (
            f"job {name!r} must need BOTH merge and go-merge -- it has to see "
            f"every family's immutable tag before deciding for any of them, "
            f"got needs={needs_list}"
        )


def test_merge_matrices_no_longer_decide_moving_tags_per_leg() -> None:
    """`merge`/`go-merge` publish only immutable tags now. If either job's
    `Docker meta` step (or any step) still computes `is_main_head` or
    still emits `type=raw,value=latest`/`type=ref,event=branch`, the
    per-leg race this ticket exists to close is still live regardless of
    whether a fan-in job also exists alongside it -- two mechanisms
    deciding the same thing is the two-validators shape, not a fix."""
    jobs = _jobs()
    for job_name in ("merge", "go-merge"):
        job = jobs.get(job_name)
        assert job is not None, f"expected a {job_name!r} job in docker-images.yml"
        names = _step_names(job)
        assert not any(
            "main's current head" in n or "mainhead" in n.lower() for n in names
        ), (
            f"{job_name}: still has a per-leg main-head decision step "
            f"({names}) -- CHAOS-4947 moves this to the fan-in job"
        )
        for step in _steps(job):
            if step.get("id") == "meta":
                tags = str((step.get("with") or {}).get("tags", ""))
                assert "type=raw,value=latest" not in tags, (
                    f"{job_name}'s Docker meta step still emits a `latest` tag "
                    "directly -- that decision belongs in the fan-in job only"
                )
                assert "type=ref,event=branch" not in tags, (
                    f"{job_name}'s Docker meta step still emits a branch-name "
                    "tag directly -- that decision belongs in the fan-in job too "
                    "(it moved together with `latest` before, and still should)"
                )


def test_fan_in_gate_reads_ancestry_not_equality() -> None:
    """The fix has two parts and it is easy to ship only the first: fan-in
    alone (still comparing the built commit to main's LIVE head with `=`)
    fixes the within-one-run race but leaves the stranding case team-lead
    ruled on -- an overtaken-but-still-ahead-of-`latest` build would still
    decline outright. `git merge-base --is-ancestor` is the actual
    invariant; a bare string-equality check on a "current head" value is
    the regression this test exists to catch even if a fan-in job is
    correctly in place."""
    fan_in_jobs = [
        job for name, job in _jobs().items() if "fan-in" in name or "fan_in" in name
    ]
    assert fan_in_jobs, "no fan-in job found (see the other test in this file)"

    combined_run_text = "\n".join(
        str(step.get("run", "")) for job in fan_in_jobs for step in _steps(job)
    )
    assert "merge-base" in combined_run_text and "--is-ancestor" in combined_run_text, (
        "the fan-in job doesn't call `git merge-base --is-ancestor` anywhere -- "
        "CHAOS-4947 requires a descendant check, not equals-head string "
        "comparison, to avoid stranding `latest` behind an overtaken-but-"
        "still-ahead build"
    )

    # The label this reads has to be the OCI revision label, not the
    # `sha-<7>` tag string -- the ticket explicitly rules out the latter
    # ("a 7-char prefix is not guaranteed unique and the string->SHA
    # resolution can fail open").
    assert "org.opencontainers.image.revision" in combined_run_text, (
        "the fan-in job's ancestry base must come from the "
        "org.opencontainers.image.revision OCI label (read via `imagetools "
        "inspect`), not parsed out of a sha-<7> tag string"
    )


def test_fan_in_job_serialises_via_a_never_cancelled_concurrency_group() -> None:
    """The re-read-immediately-before-tagging inside the fan-in job
    NARROWS the cross-run race, it doesn't close it (team-lead review):
    two fan-in runs at different commits can each pass their own ancestor
    check against a `latest` that predates the other's write, then tag in
    the wrong order regardless. A dedicated concurrency group serializes
    every fan-in run against every other one -- and it must never cancel a
    queued run, because cancelling one here means that commit's moving-tag
    decision never happens at all, not that it happens late."""
    jobs = _jobs()
    fan_in_jobs = {
        name: job for name, job in jobs.items() if "fan-in" in name or "fan_in" in name
    }
    assert fan_in_jobs, "no fan-in job found (see the other test in this file)"
    for name, job in fan_in_jobs.items():
        concurrency = job.get("concurrency")
        assert isinstance(concurrency, dict) and concurrency.get("group"), (
            f"{name}: no concurrency group set -- fan-in runs for different "
            "commits can race each other even with the ancestor check in place"
        )
        assert concurrency.get("cancel-in-progress") is False, (
            f"{name}: concurrency.cancel-in-progress must be explicitly false -- "
            "cancelling a queued fan-in run silently drops that commit's "
            "moving-tag decision entirely, the exact failure shape this ticket "
            "exists to close"
        )


def test_platform_label_agreement_does_not_fall_back_to_whichever_exists() -> None:
    """084-prod review: with `base_sha="${amd64_rev:-${arm64_rev}}"`, a
    family with exactly ONE platform labeled (the other empty) falls
    through the "both present and differ" disagree-check untouched and
    silently takes whichever value exists -- which IS "guessing which
    platform is authoritative", the exact thing the disagree-case comment
    says it refuses. Reachable now, not theoretical: every family has a
    labelless `:latest` during this PR's own migration, and a mixed
    amd64-old/arm64-new index from an interrupted publish is not
    hypothetical (084-prod: two partial publishes the same night this was
    reviewed). The fix requires BOTH platform labels present (and equal)
    before trusting either; anything else -- zero present, or exactly
    one -- must fall through to the unlabeled/digest-walk path, not decline
    outright and not guess."""
    fan_in_jobs = [
        job for name, job in _jobs().items() if "fan-in" in name or "fan_in" in name
    ]
    assert fan_in_jobs, "no fan-in job found (see the other test in this file)"
    combined_run_text = "\n".join(
        str(step.get("run", "")) for job in fan_in_jobs for step in _steps(job)
    )
    assert "${amd64_rev:-${arm64_rev}}" not in combined_run_text, (
        "found the exact `${amd64_rev:-${arm64_rev}}` pattern -- this silently "
        "takes whichever platform's revision label exists when only one is "
        "present, which is guessing, not agreement. Both platform labels must "
        "be required present (and equal) before trusting either."
    )
    assert '[ -n "${amd64_rev}" ] && [ -n "${arm64_rev}" ]' in combined_run_text, (
        "no explicit both-platforms-present guard found before the base_sha "
        "assignment -- a family with exactly one platform labeled must not "
        "be treated as agreeing"
    )


def test_fan_in_job_has_a_bounded_fallback_for_unlabeled_families() -> None:
    """dev-hops-runner and dev-hops-api predate this ticket's revision
    label and currently carry no Labels map at all (measured, lane-084-
    prod) -- a labelless `:latest` is not the same as a missing `:latest`
    (bootstrap) and needs its own resolution path, not a permanent
    decline. The fallback must be a BOUNDED walk (an unbounded one is a
    runaway API-call risk against the registry) and must use digest
    equality against successive candidate commits, not a single lookup --
    digest equality alone only answers "is `latest` at commit X" for a
    known X (ci-flakes review), so the fallback has to supply that X by
    walking, not assume it."""
    fan_in_jobs = [
        job for name, job in _jobs().items() if "fan-in" in name or "fan_in" in name
    ]
    assert fan_in_jobs, "no fan-in job found (see the other test in this file)"
    combined_run_text = "\n".join(
        str(step.get("run", "")) for job in fan_in_jobs for step in _steps(job)
    )
    assert "git log --first-parent" in combined_run_text, (
        "the fan-in job's unlabeled-family fallback must walk first-parent "
        "history from the built commit, not the label alone"
    )
    assert "-n 50" in combined_run_text or "-n50" in combined_run_text, (
        "the fan-in job's history walk for unlabeled families must be bounded "
        "-- an unbounded walk against the registry is a runaway API-call risk"
    )


def test_dockerfiles_label_the_revision_the_fan_in_job_reads() -> None:
    """The fan-in job's ancestry check is only as good as the label it
    reads. Both Dockerfiles must set `org.opencontainers.image.revision`
    via a Dockerfile LABEL (not docker/build-push-action's own `labels:`
    input -- team-lead review: that would be a SECOND mechanism setting
    the same key, once go-worker.Dockerfile's own LABEL block is
    accounted for). If either Dockerfile stops labelling revision, every
    family it builds looks like a permanent bootstrap case -- the fan-in
    job would tag `latest` unconditionally on every run, silently worse
    than the bug being fixed: no ordering protection at all, dressed up
    as one. And if a `labels:` input ever gets added back onto either
    build-push-action step, that's the second-mechanism regression this
    test also has to catch."""
    dockerfiles = {
        "build": ROOT / "docker" / "Dockerfile",
        "go-build": ROOT / "docker" / "go-worker.Dockerfile",
    }
    jobs = _jobs()
    for job_name, dockerfile in dockerfiles.items():
        job = jobs.get(job_name)
        assert job is not None, f"expected a {job_name!r} job in docker-images.yml"
        build_steps = [s for s in _steps(job) if s.get("id") == "build"]
        assert build_steps, f"{job_name}: no step with id: build found"
        with_block = build_steps[0].get("with") or {}

        assert "org.opencontainers.image.revision=" not in str(
            with_block.get("labels", "")
        ), (
            f"{job_name}'s build step sets a `labels:` input containing "
            "org.opencontainers.image.revision -- that's a second mechanism "
            "alongside the Dockerfile LABEL, not a replacement for it; the two "
            "can disagree, and only one should exist"
        )

        assert dockerfile.exists(), f"expected {dockerfile} to exist"
        dockerfile_text = dockerfile.read_text(encoding="utf-8")
        assert "org.opencontainers.image.revision=" in dockerfile_text, (
            f"{dockerfile} has no org.opencontainers.image.revision LABEL -- "
            "the fan-in job's ancestry check has nothing to read for this "
            "family and would treat it as a permanent bootstrap case"
        )

        build_args = str(with_block.get("build-args", ""))
        assert "COMMIT=" in build_args, (
            f"{job_name}'s build-args don't pass COMMIT -- the Dockerfile's "
            "LABEL references it, but nothing supplies a value at build time"
        )


def _fan_in_run_text() -> str:
    fan_in_jobs = [
        job for name, job in _jobs().items() if "fan-in" in name or "fan_in" in name
    ]
    assert fan_in_jobs, "no fan-in job found (see the other tests in this file)"
    return "\n".join(
        str(step.get("run", "")) for job in fan_in_jobs for step in _steps(job)
    )


def test_branch_tag_sanitizer_satisfies_the_full_docker_grammar() -> None:
    """ci-flakes review (delta on b172ebf0): Docker's tag grammar is
    `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}` -- the first character is
    restricted AND the total length is capped at 128. `tr -c
    'A-Za-z0-9_.-' '-'` alone only fixes interior characters, leaving a
    real, valid git branch like `+foo` or `@work` sanitized to an invalid
    leading `-` (verified against real bash: `+foo` -> `-foo`). A branch
    name at or beyond 128 characters (or one that grows by one character
    from the leading-character prefix) is a second, independent way to
    violate the same grammar. Both guards must be present, and the length
    cap must apply AFTER any prefix is added -- capping first and then
    prefixing could hand back a 129-character tag."""
    run_text = _fan_in_run_text()
    assert "tr -c 'A-Za-z0-9_.-' '-'" in run_text, (
        "the branch-tag sanitizer's character-class translation is missing "
        "or was changed -- re-verify it against the Docker tag grammar "
        "before trusting a different pattern"
    )
    assert "[A-Za-z0-9_]*" in run_text, (
        "no leading-character guard found after the sanitizer -- a branch "
        "like +foo or @work sanitizes to an invalid leading '-' without it"
    )
    assert ":0:128" in run_text, (
        "no 128-character length cap found on the sanitized branch tag -- "
        "Docker's tag grammar caps total length at 128 characters and a "
        "long branch name (or one padded by the leading-character guard) "
        "can still exceed it"
    )
    # The cap must come after the prefix, not before -- assert the
    # truncation slice appears after the case-guard's prefixing line in
    # the source text (a real ordering bug, not just presence).
    prefix_pos = run_text.index('safe_branch_tag="x${safe_branch_tag}"')
    cap_pos = run_text.index(":0:128")
    assert cap_pos > prefix_pos, (
        "the 128-char truncation appears before the leading-character "
        "prefix is added -- truncating first then prefixing can still "
        "yield a 129-character tag"
    )


def test_digest_walk_candidates_use_single_attempt_no_retry() -> None:
    """ci-flakes review (delta on b172ebf0): retrying is only worth paying
    for where absence is anomalous. The digest-walk's per-candidate check
    (up to 50 candidates) treats absence as the EXPECTED, common outcome
    -- a commit whose CI failed before publishing, or a future CHAOS-4948
    skip -- so retrying every miss 3x with sleeps between would turn a
    fast walk into a slow one for no behavioural benefit. `_inspect_once`
    (single attempt) must be the one used inside the candidate loop;
    `_inspect_retry`/`_inspect_retry_classify` (multi-attempt) are for the
    :latest and source-tag checks, where absence IS anomalous. Conflating
    the two for "code cleanliness" would silently reintroduce the slow
    walk this split exists to avoid."""
    run_text = _fan_in_run_text()
    walk_loop_start = run_text.index("while IFS= read -r candidate_sha")
    walk_loop_end = run_text.index(
        'done < <(git log --first-parent -n 50 --format=%H "${BUILT_SHA}")'
    )
    walk_loop_body = run_text[walk_loop_start:walk_loop_end]
    assert "_inspect_once" in walk_loop_body, (
        "the digest-walk candidate loop no longer calls _inspect_once -- "
        "if it now calls a retrying helper instead, the walk pays for "
        "retries on an outcome (absence) that is expected, not anomalous"
    )
    # ci-flakes review (delta on fa119b2d): shell calls have NO trailing
    # parenthesis (`_inspect_retry "${ref}"`, not `_inspect_retry(...)`)
    # -- checking for the literal substring "_inspect_retry(" only ever
    # matches the *definition* line (`_inspect_retry() {`), which lives
    # outside this loop's body regardless of which helper the loop
    # actually calls. That made the assertion vacuously true and unable
    # to catch a reversion to the retrying helper.
    #
    # A first fix attempt (`\b_inspect_retry\b` word-boundary regex) is
    # ALSO wrong, self-caught by actually running it: a comment a few
    # lines up says "...not `_inspect_retry`:" in prose, and `\b` sits
    # between the backtick and the word just fine, so the regex matched
    # the comment, not a call. Match the CALL SHAPE instead -- a real
    # shell invocation is always `_inspect_retry "<something>"`, name
    # then a space then an opening quote; the definition has no space
    # before its `(`, and this file's prose mentions wrap the name in
    # backticks with no following space+quote. Verified this distinguishes
    # the two by running it against the actual file before trusting it.
    assert '_inspect_retry "' not in walk_loop_body, (
        "the digest-walk candidate loop calls the multi-attempt "
        "_inspect_retry helper -- absence is the expected, common outcome "
        "for most candidates in a 50-commit walk, and retrying every miss "
        "would turn a fast walk into a slow one for no benefit"
    )
    assert '_inspect_retry_classify "' not in walk_loop_body, (
        "the digest-walk candidate loop calls the multi-attempt "
        "_inspect_retry_classify helper -- same problem as _inspect_retry: "
        "absence is expected here, not anomalous, so retrying wastes time "
        "for no behavioural benefit"
    )


def test_latest_check_fails_closed_on_unknown_registry_errors() -> None:
    """team-lead ruling: a registry-read failure on `:latest` must not be
    collapsed into a single "doesn't exist" outcome (that's the P1 this
    item exists to close -- a transient failure on an EXISTING `:latest`
    would otherwise trigger an unconditional overwrite). Live-verified on
    bigboy (docker/buildx v0.37.0, Docker Engine 29.7.2, against ghcr.io,
    2026-09-03): a
    genuinely absent tag exits 1 with stderr exactly `ERROR: <ref>: not
    found`; a present tag exits 0 with empty stderr. Only that verified
    substring may be trusted as CONFIRMED ABSENT -- this test asserts on
    ONLY the verified pattern; earlier drafts also guessed at "manifest
    unknown"/"name unknown", which were never observed on this registry
    and this buildx version and must not be reintroduced as if they were
    verified. Anything that doesn't match must be classified UNKNOWN and
    fail closed (decline, never bootstrap)."""
    run_text = _fan_in_run_text()
    assert "_inspect_classify" in run_text, (
        "no _inspect_classify helper found -- the :latest check must "
        "distinguish CONFIRMED ABSENT from UNKNOWN registry failures "
        "instead of collapsing every failure into one outcome"
    )
    assert '"${ref}: not found"' in run_text, (
        "the classifier doesn't match the live-verified 'not found' stderr "
        "substring -- this is the only pattern actually observed against "
        "ghcr.io with this buildx version, and it's what distinguishes "
        "CONFIRMED ABSENT from UNKNOWN"
    )
    assert "manifest unknown" not in run_text and "name unknown" not in run_text, (
        "found an unverified guessed error-text pattern ('manifest unknown' "
        "or 'name unknown') in the fan-in job -- these were never confirmed "
        "against a real registry response and must not be trusted as if "
        "they were; only the live-verified 'not found' substring may be "
        "used to classify CONFIRMED ABSENT"
    )
    assert "_inspect_retry_classify" in run_text, (
        "no _inspect_retry_classify helper found -- the :latest check must "
        "retry only the UNKNOWN outcome (a genuine ambiguity worth ruling "
        "out as transient) while short-circuiting immediately on CONFIRMED "
        "ABSENT (nothing to retry, the ref does not exist)"
    )
    # The :latest call site must branch on all three outcomes: found (0),
    # confirmed absent (1, bootstrap), unknown (2, fail closed/decline).
    latest_call_pos = run_text.index('_inspect_retry_classify "${image}:latest"')
    tail = run_text[latest_call_pos : latest_call_pos + 1200]
    assert '"${latest_rc}" -eq 1' in tail, (
        "no branch on rc==1 (CONFIRMED ABSENT) found near the :latest call "
        "site -- the three-way outcome must be handled explicitly"
    )
    assert '"${latest_rc}" -eq 2' in tail, (
        "no branch on rc==2 (UNKNOWN) found near the :latest call site -- "
        "an unrecognized registry failure must fail closed (decline), "
        "never fall through to the bootstrap/overwrite path"
    )
    # NOTE: the `::error::` annotation for the rc==0-yet-empty-stdout case
    # is checked BEHAVIOURALLY, not here -- see
    # test_latest_check_behaviourally_bootstraps_only_on_confirmed_absence.
    # ci-flakes review: a source-text check only proves the literal was
    # typed somewhere, not that it's emitted on the path that should emit
    # it (a guard moved to the wrong branch would still pass a
    # source-text check). The behavioural test captures actual stdout on
    # the empty-read path specifically, plus a negative control on the
    # present-read path.


def _latest_tag_step_script() -> str:
    """The exact `run:` text of the fan-in job's tag-application step, as
    PyYAML parses it (block-scalar indentation already stripped) -- this
    is what actually executes under bash, not the raw indented file text.

    ci-flakes review: the original version returned on the FIRST match
    with no exactly-one assertion -- fine today (there is exactly one),
    but if a second fan-in-shaped job ever grew a similar step, one of
    them would silently go untested while this helper kept returning
    the other and the suite reported success. Collect every match and
    assert there is exactly one, same discipline as ci-flakes' own
    `_guard_script()`."""
    matches = [
        run
        for name, job in _jobs().items()
        if "fan-in" in name or "fan_in" in name
        for step in _steps(job)
        for run in [step.get("run", "")]
        if "_inspect_classify" in run
    ]
    assert len(matches) == 1, (
        "expected exactly one fan-in step whose run: text contains "
        f"_inspect_classify, found {len(matches)} -- if this grew to more "
        "than one, every match after the first would silently go untested "
        "by this behavioural harness"
    )
    return matches[0]


# Records every `imagetools create` invocation to $RECORD_FILE and answers
# `imagetools inspect` deterministically per $SCENARIO, so a test can
# observe what the fan-in step actually DOES, not just what its source
# text mentions (ci-flakes review, delta on fa119b2d: a swapped rc==1/
# rc==2 mapping -- the exact P1 this item exists to close -- left every
# purely-source-text assertion in this file green).
_LABELLED_REVISION = "baseshabaseshabaseshabaseshabaseshabase"
_DOCKER_SHIM = r"""#!/usr/bin/env bash
if [ "$1" = "buildx" ] && [ "$2" = "imagetools" ] && [ "$3" = "inspect" ]; then
  ref="$4"
  case "$ref" in
    *:latest)
      case "$SCENARIO" in
        absent)
          echo "ERROR: ${ref}: not found" >&2
          exit 1
          ;;
        unknown)
          echo "ERROR: failed to do request: connection reset by peer" >&2
          exit 1
          ;;
        empty_success)
          # exit 0 but print nothing: the rc==0-yet-empty-stdout case.
          exit 0
          ;;
        unlabelled)
          # exit 0, valid JSON, but no revision label on either platform
          # -- falls through to the digest-walk fallback, same as
          # dev-hops-runner/dev-hops-api during this PR's own migration.
          echo '{"manifest":{"digest":"sha256:aaaa"},"image":{"linux/amd64":{"config":{"Labels":{}}},"linux/arm64":{"config":{"Labels":{}}}}}'
          exit 0
          ;;
        labelled)
          # BOTH platforms carry the SAME revision label -- this is the
          # only scenario that reaches the actual descendant-vs-decline
          # `git merge-base --is-ancestor` check, the thing CHAOS-4947
          # exists to make. Whether it's a descendant is controlled by
          # the `git` shim below via $MERGE_BASE_IS_ANCESTOR, not here.
          echo '{"manifest":{"digest":"sha256:aaaa"},"image":{"linux/amd64":{"config":{"Labels":{"org.opencontainers.image.revision":"'"${LABELLED_REVISION}"'"}}},"linux/arm64":{"config":{"Labels":{"org.opencontainers.image.revision":"'"${LABELLED_REVISION}"'"}}}}}'
          exit 0
          ;;
        *)
          echo "unrecognized SCENARIO: ${SCENARIO}" >&2
          exit 99
          ;;
      esac
      ;;
    *)
      # This branch answers BOTH the just-published sha- source-tag probe
      # AND the digest-walk's per-candidate sha- probes (same ref shape,
      # `<image>:sha-<short>`) -- they're distinguished only by which
      # short-sha is being asked about, not by any other structural cue.
      #
      # codex round 2, P3 (my own least-sure item from round 2's prompt,
      # confirmed both ways): the harness's `git log` shim always returned
      # empty history, so the digest-walk's actual found-a-match path
      # (walk N candidates, find one whose digest equals :latest's,
      # `break`, tag from it) was NEVER exercised -- only "walked, found
      # nothing, declined". A regression in the match/break/found_base
      # logic passed every existing row. If $DIGEST_WALK_MATCH_SHORT is
      # set, the candidate whose short-sha matches it gets the SAME
      # digest as :latest's (sha256:aaaa, the `unlabelled` scenario's
      # fixed digest) -- every OTHER candidate gets the existing default
      # (sha256:bbbb, guaranteed != aaaa), so the walk must actually
      # iterate past real misses to find the real match, not just accept
      # the first candidate it's handed.
      candidate_short="${ref##*:sha-}"
      # 4752-go's peer read of #2167 (r3 order, team-lead): the source-tag
      # probe (this SAME ref shape, `<image>:sha-deadbee` for the fixed
      # $BUILT_SHA every test here uses) had no scenario control at all --
      # it always "found" the ref, so the source_rc==1/rc==2 branches at
      # the call site (SKIP vs `::error::` decline) were never exercised.
      # $SOURCE_TAG_SCENARIO, when set, answers ONLY for that exact
      # short-sha (never a digest-walk candidate's, which uses a
      # different, unrelated sha) -- everything else keeps the prior
      # unconditional-success behaviour.
      # codex round 4, P2: a MIXED run (some families UNKNOWN, others
      # CONFIRMED ABSENT, none actually found) needed its own per-family
      # override to construct -- $SOURCE_TAG_SCENARIO alone answers the
      # SAME way for every family (they share one short_sha). When
      # $SOURCE_TAG_OVERRIDE_FAMILY is set and this ref's image name
      # matches it, $SOURCE_TAG_OVERRIDE_SCENARIO wins for THIS family
      # only; every other family still gets the uniform
      # $SOURCE_TAG_SCENARIO.
      family_scenario="${SOURCE_TAG_SCENARIO:-}"
      if [ -n "${SOURCE_TAG_OVERRIDE_FAMILY:-}" ]; then
        case "${ref}" in
          *"/${SOURCE_TAG_OVERRIDE_FAMILY}:sha-"*)
            family_scenario="${SOURCE_TAG_OVERRIDE_SCENARIO:-}"
            ;;
        esac
      fi
      if [ -n "${family_scenario}" ] && [ "${candidate_short}" = "deadbee" ]; then
        case "${family_scenario}" in
          absent)
            echo "ERROR: ${ref}: not found" >&2
            exit 1
            ;;
          unknown)
            echo "ERROR: failed to do request: connection reset by peer" >&2
            exit 1
            ;;
          *)
            echo "unrecognized source-tag scenario: ${family_scenario}" >&2
            exit 99
            ;;
        esac
      fi
      if [ -n "${DIGEST_WALK_MATCH_SHORT:-}" ] && [ "${candidate_short}" = "${DIGEST_WALK_MATCH_SHORT}" ]; then
        echo '{"manifest":{"digest":"sha256:aaaa"}}'
      else
        echo '{"manifest":{"digest":"sha256:bbbb"}}'
      fi
      exit 0
      ;;
  esac
elif [ "$1" = "buildx" ] && [ "$2" = "imagetools" ] && [ "$3" = "create" ]; then
  echo "CREATE: $*" >> "${RECORD_FILE}"
  exit 0
fi
exit 0
"""

# A `git` shim with TWO SEPARATE marker files, one per subcommand this
# step can call -- not one shared "was git called at all" flag.
#
# ci-flakes review (delta on ace5d86a): a single shared marker only fails
# to conflate `git log` (digest-walk fallback) with `git merge-base
# --is-ancestor` (labelled-ancestry check) BECAUSE the three scenarios
# that assert on it today (absent/unknown/empty_success) all `continue`
# before the script can reach either call -- a property of the CODE
# UNDER TEST's current control flow, not of the marker itself. If a
# guard ever moves later, or a future scenario reaches the label code
# while still checking the old marker, the two meanings silently merge
# and nothing goes red. Two markers make that structurally impossible
# instead of merely coincidentally absent -- "coincidental correctness
# is a defect with good luck" (ci-flakes, this same review thread).
#
# For `git log` (the digest-walk fallback) this shim returns NOTHING
# (empty history) after marking it called, UNLESS $DIGEST_WALK_CANDIDATES
# is set (newline-separated full SHAs, oldest-relevant-first-parent order)
# -- in which case it prints exactly that, letting a test construct a
# specific walk history (codex round 2: needed to exercise the found-a-
# match path, which an always-empty history could never reach). Reaching
# the fallback at all (empty-history case) is itself the observable the
# empty/unknown/absent scenarios check, independent of what the (real,
# unrelated) checkout this test runs inside happens to contain for a
# nonsense BUILT_SHA. For `git merge-base --is-ancestor` (the labelled
# scenario's actual ancestry decision) it answers per
# $MERGE_BASE_IS_ANCESTOR: exit 0 (is an ancestor / descendant) or exit 1
# (is not).
_GIT_SHIM = r"""#!/usr/bin/env bash
if [ "$1" = "log" ]; then
  touch "${GIT_LOG_CALLED_FILE}"
  if [ -n "${DIGEST_WALK_CANDIDATES:-}" ]; then
    printf '%s\n' "${DIGEST_WALK_CANDIDATES}"
  fi
  exit 0
fi
if [ "$1" = "merge-base" ] && [ "$2" = "--is-ancestor" ]; then
  touch "${GIT_MERGE_BASE_CALLED_FILE}"
  if [ "${MERGE_BASE_IS_ANCESTOR}" = "true" ]; then
    exit 0
  else
    exit 1
  fi
fi
exit 0
"""


def _run_latest_tag_step(
    scenario: str,
    merge_base_is_ancestor: bool = True,
    digest_walk_candidates: list[str] | None = None,
    digest_walk_match_short: str = "",
    source_tag_scenario: str = "",
    assert_success: bool = True,
    source_tag_override_family: str = "",
    source_tag_override_scenario: str = "",
) -> tuple[str, bool, bool, str, int]:
    """Run the fan-in job's ACTUAL tag-application script under bash, with
    `docker` shimmed to answer deterministically per `scenario` and
    `git` shimmed to record `log` and `merge-base --is-ancestor`
    invocations SEPARATELY (see `_GIT_SHIM`'s comment for why one shared
    marker isn't good enough), and to answer `merge-base --is-ancestor`
    per `merge_base_is_ancestor` for the `labelled` scenario. Passing
    `digest_walk_candidates` (full SHAs, first-parent order) feeds a
    scripted `git log` history to the digest-walk fallback instead of the
    default empty one; `digest_walk_match_short` names which candidate's
    short-sha should report the SAME digest as :latest's (everything else
    gets a deliberate miss) -- both together let a test exercise the
    found-a-match path, not just "walked, found nothing". Passing
    `source_tag_scenario` (`"absent"` or `"unknown"`) scripts the
    source-tag probe itself to FAIL that way instead of always finding
    its ref -- every family shares one `short_sha`, so this affects all
    nine identically and none of them reach the :latest check at all
    (the loop `continue`s before it). Returns (recorded `imagetools
    create` invocations, whether `git log` was called, whether `git
    merge-base --is-ancestor` was called, captured stdout, the script's
    exit code). `assert_success=False` (default True) skips this
    helper's own zero-exit assertion, for a scenario -- like every family
    hitting UNKNOWN on the source tag -- that is EXPECTED to exit
    non-zero (docker-images.yml's own post-loop `source_unknown_count`
    check); the returncode is still returned either way so the caller
    can assert on it explicitly instead of this helper silently
    accepting whatever it got. `IS_MAIN_REF=true` and, when
    `source_tag_scenario` is left empty (the default), a source-tag
    probe that always "finds" its ref, so every family reaches the
    :latest check under test."""
    script = _latest_tag_step_script()
    with tempfile.TemporaryDirectory() as tmp:
        bin_dir = Path(tmp) / "bin"
        bin_dir.mkdir()
        docker_shim = bin_dir / "docker"
        docker_shim.write_text(_DOCKER_SHIM, encoding="utf-8")
        docker_shim.chmod(
            docker_shim.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH
        )
        git_shim = bin_dir / "git"
        git_shim.write_text(_GIT_SHIM, encoding="utf-8")
        git_shim.chmod(
            git_shim.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH
        )

        script_path = Path(tmp) / "fanin.sh"
        script_path.write_text(script, encoding="utf-8")

        record_file = Path(tmp) / "record.txt"
        record_file.write_text("", encoding="utf-8")
        git_log_called_file = Path(tmp) / "git_log_called.txt"
        git_merge_base_called_file = Path(tmp) / "git_merge_base_called.txt"

        env = dict(os.environ)
        env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
        env.update(
            {
                "BUILT_SHA": "deadbeef1234567890deadbeef1234567890dead0",
                "OWNER": "full-chaos",
                "IS_RELEASE": "false",
                "IS_MAIN_REF": "true",
                "REF_IS_BRANCH": "false",
                "BRANCH_TAG": "",
                "SCENARIO": scenario,
                "RECORD_FILE": str(record_file),
                "GIT_LOG_CALLED_FILE": str(git_log_called_file),
                "GIT_MERGE_BASE_CALLED_FILE": str(git_merge_base_called_file),
                "LABELLED_REVISION": _LABELLED_REVISION,
                "MERGE_BASE_IS_ANCESTOR": "true" if merge_base_is_ancestor else "false",
                "DIGEST_WALK_CANDIDATES": "\n".join(digest_walk_candidates or []),
                "DIGEST_WALK_MATCH_SHORT": digest_walk_match_short,
                "SOURCE_TAG_SCENARIO": source_tag_scenario,
                "SOURCE_TAG_OVERRIDE_FAMILY": source_tag_override_family,
                "SOURCE_TAG_OVERRIDE_SCENARIO": source_tag_override_scenario,
            }
        )
        result = subprocess.run(
            ["bash", str(script_path)],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        # codex round 3, P2: an ALL-families UNKNOWN source-tag scenario
        # is now a deliberate non-zero exit (docker-images.yml's
        # post-loop source_unknown_count/family_total check) -- the old
        # unconditional `assert returncode == 0` would itself fail any
        # test exercising that path. `assert_success=False` opts out;
        # the returncode is returned (5th element) so the caller can
        # assert on it explicitly instead of this helper silently
        # accepting any exit code.
        if assert_success:
            assert result.returncode == 0, (
                f"fan-in script exited {result.returncode} under scenario "
                f"{scenario!r} -- stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        return (
            record_file.read_text(encoding="utf-8"),
            git_log_called_file.exists(),
            git_merge_base_called_file.exists(),
            result.stdout,
            result.returncode,
        )


def test_latest_check_behaviourally_bootstraps_only_on_confirmed_absence() -> None:
    """Source-text assertions cannot tell a CORRECT rc==1/rc==2 mapping
    from a SWAPPED one -- demonstrated directly (not argued): swapping
    which branch bootstraps and which declines, the exact P1 this item
    exists to close, left every purely-source-text assertion in this
    file green (verified before writing this test). Run the ACTUAL
    script under bash with a recording `docker` shim standing in for the
    registry, and observe what it DOES: CONFIRMED ABSENT must record an
    `imagetools create ... :latest` call; UNKNOWN must record none; a
    reported-success-but-empty read must also record none and must emit
    a `::error::` annotation (ci-flakes' third finding, and their
    follow-up: a source-text check for `::error::` only proves the
    literal was typed somewhere, not that it's emitted on the path that
    should emit it -- a guard moved to the wrong branch, or made
    unreachable, would still pass a source-text check. Asserting it in
    THIS behavioural row, alongside the git-shim discrimination, means
    the assertion can't pass by shadowing a neighbouring branch. Also
    asserts the negative control: a genuinely present (found) read
    emits no `::error::`)."""
    absent_record, absent_log_called, absent_mb_called, _absent_stdout, _absent_rc = (
        _run_latest_tag_step("absent")
    )
    assert "imagetools create" in absent_record and ":latest" in absent_record, (
        "CONFIRMED ABSENT did not record a bootstrap `imagetools create "
        "...:latest` call -- the classifier's rc==1 branch must "
        f"unconditionally tag latest+main, got: {absent_record!r}"
    )
    assert not absent_log_called and not absent_mb_called, (
        "CONFIRMED ABSENT reached the digest-walk fallback or the "
        "merge-base ancestry check (git was invoked) instead of "
        "short-circuiting straight to bootstrap"
    )

    (
        unknown_record,
        unknown_log_called,
        unknown_mb_called,
        _unknown_stdout,
        _unknown_rc,
    ) = _run_latest_tag_step("unknown")
    assert unknown_record == "", (
        "UNKNOWN registry failure recorded an `imagetools create` call -- "
        f"it must decline and tag nothing, got: {unknown_record!r}"
    )
    assert not unknown_log_called and not unknown_mb_called, (
        "UNKNOWN registry failure reached the digest-walk fallback or the "
        "merge-base ancestry check (git was invoked) instead of declining "
        "immediately"
    )

    # ci-flakes review (delta on fa119b2d), P3: rc==0 only means the
    # inspect call itself succeeded, it doesn't guarantee non-empty
    # stdout. Before the explicit `[ -z "${latest_json}" ]` guard this
    # scenario fell through into the label-reading code and then the
    # digest-walk fallback -- which happens to decline anyway for an
    # empty label/digest, so checking only the recorded creates (as
    # above) would pass whether or not the guard exists and wouldn't be
    # a real regression test. Asserting git was never invoked is the
    # part that actually distinguishes "declined immediately" from
    # "wandered into the fallback and got lucky" -- verified by removing
    # the guard and confirming this specific assertion goes red while
    # the create-count assertion alone stays green.
    empty_record, empty_log_called, empty_mb_called, empty_stdout, _empty_rc = (
        _run_latest_tag_step("empty_success")
    )
    assert empty_record == "", (
        "a reported-success (exit 0) but empty :latest read recorded an "
        "`imagetools create` call -- an empty read must fail closed the "
        f"same as UNKNOWN, got: {empty_record!r}"
    )
    assert not empty_log_called and not empty_mb_called, (
        "a reported-success (exit 0) but empty :latest read reached the "
        "digest-walk fallback or the merge-base ancestry check instead of "
        "declining immediately -- this must be handled as an explicit "
        "UNKNOWN case, not left to whatever the downstream label/digest "
        "logic happens to do with empty input"
    )
    # team-lead ruling (following ci-flakes' review of the first version
    # of this test): a source-text check for `::error::` only proves the
    # literal appears somewhere in the script, not that it's emitted on
    # THIS path -- moving the annotation to the wrong branch, or making
    # it unreachable, would still pass a source-text check but must fail
    # here. Verified: moving the `::error::` echo into the neighbouring
    # rc==2 branch (so this path never reaches it) turns this specific
    # assertion red while the create-count/git-called assertions above
    # stay green, proving it observes something they don't.
    assert "::error::" in empty_stdout, (
        "a reported-success (exit 0) but empty :latest read did not emit "
        "a `::error::` GitHub Actions annotation on its own path -- this "
        f"surprising outcome must be visible as an annotation, got stdout: "
        f"{empty_stdout!r}"
    )

    # Negative control: a genuinely present but UNLABELLED read (the
    # digest-walk fallback path -- ci-flakes review: this is what "present"
    # actually exercised before this rename, not the labelled/common case
    # the old name implied) must emit no `::error::` at all -- if it did,
    # the annotation would be noise on a routine fallback instead of a
    # signal on the genuinely surprising empty-read case.
    _found_record, found_log_called, found_mb_called, found_stdout, _found_rc = (
        _run_latest_tag_step("unlabelled")
    )
    assert found_log_called and not found_mb_called, (
        "the unlabelled scenario should reach the digest-walk fallback "
        "(git log) but never the merge-base ancestry check -- got "
        f"log_called={found_log_called}, merge_base_called={found_mb_called}"
    )
    assert "::error::" not in found_stdout, (
        "a genuinely present (unlabelled) :latest read emitted a "
        f"`::error::` annotation -- it should only fire on the empty-read "
        f"case, got stdout: {found_stdout!r}"
    )


def test_latest_check_behaviourally_tags_only_true_descendants() -> None:
    """ci-flakes review: none of the other scenarios in this file reach a
    genuinely LABELLED `:latest` -- absent/unknown/empty-success all
    short-circuit before the label logic, and the unlabelled scenario
    falls straight through to the digest-walk fallback. That means the
    actual descendant-vs-decline decision this whole ticket exists to
    make (`git merge-base --is-ancestor`) was never behaviourally
    exercised: a mutation flipping that check (e.g. inverting the exit
    code test, or swapping which branch creates vs declines) would have
    survived every row in this file.

    Uses the `labelled` scenario (both platforms carry the SAME revision
    label, so `base_sha` resolves and the merge-base check is reached)
    with the `git` shim answering `merge-base --is-ancestor` both ways
    via `merge_base_is_ancestor`: True -> the built commit IS a
    descendant of currently-latest -> must create; False -> it is NOT ->
    must decline. Verified this test goes red against a mutation that
    inverts the `git merge-base --is-ancestor` exit-code check (flipped
    the if/else bodies) before trusting it."""
    descendant_record, descendant_log_called, descendant_mb_called, _stdout, _rc = (
        _run_latest_tag_step("labelled", merge_base_is_ancestor=True)
    )
    assert descendant_mb_called and not descendant_log_called, (
        "the labelled/descendant scenario should reach the merge-base "
        "ancestry check but never the digest-walk fallback -- got "
        f"log_called={descendant_log_called}, "
        f"merge_base_called={descendant_mb_called}"
    )
    assert (
        "imagetools create" in descendant_record and ":latest" in descendant_record
    ), (
        "a labelled :latest whose recorded revision IS an ancestor of the "
        "built commit did not record an `imagetools create ...:latest` "
        f"call -- a true descendant must be tagged, got: {descendant_record!r}"
    )

    non_descendant_record, non_desc_log_called, non_desc_mb_called, _stdout2, _rc2 = (
        _run_latest_tag_step("labelled", merge_base_is_ancestor=False)
    )
    assert non_desc_mb_called and not non_desc_log_called, (
        "the labelled/non-descendant scenario should reach the merge-base "
        "ancestry check but never the digest-walk fallback -- got "
        f"log_called={non_desc_log_called}, merge_base_called={non_desc_mb_called}"
    )
    assert non_descendant_record == "", (
        "a labelled :latest whose recorded revision is NOT an ancestor of "
        "the built commit still recorded an `imagetools create` call -- "
        "a non-descendant build must decline, never overwrite a newer "
        f"currently-latest, got: {non_descendant_record!r}"
    )


def test_digest_walk_finds_a_match_partway_through_history() -> None:
    """codex round 2, P3 (my own least-sure item from round 2's prompt,
    confirmed both ways): every scenario above that reaches the digest
    walk (`unlabelled`) fed it an EMPTY `git log` history -- that proves
    "walked, found nothing, declined", never "walked, found the actual
    match, tagged from it". A regression in the match/`break`/
    `found_base` logic at the workflow's `[ -n "${candidate_digest}" ] &&
    [ "${candidate_digest}" = "${latest_digest}" ]` check passed all
    prior rows in this file.

    Feeds a 3-candidate scripted history with the match at the MIDDLE
    position (not the first candidate checked) -- proving the walk
    actually iterates past a real miss rather than accepting whatever
    it's handed first. Verified this row goes red against the mutation
    codex used (`if [ -n ... ] && [ ... = ... ]` replaced with `if
    false`) before trusting it."""
    candidates = [
        "1111111111111111111111111111111111111a",  # miss (checked first)
        "2222222222222222222222222222222222222b",  # THE MATCH (checked second)
        "3333333333333333333333333333333333333c",  # never reached (walk breaks at #2)
    ]
    record, log_called, mb_called, stdout, _rc = _run_latest_tag_step(
        "unlabelled",
        digest_walk_candidates=candidates,
        digest_walk_match_short="2222222",
    )
    assert log_called and not mb_called, (
        "expected the digest-walk (git log) to be reached, not the "
        f"merge-base ancestry check -- log_called={log_called}, "
        f"merge_base_called={mb_called}"
    )
    assert "imagetools create" in record and ":latest" in record, (
        "a digest walk with a real match partway through its scripted "
        "history did not record an `imagetools create ...:latest` call "
        f"-- got record: {record!r}, stdout: {stdout!r}"
    )
    assert "2222222222222222222222222222222222222b" in stdout, (
        "the FALLBACK log line should name the matched candidate sha -- "
        f"got stdout: {stdout!r}"
    )


def test_source_tag_check_distinguishes_confirmed_absent_from_unknown() -> None:
    """4752-go's peer read of #2167, round 3 order (team-lead): runner-
    fallback's peer read of bf11cdc5 found the SOURCE-TAG classify call
    site (source_rc==1 -> silent SKIP, source_rc==2 -> `::error::`
    decline -- see the ranked-dimensions comment above the source-tag
    probe in docker-images.yml) had NO behavioural coverage at all.
    Collapsing BOTH outcomes into the same silent-SKIP branch (deleting
    the `::error::` decline entirely -- the exact P1 the :latest site's
    own rc==1/rc==2 split already exists to close, just moved one probe
    earlier) left every existing row in this file green, because none of
    them ever script the source-tag probe's own outcome -- the docker
    shim's non-:latest branch always "found" it unconditionally.

    Runs the real script with $SOURCE_TAG_SCENARIO scripting the
    source-tag probe itself (not :latest, which these two cases never
    even reach). Both must decline before the :latest check -- no
    `imagetools create` for any of the nine families, which all share one
    `short_sha` and so all hit the same scripted outcome -- and neither
    reaches the digest-walk or merge-base checks (git untouched). The
    OBSERVABLE that actually distinguishes them is stdout: rc==1 is a
    silent SKIP (no `::error::`), rc==2 MUST emit `::error::` -- proven by
    running both, asserting on each's own text, and asserting the two
    outputs differ. Verified this test goes red against the mutation
    lane-runner-fallback described (rc==2's branch replaced with the same
    silent-SKIP text as rc==1) before trusting it."""
    absent_record, absent_log, absent_mb, absent_stdout, _absent_rc = (
        _run_latest_tag_step(
            # The :latest scenario value is irrelevant here -- source_rc==1
            # `continue`s the loop long before the :latest probe is ever
            # reached for any family.
            "labelled",
            source_tag_scenario="absent",
        )
    )
    assert absent_record == "", (
        "CONFIRMED ABSENT on the source tag still recorded an "
        f"`imagetools create` call -- got: {absent_record!r}"
    )
    assert not absent_log and not absent_mb, (
        "CONFIRMED ABSENT on the source tag reached the digest-walk "
        "fallback or the merge-base ancestry check (git was invoked) "
        "instead of skipping immediately"
    )
    assert "SKIP" in absent_stdout and "confirmed absent" in absent_stdout, (
        "expected a SKIP message naming confirmed absence, got stdout: "
        f"{absent_stdout!r}"
    )
    assert "::error::" not in absent_stdout, (
        "CONFIRMED ABSENT on the source tag must be a SILENT skip -- no "
        f"`::error::` annotation, got stdout: {absent_stdout!r}"
    )

    # codex round 3, P2: every family shares one short_sha, so this
    # scenario hits ALL NINE with source_rc==2 -- which is now the exact
    # trigger for docker-images.yml's post-loop source_unknown_count ==
    # family_total check, a deliberate non-zero exit (see
    # test_source_tag_all_unknown_fails_the_job_instead_of_a_silent_noop
    # below for the dedicated row on that mechanism). assert_success=False
    # here because this row's own point is the PER-FAMILY message shape
    # (SKIP vs `::error::`), not the job-level exit code.
    unknown_record, unknown_log, unknown_mb, unknown_stdout, unknown_rc = (
        _run_latest_tag_step(
            "labelled",
            source_tag_scenario="unknown",
            assert_success=False,
        )
    )
    assert unknown_rc != 0, (
        "all nine families hitting source_rc==2 should now fail the step "
        f"(post-loop source_unknown_count==family_total check), got "
        f"returncode={unknown_rc}"
    )
    assert unknown_record == "", (
        "UNKNOWN registry failure on the source tag still recorded an "
        f"`imagetools create` call -- got: {unknown_record!r}"
    )
    assert not unknown_log and not unknown_mb, (
        "UNKNOWN registry failure on the source tag reached the "
        "digest-walk fallback or the merge-base ancestry check (git was "
        "invoked) instead of declining immediately"
    )
    assert (
        "::error::" in unknown_stdout and "could not confirm whether" in unknown_stdout
    ), (
        "UNKNOWN registry failure on the source tag must emit a "
        f"`::error::` annotation, got stdout: {unknown_stdout!r}"
    )

    # The pair, not either row alone, is what closes the P1 lane-runner-
    # fallback found: a mutation that routes rc==2 through rc==1's own
    # silent-SKIP text passes both rows in isolation (each still declines,
    # still touches no git, still records no create) but produces
    # IDENTICAL output shape on both -- this is the assertion that mutant
    # cannot survive.
    assert "::error::" not in absent_stdout and "::error::" in unknown_stdout, (
        "the confirmed-absent and unknown source-tag outcomes must be "
        "distinguishable on the wire (silent skip vs a visible `::error::` "
        f"decline) -- got absent={absent_stdout!r}, unknown={unknown_stdout!r}"
    )


def test_source_tag_all_unknown_fails_the_job_instead_of_a_silent_noop() -> None:
    """codex round 3, P2 (reproduced live): every family shares ONE
    `short_sha`, so an UNKNOWN source-tag registry error is never a
    per-family gap -- it hits all nine simultaneously. Before this row's
    fix, that produced nine `::error::` annotations, an EMPTY create
    record (no tags moved for anyone), and a ZERO-EXIT step: a full
    publish no-op that reads as a green job unless someone actually reads
    the log for the annotations. docker-images.yml now fails the step
    when `source_present_count == 0` and `source_unknown_count > 0`
    (codex round 4, P2: widened from the original "all UNKNOWN" equality
    test, which missed a MIXED all-UNKNOWN/all-ABSENT run with zero
    actual finds -- see test_source_tag_mixed_unknown_and_absent_still_
    fails_the_job below for that scenario specifically).

    Negative control: `test_source_tag_check_distinguishes_confirmed_absent_from_unknown`'s
    own "absent" call (all nine hit source_rc==1, not 2) uses the
    DEFAULT `assert_success=True` and passes -- proving this check is
    keyed on "nothing found," not "every family declined for any reason"
    (an all-CONFIRMED-ABSENT run, e.g. a fresh org with nothing published
    yet, is a legitimate steady state and must not fail the job)."""
    record, log_called, mb_called, stdout, returncode = _run_latest_tag_step(
        "labelled",
        source_tag_scenario="unknown",
        assert_success=False,
    )
    assert returncode == 1, (
        f"expected the all-UNKNOWN source-tag scenario to exit 1, got "
        f"returncode={returncode}, stdout:\n{stdout}"
    )
    assert record == "", (
        f"the failing run still recorded an `imagetools create` call: {record!r}"
    )
    assert not log_called and not mb_called, (
        "the failing run reached the digest-walk fallback or the "
        "merge-base ancestry check instead of failing immediately after "
        "the per-family loop"
    )
    assert "0 of 9 families had a confirmed-present source tag" in stdout, (
        "expected the job-level failure annotation naming the present/"
        f"total counts, got stdout:\n{stdout}"
    )


def test_source_tag_mixed_unknown_and_absent_still_fails_the_job() -> None:
    """codex round 4, P2 (reproduced): the round-3 fix only fired when
    EVERY family's source-tag read was UNKNOWN (source_unknown_count ==
    family_total). A MIXED run -- 8 families UNKNOWN, 1 CONFIRMED ABSENT,
    zero families actually FOUND -- reached this exact silent-no-op shape
    too, because the round-3 equality test never matches when even one
    family declines via "absent" instead of "unknown": 8 != 9. Reviewer's
    own repro (matching the scenario here almost exactly, one family --
    dev-health-go-migrate -- forced to "not found" via a docker-shim
    monkeypatch) recorded rc=0, zero creates, no job-level annotation.

    Uses $SOURCE_TAG_OVERRIDE_FAMILY/$SOURCE_TAG_OVERRIDE_SCENARIO
    (source_tag_override_family/scenario) to give ONE family ("absent")
    a different outcome than the other eight ("unknown") -- constructing
    the exact mixed shape without a docker-shim monkeypatch. Verified
    this row goes RED against the round-3-only check (`source_unknown_
    count == family_total`, reverted from the round-4 `source_present_
    count == 0 and source_unknown_count > 0` form) before trusting it:
    8 != 9, so the reverted check never fires and the run exits 0."""
    record, log_called, mb_called, stdout, returncode = _run_latest_tag_step(
        "labelled",
        source_tag_scenario="unknown",
        source_tag_override_family="dev-health-go-migrate",
        source_tag_override_scenario="absent",
        assert_success=False,
    )
    assert returncode == 1, (
        f"expected the mixed UNKNOWN/ABSENT scenario (0 found) to exit 1, "
        f"got returncode={returncode}, stdout:\n{stdout}"
    )
    assert record == "", (
        f"the failing mixed run still recorded an `imagetools create` call: {record!r}"
    )
    assert not log_called and not mb_called, (
        "the failing mixed run reached the digest-walk fallback or the "
        "merge-base ancestry check instead of failing immediately after "
        "the per-family loop"
    )
    assert "0 of 9 families had a confirmed-present source tag" in stdout, (
        "expected the job-level failure annotation naming the present/"
        f"total counts, got stdout:\n{stdout}"
    )
    assert "SKIP dev-health-go-migrate" in stdout, (
        "expected the one overridden-absent family to still get its own "
        f"silent SKIP line, got stdout:\n{stdout}"
    )
