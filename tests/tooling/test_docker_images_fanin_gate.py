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
        assert not any("main's current head" in n or "mainhead" in n.lower() for n in names), (
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
    assert (
        '[ -n "${amd64_rev}" ] && [ -n "${arm64_rev}" ]' in combined_run_text
    ), (
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

        assert "org.opencontainers.image.revision=" not in str(with_block.get("labels", "")), (
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
    assert ':0:128' in run_text, (
        "no 128-character length cap found on the sanitized branch tag -- "
        "Docker's tag grammar caps total length at 128 characters and a "
        "long branch name (or one padded by the leading-character guard) "
        "can still exceed it"
    )
    # The cap must come after the prefix, not before -- assert the
    # truncation slice appears after the case-guard's prefixing line in
    # the source text (a real ordering bug, not just presence).
    prefix_pos = run_text.index("safe_branch_tag=\"x${safe_branch_tag}\"")
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
    tail = run_text[latest_call_pos:latest_call_pos + 1200]
    assert '"${latest_rc}" -eq 1' in tail, (
        "no branch on rc==1 (CONFIRMED ABSENT) found near the :latest call "
        "site -- the three-way outcome must be handled explicitly"
    )
    assert '"${latest_rc}" -eq 2' in tail, (
        "no branch on rc==2 (UNKNOWN) found near the :latest call site -- "
        "an unrecognized registry failure must fail closed (decline), "
        "never fall through to the bootstrap/overwrite path"
    )


def _latest_tag_step_script() -> str:
    """The exact `run:` text of the fan-in job's tag-application step, as
    PyYAML parses it (block-scalar indentation already stripped) -- this
    is what actually executes under bash, not the raw indented file text."""
    for name, job in _jobs().items():
        if "fan-in" not in name and "fan_in" not in name:
            continue
        for step in _steps(job):
            run = step.get("run", "")
            if "_inspect_classify" in run:
                return run
    raise AssertionError(
        "could not find the fan-in job's tag-application step (looked for "
        "a step whose run: text contains _inspect_classify)"
    )


# Records every `imagetools create` invocation to $RECORD_FILE and answers
# `imagetools inspect` deterministically per $SCENARIO, so a test can
# observe what the fan-in step actually DOES, not just what its source
# text mentions (ci-flakes review, delta on fa119b2d: a swapped rc==1/
# rc==2 mapping -- the exact P1 this item exists to close -- left every
# purely-source-text assertion in this file green).
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
        *)
          echo '{"manifest":{"digest":"sha256:aaaa"},"image":{"linux/amd64":{"config":{"Labels":{}}},"linux/arm64":{"config":{"Labels":{}}}}}'
          exit 0
          ;;
      esac
      ;;
    *)
      # the just-published sha- source-tag probe: always "found".
      echo '{"manifest":{"digest":"sha256:bbbb"}}'
      exit 0
      ;;
  esac
elif [ "$1" = "buildx" ] && [ "$2" = "imagetools" ] && [ "$3" = "create" ]; then
  echo "CREATE: $*" >> "${RECORD_FILE}"
  exit 0
fi
exit 0
"""

# A `git` shim that records whether it was ever invoked, and returns
# NOTHING for `git log` (empty history) if it is -- reaching the
# digest-walk fallback at all is itself the observable this test checks
# for the rc==0-yet-empty-stdout case, independent of what the (real,
# unrelated) checkout this test runs inside happens to contain for a
# nonsense BUILT_SHA.
_GIT_SHIM = r"""#!/usr/bin/env bash
touch "${GIT_CALLED_FILE}"
exit 0
"""


def _run_latest_tag_step(scenario: str) -> tuple[str, bool]:
    """Run the fan-in job's ACTUAL tag-application script under bash, with
    `docker` shimmed to answer deterministically per `scenario` and
    `git` shimmed to just record whether it was invoked (the digest-walk
    fallback is the only thing in this step that calls git). Returns
    (recorded `imagetools create` invocations, whether git was called).
    `IS_MAIN_REF=true` and a source-tag probe that always "finds" its
    ref, so every family reaches the :latest check under test."""
    script = _latest_tag_step_script()
    with tempfile.TemporaryDirectory() as tmp:
        bin_dir = Path(tmp) / "bin"
        bin_dir.mkdir()
        docker_shim = bin_dir / "docker"
        docker_shim.write_text(_DOCKER_SHIM, encoding="utf-8")
        docker_shim.chmod(docker_shim.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        git_shim = bin_dir / "git"
        git_shim.write_text(_GIT_SHIM, encoding="utf-8")
        git_shim.chmod(git_shim.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

        script_path = Path(tmp) / "fanin.sh"
        script_path.write_text(script, encoding="utf-8")

        record_file = Path(tmp) / "record.txt"
        record_file.write_text("", encoding="utf-8")
        git_called_file = Path(tmp) / "git_called.txt"

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
                "GIT_CALLED_FILE": str(git_called_file),
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
        assert result.returncode == 0, (
            f"fan-in script exited {result.returncode} under scenario "
            f"{scenario!r} -- stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        return record_file.read_text(encoding="utf-8"), git_called_file.exists()


def test_latest_check_behaviourally_bootstraps_only_on_confirmed_absence() -> None:
    """Source-text assertions cannot tell a CORRECT rc==1/rc==2 mapping
    from a SWAPPED one -- demonstrated directly (not argued): swapping
    which branch bootstraps and which declines, the exact P1 this item
    exists to close, left every purely-source-text assertion in this
    file green (verified before writing this test). Run the ACTUAL
    script under bash with a recording `docker` shim standing in for the
    registry, and observe what it DOES: CONFIRMED ABSENT must record an
    `imagetools create ... :latest` call; UNKNOWN must record none; a
    reported-success-but-empty read must also record none (ci-flakes'
    third finding -- rc==0 doesn't guarantee non-empty stdout, and that
    case must fail closed too, not fall through to whatever jq/digest-
    walk happens to do with empty input)."""
    absent_record, absent_git_called = _run_latest_tag_step("absent")
    assert "imagetools create" in absent_record and ":latest" in absent_record, (
        "CONFIRMED ABSENT did not record a bootstrap `imagetools create "
        "...:latest` call -- the classifier's rc==1 branch must "
        f"unconditionally tag latest+main, got: {absent_record!r}"
    )
    assert not absent_git_called, (
        "CONFIRMED ABSENT reached the digest-walk fallback (git was "
        "invoked) instead of short-circuiting straight to bootstrap"
    )

    unknown_record, unknown_git_called = _run_latest_tag_step("unknown")
    assert unknown_record == "", (
        "UNKNOWN registry failure recorded an `imagetools create` call -- "
        f"it must decline and tag nothing, got: {unknown_record!r}"
    )
    assert not unknown_git_called, (
        "UNKNOWN registry failure reached the digest-walk fallback (git "
        "was invoked) instead of declining immediately"
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
    empty_record, empty_git_called = _run_latest_tag_step("empty_success")
    assert empty_record == "", (
        "a reported-success (exit 0) but empty :latest read recorded an "
        "`imagetools create` call -- an empty read must fail closed the "
        f"same as UNKNOWN, got: {empty_record!r}"
    )
    assert not empty_git_called, (
        "a reported-success (exit 0) but empty :latest read reached the "
        "digest-walk fallback instead of declining immediately -- this "
        "must be handled as an explicit UNKNOWN case, not left to whatever "
        "the downstream label/digest logic happens to do with empty input"
    )
