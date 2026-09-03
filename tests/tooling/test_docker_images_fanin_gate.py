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
