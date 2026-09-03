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


def test_build_jobs_set_the_revision_label_the_fan_in_job_reads() -> None:
    """The fan-in job's ancestry check is only as good as the label it
    reads. If `build`/`go-build` stop setting
    `org.opencontainers.image.revision` (or never did), every family looks
    like a permanent bootstrap case -- the fan-in job would tag `latest`
    unconditionally on every run, which is silently worse than the bug
    being fixed: no ordering protection at all, dressed up as one."""
    jobs = _jobs()
    for job_name in ("build", "go-build"):
        job = jobs.get(job_name)
        assert job is not None, f"expected a {job_name!r} job in docker-images.yml"
        build_steps = [s for s in _steps(job) if s.get("id") == "build"]
        assert build_steps, f"{job_name}: no step with id: build found"
        labels = str((build_steps[0].get("with") or {}).get("labels", ""))
        assert "org.opencontainers.image.revision=" in labels, (
            f"{job_name}'s build step doesn't set the "
            "org.opencontainers.image.revision label -- the fan-in job's "
            "ancestry check has nothing to read and would treat every family "
            "as a permanent bootstrap case"
        )
