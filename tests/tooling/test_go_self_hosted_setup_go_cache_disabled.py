"""Every self-hosted job's `actions/setup-go` step must set `cache: false`.

WHY THIS TEST EXISTS
---------------------
Team-lead ruling (09-03, hotfix on ops main f31ef12c8): the self-hosted
`oci-arc-runners` pool exports `GOMODCACHE`/`GOCACHE` to a shared hostPath --
every pod on the pool sees the SAME cache directories on the underlying
node, not an isolated per-job one the way a GitHub-hosted runner's ephemeral
filesystem would be. `actions/setup-go`'s DEFAULT behavior (`cache` unset,
which the action treats as `true`) downloads and extracts a cache tarball
into `GOMODCACHE`/`GOCACHE` at the start of every run -- two jobs landing on
the pool concurrently would extract into the SAME shared directories at the
same time, a real concurrent-write hazard the action's own cache restore/
save steps were never designed to share safely. `go-arm64-numeric-parity-
self-hosted` (from #2145, runs on every PR) is exactly this shape.

WHAT THIS ASSERTS
-----------------
For every job in `.github/workflows/go.yml` whose `runs-on` names
`self-hosted` (as a list element or within a runs-on string), every step
that uses `actions/setup-go@...` must set `with.cache: false` explicitly --
not merely omit `cache` (which defaults to enabled), and not a truthy
string form (`"false"` is a non-empty Python string and its own bug class
this test also guards against, since it is truthy to a shell/other tooling
even though the action's own docs treat it as boolean input).

Scoped to `go.yml` specifically (mirrors this repo's sibling shape tests --
see `test_go_arm64_numeric_parity_inner_test_timeout.py` -- which use the
same single-workflow-file convention); a repo has other workflow files this
guard does not currently cover.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "go.yml"


def _is_self_hosted(runs_on: object) -> bool:
    """`runs-on` is a single string, a list of labels, or (rarely) a
    `{group: ..., labels: [...]}` mapping -- this repo's own jobs only use
    the first two forms, but check for "self-hosted" by substring/membership
    across all three so a future job in either shape is still covered."""
    if isinstance(runs_on, str):
        return "self-hosted" in runs_on
    if isinstance(runs_on, list):
        return any(
            isinstance(label, str) and "self-hosted" in label for label in runs_on
        )
    if isinstance(runs_on, dict):
        labels = runs_on.get("labels")
        if isinstance(labels, str):
            return "self-hosted" in labels
        if isinstance(labels, list):
            return any(
                isinstance(label, str) and "self-hosted" in label for label in labels
            )
    return False


def _setup_go_steps(job: dict[str, object]) -> list[dict[str, object]]:
    steps = job.get("steps") or []
    assert isinstance(steps, list), "job `steps` is not a list"
    return [
        step
        for step in steps
        if isinstance(step, dict)
        and isinstance(step.get("uses"), str)
        and step["uses"].startswith("actions/setup-go@")
    ]


def _assert_self_hosted_setup_go_steps_disable_cache(
    document: dict[str, object],
) -> None:
    """The guard's actual logic, factored out so a regression test can feed
    it a CONSTRUCTED document (a self-hosted job with cache left on, or set
    to a truthy-string lookalike) without needing a second copy of go.yml."""
    jobs = document.get("jobs") or {}
    assert isinstance(jobs, dict), "workflow document has no `jobs` mapping"
    for job_name, job in jobs.items():
        if not isinstance(job, dict) or not _is_self_hosted(job.get("runs-on")):
            continue
        for step in _setup_go_steps(job):
            with_block = step.get("with") or {}
            assert isinstance(with_block, dict), (
                f"job {job_name!r}'s actions/setup-go step's `with` is not a mapping"
            )
            cache_value = with_block.get("cache")
            assert cache_value is False, (
                f"job {job_name!r} runs on self-hosted ({job.get('runs-on')!r}) "
                f"and its actions/setup-go step has `cache: {cache_value!r}` "
                "(want the literal YAML boolean `false`) -- the self-hosted "
                "oci-arc-runners pool exports GOMODCACHE/GOCACHE to a SHARED "
                "hostPath, so setup-go's default cache restore/extract would "
                "run concurrently against the same directories across every "
                "job the pool schedules at once. Add `cache: false` to this "
                "step's `with:` block."
            )


def test_self_hosted_jobs_disable_setup_go_cache() -> None:
    document = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")) or {}
    _assert_self_hosted_setup_go_steps_disable_cache(document)


def test_guard_rejects_a_self_hosted_job_that_leaves_cache_on_by_default() -> None:
    """Reproduced as a permanent regression: a self-hosted job whose
    actions/setup-go step omits `cache` entirely (the action's own default,
    `true`) must fail -- silence is not opt-out."""
    decoy: dict[str, object] = {
        "jobs": {
            "some-self-hosted-job": {
                "runs-on": ["self-hosted", "oci-arc-runners"],
                "steps": [
                    {"uses": "actions/checkout@deadbeef"},
                    {
                        "name": "Set up Go",
                        "uses": "actions/setup-go@deadbeef",
                        "with": {"go-version-file": "go.mod"},
                    },
                ],
            }
        }
    }
    with pytest.raises(AssertionError, match="want the literal YAML boolean"):
        _assert_self_hosted_setup_go_steps_disable_cache(decoy)


def test_guard_rejects_a_truthy_string_cache_value() -> None:
    """`cache: "false"` (a non-empty Python string after YAML parsing) is
    truthy to plain Python `if` checks and to most shell tooling -- this
    guard must not be fooled by that lookalike; only the literal boolean
    `False` passes."""
    decoy: dict[str, object] = {
        "jobs": {
            "some-self-hosted-job": {
                "runs-on": "self-hosted",
                "steps": [
                    {
                        "name": "Set up Go",
                        "uses": "actions/setup-go@deadbeef",
                        "with": {"cache": "false"},
                    },
                ],
            }
        }
    }
    with pytest.raises(AssertionError, match="want the literal YAML boolean"):
        _assert_self_hosted_setup_go_steps_disable_cache(decoy)


def test_guard_ignores_non_self_hosted_jobs_with_cache_left_on() -> None:
    """A GitHub-hosted runner's filesystem is ephemeral and per-job, so
    setup-go's default cache is fine there -- this guard must not flag a
    job that never touches the shared hostPath."""
    decoy: dict[str, object] = {
        "jobs": {
            "an-ordinary-hosted-job": {
                "runs-on": "ubuntu-latest",
                "steps": [
                    {
                        "name": "Set up Go",
                        "uses": "actions/setup-go@deadbeef",
                        "with": {"go-version-file": "go.mod"},
                    },
                ],
            }
        }
    }
    _assert_self_hosted_setup_go_steps_disable_cache(decoy)  # must not raise


def test_guard_passes_a_self_hosted_job_with_cache_explicitly_disabled() -> None:
    decoy: dict[str, object] = {
        "jobs": {
            "some-self-hosted-job": {
                "runs-on": ["self-hosted", "oci-arc-runners"],
                "steps": [
                    {
                        "name": "Set up Go",
                        "uses": "actions/setup-go@deadbeef",
                        "with": {"go-version-file": "go.mod", "cache": False},
                    },
                ],
            }
        }
    }
    _assert_self_hosted_setup_go_steps_disable_cache(decoy)  # must not raise
