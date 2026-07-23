import os
import subprocess
from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).parents[1] / ".github/workflows/governance.yml"


def _governance_context_script() -> str:
    workflow: dict[str, object] = yaml.safe_load(
        WORKFLOW_PATH.read_text(encoding="utf-8")
    )
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    governance = jobs["governance"]
    assert isinstance(governance, dict)
    steps = governance["steps"]
    assert isinstance(steps, list)
    context_step = next(
        step
        for step in steps
        if isinstance(step, dict) and step.get("id") == "governance-context"
    )
    script = context_step["run"]
    assert isinstance(script, str)
    return script


def test_governance_workflow_uses_merge_group_shas() -> None:
    # Given a governance workflow triggered for pull requests and merge groups
    # When its event-specific SHA inputs are inspected
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    # Then merge-queue runs can compare the queue group's base and head commits
    assert (
        "BASE_SHA: ${{ github.event.pull_request.base.sha || "
        "github.event.merge_group.base_sha }}" in workflow
    )
    assert (
        "HEAD_SHA: ${{ github.event.pull_request.head.sha || "
        "github.event.merge_group.head_sha }}" in workflow
    )


def test_governance_workflow_resolves_all_merge_group_pr_bodies(
    tmp_path: Path,
) -> None:
    # Given merge-group payloads omit pull_request metadata
    fake_gh = tmp_path / "gh"
    gh_args = tmp_path / "gh-args.txt"
    github_output = tmp_path / "github-output.txt"
    fake_gh.write_text(
        "#!/usr/bin/env bash\n"
        'printf \'%s\\n\' "$@" > "$GH_ARGS_FILE"\n'
        "printf 'first PR evidence\\nsecond PR evidence\\n'\n",
        encoding="utf-8",
    )
    fake_gh.chmod(0o755)

    # When the governance workflow prepares its policy inputs
    result = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", _governance_context_script()],
        check=False,
        capture_output=True,
        text=True,
        env={
            "EVENT_NAME": "merge_group",
            "MERGE_GROUP_HEAD_REF": "refs/heads/gh-readonly-queue/main/pr-1262-deadbeef",
            "PR_BODY": "",
            "GH_TOKEN": "test-token",
            "GITHUB_REPOSITORY": "full-chaos/dev-health-ops",
            "GITHUB_REPOSITORY_OWNER": "full-chaos",
            "REPOSITORY_NAME": "dev-health-ops",
            "GITHUB_OUTPUT": str(github_output),
            "GH_ARGS_FILE": str(gh_args),
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
        },
    )

    # Then it resolves the queued pull request body instead of passing an empty value
    assert result.returncode == 0, result.stdout + result.stderr
    assert "first PR evidence\nsecond PR evidence" in github_output.read_text(
        encoding="utf-8"
    )
    invocation = gh_args.read_text(encoding="utf-8")
    assert "mergeQueue" in invocation
    assert "--paginate" in invocation
    assert "--slurp" in invocation
    assert "entries(first:100,after:$endCursor)" in invocation
    assert "pageInfo{hasNextPage endCursor}" in invocation
    assert "position <= $current.position" in invocation
