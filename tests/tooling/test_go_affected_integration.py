"""CHAOS-9947: pull requests run only the integration shards a change can reach.

Two layers. The selector (ci/go_affected_integration.py) is run on a synthetic
`go list` document so every rule is asserted, and on the real module so the
claim "a one-package change selects only its reverse closure, a shared change
selects everything" is executed rather than argued. The planner
(`check_go.sh integration-shard-plan`) is then run with each selection file to
show the matrix really shrinks, and that an unreadable selection fails.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "go_affected_integration.py"
CHECK_GO = ROOT / "ci" / "check_go.sh"
WORKFLOW = ROOT / ".github" / "workflows" / "go.yml"

_spec = importlib.util.spec_from_file_location("go_affected_integration", SCRIPT)
assert _spec is not None and _spec.loader is not None
affected = importlib.util.module_from_spec(_spec)
sys.modules["go_affected_integration"] = affected
_spec.loader.exec_module(affected)

MOD = "example.com/m"
ROOT_DIR = "/work/m"


def _pkg(rel: str, **extra: object) -> dict:
    return {
        "ImportPath": f"{MOD}/{rel}",
        "Dir": f"{ROOT_DIR}/{rel}",
        "Module": {"Path": MOD},
        **extra,
    }


def _binary(rel: str, *deps: str) -> dict:
    return {
        "ImportPath": f"{MOD}/{rel}.test",
        "Deps": [f"{MOD}/{d} [{MOD}/{rel}.test]" for d in deps],
    }


# leaf <- mid <- top ; other is unrelated. Integration packages: mid, top, other.
GOLIST = [
    _pkg("internal/leaf"),
    _pkg("internal/mid", EmbedFiles=["sql/001.sql"]),
    _pkg("internal/top"),
    _pkg("internal/other"),
    _binary("internal/mid", "internal/leaf"),
    _binary("internal/top", "internal/mid", "internal/leaf"),
    _binary("internal/other"),
]
KEYS = ["internal/mid", "internal/other", "internal/top"]


def _select(*changed: str) -> tuple[str, list[str]]:
    return affected.select(list(changed), GOLIST, KEYS, ROOT_DIR)


def test_leaf_change_selects_its_reverse_closure_only() -> None:
    assert _select("internal/leaf/leaf.go") == (
        "AFFECTED",
        ["internal/mid", "internal/top"],
    )


def test_top_change_selects_only_itself() -> None:
    assert _select("internal/top/x_test.go") == ("AFFECTED", ["internal/top"])


def test_unrelated_change_selects_the_unrelated_package() -> None:
    assert _select("internal/other/o.go") == ("AFFECTED", ["internal/other"])


@pytest.mark.parametrize(
    "path",
    [
        "ci/check_go.sh",
        ".github/workflows/go.yml",
        "go.mod",
        "go.sum",
        "internal/testsupport/containers/harness.go",
    ],
)
def test_shared_changes_run_the_full_suite(path: str) -> None:
    verdict, keys = _select("internal/leaf/leaf.go", path)
    assert verdict.startswith("FULL shared change") and keys == []


def test_go_file_in_an_unlisted_directory_fails_open() -> None:
    verdict, _ = _select("internal/ghost/g.go")
    assert verdict.startswith("FULL Go file outside any listed package")


def test_data_file_counts_only_as_testdata_or_embed() -> None:
    assert _select("internal/mid/sql/001.sql") == (
        "AFFECTED",
        ["internal/mid", "internal/top"],
    )
    assert _select("internal/leaf/testdata/a.json")[0] == "AFFECTED"
    # A file the package neither embeds nor keeps under testdata may be read
    # at run time by any test: not placeable, so the full suite.
    assert _select("internal/mid/fixtures/a.json")[0].startswith("FULL non-Go")
    assert _select("src/dev_health_ops/x.py")[0].startswith("FULL non-Go")


def test_docs_and_markdown_are_not_ignored() -> None:
    # A Go test may read any file at run time (docs, README), so an
    # unplaceable Markdown file is the full suite, never "nothing".
    assert _select("docs/guide/a.md")[0].startswith("FULL non-Go")
    assert _select("README.md")[0].startswith("FULL non-Go")


def test_embedded_markdown_is_placed_by_its_owner() -> None:
    golist = [
        _pkg("internal/mid", EmbedFiles=["sql/README.md"]),
        _pkg("internal/top"),
        _binary("internal/mid"),
        _binary("internal/top", "internal/mid"),
    ]
    assert affected.select(
        ["internal/mid/sql/README.md"],
        golist,
        ["internal/mid", "internal/top"],
        ROOT_DIR,
    ) == ("AFFECTED", ["internal/mid", "internal/top"])


def test_go_file_under_testdata_is_a_fixture_of_its_package() -> None:
    assert _select("internal/leaf/testdata/plant/plant.go") == (
        "AFFECTED",
        ["internal/mid", "internal/top"],
    )


def test_package_without_a_test_binary_fails_open() -> None:
    verdict, _ = affected.select(
        ["internal/leaf/leaf.go"], GOLIST, [*KEYS, "internal/nobinary"], ROOT_DIR
    )
    assert verdict.startswith("FULL integration package without a test binary")


def test_empty_go_list_is_an_error_not_a_pass() -> None:
    with pytest.raises(SystemExit):
        affected.select(["internal/leaf/leaf.go"], [], KEYS, ROOT_DIR)


def test_cli_reads_nul_separated_input(tmp_path: Path) -> None:
    (tmp_path / "golist.json").write_text(
        "\n".join(json.dumps(e) for e in GOLIST), encoding="utf-8"
    )
    (tmp_path / "keys.txt").write_text("\n".join(KEYS) + "\n", encoding="utf-8")
    (tmp_path / "changed.txt").write_bytes(b"internal/leaf/leaf.go\0")
    proc = subprocess.run(
        [
            sys.executable,
            "-I",
            str(SCRIPT),
            "--changed",
            str(tmp_path / "changed.txt"),
            "--golist",
            str(tmp_path / "golist.json"),
            "--keys",
            str(tmp_path / "keys.txt"),
            "--root",
            ROOT_DIR,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc.stdout.splitlines() == ["AFFECTED 2", "internal/mid", "internal/top"]


needs_go = pytest.mark.skipif(
    shutil.which("go") is None, reason="drives the real go list and planner"
)


def _real_selection(tmp_path: Path, changed: bytes) -> list[str]:
    env = {**os.environ, "GOFLAGS": ""}
    keys = subprocess.run(
        ["bash", str(CHECK_GO), "integration-coverage"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
        env=env,
        timeout=300,
    ).stdout
    (tmp_path / "keys.txt").write_text(
        "\n".join(re.findall(r"^  RUN  (\S+)$", keys, re.MULTILINE)) + "\n",
        encoding="utf-8",
    )
    with (tmp_path / "golist.json").open("w", encoding="utf-8") as handle:
        subprocess.run(
            [
                "go",
                "list",
                "-mod=readonly",
                "-tags=integration",
                "-test",
                "-json",
                "./...",
            ],
            cwd=ROOT,
            stdout=handle,
            check=True,
            env=env,
            timeout=300,
        )
    (tmp_path / "changed.txt").write_bytes(changed)
    proc = subprocess.run(
        [
            sys.executable,
            "-I",
            str(SCRIPT),
            "--changed",
            str(tmp_path / "changed.txt"),
            "--golist",
            str(tmp_path / "golist.json"),
            "--keys",
            str(tmp_path / "keys.txt"),
            "--root",
            str(ROOT),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    (tmp_path / "selection.txt").write_text(proc.stdout, encoding="utf-8")
    return proc.stdout.splitlines()


@needs_go
def test_real_module_one_package_change_is_a_strict_subset(tmp_path: Path) -> None:
    lines = _real_selection(tmp_path, b"internal/admincli/x.go\0")
    total = len((tmp_path / "keys.txt").read_text().split())
    count = int(lines[0].split()[1])
    assert lines[0].startswith("AFFECTED ")
    assert "internal/admincli" in lines[1:]
    assert 1 <= count < total // 2


@needs_go
def test_real_module_shared_change_is_full(tmp_path: Path) -> None:
    assert _real_selection(tmp_path, b"ci/check_go.sh\0")[0].startswith("FULL")


def _plan(selection: Path | None, out: Path) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "GITHUB_OUTPUT": str(out)}
    env.pop("INTEGRATION_AFFECTED_FILE", None)
    if selection is not None:
        env["INTEGRATION_AFFECTED_FILE"] = str(selection)
    return subprocess.run(
        ["bash", str(CHECK_GO), "integration-shard-plan"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


def _outputs(path: Path) -> dict[str, str]:
    return dict(
        line.split("=", 1) for line in path.read_text().splitlines() if "=" in line
    )


@needs_go
def test_planner_matrix_shrinks_with_a_selection_and_stays_full_without(
    tmp_path: Path,
) -> None:
    full = _plan(None, tmp_path / "full.out")
    assert full.returncode == 0, full.stderr[-1500:]
    full_legs = int(_outputs(tmp_path / "full.out")["legs"])

    _real_selection(tmp_path, b"internal/admincli/x.go\0")
    few = _plan(tmp_path / "selection.txt", tmp_path / "few.out")
    assert few.returncode == 0, few.stderr[-1500:]
    few_out = _outputs(tmp_path / "few.out")
    assert 1 <= int(few_out["legs"]) < full_legs
    assert json.loads(few_out["matrix"])["include"]

    (tmp_path / "full_sel.txt").write_text("FULL shared change: ci/x\n")
    same = _plan(tmp_path / "full_sel.txt", tmp_path / "same.out")
    assert int(_outputs(tmp_path / "same.out")["legs"]) == full_legs, same.stderr

    (tmp_path / "none.txt").write_text("AFFECTED 0\n")
    none = _plan(tmp_path / "none.txt", tmp_path / "none.out")
    assert none.returncode == 0, none.stderr[-1500:]
    assert _outputs(tmp_path / "none.out")["legs"] == "0"
    assert json.loads(_outputs(tmp_path / "none.out")["matrix"]) == {"include": []}


@needs_go
@pytest.mark.parametrize(
    "body",
    [
        "AFFECTED 1\ninternal/not-a-package\n",
        "AFFECTED 2\ninternal/admincli\n",
        "AFFECTED 2\ninternal/admincli\ninternal/admincli\n",
        "oops\n",
    ],
)
def test_planner_rejects_a_malformed_selection(tmp_path: Path, body: str) -> None:
    bad = tmp_path / "bad.txt"
    bad.write_text(body)
    proc = _plan(bad, tmp_path / "bad.out")
    assert proc.returncode != 0


def test_workflow_gates_shards_on_legs_and_aggregator_accepts_only_explicit_zero() -> (
    None
):
    jobs = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]
    plan = jobs["go-storage-integration-plan"]
    assert plan["outputs"]["legs"] == "${{ steps.plan.outputs.legs }}"
    step = next(
        s
        for s in plan["steps"]
        if s.get("name", "").startswith("Select the integration")
    )
    assert step["if"] == "github.event_name == 'pull_request'"
    run = jobs["go-storage-integration"]["steps"][0]["run"]
    assert '"${PLAN_LEGS}" = "0"' in run and '"${SHARD_RESULT}" = "skipped"' in run
