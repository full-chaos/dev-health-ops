"""CHAOS-8693: stack.sh refuses what it must refuse and hands compose exactly the stack.files chain.

`docker` is a stub on PATH that records its argv; nothing touches a real stack.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BIGBOY = ROOT / "ci" / "bigboy"


def _tree(tmp_path: Path, *, skip: str = "") -> tuple[Path, Path, dict[str, str]]:
    r = tmp_path / "root"
    (r / "compose").mkdir(parents=True)
    (r / ".remember/lanes/team-lead").mkdir(parents=True)
    (r / "ops").mkdir()
    (r / "_records").mkdir()
    (r / "ops/.env").write_text("")
    for rel in (
        "compose.yml",
        "compose/compose.go.workers.yml",
        ".remember/lanes/team-lead/reconciler-sweep-override.yml",
        "compose/compose.bigboy.images.yml",
    ):
        if rel != skip:
            (r / rel).write_text("services: {}\n")
    tools = tmp_path / "tools"
    shutil.copytree(BIGBOY, tools)
    (tools / "stack.files").write_text((BIGBOY / "stack.files").read_text())
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "docker.log"
    stub = bin_dir / "docker"
    stub.write_text(
        '#!/usr/bin/env bash\necho "$*" >> "$DOCKER_LOG"\nenv | grep "^COMPOSE_" | sed "s/^/ENV /" >> "$DOCKER_LOG" || true\n'
        'if [ -n "${STUB_CONFIG_FAIL:-}" ]; then case "$*" in *" config "*) echo "resolved=SENTINEL" >&2; exit 1;; esac; fi\n'
        'case "$*" in *"config --images"*) echo ghcr.io/full-chaos/dev-health-web@sha256:'
        + "a" * 64
        + ";; esac\n"
    )
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "DOCKER_LOG": str(log),
        "BIGBOY_ROOT": str(r),
        "BIGBOY_TOOLS_DIR": str(tools),
        "BIGBOY_OPERATOR_IMAGE": "ghcr.io/full-chaos/dev-health-go-operator@sha256:"
        + "b" * 64,
    }
    return r, tools, env


def _run(
    tools: Path, env: dict[str, str], *args: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(tools / "stack.sh"), *args], env=env, capture_output=True, text=True
    )


def test_unknown_verb_is_refused(tmp_path: Path) -> None:
    _, tools, env = _tree(tmp_path)
    res = _run(tools, env, "rm")
    assert res.returncode == 2 and "unknown verb" in res.stderr
    assert not Path(env["DOCKER_LOG"]).exists()


def test_missing_overlay_file_is_refused(tmp_path: Path) -> None:
    _, tools, env = _tree(tmp_path, skip="compose/compose.bigboy.images.yml")
    res = _run(tools, env, "ps")
    assert res.returncode == 2 and "overlay file missing" in res.stderr
    assert not Path(env["DOCKER_LOG"]).exists()


def test_up_and_restart_need_a_service(tmp_path: Path) -> None:
    _, tools, env = _tree(tmp_path)
    assert _run(tools, env, "up").returncode == 2
    assert _run(tools, env, "restart").returncode == 2
    assert _run(tools, env, "restart", "a", "b").returncode == 2


def test_config_refuses_unredactable_arguments(tmp_path: Path) -> None:
    _, tools, env = _tree(tmp_path)
    res = _run(tools, env, "config", "--no-interpolate")
    assert res.returncode == 2


def test_restart_runs_the_stack_files_chain_and_logs_one_line(tmp_path: Path) -> None:
    r, tools, env = _tree(tmp_path)
    res = _run(tools, env, "restart", "web")
    assert res.returncode == 0, res.stderr
    cmd = [
        ln
        for ln in Path(env["DOCKER_LOG"]).read_text().splitlines()
        if ln.endswith("restart web")
    ][0]
    listed = _run(tools, env, "files").stdout.split()
    expected = (
        "compose -p dev-health --env-file ops/.env "
        + " ".join(f"-f {f}" for f in listed)
        + " restart web"
    )
    assert cmd == expected
    assert "stack.sh image ghcr.io/full-chaos/dev-health-web@sha256:" in res.stderr
    lines = (r / "_records/stack-runs.log").read_text().splitlines()
    assert len(lines) == 1 and "verb=restart" in lines[0] and "service=web" in lines[0]
    assert "images=dev-health-web@aaaaaaaa" in lines[0] and lines[0].endswith("rc=0")


def test_read_verbs_write_no_run_log(tmp_path: Path) -> None:
    r, tools, env = _tree(tmp_path)
    assert _run(tools, env, "ps").returncode == 0
    assert _run(tools, env, "config", "--quiet").returncode == 0
    assert not (r / "_records/stack-runs.log").exists()


def test_venue_set_adds_its_profile(tmp_path: Path) -> None:
    _, tools, env = _tree(tmp_path)
    assert _run(tools, env, "-s", "river-apply", "ps").returncode == 0
    assert "--profile venue" in Path(env["DOCKER_LOG"]).read_text()


def test_failing_config_never_prints_compose_stderr(tmp_path: Path) -> None:
    _, tools, env = _tree(tmp_path)
    res = _run(tools, {**env, "STUB_CONFIG_FAIL": "1"}, "restart", "web")
    assert res.returncode == 2
    assert "SENTINEL" not in res.stdout + res.stderr
    assert "restart web" not in Path(env["DOCKER_LOG"]).read_text()
    flag = _run(tools, {**env, "STUB_CONFIG_FAIL": "1"}, "config", "--quiet")
    assert flag.returncode == 2 and "SENTINEL" not in flag.stdout + flag.stderr


def test_caller_cannot_replace_the_file_list_or_project(tmp_path: Path) -> None:
    _, tools, env = _tree(tmp_path)
    other = tmp_path / "other.files"
    other.write_text("stack project hacked-project\nstack root compose.yml\n")
    res = _run(tools, {**env, "STACK_FILES": str(other)}, "restart", "web")
    assert res.returncode == 0, res.stderr
    log = Path(env["DOCKER_LOG"]).read_text()
    assert "-p dev-health " in log and "hacked-project" not in log


def test_operator_image_is_only_required_when_a_file_uses_it(tmp_path: Path) -> None:
    r, tools, env = _tree(tmp_path)
    env = {k: v for k, v in env.items() if k != "BIGBOY_OPERATOR_IMAGE"}
    res = _run(tools, {**env, "DHO_SMOKE_CATALOG_FILE": "x"}, "-s", "smoke", "ps")
    assert res.returncode == 0, res.stderr
    assert "--format" not in Path(env["DOCKER_LOG"]).read_text()
    refused = _run(tools, env, "ps")
    assert refused.returncode == 2 and "BIGBOY_OPERATOR_IMAGE" in refused.stderr
    assert "--format" in Path(env["DOCKER_LOG"]).read_text()


def test_inherited_compose_variables_are_dropped(tmp_path: Path) -> None:
    _, tools, env = _tree(tmp_path)
    inherited = {
        **env,
        "COMPOSE_PROFILES": "secret-profile",
        "COMPOSE_FILE": "x.yml",
        "COMPOSE_PROJECT_NAME": "hacked",
        "COMPOSE_ENV_FILES": "y.env",
    }
    res = _run(tools, inherited, "restart", "web")
    assert res.returncode == 0, res.stderr
    envs = {
        ln
        for ln in Path(env["DOCKER_LOG"]).read_text().splitlines()
        if ln.startswith("ENV COMPOSE_")
    }
    assert envs == {"ENV COMPOSE_PROFILES="}


def test_relative_root_and_tools_paths_work(tmp_path: Path) -> None:
    r, tools, env = _tree(tmp_path)
    rel = {**env, "BIGBOY_ROOT": "root", "BIGBOY_TOOLS_DIR": "tools"}
    res = subprocess.run(
        [str(tools / "stack.sh"), "restart", "web"],
        env=rel,
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, res.stderr
    assert f"-f {r}/compose.yml" in Path(env["DOCKER_LOG"]).read_text()


def test_service_name_with_a_newline_is_refused(tmp_path: Path) -> None:
    r, tools, env = _tree(tmp_path)
    assert _run(tools, env, "restart", "web\nINJECTED").returncode == 2
    assert _run(tools, env, "up", "web", "a b").returncode == 2
    assert not (r / "_records/stack-runs.log").exists()


def test_read_verbs_print_nothing_on_stderr(tmp_path: Path) -> None:
    _, tools, env = _tree(tmp_path)
    assert _run(tools, env, "config", "--quiet").stderr == ""


def test_repin_web_child_gets_absolute_root_and_tools_paths(tmp_path: Path) -> None:
    r, tools, env = _tree(tmp_path)
    seen = tmp_path / "child.env"
    child = tools / "bigboy-repin-web.sh"
    child.write_text(
        f'#!/usr/bin/env bash\necho "$BIGBOY_ROOT $BIGBOY_TOOLS_DIR" > {seen}\n'
    )
    child.chmod(child.stat().st_mode | stat.S_IEXEC)
    rel = {**env, "BIGBOY_ROOT": "root", "BIGBOY_TOOLS_DIR": "tools"}
    res = subprocess.run(
        [str(tools / "stack.sh"), "repin-web"],
        env=rel,
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, res.stderr
    assert seen.read_text().split() == [str(r), str(tools)]


_REAL_COMPOSE = (
    subprocess.run(["docker", "compose", "version"], capture_output=True).returncode
    == 0
)
_DHO = "ghcr.io/full-chaos/dev-health-go-dho@sha256:" + "d" * 64


def _real_tree(tmp_path: Path, *, env_file: str = "") -> tuple[Path, dict[str, str]]:
    """A tiny tree rendered by the REAL docker compose (config only, no daemon call): no stub on PATH."""
    r = tmp_path / "root"
    (r / "compose").mkdir(parents=True)
    (r / "ops").mkdir()
    (r / "ops/.env").write_text(env_file)
    (r / "compose.yml").write_text(
        "name: dev-health\nnetworks:\n  dev-health: {}\nservices:\n"
        "  main:\n    image: busybox\n  extra:\n    image: busybox\n    profiles: [secret-profile]\n"
    )
    (r / "compose/compose.bigboy.images.yml").write_text(
        f"# pins\n# {_DHO}\nservices: {{}}\n"
    )
    tools = tmp_path / "tools"
    shutil.copytree(BIGBOY, tools)
    (tools / "stack.files").write_text(
        "stack project dev-health\nstack env-file ops/.env\nstack root compose.yml\n"
        "stack root compose/compose.bigboy.images.yml\n"
        "river-apply include stack\nriver-apply profile venue\nriver-apply tools compose.bigboy.river-apply.yml\n"
        "hook-check include stack\nhook-check profile venue\nhook-check tools compose.bigboy.hook-check.yml\n"
        "hook-check needs HOOK_OPERATOR_IMAGE\n"
    )
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("COMPOSE_", "BIGBOY_", "HOOK_", "RIVER_"))
    }
    env |= {"BIGBOY_ROOT": str(r), "BIGBOY_TOOLS_DIR": str(tools)}
    return tools, env


@pytest.mark.skipif(not _REAL_COMPOSE, reason="needs docker compose")
def test_river_apply_set_renders_with_the_derived_dho_image_under_real_compose(
    tmp_path: Path,
) -> None:
    tools, env = _real_tree(tmp_path)
    res = _run(tools, env, "-s", "river-apply", "config", "--quiet")
    assert res.returncode == 0, res.stderr


@pytest.mark.skipif(not _REAL_COMPOSE, reason="needs docker compose")
def test_hook_check_set_names_the_variable_its_step_script_must_export(
    tmp_path: Path,
) -> None:
    tools, env = _real_tree(tmp_path)
    refused = _run(tools, env, "-s", "hook-check", "config", "--quiet")
    assert refused.returncode == 2 and "needs HOOK_OPERATOR_IMAGE" in refused.stderr
    ok = _run(
        tools,
        {**env, "HOOK_OPERATOR_IMAGE": _DHO},
        "-s",
        "hook-check",
        "config",
        "--quiet",
    )
    assert ok.returncode == 0, ok.stderr


@pytest.mark.skipif(not _REAL_COMPOSE, reason="needs docker compose")
def test_env_file_compose_profiles_cannot_add_a_profile_under_real_compose(
    tmp_path: Path,
) -> None:
    tools, env = _real_tree(tmp_path, env_file="COMPOSE_PROFILES=secret-profile\n")
    res = _run(tools, env, "config", "--services")
    assert res.returncode == 0, res.stderr
    assert res.stdout.split() == ["main"]
