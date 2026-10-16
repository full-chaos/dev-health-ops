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
STACK_SH = BIGBOY / "stack.sh"


def _tree(tmp_path: Path, *, skip: str = "") -> tuple[Path, Path, dict[str, str]]:
    r = tmp_path / "root"
    (r / "compose").mkdir(parents=True)
    (r / ".remember/lanes/team-lead").mkdir(parents=True)
    (r / "ops").mkdir()
    (r / "_records").mkdir()
    (r / "ops/.env").write_text("")
    for rel in ("_records/compose.bigboy.images.yml",):
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
    _, tools, env = _tree(tmp_path, skip="_records/compose.bigboy.images.yml")
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
        f"compose -p dev-health --project-directory {r} --env-file ops/.env "
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
    refused = _run(tools, env, "up", "go-worker")
    assert refused.returncode == 2 and "BIGBOY_OPERATOR_IMAGE" in refused.stderr


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
    assert f"-f {tools}/compose.bigboy.base.yml" in Path(env["DOCKER_LOG"]).read_text()


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
    (r / "_records").mkdir(parents=True)
    (r / "ops").mkdir()
    (r / "ops/.env").write_text(env_file)
    (r / "compose.yml").write_text(
        "name: dev-health\nnetworks:\n  dev-health: {}\nservices:\n"
        "  main:\n    image: busybox\n  extra:\n    image: busybox\n    profiles: [secret-profile]\n"
    )
    (r / "_records/compose.bigboy.images.yml").write_text(
        f"# pins\n# {_DHO}\nservices: {{}}\n"
    )
    tools = tmp_path / "tools"
    shutil.copytree(BIGBOY, tools)
    (tools / "stack.files").write_text(
        "stack project dev-health\nstack env-file ops/.env\nstack root compose.yml\n"
        "stack root _records/compose.bigboy.images.yml\n"
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


@pytest.mark.skipif(not _REAL_COMPOSE, reason="needs docker compose")
def test_live_chain_renders_from_ops_files_plus_root_state_only(tmp_path: Path) -> None:
    """The base and go-worker files come from ci/bigboy; the root holds only the pin file, the lane override and
    host state files (CHAOS-8693: no host compose.yml, no host compose/ static files)."""
    r = tmp_path / "root"
    for d in ("ops", ".go-api-dev", "_records"):
        (r / d).mkdir(parents=True)
    shutil.copy(
        BIGBOY / "compose.bigboy.images.yml", r / "_records/compose.bigboy.images.yml"
    )
    (r / "ops/.env").write_text("")
    (r / ".go-api-dev/go-api.env").write_text("")
    env = {
        k: v for k, v in os.environ.items() if not k.startswith(("COMPOSE_", "BIGBOY_"))
    }
    env |= {
        "BIGBOY_ROOT": str(r),
        "BIGBOY_TOOLS_DIR": str(BIGBOY),
        "BIGBOY_OPERATOR_IMAGE": _DHO,
    }
    res = subprocess.run(
        [str(STACK_SH), "config", "--quiet"], env=env, capture_output=True, text=True
    )
    assert res.returncode == 0, res.stderr
    assert (BIGBOY / "compose.bigboy.base.yml").is_file() and not (
        r / "compose.yml"
    ).exists()


def test_check_host_copies_flags_a_static_copy_that_differs(tmp_path: Path) -> None:
    r = tmp_path / "root"
    (r / "compose").mkdir(parents=True)
    (r / "_records").mkdir()
    shutil.copy(BIGBOY / "compose.bigboy.base.yml", r / "compose.yml")
    shutil.copy(
        BIGBOY / "compose.bigboy.go-workers.yml", r / "compose/compose.go.workers.yml"
    )
    shutil.copy(
        BIGBOY / "compose.bigboy.images.yml", r / "_records/compose.bigboy.images.yml"
    )
    script = BIGBOY / "check-host-copies.sh"
    env = {**os.environ, "BIGBOY_ROOT": str(r)}
    ok = subprocess.run([str(script)], env=env, capture_output=True, text=True)
    assert ok.returncode == 0 and ok.stdout.count("SAME") == 3, ok.stdout
    (r / "compose.yml").write_text((r / "compose.yml").read_text() + "# drift\n")
    bad = subprocess.run([str(script)], env=env, capture_output=True, text=True)
    assert bad.returncode == 1 and "DIFF compose.bigboy.base.yml" in bad.stdout
    (r / "_records/compose.bigboy.images.yml").write_text("services: {}\n")
    pin = subprocess.run(
        [str(script)],
        env={**env, "BIGBOY_ROOT": str(r)},
        capture_output=True,
        text=True,
    )
    assert "INFO pin file differs" in pin.stdout


@pytest.mark.skipif(not _REAL_COMPOSE, reason="needs docker compose")
def test_relative_paths_of_the_tracked_base_resolve_against_the_bigboy_root(
    tmp_path: Path,
) -> None:
    """The base file now lives in ci/bigboy, not at the root: without --project-directory its `./ops` build contexts
    and `./.go-api-dev` bind mounts would resolve against ci/bigboy."""
    r = tmp_path / "root"
    for d in ("ops", ".go-api-dev", "_records"):
        (r / d).mkdir(parents=True)
    shutil.copy(
        BIGBOY / "compose.bigboy.images.yml", r / "_records/compose.bigboy.images.yml"
    )
    (r / "ops/.env").write_text("")
    (r / ".go-api-dev/go-api.env").write_text("")
    env = {
        k: v for k, v in os.environ.items() if not k.startswith(("COMPOSE_", "BIGBOY_"))
    }
    env |= {
        "BIGBOY_ROOT": str(r),
        "BIGBOY_TOOLS_DIR": str(BIGBOY),
        "BIGBOY_OPERATOR_IMAGE": _DHO,
    }
    env["DHO_API_CH_USERS_XML"] = str(r / ".go-api-dev/dho_api_ch.xml")
    real = shutil.which("docker")
    shim = tmp_path / "shim"
    shim.mkdir()
    log = tmp_path / "shim.log"
    (shim / "docker").write_text(
        f'#!/usr/bin/env bash\necho "$*" >> {log}\nexec {real} "$@"\n'
    )
    (shim / "docker").chmod(0o755)
    through = subprocess.run(
        [str(STACK_SH), "config", "--quiet"],
        env={**env, "PATH": f"{shim}:{env['PATH']}"},
        capture_output=True,
        text=True,
    )
    assert through.returncode == 0, through.stderr
    assert f"--project-directory {r} " in log.read_text()
    files = subprocess.run(
        [str(STACK_SH), "files"], env=env, capture_output=True, text=True
    ).stdout.split()
    cmd = [
        "docker",
        "compose",
        "-p",
        "dev-health",
        "--project-directory",
        str(r),
        "--env-file",
        "ops/.env",
    ]
    for f in files:
        cmd += ["-f", f]
    out = subprocess.run(
        [*cmd, "config", "--format", "json"],
        env=env,
        cwd=r,
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0, out.stderr[-300:]
    import json

    services = json.loads(out.stdout)["services"]
    # the reconciler sweep is `active` from the tracked workers overlay alone (the lane override file is not in the chain)
    assert (
        services["go-reconciler"]["environment"]["SYNC_UNRECLAIMABLE_SWEEP"] == "active"
    )
    paths = [s["build"]["context"] for s in services.values() if "build" in s]
    paths += [
        v["source"]
        for s in services.values()
        for v in s.get("volumes", [])
        if v.get("type") == "bind"
    ]
    assert any(p.startswith(str(r)) for p in paths)
    assert not [p for p in paths if p.startswith(str(BIGBOY))]


def test_run_is_a_logged_one_off_with_rm_no_deps_and_no_tty(tmp_path: Path) -> None:
    r, tools, env = _tree(tmp_path)
    res = _run(
        tools,
        env,
        "-s",
        "river-apply",
        "run",
        "venue-river",
        "migrate",
        "river",
        "--apply-and-check",
    )
    assert res.returncode == 0, res.stderr
    cmd = [
        ln for ln in Path(env["DOCKER_LOG"]).read_text().splitlines() if " run " in ln
    ][0]
    assert cmd.endswith(
        "run --rm --no-deps -T venue-river migrate river --apply-and-check"
    )
    assert "--profile venue" in cmd
    line = (r / "_records/stack-runs.log").read_text().splitlines()
    assert len(line) == 1 and "verb=run" in line[0] and "service=venue-river" in line[0]


def test_exec_passes_stdin_through_and_is_not_logged(tmp_path: Path) -> None:
    r, tools, env = _tree(tmp_path)
    res = subprocess.run(
        [str(tools / "stack.sh"), "exec", "postgres", "psql", "-X"],
        env=env,
        input="select 1;\n",
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, res.stderr
    assert "exec -T postgres psql -X" in Path(env["DOCKER_LOG"]).read_text()
    assert not (r / "_records/stack-runs.log").exists()


def test_run_and_exec_refuse_a_missing_or_bad_service(tmp_path: Path) -> None:
    _, tools, env = _tree(tmp_path)
    for verb in ("run", "exec"):
        assert _run(tools, env, verb).returncode == 2
        assert _run(tools, env, verb, "--rm").returncode == 2
        assert _run(tools, env, verb, "web\nx").returncode == 2


@pytest.mark.skipif(not _REAL_COMPOSE, reason="needs docker compose")
def test_smoke_set_activates_the_web_smoke_service_so_the_credential_gate_sees_its_variables(
    tmp_path: Path,
) -> None:
    """web-path-smoke.sh decides from `stack.sh -s smoke config` (NAME=<length> lines) whether the admin email and the
    password file resolve. web-smoke sits in the profile `smoke`: the set must activate it, or the rendered config
    holds no DHO_SMOKE_ADMIN_* entry and the gate aborts with a misleading BLANK before the smoke runs."""
    r = tmp_path / "root"
    for d in ("ops", ".go-api-dev", "_records", "compose", ".remember/lanes/team-lead"):
        (r / d).mkdir(parents=True)
    for pin in (
        "compose/compose.bigboy.images.yml",
        "_records/compose.bigboy.images.yml",
    ):
        shutil.copy(BIGBOY / "compose.bigboy.images.yml", r / pin)
    (r / ".remember/lanes/team-lead/reconciler-sweep-override.yml").write_text(
        "services: {}\n"
    )
    (r / "ops/.env").write_text("")
    (r / ".go-api-dev/go-api.env").write_text("")
    env = {
        k: v for k, v in os.environ.items() if not k.startswith(("COMPOSE_", "BIGBOY_"))
    }
    env |= {
        "BIGBOY_ROOT": str(r),
        "BIGBOY_TOOLS_DIR": str(BIGBOY),
        "DHO_SMOKE_ADMIN_EMAIL": "admin@example.test",
        "DHO_SMOKE_ADMIN_PASSWORD_FILE": str(tmp_path / "pw"),
        "DHO_SMOKE_CATALOG_FILE": str(tmp_path / "catalog.json"),
    }
    (tmp_path / "pw").write_text("x")
    (tmp_path / "catalog.json").write_text("{}")
    res = subprocess.run(
        [str(STACK_SH), "-s", "smoke", "config"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, res.stderr[-300:]
    lengths = dict(
        line.split("=", 1) for line in res.stdout.splitlines() if "=" in line
    )
    assert (
        int(lengths["DHO_SMOKE_ADMIN_EMAIL"]) > 0
        and int(lengths["DHO_SMOKE_ADMIN_PASSWORD_FILE"]) > 0
    ), lengths


def test_the_cut_steps_that_call_stack_sh_run_in_the_root_they_are_given(
    tmp_path: Path,
) -> None:
    """A cut launched against BIGBOY_ROOT=<scratch> must not reach the real bigboy root through a step script that
    hard-codes /home/ubuntu/devhealth: every compose call of these scripts runs with the scratch root as cwd."""
    r = tmp_path / "root"
    for d in ("ops", "_records/rec"):
        (r / d).mkdir(parents=True)
    (r / "ops/.env").write_text("")
    (r / "_records/compose.bigboy.images.yml").write_text(f"image: {_DHO}\n")
    (r / "_records/rec/sha.txt").write_text("1234567abcdef\n")
    exp = tmp_path / "exp.txt"
    exp.write_text("r t SELECT\n")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "cwd.log"
    shim = bin_dir / "docker"
    shim.write_text(
        '#!/usr/bin/env bash\necho "$PWD :: $*" >> "$CWD_LOG"\n'
        'case "$*" in *buildx*) echo \'{"digest":"sha256:'
        + "e" * 64
        + '"}\';; *" ps "*) echo ghcr.io/x/op@sha256:'
        + "f" * 64
        + ";; esac\nexit 0\n"
    )
    shim.chmod(0o755)
    env = {
        k: v for k, v in os.environ.items() if not k.startswith(("COMPOSE_", "BIGBOY_"))
    }
    env |= {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "CWD_LOG": str(log),
        "BIGBOY_ROOT": str(r),
        "DHO_SMOKE_CATALOG_FILE": str(tmp_path / "catalog.json"),
        "DHO_SMOKE_OPS_SHA": "0" * 40,
    }
    (tmp_path / "catalog.json").write_text("{}")
    cases = {
        "bigboy-river-apply.sh": [],
        "bigboy-grants-check.sh": [str(r / "_records/rec")],
        "pg-grants-check.sh": ["bigboy", str(exp)],
        "bigboy-hook-check.sh": [str(r / "_records/rec")],
        "web-path-smoke.sh": [],
    }
    for name, args in cases.items():
        log.write_text("")
        subprocess.run(
            ["bash", str(BIGBOY / name), *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        calls = [
            ln
            for ln in log.read_text().splitlines()
            if " compose " in f" {ln} " or ln.endswith(" compose")
        ]
        assert calls, name
        outside = [ln for ln in calls if not ln.startswith(str(r.resolve()) + " ::")]
        assert not outside, (name, outside[:2])


def test_a_verb_that_starts_no_worker_needs_no_operator_image(tmp_path: Path) -> None:
    """With BIGBOY_OPERATOR_IMAGE unset and no go-reconciler running (an isolated root, a first look), exec/config/ps
    still work: the worker overlay's image only has to interpolate. Anything that would start a worker stays refused."""
    _, tools, env = _tree(tmp_path)
    env = {k: v for k, v in env.items() if k != "BIGBOY_OPERATOR_IMAGE"}
    for args in (
        ("exec", "clickhouse", "true"),
        ("config", "--quiet"),
        ("ps",),
        ("run", "migrate"),
    ):
        res = _run(tools, env, *args)
        assert res.returncode == 0, (args, res.stderr)
    for args in (
        ("up", "go-worker"),
        ("exec", "go-reconciler", "true"),
        ("run", "go-worker", "x"),
        ("restart", "go-worker"),
    ):
        res = _run(tools, env, *args)
        assert res.returncode == 2 and "BIGBOY_OPERATOR_IMAGE" in res.stderr, (
            args,
            res.stderr,
        )


@pytest.mark.skipif(not _REAL_COMPOSE, reason="needs docker compose")
def test_every_set_renders_the_reconciler_with_the_sweep_active(tmp_path: Path) -> None:
    """The compiled default of --unreclaimable-sweep is shadow, and the old lane override sat in the base of every set;
    a set that renders go-reconciler without SYNC_UNRECLAIMABLE_SWEEP=active is the 'override dropped' regression."""
    import json

    r = tmp_path / "root"
    for d in ("ops", ".go-api-dev", "_records"):
        (r / d).mkdir(parents=True)
    shutil.copy(
        BIGBOY / "compose.bigboy.images.yml", r / "_records/compose.bigboy.images.yml"
    )
    (r / "ops/.env").write_text("")
    (r / ".go-api-dev/go-api.env").write_text("")
    (tmp_path / "pw").write_text("x")
    (tmp_path / "catalog.json").write_text("{}")
    env = {
        k: v for k, v in os.environ.items() if not k.startswith(("COMPOSE_", "BIGBOY_"))
    }
    env |= {
        "BIGBOY_ROOT": str(r),
        "BIGBOY_TOOLS_DIR": str(BIGBOY),
        "BIGBOY_OPERATOR_IMAGE": _DHO,
        "DHO_API_CH_USERS_XML": str(r / ".go-api-dev/dho_api_ch.xml"),
        "HOOK_OPERATOR_IMAGE": _DHO,
        "PROVE_ORG": "o",
        "PROVE_ARTIFACT_DIR": str(tmp_path),
        "DHO_SMOKE_ADMIN_EMAIL": "a@example.test",
        "DHO_SMOKE_ADMIN_PASSWORD_FILE": str(tmp_path / "pw"),
        "DHO_SMOKE_CATALOG_FILE": str(tmp_path / "catalog.json"),
        "DHO_SMOKE_DIR": str(BIGBOY),
    }
    sets = [
        ln.split()[0]
        for ln in (BIGBOY / "stack.files").read_text().splitlines()
        if ln.strip() and not ln.startswith("#") and ln.split()[1] == "project"
    ]
    assert {"stack", "smoke", "prove-base"} <= set(sets), sets
    for name in sets:
        files = subprocess.run(
            [str(STACK_SH), "-s", name, "files"],
            env=env,
            capture_output=True,
            text=True,
        ).stdout.split()
        cmd = [
            "docker",
            "compose",
            "-p",
            "dev-health",
            "--project-directory",
            str(r),
            "--env-file",
            "ops/.env",
        ]
        for f in files:
            cmd += ["-f", f]
        out = subprocess.run(
            [*cmd, "config", "--format", "json"],
            env=env,
            cwd=r,
            capture_output=True,
            text=True,
        )
        assert out.returncode == 0, (name, out.stderr[-300:])
        reconciler = json.loads(out.stdout)["services"].get("go-reconciler")
        assert reconciler is not None, name
        assert reconciler["environment"].get("SYNC_UNRECLAIMABLE_SWEEP") == "active", (
            name
        )


def test_up_of_a_non_worker_service_needs_no_operator_image(tmp_path: Path) -> None:
    """query-api/go-api/web run the dho/web pins; only services whose image is BIGBOY_OPERATOR_IMAGE need it."""
    _, tools, env = _tree(tmp_path)
    env = {k: v for k, v in env.items() if k != "BIGBOY_OPERATOR_IMAGE"}
    ok = _run(tools, env, "up", "query-api", "go-api", "web")
    assert ok.returncode == 0, ok.stderr
    for args in (
        ("up", "go-worker"),
        ("up", "web", "go-scheduler"),
        ("restart", "go-reconciler"),
    ):
        res = _run(tools, env, *args)
        assert res.returncode == 2 and "BIGBOY_OPERATOR_IMAGE" in res.stderr, (
            args,
            res.stderr,
        )


def test_a_worker_verb_requires_the_operator_image_explicitly_and_probes_nothing(
    tmp_path: Path,
) -> None:
    """No autodetection: with BIGBOY_OPERATOR_IMAGE unset, starting an operator-image service fails closed with a
    message that names the variable, and no compose call (no `ps` probe) is made. With it set, the verb runs."""
    _, tools, env = _tree(tmp_path)
    unset = {k: v for k, v in env.items() if k != "BIGBOY_OPERATOR_IMAGE"}
    for args in (
        ("up", "go-worker"),
        ("restart", "go-reconciler"),
        ("run", "go-worker", "x"),
        ("exec", "go-scheduler", "true"),
    ):
        res = _run(tools, unset, *args)
        assert res.returncode == 2 and "export BIGBOY_OPERATOR_IMAGE" in res.stderr, (
            args,
            res.stderr,
        )
    log = Path(env["DOCKER_LOG"])
    assert not log.exists() or "ps --format" not in log.read_text(), log.read_text()
    ok = _run(tools, env, "up", "go-worker")
    assert ok.returncode == 0, ok.stderr


def test_a_relative_bigboy_root_reaches_stack_sh_once_and_absolute(
    tmp_path: Path,
) -> None:
    """The step scripts cd into the root, then hand it on: a relative BIGBOY_ROOT must not be resolved twice."""
    r = tmp_path / "root"
    for d in ("ops", "_records/rec"):
        (r / d).mkdir(parents=True)
    (r / "ops/.env").write_text("")
    (r / "_records/compose.bigboy.images.yml").write_text(f"image: {_DHO}\n")
    (r / "_records/rec/sha.txt").write_text("1234567abcdef\n")
    exp = tmp_path / "exp.txt"
    exp.write_text("r t SELECT\n")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "cwd.log"
    shim = bin_dir / "docker"
    shim.write_text(
        '#!/usr/bin/env bash\necho "$PWD :: $*" >> "$CWD_LOG"\n'
        'case "$*" in *buildx*) echo \'{"digest":"sha256:'
        + "e" * 64
        + '"}\';; *" ps "*) echo ghcr.io/x/op@sha256:'
        + "f" * 64
        + ";; esac\nexit 0\n"
    )
    shim.chmod(0o755)
    (tmp_path / "catalog.json").write_text("{}")
    env = {
        k: v for k, v in os.environ.items() if not k.startswith(("COMPOSE_", "BIGBOY_"))
    }
    env |= {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "CWD_LOG": str(log),
        "BIGBOY_ROOT": "root",
        "DHO_SMOKE_CATALOG_FILE": str(tmp_path / "catalog.json"),
        "DHO_SMOKE_OPS_SHA": "0" * 40,
    }
    cases = {
        "bigboy-river-apply.sh": [],
        "bigboy-grants-check.sh": [str(r / "_records/rec")],
        "bigboy-hook-check.sh": [str(r / "_records/rec")],
        "web-path-smoke.sh": [],
    }
    for name, args in cases.items():
        log.write_text("")
        done = subprocess.run(
            ["bash", str(BIGBOY / name), *args],
            env=env,
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert "is not a directory" not in done.stdout + done.stderr, (
            name,
            done.stdout,
            done.stderr,
        )
        calls = [ln for ln in log.read_text().splitlines() if " compose " in f" {ln} "]
        assert calls, name
        assert all(ln.startswith(str(r.resolve()) + " ::") for ln in calls), (
            name,
            calls[:2],
        )
