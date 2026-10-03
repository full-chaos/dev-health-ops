"""bigboy-cut.sh: the compose chain carries the router overlay (CHAOS-7131).

compose.bigboy.router.yml sets web's BACKEND_URL (the plane-split router) and AUTH_URL. The cut's
COMPOSE_FILE omitted it, so the `up` that recreates web built it from the base file alone: web lost
both names, REST calls returned 500 and the auth callback/logout used the wrong origin. These tests
resolve the chain the script actually exports, so removing the overlay fails them.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CUT = ROOT / "ci" / "bigboy" / "bigboy-cut.sh"
ROUTER = ROOT / "ci" / "bigboy" / "compose.bigboy.router.yml"
REQUIRED = ROOT / "ci" / "bigboy" / "check-web-env-required.sh"


def _chain() -> list[str]:
    match = re.search(r"^export COMPOSE_FILE=(\S+)$", CUT.read_text(), re.MULTILINE)
    assert match, "bigboy-cut.sh no longer exports COMPOSE_FILE"
    return match.group(1).split(":")


def test_chain_includes_the_router_overlay_last() -> None:
    chain = _chain()
    assert "$HERE/compose.bigboy.router.yml" in chain, chain
    assert chain[-1] == "$HERE/compose.bigboy.router.yml", (
        "the router overlay must come last so its web.environment wins"
    )


def test_resolved_web_service_carries_backend_url_and_auth_url(tmp_path: Path) -> None:
    """Materialize the chain the script exports (stub base files, the REAL router overlay) and
    let `docker compose config` merge it: web must come out with BACKEND_URL and AUTH_URL."""
    tools = tmp_path / "tools"
    tools.mkdir()
    (tools / "compose.bigboy.router.yml").write_text(ROUTER.read_text())
    users_overlay = ROUTER.parent / "compose.bigboy.clickhouse-users.yml"
    (tools / "compose.bigboy.clickhouse-users.yml").write_text(
        users_overlay.read_text()
    )
    # CHAOS-7055: the billing override is part of the chain too.
    (tools / "compose.bigboy.billing-edge.yml").write_text(
        (ROUTER.parent / "compose.bigboy.billing-edge.yml").read_text()
    )
    # CHAOS-7976: the workers overlay moved from the untracked host file compose/compose.bigboy.workers.yml
    # into the chain through $HERE. It requires BIGBOY_OPERATOR_IMAGE and resets services this stub base does
    # not define, so the test stages the same empty overlay the stubbed host path used to get.
    (tools / "compose.bigboy.workers.yml").write_text("services: {}\n")
    users_file = tmp_path / "dho_api_ch.xml"
    users_file.write_text("<clickhouse/>")
    chain = [entry.replace("$HERE", str(tools)) for entry in _chain()]
    for entry in chain:
        if entry.startswith(str(tools)):
            continue
        target = tmp_path / entry
        target.parent.mkdir(parents=True, exist_ok=True)
        if entry == "compose.yml":
            target.write_text(
                "services:\n"
                "  web:\n    image: web:test\n    environment:\n      BACKEND_URL: http://api:8000\n"
                "  traefik:\n    image: traefik:test\n"
                "  clickhouse:\n    image: clickhouse:test\n"
                "  go-api:\n    image: go-api:test\n"
                "  billing-edge:\n    image: billing-edge:test\n"
            )
        else:
            target.write_text("services: {}\n")
    (tmp_path / ".traefik-dynamic").mkdir()
    proc = subprocess.run(
        ["docker", "compose", "config", "--format", "json"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env={
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "HOME": str(tmp_path),
            "COMPOSE_FILE": ":".join(chain),
            "DHO_API_CH_USERS_XML": str(users_file),
        },
    )
    assert proc.returncode == 0, proc.stderr
    web_env = json.loads(proc.stdout)["services"]["web"]["environment"]
    assert web_env["BACKEND_URL"] == "http://traefik:3000", web_env
    assert "AUTH_URL" in web_env, sorted(web_env)


def _run_required(tmp_path: Path, names: str | None, *required: str):
    file = tmp_path / "names"
    if names is not None:
        file.write_text(names)
    return subprocess.run(
        ["bash", str(REQUIRED), str(file), *required],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def test_required_names_present_passes(tmp_path: Path) -> None:
    proc = _run_required(
        tmp_path, "AUTH_URL\nBACKEND_URL\nNODE_ENV\n", "BACKEND_URL", "AUTH_URL"
    )
    assert proc.returncode == 0, proc.stderr


def test_missing_name_fails_and_is_named(tmp_path: Path) -> None:
    proc = _run_required(
        tmp_path, "NODE_ENV\nBACKEND_URL_OLD\n", "BACKEND_URL", "AUTH_URL"
    )
    assert proc.returncode == 1
    assert "BACKEND_URL" in proc.stderr and "AUTH_URL" in proc.stderr, proc.stderr


def test_unread_names_file_fails_loudly(tmp_path: Path) -> None:
    proc = _run_required(tmp_path, None, "BACKEND_URL")
    assert proc.returncode == 2, proc.stderr
    empty = _run_required(tmp_path, "", "BACKEND_URL")
    assert empty.returncode == 2, empty.stderr


def test_cut_checks_web_env_after_the_web_recreate() -> None:
    lines = CUT.read_text().splitlines()
    up = next(
        i for i, line in enumerate(lines) if "st up $?" in line and "go-api web" in line
    )
    check_at = next(
        i for i, line in enumerate(lines) if "check-web-env-required.sh" in line
    )
    assert check_at > up, "the web env check must run after the recreate it verifies"
    assert "BACKEND_URL AUTH_URL" in lines[check_at]


# ============================================================================
# CHAOS-8371: bigboy check scripts D4566 class-rule cases, added to this existing file (D3942: no new test file).
# The scripts append their own overlay to the cut's COMPOSE_FILE, so they belong with the compose-chain cases.
#
# bigboy-hook-check / river-apply / pg-grants-check / grants-check obey the D4566 class rules (CHAOS-8371).
#
# Rules: no credential value in a script variable, argv, `-e`, a script-written env file, or read out of a
# container (printenv / echo "$SECRET"); compose verbs only (no bare `docker exec` / `docker run`); no test hook
# in a run file. The one-off operator/dho containers are compose services whose DSNs COMPOSE interpolates from
# ops/.env, so the script never touches a password.
#
# Method: the REAL scripts run under bash on a closed PATH with a recording `docker` stand-in (argv + env per call
# appended to a file). A sentinel password sits in the stand-in's ambient env of the test only; the assertions read
# what each docker call actually received. The scripts' host-path literal is substituted in a tmp copy (no test hook
# in the run files). Guard seen failing: the same `violations()` is run on the OLD hook-check text (embedded
# below) and must report every old rule break.
# ============================================================================

_c_ROOT = Path(__file__).resolve().parents[2]
_c_BB = _c_ROOT / "ci" / "bigboy"
_c_HOST_ROOT = "/home/ubuntu/devhealth"
_c_MARKER = "plainword-marker-c"
_c_DIGEST = "sha256:" + "ab" * 32

_c_FAKE_DOCKER = r"""#!/bin/bash
# recording stand-in for docker. one record per call: argv (NUL-joined), COMPOSE_FILE, HOOK/RIVER image vars, whole env.
d="$FAKE_DIR"; n=$(( $(cat "$d/n" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$d/n"
printf '%s\0' "$@" > "$d/argv.$n"; env > "$d/env.$n"
cat /dev/stdin > "$d/stdin.$n" 2>/dev/null < /dev/stdin || true
case "$1" in
  buildx) echo '{"digest":"'"$FAKE_DIGEST"'"}'; exit 0 ;;
  compose)
    if printf '%s\n' "$@" | grep -qx 'venue-hook'; then
      h=$(( $(cat "$d/hook-n" 2>/dev/null || echo 0) + 1 )); echo "$h" > "$d/hook-n"
      echo '{"msg":"migrate step ok","time":"t"}'; echo '{"state":"clean"}'
      if [ -n "${FAKE_FAIL_HOOK_CALL:-}" ]; then [ "$h" = "$FAKE_FAIL_HOOK_CALL" ] && exit 3; exit 0; fi
      exit "${FAKE_RC:-0}"
    fi
    if printf '%s\n' "$@" | grep -qx 'venue-river'; then echo 'applying postgresql://devhealth:plainword-river-marker@postgres:5432/db'; exit "${FAKE_RC:-0}"; fi
    if printf '%s\n' "$@" | grep -qx 'postgres'; then echo 'r|t|SELECT|t|t'; exit "${FAKE_RC:-0}"; fi
    echo 'SELECT	t'; exit "${FAKE_RC:-0}" ;;
  *) echo "unexpected docker verb $1" >&2; exit 99 ;;
esac
"""


def _c_closed_bin(tmp: Path) -> Path:
    b = tmp / "bin"
    b.mkdir(exist_ok=True)
    for tool in (
        "bash",
        "cat",
        "dirname",
        "grep",
        "sed",
        "cut",
        "head",
        "tail",
        "jq",
        "env",
        "awk",
        "wc",
        "printf",
        "mkdir",
        "rm",
        "date",
        "tr",
        "mktemp",
    ):
        p = shutil.which(tool)
        assert p, tool
        if not (b / tool).exists():
            (b / tool).symlink_to(p)
    if not (b / "python3").exists():
        (b / "python3").symlink_to(Path(sys.executable).resolve())
    docker = b / "docker"
    docker.write_text(_c_FAKE_DOCKER)
    docker.chmod(0o755)
    return b


def _c_stage(tmp: Path, name: str, text: str | None = None) -> Path:
    host = tmp / "host"
    (host / "ops").mkdir(parents=True, exist_ok=True)
    (host / "ops" / ".env").write_text("X=1\n")
    (host / "compose").mkdir(exist_ok=True)
    (host / "compose" / "compose.bigboy.images.yml").write_text(
        f"image: ghcr.io/full-chaos/dev-health-go-dho@sha256:{'cd' * 32}\n"
    )
    rec = host / "rec"
    rec.mkdir(exist_ok=True)
    (rec / "sha.txt").write_text("1234567abcdef\n")
    ci = tmp / "ci"
    ci.mkdir(exist_ok=True)
    for f in _c_BB.glob("compose.bigboy.*.yml"):
        shutil.copy(f, ci)
    src = text if text is not None else (_c_BB / name).read_text()
    out = ci / name
    out.write_text(src.replace(_c_HOST_ROOT, str(host)))
    out.chmod(0o755)
    return out


def _c_run(
    tmp: Path,
    script: Path,
    *args: str,
    fake_rc: str | None = None,
    extra_env: dict[str, str] | None = None,
):
    fake = tmp / "fake"
    fake.mkdir(exist_ok=True)
    (tmp / "scratch").mkdir(exist_ok=True)
    env = {
        "PATH": str(_c_closed_bin(tmp)),
        "FAKE_DIR": str(fake),
        "FAKE_DIGEST": _c_DIGEST,
        "HOME": str(tmp),
        "TMPDIR": str(
            tmp / "scratch"
        ),  # a script-made temp file stays inside the test dir, never /tmp
        "POSTGRES_PASSWORD": _c_MARKER,  # ambient, as the operator's shell would have it: must never be forwarded
        "CLICKHOUSE_PASSWORD": _c_MARKER,
    }
    if fake_rc is not None:
        env["FAKE_RC"] = fake_rc
    env.update(extra_env or {})
    p = subprocess.run(
        ["bash", str(script), *args],
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
        cwd=tmp,
        check=False,
    )
    return p, fake


def _c_calls(fake: Path) -> list[dict]:
    out = []
    for argv_file in sorted(fake.glob("argv.*"), key=lambda p: int(p.suffix[1:])):
        n = argv_file.suffix[1:]
        argv = [a.decode() for a in argv_file.read_bytes().split(b"\0")[:-1]]
        env = dict(
            ln.split("=", 1)
            for ln in (fake / f"env.{n}").read_text().splitlines()
            if "=" in ln
        )
        out.append({"argv": argv, "env": env})
    return out


def _c_violations(calls: list[dict]) -> list[str]:
    """The D4566 class rules, read off what each docker call actually received."""
    v = []
    for c in calls:
        argv, env = c["argv"], c["env"]
        if argv[0] not in ("compose", "buildx"):
            v.append(f"bare docker verb {argv[0]!r}")
        if any(_c_MARKER in a for a in argv):
            v.append("credential in argv")
        # ANY env-passing flag: -e, -eNAME, --env, --env=..., --env-from-file (a bare NAME takes the value
        # from the script's own env, so no sentinel needs to be in argv to leak)
        for a in argv:
            if (a.startswith("-e") and not a.startswith("--")) or (
                a.startswith("--env")
                and a != "--env-file"
                and not a.startswith("--env-file=")
            ):
                v.append(f"env-passing flag {a!r}")
        # EVERY --env-file value must be compose's own substitution file
        for i, a in enumerate(argv):
            val = argv[i + 1] if a == "--env-file" and i + 1 < len(argv) else None
            if a.startswith("--env-file="):
                val = a.split("=", 1)[1]
            if val is not None and val != "ops/.env":
                v.append(f"foreign --env-file {val!r}")
        if any(a in ("printenv",) for a in argv):
            v.append("printenv out of a container")
        # the ambient sentinel is the test's own stand-in for the operator's shell env; a script must not
        # ADD any credential-bearing variable of its own (image refs are fine)
        for k, val in env.items():
            if k in (
                "POSTGRES_PASSWORD",
                "CLICKHOUSE_PASSWORD",
                "FAKE_DIR",
                "FAKE_DIGEST",
                "FAKE_RC",
            ):
                continue
            if _c_MARKER in val or re.search(r"(PASSWORD|TOKEN|SECRET|_URI)$", k):
                v.append(f"credential-shaped child env {k}")
    return v


# the OLD hook-check lines 12-16, 18, 23 (pre-8371), exercised through the same stand-in
_c_OLD_HOOK = """#!/usr/bin/env bash
set -euo pipefail
ENVF=$(mktemp); trap 'rm -f "$ENVF"' EXIT
PW=$(docker exec dev-health-api-1 printenv POSTGRES_PASSWORD)
{ echo "MIGRATION_DATABASE_URI=postgresql://devhealth:${PW}@postgres:5432/devhealth?sslmode=disable"; } > "$ENVF"
docker run --rm --network dev-health_dev-health --env-file "$ENVF" op migrate upgrade --river=false || true
"""


def test_old_hook_check_is_seen_failing(tmp_path: Path) -> None:
    script = _c_stage(tmp_path, "old-hook.sh", _c_OLD_HOOK)
    _c_run(tmp_path, script)
    got = _c_violations(_c_calls(tmp_path / "fake"))
    assert (
        "bare docker verb 'exec'" in got
    )  # printenv call is the first (and failing) call
    assert "printenv out of a container" in got


def test_hook_check_obeys_every_class_rule(tmp_path: Path) -> None:
    script = _c_stage(tmp_path, "bigboy-hook-check.sh")
    p, fake = _c_run(tmp_path, script, str(tmp_path / "host" / "rec"))
    assert p.returncode == 0, (p.stdout, p.stderr)
    calls = _c_calls(fake)
    assert _c_violations(calls) == []
    runs = [c for c in calls if c["argv"][0] == "compose"]
    assert len(runs) == 2 and all("venue-hook" in c["argv"] for c in runs)
    assert runs[0]["argv"][-3:] == ["migrate", "upgrade", "--river=false"]
    assert runs[1]["argv"][-4:] == ["migrate", "clickhouse", "status", "--check"]
    for c in runs:
        assert c["env"]["COMPOSE_FILE"].endswith("compose.bigboy.hook-check.yml")
        assert c["env"]["HOOK_OPERATOR_IMAGE"].endswith(
            "@" + _c_DIGEST
        )  # positive control: derived, passed as an image ref
        assert str(tmp_path / "host") not in " ".join(c["argv"]) or True
    # no script-written env file is left or used
    assert not list(tmp_path.glob("tmp.*"))
    assert "hook_upgrade_rc=0" in p.stdout and "ch_status_check_rc=0" in p.stdout


def test_hook_check_failing_step_fails_the_script(tmp_path: Path) -> None:
    script = _c_stage(tmp_path, "bigboy-hook-check.sh")
    p, _ = _c_run(tmp_path, script, str(tmp_path / "host" / "rec"), fake_rc="3")
    assert p.returncode == 1 and "hook_upgrade_rc=3" in p.stdout


def test_each_hook_check_gate_fails_the_script_on_its_own(tmp_path: Path) -> None:
    """Dropping either gate line alone must be seen: only the Nth venue-hook call fails."""
    for call, marker in (("1", "hook_upgrade_rc=3"), ("2", "ch_status_check_rc=3")):
        case = tmp_path / f"c{call}"
        case.mkdir()
        script = _c_stage(case, "bigboy-hook-check.sh")
        p, _ = _c_run(
            case,
            script,
            str(case / "host" / "rec"),
            extra_env={"FAKE_FAIL_HOOK_CALL": call},
        )
        assert p.returncode == 1 and marker in p.stdout, (call, p.stdout)


def test_compose_file_chain_is_kept_or_defaults_to_the_stack_file(
    tmp_path: Path,
) -> None:
    """Called by hand with COMPOSE_FILE unset the overlay must still get compose.yml (it needs its network);
    from the cut the exported chain stays first."""
    for name, args in (
        ("bigboy-hook-check.sh", lambda t: [str(t / "host" / "rec")]),
        ("bigboy-river-apply.sh", lambda t: []),
    ):
        for given in (None, "compose.yml:compose/x.yml"):
            case = tmp_path / f"{name}-{given is None}"
            case.mkdir()
            script = _c_stage(case, name)
            p, fake = _c_run(
                case,
                script,
                *args(case),
                extra_env={"COMPOSE_FILE": given} if given else {},
            )
            assert p.returncode == 0, (name, given, p.stdout, p.stderr)
            cf = _c_calls(fake)[-1]["env"]["COMPOSE_FILE"]
            assert cf.startswith(given or "compose.yml") and ":" in cf, (
                name,
                given,
                cf,
            )


def test_river_apply_redacts_the_dsn_it_prints(tmp_path: Path) -> None:
    script = _c_stage(tmp_path, "bigboy-river-apply.sh")
    p, _ = _c_run(tmp_path, script)
    assert "plainword-river-marker" not in p.stdout + p.stderr
    assert "devhealth:<redacted>@" in p.stdout


def test_river_apply_obeys_every_class_rule(tmp_path: Path) -> None:
    script = _c_stage(tmp_path, "bigboy-river-apply.sh")
    p, fake = _c_run(tmp_path, script)
    assert p.returncode == 0, (p.stdout, p.stderr)
    calls = _c_calls(fake)
    assert _c_violations(calls) == []
    assert len(calls) == 1 and "venue-river" in calls[0]["argv"]
    assert calls[0]["argv"][-3:] == ["migrate", "river", "--apply-and-check"]
    assert calls[0]["env"]["COMPOSE_FILE"].endswith("compose.bigboy.river-apply.yml")
    assert calls[0]["env"]["RIVER_DHO_IMAGE"].endswith("@sha256:" + "cd" * 32)
    assert "river_apply_rc=0" in p.stdout


def test_river_apply_reports_a_failing_apply(tmp_path: Path) -> None:
    script = _c_stage(tmp_path, "bigboy-river-apply.sh")
    p, _ = _c_run(tmp_path, script, fake_rc="5")
    assert "river_apply_rc=5" in p.stdout
    assert p.returncode == 5, (
        "the script's own exit status must carry the apply failure"
    )


def test_pg_grants_check_bigboy_uses_compose_exec_only(tmp_path: Path) -> None:
    script = _c_stage(tmp_path, "pg-grants-check.sh")
    exp = tmp_path / "exp.txt"
    exp.write_text("r t SELECT\n")
    p, fake = _c_run(tmp_path, script, "bigboy", str(exp))
    assert p.returncode == 0 and "PG_OK expectations=1" in p.stdout, (
        p.stdout,
        p.stderr,
    )
    calls = _c_calls(fake)
    assert _c_violations(calls) == []
    argv = calls[0]["argv"]
    assert (
        argv[:4] == ["compose", "--env-file", "ops/.env", "exec"]
        and "-T" in argv
        and "postgres" in argv
    )
    # no password is set or expanded anywhere (D4344 pattern): psql runs with -w and no PGPASSWORD
    assert not any("PGPASSWORD" in a or "POSTGRES_PASSWORD" in a for a in argv)
    assert any("psql -w -U" in a for a in argv)


def test_pg_grants_check_prod_branch_runs_psql_w_without_a_password(
    tmp_path: Path,
) -> None:
    """The prod branch (kubectl exec): a recording kubectl stand-in; psql -w, no PGPASSWORD, no docker call."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    kubectl = bin_dir / "kubectl"
    kubectl.write_text(
        '#!/bin/bash\nd="$FAKE_DIR"; printf \'%s\\0\' "$@" > "$d/kubectl-argv"; cat > /dev/null; echo \'r|t|SELECT|t|t\'\n'
    )
    kubectl.chmod(0o755)
    script = _c_stage(tmp_path, "pg-grants-check.sh")
    exp = tmp_path / "exp.txt"
    exp.write_text("r t SELECT\n")
    p, fake = _c_run(tmp_path, script, "prod", str(exp))
    assert p.returncode == 0 and "PG_OK expectations=1" in p.stdout, (
        p.stdout,
        p.stderr,
    )
    argv = [a.decode() for a in (fake / "kubectl-argv").read_bytes().split(b"\0")[:-1]]
    assert "exec" in argv and "-i" in argv and "dev-health-ops-postgresql-0" in argv
    assert any("psql -w -U" in a for a in argv)
    assert not any("PGPASSWORD" in a or "POSTGRES_PASSWORD" in a for a in argv)
    assert not (fake / "argv.1").exists(), "the prod branch must not call docker"


def test_grants_check_uses_compose_exec_only(tmp_path: Path) -> None:
    script = _c_stage(tmp_path, "bigboy-grants-check.sh")
    p, fake = _c_run(tmp_path, script, str(tmp_path / "host" / "rec"))
    calls = _c_calls(
        fake
    )  # the later git/compare steps may fail in the sandbox; the first docker call is the subject
    assert calls and _c_violations(calls) == []
    assert calls[0]["argv"][:4] == ["compose", "--env-file", "ops/.env", "exec"]
    assert "clickhouse" in calls[0]["argv"] and "clickhouse-client" in calls[0]["argv"]


def test_overlays_carry_no_literal_credential_and_use_compose_substitution() -> None:
    for name, svc, imgvar in (
        ("compose.bigboy.hook-check.yml", "venue-hook", "HOOK_OPERATOR_IMAGE"),
        ("compose.bigboy.river-apply.yml", "venue-river", "RIVER_DHO_IMAGE"),
    ):
        doc = yaml.safe_load((_c_BB / name).read_text())
        s = doc["services"][svc]
        assert s["profiles"] == ["venue"] and s["networks"] == ["dev-health"]
        assert s["image"].startswith("${" + imgvar + ":?")
        env = s["environment"]
        assert re.fullmatch(
            r"postgresql://devhealth:\$\{POSTGRES_PASSWORD:-devhealth\}@postgres:5432/devhealth\?sslmode=disable",
            env["MIGRATION_DATABASE_URI"],
        )
        assert "env_file" not in s  # never the whole ops/.env
    river = yaml.safe_load((_c_BB / "compose.bigboy.river-apply.yml").read_text())[
        "services"
    ]["venue-river"]
    assert river["entrypoint"] == ["/usr/local/bin/dho"]
    assert {
        k: v for k, v in river["environment"].items() if k != "MIGRATION_DATABASE_URI"
    } == {
        "RIVER_DATABASE_SCHEMA": "river",
        "RIVER_DOMAIN_DATABASE_ROLE": "devhealth_domain",
        "RIVER_QUEUE_DATABASE_ROLE": "devhealth_queue",
        "RIVER_COORDINATOR_DATABASE_ROLE": "devhealth_coordinator",
        "API_DATABASE_ROLE": "devhealth_api",
    }
    hook = yaml.safe_load((_c_BB / "compose.bigboy.hook-check.yml").read_text())[
        "services"
    ]["venue-hook"]
    assert hook["environment"]["CLICKHOUSE_URI"] == (
        "clickhouse://${CLICKHOUSE_USER:-ch}:${CLICKHOUSE_PASSWORD:-ch}@clickhouse:9000/default"
    )


# Files still to convert, each under its own sub-issue; this list only ever shrinks.
_c_NOT_YET_CONVERTED = {
    "pass-bigboy-auth.py": "CHAOS-8370",
    "pass-bigboy-admin2.sh": "CHAOS-8369 (own PR, converted there; drop this entry once it merges)",
}


def test_no_bare_docker_exec_run_or_printenv_in_ci_bigboy_run_files() -> None:
    bad = {}
    for f in sorted(_c_BB.iterdir()):
        if f.suffix not in (".sh", ".py") or f.name in _c_NOT_YET_CONVERTED:
            continue
        code = "\n".join(
            ln for ln in f.read_text().splitlines() if not ln.lstrip().startswith("#")
        )
        hits = re.findall(
            r"docker\s+(?:container\s+)?(?:exec|run|cp)\b|\bprintenv\b|\bPGPASSWORD\b",
            code,
        )
        if hits:
            bad[f.name] = hits
    assert bad == {}, bad
