"""bigboy-cut.sh runs the ONE compose file, and the router settings are in it (CHAOS-7131, CHAOS-9962).

compose.bigboy.yml sets web's BACKEND_URL (the plane-split router) and AUTH_URL. The cut used to chain a router overlay and a
`up` built from a chain that omitted it left web without both names (REST calls 500'd, the auth callback used the wrong
origin). With one file there is no chain to omit it from; these tests resolve the real file, so removing the settings fails them.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CUT = ROOT / "ci" / "bigboy" / "bigboy-cut.sh"
REQUIRED = ROOT / "ci" / "bigboy" / "check-web-env-required.sh"


def test_the_cut_runs_the_one_compose_file_and_no_overlay_chain() -> None:
    """CHAOS-9962: the router settings, the ClickHouse users mount, the billing edge and every other overlay are part of
    ci/bigboy/compose.bigboy.yml; the cut names that one file and no other."""
    text = CUT.read_text()
    assert '-f "$HERE/compose.bigboy.yml"' in text
    assert text.count('-f "$HERE/compose.bigboy.yml"') == 1, (
        "the cut must hand compose exactly one -f"
    )
    assert "COMPOSE_FILE" not in text.replace("grep '^COMPOSE_'", "")
    for old in ("stack.sh", "stack.files", "compose.bigboy.router.yml"):
        assert old not in text.replace("# ", ""), old


def test_resolved_web_service_carries_backend_url_and_auth_url(tmp_path: Path) -> None:
    """Let `docker compose config` resolve the real file: web must come out with BACKEND_URL and AUTH_URL (CHAOS-7131: a web
    recreated without them lost the plane-split router and the auth origin)."""
    from tests.tooling import test_bigboy_one_compose_file as bb

    web_env = bb.services(bb.scratch_root(tmp_path))["web"]["environment"]
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
    if printf '%s\n' "$@" | grep -qx 'config'; then echo "ghcr.io/full-chaos/dev-health-go-dho@$FAKE_DIGEST"; exit 0; fi
    if printf '%s\n' "$@" | grep -qx 'ps'; then [ -n "${FAKE_NO_RECONCILER:-}" ] && exit 0; echo "ghcr.io/full-chaos/dev-health-go-operator@$FAKE_DIGEST"; exit 0; fi
    if printf '%s\n' "$@" | grep -qx 'venue-hook'; then
      h=$(( $(cat "$d/hook-n" 2>/dev/null || echo 0) + 1 )); echo "$h" > "$d/hook-n"
      echo '{"msg":"migrate step ok","time":"t"}'; echo '{"state":"clean"}'
      if [ -n "${FAKE_FAIL_HOOK_CALL:-}" ]; then [ "$h" = "$FAKE_FAIL_HOOK_CALL" ] && exit 3; exit 0; fi
      exit "${FAKE_RC:-0}"
    fi
    if printf '%s\n' "$@" | grep -qx 'venue-river'; then echo 'applying postgresql://devhealth:plainword-river-marker@postgres:5432/db then postgresql://other:plainword-second-marker@h:5432/x'; exit "${FAKE_RC:-0}"; fi
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
        "sort",
        "paste",
        "git",
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
    (host / "_records").mkdir(exist_ok=True)
    (host / "_records" / "images.env").write_text(
        f"BIGBOY_DHO_IMAGE=ghcr.io/full-chaos/dev-health-go-dho@sha256:{'cd' * 32}\n"
    )
    rec = host / "rec"
    rec.mkdir(exist_ok=True)
    (rec / "sha.txt").write_text("1234567abcdef\n")
    ci = tmp / "ci"
    ci.mkdir(exist_ok=True)
    for f in ("compose.bigboy.yml", "compose-config-redacted.sh"):
        shutil.copy(_c_BB / f, ci)
    (host / ".remember/lanes/team-lead").mkdir(parents=True, exist_ok=True)
    (host / ".remember/lanes/team-lead/reconciler-sweep-override.yml").write_text(
        "services: {}\n"
    )
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
        # EVERY --env-file value must be compose's own substitution file (ops/.env) or the image pins file (digests only)
        for i, a in enumerate(argv):
            val = argv[i + 1] if a == "--env-file" and i + 1 < len(argv) else None
            if a.startswith("--env-file="):
                val = a.split("=", 1)[1]
            if val is not None and not re.fullmatch(
                r"(?:/[^\s]*/)?(?:ops/\.env|_records/images\.env)", val
            ):
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
    runs = [c for c in calls if c["argv"][0] == "compose" and "venue-hook" in c["argv"]]
    assert len(runs) == 2 and all("run" in c["argv"] for c in runs)
    assert runs[0]["argv"][-3:] == ["migrate", "upgrade", "--river=false"]
    assert runs[1]["argv"][-4:] == ["migrate", "clickhouse", "status", "--check"]
    for c in runs:
        assert (
            "COMPOSE_FILE" not in c["env"]
        )  # CHAOS-9962: the one file is a -f argument
        assert any(a.endswith("/compose.bigboy.yml") for a in c["argv"])
        assert "venue-hook" in c["argv"]  # the one-off has its own profile
        assert c["env"]["HOOK_OPERATOR_IMAGE"].endswith(
            "@" + _c_DIGEST
        )  # positive control: derived, passed as an image ref
        assert str(tmp_path / "host" / "rec") not in " ".join(
            c["argv"]
        )  # no record path in the one-off's argv
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


def test_an_inherited_compose_file_never_decides_the_chain(tmp_path: Path) -> None:
    """CHAOS-8693: hook-check and river-apply run the one compose file whatever COMPOSE_FILE the caller exported."""
    for name, args in (
        ("bigboy-hook-check.sh", lambda t: [str(t / "host" / "rec")]),
        ("bigboy-river-apply.sh", lambda t: []),
    ):
        case = tmp_path / name
        case.mkdir()
        script = _c_stage(case, name)
        p, fake = _c_run(
            case,
            script,
            *args(case),
            extra_env={"COMPOSE_FILE": "compose.yml:compose/x.yml"},
        )
        assert p.returncode == 0, (name, p.stdout, p.stderr)
        last = _c_calls(fake)[-1]
        assert "COMPOSE_FILE" not in last["env"], name
        files = [last["argv"][i + 1] for i, a in enumerate(last["argv"]) if a == "-f"]
        assert len(files) == 1 and files[0].endswith("/compose.bigboy.yml"), (
            name,
            files,
        )
        assert (
            "--profile" in last["argv"]
            and "venue" in last["argv"]
            and any(
                profile in last["argv"] for profile in ("venue-hook", "venue-river")
            )
        ), name


def test_river_apply_by_hand_needs_no_running_reconciler(tmp_path: Path) -> None:
    """Standalone (BIGBOY_OPERATOR_IMAGE unset, no go-reconciler running) the apply still runs: the one-off starts
    no worker (--no-deps), so the worker overlay's image reference only has to interpolate."""
    script = _c_stage(tmp_path, "bigboy-river-apply.sh")
    p, fake = _c_run(tmp_path, script, extra_env={"FAKE_NO_RECONCILER": "1"})
    assert p.returncode == 0, (p.stdout, p.stderr)
    assert [c for c in _c_calls(fake) if "venue-river" in c["argv"]]
    assert "river_apply_rc=0" in p.stdout


def test_river_apply_redacts_the_dsn_it_prints(tmp_path: Path) -> None:
    script = _c_stage(tmp_path, "bigboy-river-apply.sh")
    p, _ = _c_run(tmp_path, script)
    assert "plainword-river-marker" not in p.stdout + p.stderr
    assert (
        "plainword-second-marker" not in p.stdout + p.stderr
    )  # BOTH DSNs of one line (the sed needs its g)
    assert "devhealth:<redacted>@" in p.stdout


def test_river_apply_obeys_every_class_rule(tmp_path: Path) -> None:
    script = _c_stage(tmp_path, "bigboy-river-apply.sh")
    p, fake = _c_run(tmp_path, script)
    assert p.returncode == 0, (p.stdout, p.stderr)
    calls = _c_calls(fake)
    assert _c_violations(calls) == []
    runs = [c for c in calls if "venue-river" in c["argv"]]
    assert len(runs) == 1
    assert runs[0]["argv"][-3:] == ["migrate", "river", "--apply-and-check"]
    assert "COMPOSE_FILE" not in runs[0]["env"]
    assert any(a.endswith("/compose.bigboy.yml") for a in runs[0]["argv"])
    assert "venue-river" in runs[0]["argv"]
    assert (
        "image=sha256:" + "cd" * 32 in p.stdout
    )  # the dho pin of _records/images.env, as an image reference only
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
    argv = [c for c in calls if "exec" in c["argv"]][0]["argv"]
    assert (
        argv[:3] == ["compose", "-p", "dev-health"]
        and argv[argv.index("--env-file") + 1] == "ops/.env"
    )
    assert argv.index("exec") < argv.index("-T") < argv.index("postgres")
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
    )  # the later git/compare steps may fail in the sandbox; the exec call is the subject
    assert calls and _c_violations(calls) == []
    ex = [c for c in calls if "exec" in c["argv"]][0]["argv"]
    assert (
        ex[:3] == ["compose", "-p", "dev-health"]
        and ex[ex.index("--env-file") + 1] == "ops/.env"
    )
    assert "clickhouse" in ex and "clickhouse-client" in ex and "-T" in ex


def _c_env(service: dict) -> dict:
    env = service["environment"]
    return dict(i.split("=", 1) for i in env) if isinstance(env, list) else env


def test_one_offs_carry_no_literal_credential_and_use_compose_substitution() -> None:
    doc = yaml.safe_load((_c_BB / "compose.bigboy.yml").read_text())
    for svc, image_prefix in (
        ("venue-hook", "${HOOK_OPERATOR_IMAGE:-"),
        ("venue-river", "${BIGBOY_DHO_IMAGE:?"),
    ):
        s = doc["services"][svc]
        assert s["profiles"] == [svc] and list(s["networks"]) == ["dev-health"]
        assert s["image"].startswith(image_prefix), s["image"]
        assert re.fullmatch(
            r"postgresql://devhealth:\$\{POSTGRES_PASSWORD:-devhealth\}@postgres:5432/devhealth\?sslmode=disable",
            _c_env(s)["MIGRATION_DATABASE_URI"],
        )
        assert "env_file" not in s  # never the whole ops/.env
    river = doc["services"]["venue-river"]
    assert river["entrypoint"] == ["/usr/local/bin/dho"]
    assert {
        k: v for k, v in _c_env(river).items() if k != "MIGRATION_DATABASE_URI"
    } == {
        "RIVER_DATABASE_SCHEMA": "river",
        "RIVER_DOMAIN_DATABASE_ROLE": "devhealth_domain",
        "RIVER_QUEUE_DATABASE_ROLE": "devhealth_queue",
        "RIVER_COORDINATOR_DATABASE_ROLE": "devhealth_coordinator",
        "API_DATABASE_ROLE": "devhealth_api",
    }
    hook = doc["services"]["venue-hook"]
    assert _c_env(hook)["CLICKHOUSE_URI"] == (
        "clickhouse://${CLICKHOUSE_USER:-ch}:${CLICKHOUSE_PASSWORD:-ch}@clickhouse:9000/default"
    )


# Files still to convert, each under its own sub-issue; this list only ever shrinks. It is empty:
# the two pass scripts it held ran inside the Python api container and are deleted (CHAOS-8361).
_c_NOT_YET_CONVERTED: dict[str, str] = {}


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


# ---- CHAOS-8371 second-vet delta: pin the one-off's shape and the credential rule on every line of the five scripts ----

_C_SCRIPTS = (
    "bigboy-hook-check.sh",
    "bigboy-river-apply.sh",
    "pg-grants-check.sh",
    "bigboy-grants-check.sh",
    "bigboy-graphql-prove.sh",
)


def _c_code(name: str) -> list[str]:
    return [
        ln
        for ln in (_c_BB / name).read_text().splitlines()
        if not ln.lstrip().startswith("#")
    ]


def test_no_env_passing_flag_on_any_docker_line_and_no_password_expansion() -> None:
    for name in _C_SCRIPTS:
        for ln in _c_code(name):
            if "docker " in ln or "HC=(" in ln:
                # the one allowed name-only -e: PROVE_ORG is the local org's uuid (read-only data), not a credential
                ln = ln.replace(" -e PROVE_ORG ", " ")
                assert not re.search(r"(?<=\s)-e[A-Za-z_=]*(?=\s|$)", ln), (name, ln)
                assert not re.search(r"--env(?!-file)", ln), (name, ln)
            # no *PASSWORD* expansion into anything in a code line (a script variable, an argument, a file)
            assert not re.search(r"\$\{?[A-Za-z_]*PASSWORD", ln), (name, ln)
            assert not re.search(r"(?i)\bpw\w*=", ln), (name, ln)


def test_graphql_prove_psql_line_is_a_compose_exec_on_the_stack_files() -> None:
    line = next(ln for ln in _c_code("bigboy-graphql-prove.sh") if "PROVE_ORG=$(" in ln)
    assert '"${DC[@]}" --profile venue exec -T postgres psql -U devhealth' in line, line


def test_the_one_offs_keep_their_compose_shape(tmp_path: Path) -> None:
    """-p dev-health, --env-file ops/.env, --profile <the one-off's own profile>, run --rm --no-deps -T on the one-off service: pinned for BOTH."""
    for name, service, args in (
        ("bigboy-hook-check.sh", "venue-hook", True),
        ("bigboy-river-apply.sh", "venue-river", False),
    ):
        case = tmp_path / name
        case.mkdir()
        script = _c_stage(case, name)
        p, fake = _c_run(case, script, *([str(case / "host" / "rec")] if args else []))
        assert p.returncode == 0, (name, p.stdout, p.stderr)
        runs = [c for c in _c_calls(fake) if service in c["argv"]]
        assert runs, name
        for c in runs:
            a = c["argv"]
            assert a[:3] == ["compose", "-p", "dev-health"], (name, a)
            assert a[a.index("--env-file") + 1] == "ops/.env", (name, a)
            for needed in (
                "--profile",
                service,
                "run",
                "--rm",
                "--no-deps",
                "-T",
                service,
            ):
                assert needed in a, (name, needed, a)
            profiles = [a[i + 1] for i, x in enumerate(a) if x == "--profile"]
            assert profiles == ["venue", service], (name, profiles)
