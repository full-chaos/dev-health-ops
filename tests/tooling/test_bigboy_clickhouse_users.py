"""CHAOS-7162: bigboy declares dho_api_ch durably and the cut checks it before go-api is recreated.

The user used to be `docker cp`ed into the running ClickHouse container; a recreate rebuilt users.d from
the image, the user was lost and go-api failed every ClickHouse call with code 516. It is now a host file
mounted into users.d, and a 0600 host-user-owned file would make clickhouse-server (uid 101) exit, so the
check refuses one. Nothing here prints or asserts on a credential value: only names, modes and a hash.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import os
import shutil
import stat
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "ci" / "bigboy"
OVERLAY = TOOLS / "compose.bigboy.clickhouse-users.yml"
CHECK = TOOLS / "check-dho-api-ch-user.sh"
RENDER = TOOLS / "render-dho-api-ch-users.py"
CUT = TOOLS / "bigboy-cut.sh"

_PASSWORD = "the-api-password"
_CUT_PASSWORD = "the-cut-test-password"
_HASH = hashlib.sha256(_PASSWORD.encode()).hexdigest()

_MANIFEST = """package clickhouse

// func APIPosture is quoted here in comments: password="leaked" clickhouse://u:p@h/db
func APIPosture(database string) Posture {
	return Posture{RequiredTables: []TableGrant{
		{Database: database, Table: "teams", AllowInsert: true, AllowSelect: true, AllowDelete: true},
		// marker-comment: <password>plain</password> Table: "hidden"
		{Database: database, Table: "team_sync_policies", AllowSelect: true},
		{Database: database, Table: "team_memberships", AllowInsert: true},
		{Database: database, Table: "no_privileges"},
	}}
}

func other() {
	_ = TableGrant{Database: database, Table: "outside", AllowSelect: true}
}
"""


def _load_renderer():
    spec = importlib.util.spec_from_file_location("dho_api_ch_render", RENDER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


#: The canonical render of the fixture manifest for the fixture credential: the ONLY file the check accepts.
_XML = _load_renderer().render(
    _load_renderer().grants_from_manifest(_MANIFEST), _PASSWORD
)


def _users_file(tmp_path: Path, *, mode: int = 0o644, body: str = _XML) -> Path:
    file = tmp_path / "dho_api_ch.xml"
    file.write_text(body)
    file.chmod(mode)
    return file


_SCRATCHES: list[str] = []

#: Every external command the check runs. Each gets a PATH shim that records argv and environment, and PATH holds
#: nothing else: a command the check starts that is not listed fails the run (rc != 0), so the set cannot go stale.
_CHECK_COMMANDS = (
    "stat id sed tail tr wc cut awk cmp head dirname mktemp chmod rm sha256sum python3 cat"
).split()


@pytest.fixture(autouse=True)
def _remove_scratches():
    yield
    while _SCRATCHES:
        shutil.rmtree(_SCRATCHES.pop(), ignore_errors=True)


def _scratch(*, mode: int = 0o700) -> Path:
    """A private TMPDIR for the check, outside /tmp (the check refuses /tmp, so pytest's tmp_path cannot be it)."""
    base = "/dev/shm" if os.access("/dev/shm", os.W_OK) else str(ROOT / "tests")
    path = tempfile.mkdtemp(prefix="dho-check-scratch-", dir=base)
    os.chmod(path, mode)
    _SCRATCHES.append(path)
    return Path(path)


def _docker(
    tmp_path: Path,
    *,
    count: str | None,
    rc: int = 0,
    login_ok: bool = True,
    password: str = "the-api-password",
) -> dict[str, str]:
    """PATH shims for the whole check. `docker` is a stub of `docker compose ... exec ... clickhouse-client`:
    it answers the system.users count and logs in only when the config on its STDIN carries `password` (that
    stdin is also saved, so the one channel the credential may use is observable). Every other command the
    check runs is a recording shim around the real binary. All shims append argv (one word per line) to
    child-argv.log and their environment to child-env.log."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    args_log = tmp_path / "docker-args.log"
    argv_log = tmp_path / "child-argv.log"
    env_log = tmp_path / "child-env.log"
    stdin_log = tmp_path / "docker-stdin.log"
    for name in _CHECK_COMMANDS:
        real = shutil.which(name, path="/usr/bin:/bin")
        assert real, name
        shim = bin_dir / name
        shim.write_text(
            "#!/bin/bash\n"
            f'{{ echo "== {name}"; printf "%s\\n" "$@"; }} >> "{argv_log}"\n'
            f'{{ echo "== {name}"; /usr/bin/env; }} >> "{env_log}"\n'
            f'exec {real} "$@"\n'
        )
        shim.chmod(0o755)
    answer = f'echo "{count}"' if count is not None else "true"
    login = (
        'if [ "$(cat "'
        + str(stdin_log)
        + '")" = "<clickhouse><user>dho_api_ch</user><password>'
        + password
        + '</password></clickhouse>" ]; then echo dho_api_ch; exit 0; fi; exit 1'
        if login_ok
        else "exit 1"
    )
    docker = bin_dir / "docker"
    docker.write_text(
        "#!/bin/bash\n"
        "PATH=/usr/bin:/bin\n"
        f'echo "$*" >> "{args_log}"\n'
        f'{{ echo "== docker"; printf "%s\\n" "$@"; }} >> "{argv_log}"\n'
        f'{{ echo "== docker"; env; }} >> "{env_log}"\n'
        'case "$*" in *"--config-file /dev/stdin"*) cat > "'
        + str(stdin_log)
        + '" ;; esac\n'
        'case "$*" in *"--user dho_api_ch"*|*"--config-file /dev/stdin"*)\n'
        f"  {login} ;;\n"
        "esac\n"
        f"{answer}\nexit {rc}\n"
    )
    docker.chmod(0o755)
    return {"PATH": str(bin_dir)}


def _check(
    tmp_path: Path,
    file: Path | None,
    *,
    count: str | None = "1",
    rc: int = 0,
    login_ok: bool = True,
    api_password: str | None = "the-api-password",
    creds_line: str | None = None,
    scratch: Path | None = None,
    tmpdir: str | None = None,
    script: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run the check with a bare environment (no inherited variable), PATH holding only shims, and xtrace going
    to a 0600 file (tmp_path/xtrace.log). The scratch dir used is returned in proc.scratch."""
    env = _docker(tmp_path, count=count, rc=rc, login_ok=login_ok)
    scratch = scratch or _scratch()
    env["TMPDIR"] = tmpdir if tmpdir is not None else str(scratch)
    creds = tmp_path / "go-api.creds"
    creds.write_text(
        creds_line
        if creds_line is not None
        else (
            f"API_CH_PASSWORD='{api_password}'\n"
            if api_password is not None
            else "OTHER=1\n"
        )
    )
    creds.chmod(0o600)
    (tmp_path / "authorization.go").write_text(_MANIFEST)
    env["DHO_API_CH_POSTURE_GO"] = str(tmp_path / "authorization.go")
    env["XT"] = str(tmp_path / "xtrace.log")
    args = [str(file), str(creds)] if file is not None else []
    proc = subprocess.run(
        [
            "/bin/bash",
            "-c",
            'umask 077; exec 9>"$XT"; BASH_XTRACEFD=9 exec /bin/bash -x "$0" "$@"',
            str(script or CHECK),
            *args,
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )
    proc.scratch = scratch  # type: ignore[attr-defined]
    return proc


def test_a_readable_declared_file_and_a_live_user_pass(tmp_path: Path) -> None:
    proc = _check(tmp_path, _users_file(tmp_path))
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    assert "ch_api_user_file=ok" in proc.stdout and "ch_api_user_live=ok" in proc.stdout
    assert "ch_api_user_auth=ok" in proc.stdout
    assert _HASH not in proc.stdout + proc.stderr


def test_a_0600_file_is_refused_because_clickhouse_would_exit_on_it(
    tmp_path: Path,
) -> None:
    proc = _check(tmp_path, _users_file(tmp_path, mode=0o600))
    assert proc.returncode == 1, (proc.stdout, proc.stderr)
    assert "not world-readable" in proc.stderr and "uid 101" in proc.stderr
    assert _HASH not in proc.stdout + proc.stderr


def test_a_missing_file_or_a_directory_is_refused(tmp_path: Path) -> None:
    assert _check(tmp_path, tmp_path / "absent.xml").returncode == 1
    directory = tmp_path / "made-by-docker"
    directory.mkdir()
    proc = _check(tmp_path, directory)
    assert proc.returncode == 1 and "not a regular file" in proc.stderr
    assert _check(tmp_path, None).returncode == 1


def test_a_user_missing_from_the_live_server_fails_with_its_own_code(
    tmp_path: Path,
) -> None:
    file = _users_file(tmp_path)
    for count, rc in (("0", 0), (None, 1)):
        proc = _check(tmp_path, file, count=count, rc=rc)
        assert proc.returncode == 2, (count, rc, proc.stdout, proc.stderr)
        assert "CH_API_USER_LIVE_FAIL" in proc.stderr and "516" in proc.stderr


def test_the_overlay_mounts_the_host_file_into_users_d_and_requires_the_path(
    tmp_path: Path,
) -> None:
    base = tmp_path / "compose.yml"
    base.write_text("services:\n  clickhouse:\n    image: clickhouse:test\n")
    users = _users_file(tmp_path)
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": str(tmp_path)}
    argv = ["docker", "compose", "-f", str(base), "-f", str(OVERLAY), "config"]
    ok = subprocess.run(
        [*argv, "--format", "json"],
        capture_output=True,
        text=True,
        env={**env, "DHO_API_CH_USERS_XML": str(users)},
        timeout=60,
    )
    assert ok.returncode == 0, ok.stderr
    (volume,) = json.loads(ok.stdout)["services"]["clickhouse"]["volumes"]
    assert volume["source"] == str(users), volume
    assert volume["target"] == "/etc/clickhouse-server/users.d/dho_api_ch.xml"
    assert volume["read_only"] is True
    missing = subprocess.run(argv, capture_output=True, text=True, env=env, timeout=60)
    assert missing.returncode != 0
    assert "DHO_API_CH_USERS_XML" in missing.stderr


def test_the_cut_checks_dho_api_ch_before_it_recreates_go_api() -> None:
    lines = CUT.read_text().splitlines()
    check = next(
        i for i, line in enumerate(lines) if "check-dho-api-ch-user.sh" in line
    )
    up = next(
        i
        for i, line in enumerate(lines)
        if "up -d --no-deps --no-build query-api go-api web" in line
    )
    assert check < up, "the dho_api_ch check must run before go-api is recreated"
    gate = "\n".join(lines[check : check + 3])
    assert "exit 1" in gate and "ABORTING before go-api" in gate, gate
    chain = next(line for line in lines if line.startswith("export COMPOSE_FILE="))
    assert "$HERE/compose.bigboy.clickhouse-users.yml" in chain
    assert chain.rstrip().endswith("$HERE/compose.bigboy.router.yml"), chain
    assert any(line.startswith("export DHO_API_CH_USERS_XML=") for line in lines)


def test_a_user_that_cannot_log_in_with_the_api_credential_is_refused(
    tmp_path: Path,
) -> None:
    """Present in system.users is not usable: the declared hash must match the credential go-api uses."""
    file = _users_file(tmp_path)
    wrong = _check(tmp_path, file, login_ok=False)
    assert wrong.returncode == 3, (wrong.stdout, wrong.stderr)
    assert "CH_API_USER_AUTH_FAIL" in wrong.stderr and "516" in wrong.stderr
    unset = _check(tmp_path, file, api_password=None)
    assert unset.returncode == 3 and "no API_CH_PASSWORD value" in unset.stderr


def test_the_api_credential_is_only_ever_on_dockers_stdin(tmp_path: Path) -> None:
    """CHAOS-8382 guard, generic: the check runs with a bare env, every command it starts is a recording shim,
    and it runs under xtrace. The secret may appear nowhere (any argv, any child env, the xtrace, the output)
    except the stdin of docker, which is the one channel. Planted forms (each goes RED): password in a child
    env (export / VAR=... cmd), in another child's argv, in a shell variable (xtrace), in `docker -e`."""
    secret = "the-api-password"
    proc = _check(tmp_path, _users_file(tmp_path))
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    argv = (tmp_path / "child-argv.log").read_text()
    child_env = (tmp_path / "child-env.log").read_text()
    xtrace = (tmp_path / "xtrace.log").read_text()
    stdin = (tmp_path / "docker-stdin.log").read_text()
    assert stat.S_IMODE((tmp_path / "xtrace.log").stat().st_mode) == 0o600
    assert "== docker" in argv and "== sed" in argv and "== sha256sum" in argv
    assert "+ " in xtrace and "api_password" in xtrace, (
        "xtrace is empty: this test would prove nothing"
    )
    assert secret in stdin, "the login config never reached docker's stdin"
    for name, text in (
        ("argv", argv),
        ("child env", child_env),
        ("xtrace", xtrace),
        ("stdout", proc.stdout),
        ("stderr", proc.stderr),
        ("docker-args", (tmp_path / "docker-args.log").read_text()),
    ):
        assert secret not in text, f"credential in {name}"
    assert "CLICKHOUSE_PASSWORD" not in child_env + argv
    # the renderer's env may name API_CH_PASSWORD, but only with the placeholder (not a credential)
    for line in child_env.splitlines():
        if line.startswith("API_CH_PASSWORD="):
            assert line == "API_CH_PASSWORD=placeholder-not-a-credential", (
                "real password in a child env"
            )
    args = (tmp_path / "docker-args.log").read_text()
    assert "--config-file /dev/stdin" in args and " -e " not in args, args
    for line in (
        args.splitlines()
    ):  # compose verbs on the service, never a bare `docker exec` on a container name
        assert line.startswith("compose --env-file "), line
        assert " exec -T " in line and " clickhouse clickhouse-client " in line, line


def test_the_scratch_is_made_under_the_callers_tmpdir_with_mode_0700(
    tmp_path: Path,
) -> None:
    scratch = _scratch()
    proc = _check(tmp_path, _users_file(tmp_path), scratch=scratch)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    argv = (tmp_path / "child-argv.log").read_text().split("== ")
    mktemp = next(x for x in argv if x.startswith("mktemp\n")).split("\n")
    assert mktemp[1:4] == ["-d", "-p", str(scratch.resolve())], mktemp
    chmod = [x.split("\n") for x in argv if x.startswith("chmod\n")]
    assert chmod and all(
        c[1] == "700" and c[2].startswith(str(scratch.resolve()) + "/") for c in chmod
    ), chmod
    assert [p.name for p in scratch.iterdir()] == [], "scratch not cleaned"


def test_a_tmpdir_in_or_resolving_into_tmp_or_not_private_is_refused(
    tmp_path: Path,
) -> None:
    file = _users_file(tmp_path)
    under_tmp = Path(tempfile.mkdtemp(prefix="dho-sub-", dir="/tmp"))
    os.chmod(under_tmp, 0o700)
    link = tmp_path / "link-to-tmp"
    link.symlink_to("/tmp")
    shm_open = _scratch(mode=0o750)  # not 0700: group-readable
    holder = _scratch()
    (holder / "lnk").symlink_to(
        under_tmp
    )  # a symlink OUTSIDE /tmp that resolves into /tmp (0700, ours)
    try:
        spellings = [
            "/tmp//", "//tmp", "/tmp/.", "/tmp", "/var/tmp", str(under_tmp),
            str(under_tmp) + "/../" + under_tmp.name, str(link),
            str(holder / "lnk"),
            # a `..` path from outside /tmp that lands in /tmp (only realpath catches it)
            str(holder) + "/" + "../" * len(holder.parts[1:]) + "tmp/" + under_tmp.name,
            "/" + str(under_tmp), str(link) + "/",
            str(tmp_path / "absent"), "", str(shm_open),
        ]  # fmt: skip
        for tmpdir in spellings:
            proc = _check(tmp_path, file, tmpdir=tmpdir)
            assert proc.returncode == 1 and "TMPDIR" in proc.stderr, (
                tmpdir,
                proc.stderr,
            )
            assert not (tmp_path / "docker-args.log").exists(), tmpdir
        env_unset = _check(tmp_path, file, tmpdir="")  # TMPDIR set empty
        assert env_unset.returncode == 1
    finally:
        shutil.rmtree(under_tmp, ignore_errors=True)


def test_the_cut_hands_the_credentials_path_to_the_check_and_the_password_to_no_child(
    tmp_path: Path,
) -> None:
    """Executed: the real cut with a secret-bearing creds file under its root, a fake check and a docker stub that
    both log their environment. The secret must be in no env (so no env prefix, `set -a`, `allexport` or export
    reaches a child) and not in the check's argv; the check gets the creds PATH as arg 2."""
    proc, calls = _cut_with_a_fake_check(tmp_path, 0)
    assert "STEP ch-api-user rc=0" in proc.stdout, (proc.stdout, proc.stderr)
    check_log = (tmp_path / "fake-check.log").read_text()
    assert "args=" in check_log and "go-api.creds" in check_log, check_log
    secret = _CUT_PASSWORD
    for log in ("fake-check.log", "docker-env.log"):
        text = (tmp_path / log).read_text()
        assert text.strip(), f"{log} is empty: this test would prove nothing"
        assert secret not in text, f"credential in {log}"
    assert secret not in proc.stdout + proc.stderr


def _cut_with_a_fake_check(
    tmp_path: Path, check_rc: int
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    """Run the real cut end to end past the carry block with a tools directory whose check is a fake
    returning `check_rc`; the stub docker logs every call so a `compose ... up` is observable."""
    entry = importlib.import_module("tests.tooling.test_bigboy_cut_entry_point")
    root = entry._build_bigboy_root(tmp_path)
    tools = tmp_path / "tools"
    tools.mkdir()
    for source in TOOLS.iterdir():
        if source.is_file() and source.name != CHECK.name:
            (tools / source.name).symlink_to(source)
    fake = tools / CHECK.name
    root.joinpath(".go-api-dev").mkdir(exist_ok=True)
    creds = root / ".go-api-dev" / "go-api.creds"
    creds.write_text(f"API_CH_PASSWORD='{_CUT_PASSWORD}'\n")
    creds.chmod(0o600)
    fake.write_text(
        "#!/usr/bin/env bash\necho fake-check\n"
        f'{{ echo "args=$*"; env; }} >> "{tmp_path / "fake-check.log"}"\n'
        f"exit {check_rc}\n"
    )
    fake.chmod(0o755)
    stub_bin = tmp_path / "stubbin"
    stub_bin.mkdir()
    entry._docker_stub(
        stub_bin, routing_response=entry._routing_json("digest_unchanged"), routing_rc=0
    )
    entry._gh_stub(stub_bin)
    docker_stub = stub_bin / "docker"
    lines = docker_stub.read_text().splitlines(keepends=True)
    lines.insert(1, f'env >> "{tmp_path / "docker-env.log"}"\n')
    docker_stub.write_text("".join(lines))
    args_log = tmp_path / "docker-args.log"
    proc = subprocess.run(
        ["bash", str(CUT), entry._OLD8, entry._NEW],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={
            "PATH": f"{stub_bin}:/usr/bin:/bin",
            "BIGBOY_ROOT": str(root),
            "BIGBOY_TOOLS_DIR": str(tools),
            "DOCKER_STUB_ARGS_LOG": str(args_log),
        },
    )
    calls = args_log.read_text().splitlines() if args_log.exists() else []
    return proc, calls


def test_the_cut_aborts_before_go_api_up_when_the_check_fails(tmp_path: Path) -> None:
    """Executed, not read: with the check returning 2 the cut prints its STEP, exits non-zero and never
    issues the compose `up` that recreates go-api."""
    proc, calls = _cut_with_a_fake_check(tmp_path, 2)
    assert "STEP ch-api-user rc=2" in proc.stdout, (proc.stdout, proc.stderr)
    assert proc.returncode != 0
    assert "ABORTING before go-api is recreated" in proc.stderr
    assert not [c for c in calls if " up " in f" {c} " and "go-api" in c], calls
    assert "STEP up rc=" not in proc.stdout


def test_the_cut_goes_on_to_recreate_go_api_when_the_check_passes(
    tmp_path: Path,
) -> None:
    proc, calls = _cut_with_a_fake_check(tmp_path, 0)
    assert "STEP ch-api-user rc=0" in proc.stdout, (proc.stdout, proc.stderr)
    assert "STEP up rc=" in proc.stdout, proc.stdout
    assert [c for c in calls if " up " in f" {c} " and "go-api" in c], calls


def _manifest(tmp_path: Path, text: str = _MANIFEST) -> Path:
    file = tmp_path / "authorization.go"
    file.write_text(text)
    return file


def _render(tmp_path: Path, manifest: Path, **env: str):
    return subprocess.run(
        ["python3", str(RENDER), str(manifest), str(tmp_path / "out.xml")],
        capture_output=True,
        text=True,
        env={"PATH": os.environ["PATH"], **env},
        timeout=30,
    )


def test_render_output_is_exactly_the_fixed_table_with_the_hash_and_the_manifest_grants(
    tmp_path: Path,
) -> None:
    """Whole-output equality: any element, attribute, comment or text beyond the table would differ."""
    password = "the-api-password"
    proc = _render(tmp_path, _manifest(tmp_path), API_CH_PASSWORD=password)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    out = tmp_path / "out.xml"
    assert stat.S_IMODE(out.stat().st_mode) == 0o644
    digest = hashlib.sha256(password.encode()).hexdigest()
    assert out.read_text() == (
        "<clickhouse><users><dho_api_ch><networks><ip>::/0</ip></networks>"
        "<profile>default</profile><quota>default</quota>"
        "<access_management>0</access_management>"
        f"<password_sha256_hex>{digest}</password_sha256_hex><grants>"
        "<query>GRANT SELECT, INSERT, ALTER DELETE ON default.teams</query>"
        "<query>GRANT SELECT ON default.team_sync_policies</query>"
        "<query>GRANT INSERT ON default.team_memberships</query>"
        "</grants></dho_api_ch></users></clickhouse>\n"
    )
    assert password not in proc.stdout + proc.stderr and digest not in proc.stdout
    assert "grants=3" in proc.stdout
    assert _check(tmp_path, out).returncode == 0


def test_no_text_of_the_manifest_outside_its_table_rows_reaches_the_output(
    tmp_path: Path,
) -> None:
    """There is no channel from the input to the file except the (validated) table rows."""
    proc = _render(tmp_path, _manifest(tmp_path), API_CH_PASSWORD="pw-1")
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    text = (tmp_path / "out.xml").read_text()
    for banned in (
        "leaked",
        "clickhouse://",
        "marker",
        "plain",
        "hidden",
        "outside",
        "<!--",
        "<?",
    ):
        assert banned not in text, banned
    assert "=" not in text  # no attribute anywhere


def test_the_real_manifest_renders_one_grant_per_table_with_a_privilege(
    tmp_path: Path,
) -> None:
    source = ROOT / "internal" / "storage" / "clickhouse" / "authorization.go"
    text = source.read_text()
    body = text.split("func APIPosture(", 1)[1].split("\n}\n", 1)[0]
    rows = [
        line
        for line in body.splitlines()
        if "{Database: database, Table:" in line
        and any(
            f"{flag}: true" in line
            for flag in ("AllowSelect", "AllowInsert", "AllowDelete")
        )
    ]
    assert rows, "the manifest could not be read: this test would prove nothing"
    proc = _render(tmp_path, source, API_CH_PASSWORD="pw-1")
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    out = (tmp_path / "out.xml").read_text()
    assert out.count("<query>") == len(rows)
    assert "<query>GRANT SELECT, INSERT, ALTER DELETE ON default.teams</query>" in out


def test_render_refuses_when_the_password_would_appear_in_the_output(
    tmp_path: Path,
) -> None:
    """The password 'default' is also the profile name: the file would then hold the credential."""
    proc = _render(tmp_path, _manifest(tmp_path), API_CH_PASSWORD="default")
    assert proc.returncode != 0, (proc.stdout, proc.stderr)
    assert "nothing written" in proc.stderr
    assert not (tmp_path / "out.xml").exists()
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".out.xml")]


def test_render_refuses_missing_inputs_and_an_unreadable_manifest(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    assert _render(tmp_path, manifest).returncode == 2
    assert (
        _render(tmp_path, tmp_path / "absent.go", API_CH_PASSWORD="x").returncode == 2
    )
    for name, text in (
        ("no posture", "package clickhouse\n"),
        ("no rows", "func APIPosture(d string) {\n}\n"),
    ):
        proc = _render(tmp_path, _manifest(tmp_path, text), API_CH_PASSWORD="x")
        assert proc.returncode != 0 and "nothing written" in proc.stderr, name
        assert not (tmp_path / "out.xml").exists(), name


def test_rendered_grants_equal_the_golden_the_go_manifest_test_is_pinned_to(
    tmp_path: Path,
) -> None:
    """Two implementations of one grant list (Go GrantStatements(APIPosture), this renderer) are pinned to
    the SAME checked-in file, exact order, so neither can drift alone."""
    golden = (
        (
            ROOT
            / "internal"
            / "storage"
            / "clickhouse"
            / "testdata"
            / "api_grants.golden"
        )
        .read_text()
        .splitlines()
    )
    assert golden, "the golden is empty: this test would prove nothing"
    source = ROOT / "internal" / "storage" / "clickhouse" / "authorization.go"
    password = "Zq7-throwaway-credential"
    proc = _render(tmp_path, source, API_CH_PASSWORD=password)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    out = (tmp_path / "out.xml").read_text()
    grants = [q.text for q in ET.fromstring(out).iter("query")]
    assert grants == golden
    assert password not in out


def _differences() -> dict[str, str]:
    """Every way a mounted file can differ from the canonical render, planted one at a time."""
    first_query = "<query>GRANT SELECT, INSERT, ALTER DELETE ON default.teams</query>"
    assert first_query in _XML
    return {
        "a comment": _XML.replace("<users>", "<!-- note --><users>", 1),
        "the credential in a comment": _XML.replace(
            "</clickhouse>", f"<!-- {_PASSWORD} --></clickhouse>"
        ),
        "a malformed grant": _XML.replace(
            first_query, "<query>GRANT SELECT ON</query>"
        ),
        "an extra well-formed grant": _XML.replace(
            "</grants>", "<query>GRANT SELECT ON default.other</query></grants>"
        ),
        "a hand edit of the network": _XML.replace("::/0", "127.0.0.1"),
        "a hash for another password": _XML.replace(
            _HASH, hashlib.sha256(b"another-password").hexdigest()
        ),
        "an attribute": _XML.replace("<networks>", '<networks x="attr-value">'),
        "text in a container": _XML.replace("<grants>", "<grants>STRAY"),
        "a plaintext password element": _XML.replace(
            "</dho_api_ch>", "<password>hunter2</password></dho_api_ch>"
        ),
        "a uri": _XML.replace(
            "</clickhouse>", "<!-- clickhouse://u:pw@h:9000/db --></clickhouse>"
        ),
        "a truncated file": _XML.replace("</clickhouse>", ""),
        "a second user": _XML.replace(
            "</users>",
            "<other><password_sha256_hex>x</password_sha256_hex></other></users>",
        ),
        "trailing whitespace": _XML + "\n",
        "an empty file": "",
    }


def test_the_file_is_valid_only_if_it_equals_the_canonical_render(
    tmp_path: Path,
) -> None:
    """No parsing, no allow-list: any difference at all is refused with rc 4 before the live checks run.
    Each plant passed the earlier shape validator except a wrong hash, which failed a separate check."""
    for name, body in _differences().items():
        case = tmp_path / name.replace(" ", "-")
        case.mkdir()
        proc = _check(case, _users_file(case, body=body))
        assert proc.returncode == 4, (name, proc.stdout, proc.stderr)
        assert "CH_API_USER_FILE_MISMATCH" in proc.stderr, name
        output = proc.stdout + proc.stderr
        for secret in (_PASSWORD, _HASH, "hunter2", "pw@", "attr-value", "STRAY"):
            assert secret not in output, (name, secret)
        assert not (case / "docker-args.log").exists(), (
            name,
            "the live checks ran on a file that is not canonical",
        )


def test_the_canonical_file_passes_and_prints_only_names(tmp_path: Path) -> None:
    proc = _check(tmp_path, _users_file(tmp_path))
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    assert "canonical=yes" in proc.stdout
    assert _HASH not in proc.stdout + proc.stderr


def test_a_missing_posture_manifest_or_credential_is_refused_not_skipped(
    tmp_path: Path,
) -> None:
    file = _users_file(tmp_path)
    env = _docker(tmp_path, count="1")
    env["TMPDIR"] = str(_scratch())
    creds = tmp_path / "go-api.creds"
    creds.write_text(f"API_CH_PASSWORD={_PASSWORD}\n")
    env["DHO_API_CH_POSTURE_GO"] = str(tmp_path / "absent.go")
    proc = subprocess.run(
        ["/bin/bash", str(CHECK), str(file), str(creds)],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )
    assert proc.returncode == 1 and "canonical render cannot be computed" in proc.stderr
    empty = tmp_path / "empty.go"
    empty.write_text("package clickhouse\n")
    env["DHO_API_CH_POSTURE_GO"] = str(empty)
    proc = subprocess.run(
        ["/bin/bash", str(CHECK), str(file), str(creds)],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )
    assert (
        proc.returncode == 1 and "canonical render could not be computed" in proc.stderr
    )
    assert not (tmp_path / "docker-args.log").exists()
