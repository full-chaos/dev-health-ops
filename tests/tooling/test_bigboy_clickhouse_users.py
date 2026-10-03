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
import stat
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "ci" / "bigboy"
OVERLAY = TOOLS / "compose.bigboy.clickhouse-users.yml"
CHECK = TOOLS / "check-dho-api-ch-user.sh"
RENDER = TOOLS / "render-dho-api-ch-users.py"
CUT = TOOLS / "bigboy-cut.sh"

_PASSWORD = "the-api-password"
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


def _docker(
    tmp_path: Path,
    *,
    count: str | None,
    rc: int = 0,
    login_ok: bool = True,
    password: str = "the-api-password",
) -> dict[str, str]:
    """A stub `docker compose ... exec ... clickhouse-client`: answers the system.users count, and a login as
    dho_api_ch (`--user dho_api_ch`) succeeds only when the CLICKHOUSE_PASSWORD it was handed through the
    environment equals `password`. Every call's arguments are logged (never the environment)."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    args_log = tmp_path / "docker-args.log"
    docker = bin_dir / "docker"
    answer = f'echo "{count}"' if count is not None else "true"
    login = (
        'if [ "${CLICKHOUSE_PASSWORD:-}" = "'
        + password
        + '" ]; then echo dho_api_ch; exit 0; fi; exit 1'
        if login_ok
        else "exit 1"
    )
    docker.write_text(
        "#!/usr/bin/env bash\n"
        f'echo "$*" >> "{args_log}"\n'
        'case "$*" in *"--user dho_api_ch"*)\n'
        f"  {login} ;;\n"
        "esac\n"
        f"{answer}\nexit {rc}\n"
    )
    docker.chmod(docker.stat().st_mode | stat.S_IEXEC)
    return {"PATH": f"{bin_dir}:/usr/bin:/bin"}


def _check(
    tmp_path: Path,
    file: Path | None,
    *,
    count: str | None = "1",
    rc: int = 0,
    login_ok: bool = True,
    api_password: str | None = "the-api-password",
) -> subprocess.CompletedProcess[str]:
    env = _docker(tmp_path, count=count, rc=rc, login_ok=login_ok)
    (tmp_path / "authorization.go").write_text(_MANIFEST)
    env["DHO_API_CH_POSTURE_GO"] = str(tmp_path / "authorization.go")
    if api_password is not None:
        env["API_CH_PASSWORD"] = api_password
    args = ["bash", str(CHECK)] + ([str(file)] if file is not None else [])
    return subprocess.run(args, capture_output=True, text=True, env=env, timeout=30)


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
    assert unset.returncode == 3 and "API_CH_PASSWORD is not set" in unset.stderr


def test_the_api_credential_never_reaches_an_argument_list_or_the_output(
    tmp_path: Path,
) -> None:
    secret = "the-api-password"
    proc = _check(tmp_path, _users_file(tmp_path))
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    args = (tmp_path / "docker-args.log").read_text()
    assert "--user dho_api_ch" in args and "-e CLICKHOUSE_PASSWORD" in args, args
    # compose verbs on the service, never a bare `docker exec` on a container name
    for line in args.splitlines():
        assert line.startswith("compose --env-file "), line
        assert " exec -T " in line and " clickhouse clickhouse-client " in line, line
    assert "dev-health-clickhouse-1" not in args
    assert secret not in args and secret not in proc.stdout + proc.stderr


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
    fake.write_text(f"#!/usr/bin/env bash\necho fake-check\nexit {check_rc}\n")
    fake.chmod(0o755)
    stub_bin = tmp_path / "stubbin"
    stub_bin.mkdir()
    entry._docker_stub(
        stub_bin, routing_response=entry._routing_json("digest_unchanged"), routing_rc=0
    )
    entry._gh_stub(stub_bin)
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
    env["API_CH_PASSWORD"] = _PASSWORD
    env["DHO_API_CH_POSTURE_GO"] = str(tmp_path / "absent.go")
    proc = subprocess.run(
        ["bash", str(CHECK), str(file)],
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
        ["bash", str(CHECK), str(file)],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )
    assert (
        proc.returncode == 1 and "canonical render could not be computed" in proc.stderr
    )
    assert not (tmp_path / "docker-args.log").exists()
