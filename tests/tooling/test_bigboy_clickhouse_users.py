"""CHAOS-7162: bigboy declares dho_api_ch durably and the cut checks it before go-api is recreated.

The user used to be `docker cp`ed into the running ClickHouse container; a recreate rebuilt users.d from
the image, the user was lost and go-api failed every ClickHouse call with code 516. It is now a host file
mounted into users.d, and a 0600 host-user-owned file would make clickhouse-server (uid 101) exit, so the
check refuses one. Nothing here prints or asserts on a credential value: only names, modes and a hash.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "ci" / "bigboy"
OVERLAY = TOOLS / "compose.bigboy.clickhouse-users.yml"
CHECK = TOOLS / "check-dho-api-ch-user.sh"
RENDER = TOOLS / "render-dho-api-ch-users.py"
CUT = TOOLS / "bigboy-cut.sh"

_HASH = "a" * 64
_XML = (
    "<clickhouse><users><dho_api_ch><password_sha256_hex>"
    + _HASH
    + "</password_sha256_hex><grants><query>GRANT SELECT ON default.work_items</query>"
    "</grants></dho_api_ch></users></clickhouse>"
)


def _users_file(tmp_path: Path, *, mode: int = 0o644, body: str = _XML) -> Path:
    file = tmp_path / "dho_api_ch.xml"
    file.write_text(body)
    file.chmod(mode)
    return file


def _docker(tmp_path: Path, *, count: str | None, rc: int = 0) -> dict[str, str]:
    """A stub `docker exec ... clickhouse-client` answering `count` (or failing)."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    docker = bin_dir / "docker"
    answer = f'echo "{count}"' if count is not None else "true"
    docker.write_text(f"#!/usr/bin/env bash\n{answer}\nexit {rc}\n")
    docker.chmod(docker.stat().st_mode | stat.S_IEXEC)
    return {"PATH": f"{bin_dir}:/usr/bin:/bin"}


def _check(
    tmp_path: Path,
    file: Path | None,
    *,
    count: str | None = "1",
    rc: int = 0,
) -> subprocess.CompletedProcess[str]:
    env = _docker(tmp_path, count=count, rc=rc)
    args = ["bash", str(CHECK)] + ([str(file)] if file is not None else [])
    return subprocess.run(args, capture_output=True, text=True, env=env, timeout=30)


def test_a_readable_declared_file_and_a_live_user_pass(tmp_path: Path) -> None:
    proc = _check(tmp_path, _users_file(tmp_path))
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    assert "ch_api_user_file=ok" in proc.stdout and "ch_api_user_live=ok" in proc.stdout
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


def test_a_file_that_does_not_declare_the_user_with_a_hash_is_refused(
    tmp_path: Path,
) -> None:
    other = "<clickhouse><users><ch/></users></clickhouse>"
    assert _check(tmp_path, _users_file(tmp_path, body=other)).returncode == 1
    plaintext = "<clickhouse><users><dho_api_ch><password>x</password></dho_api_ch></users></clickhouse>"
    proc = _check(tmp_path, _users_file(tmp_path, body=plaintext))
    assert proc.returncode == 1 and "64-hex" in proc.stderr


def test_a_file_holding_a_plaintext_password_or_a_uri_is_refused(
    tmp_path: Path,
) -> None:
    """The file is mounted world-readable, so it may hold only the hash and grants."""
    for extra in (
        "<password>hunter2</password>",
        "<!-- clickhouse://dho_api_ch:pw@clickhouse:9000/default -->",
    ):
        body = _XML.replace("</dho_api_ch>", extra + "</dho_api_ch>")
        proc = _check(tmp_path, _users_file(tmp_path, body=body))
        assert proc.returncode == 1, (extra, proc.stdout, proc.stderr)
        assert "plaintext password element or a URI" in proc.stderr
        assert "hunter2" not in proc.stdout + proc.stderr
        assert "pw@" not in proc.stdout + proc.stderr


def test_a_user_missing_from_the_live_server_fails_with_its_own_code(
    tmp_path: Path,
) -> None:
    file = _users_file(tmp_path)
    for count, rc in (("0", 0), (None, 1)):
        proc = _check(tmp_path, file, count=count, rc=rc)
        assert proc.returncode == 2, (count, rc, proc.stdout, proc.stderr)
        assert "CH_API_USER_LIVE_FAIL" in proc.stderr and "516" in proc.stderr


def _release(tmp_path: Path, *, key: str = "dho_api_ch.xml") -> Path:
    release = tmp_path / "release.yaml"
    release.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "dev-health-ops-clickhouse-usersd"},
                "data": {key: _XML},
            }
        )
    )
    return release


def _render(tmp_path: Path, release: Path, **env: str):
    return subprocess.run(
        ["python3", str(RENDER), str(release), str(tmp_path / "out.xml")],
        capture_output=True,
        text=True,
        env={"PATH": os.environ["PATH"], **env},
        timeout=30,
    )


def test_render_writes_the_hash_of_the_password_0644_and_prints_no_secret(
    tmp_path: Path,
) -> None:
    password = "correct horse battery staple"
    proc = _render(tmp_path, _release(tmp_path), API_CH_PASSWORD=password)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    out = tmp_path / "out.xml"
    assert stat.S_IMODE(out.stat().st_mode) == 0o644
    digest = hashlib.sha256(password.encode()).hexdigest()
    text = out.read_text()
    assert f"<password_sha256_hex>{digest}</password_sha256_hex>" in text
    assert _HASH not in text and password not in text
    assert password not in proc.stdout + proc.stderr and digest not in proc.stdout
    assert "grants=1" in proc.stdout
    # The rendered file passes the check the cut runs.
    assert _check(tmp_path, out).returncode == 0


def test_render_refuses_a_missing_password_and_a_release_without_the_file(
    tmp_path: Path,
) -> None:
    assert _render(tmp_path, _release(tmp_path)).returncode == 2
    proc = _render(tmp_path, _release(tmp_path, key="other.xml"), API_CH_PASSWORD="x")
    assert proc.returncode != 0 and "dho_api_ch.xml" in proc.stderr
    assert not (tmp_path / "out.xml").exists()


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
        if "up -d --no-deps --no-build api query-api go-api web" in line
    )
    assert check < up, "the dho_api_ch check must run before go-api is recreated"
    gate = "\n".join(lines[check : check + 3])
    assert "exit 1" in gate and "ABORTING before go-api" in gate, gate
    chain = next(line for line in lines if line.startswith("export COMPOSE_FILE="))
    assert "$HERE/compose.bigboy.clickhouse-users.yml" in chain
    assert chain.rstrip().endswith("$HERE/compose.bigboy.router.yml"), chain
    assert any(line.startswith("export DHO_API_CH_USERS_XML=") for line in lines)
