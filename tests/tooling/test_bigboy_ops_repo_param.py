"""CHAOS-10137 item 3: every bigboy cut tool that runs git on the ops tree takes BIGBOY_OPS_REPO and never
runs git in the operator checkout /home/ubuntu/devhealth/ops. A posture manifest that cannot be parsed
fails the tool (non-zero exit), it does not exit 0."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BIGBOY = ROOT / "ci" / "bigboy"
OPERATOR_CHECKOUT = "/home/ubuntu/devhealth"

POSTURE_V1 = """package postgres

func apiPosture() []RequiredTable {
	return []RequiredTable{
		{"alpha", true, false, false},
	}
}
"""
POSTURE_V2 = POSTURE_V1.replace(
    '{"alpha", true, false, false},',
    '{"alpha", true, false, false},\n\t\t{"beta", true, true, false},',
)
CH_AUTH = 'package clickhouse\n\nvar x = []grant{{Table: "alpha"}}\n'


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
        env={
            "PATH": os.environ["PATH"],
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.com",
            "HOME": str(repo),
        },
    )
    return done.stdout.strip()


def _clone(tmp_path: Path) -> tuple[Path, str, str]:
    repo = tmp_path / "opsclone"
    pg = repo / "internal" / "storage" / "postgres"
    ch = repo / "internal" / "storage" / "clickhouse"
    pg.mkdir(parents=True)
    ch.mkdir(parents=True)
    _git(repo, "init", "-q", "-b", "main")
    (pg / "api_authorization.go").write_text(POSTURE_V1)
    (pg / "domain_authorization.go").write_text(
        POSTURE_V1.replace("apiPosture", "domainPosture")
    )
    (ch / "authorization.go").write_text(CH_AUTH)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "CHAOS-1 first")
    old = _git(repo, "rev-parse", "HEAD")
    (pg / "api_authorization.go").write_text(POSTURE_V2)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "CHAOS-6666 second")
    return repo, old, _git(repo, "rev-parse", "HEAD")


def _git_logger(tmp_path: Path) -> tuple[Path, Path]:
    """A `git` first on PATH that logs its argv, then runs the real git."""
    real = subprocess.run(
        ["bash", "-c", "command -v git"], capture_output=True, text=True, check=True
    ).stdout.strip()
    bindir = tmp_path / "gitbin"
    bindir.mkdir()
    log = tmp_path / "git.log"
    wrapper = bindir / "git"
    wrapper.write_text(
        f'#!/usr/bin/env bash\necho "$PWD :: $*" >> "{log}"\nexec {real} "$@"\n'
    )
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IXUSR)
    return bindir, log


def _env(tmp_path: Path, bindir: Path, repo: Path | None) -> dict[str, str]:
    env = {"PATH": f"{bindir}:{os.environ['PATH']}", "HOME": str(tmp_path)}
    if repo is not None:
        env["BIGBOY_OPS_REPO"] = str(repo)
    return env


def _assert_no_operator_git(log: Path) -> None:
    text = log.read_text() if log.exists() else ""
    assert OPERATOR_CHECKOUT not in text, text


def test_cut_contents_reads_git_from_the_given_clone(tmp_path: Path) -> None:
    repo, old, new = _clone(tmp_path)
    bindir, log = _git_logger(tmp_path)
    out = tmp_path / "pg-expect.txt"
    done = subprocess.run(
        ["bash", str(BIGBOY / "cut-contents.sh"), old, new, str(out)],
        capture_output=True,
        text=True,
        timeout=60,
        env=_env(tmp_path, bindir, repo),
        stdin=subprocess.DEVNULL,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    assert "commits in range: 1" in done.stdout, done.stdout
    assert "devhealth_api beta" in out.read_text()
    assert log.exists() and "-C " + str(repo) in log.read_text()
    _assert_no_operator_git(log)


def test_pg_posture_delta_uses_the_given_clone(tmp_path: Path) -> None:
    repo, old, new = _clone(tmp_path)
    bindir, log = _git_logger(tmp_path)
    out = tmp_path / "pg-expect.txt"
    done = subprocess.run(
        ["python3", str(BIGBOY / "cut-contents-pg-api-posture.py"), old, new, str(out)],
        capture_output=True,
        text=True,
        timeout=60,
        env=_env(tmp_path, bindir, repo),
    )
    assert done.returncode == 0, done.stdout + done.stderr
    assert "beta" in done.stdout and "unparsable" not in done.stdout, done.stdout
    _assert_no_operator_git(log)


def test_pg_posture_unparsable_manifest_exits_non_zero(tmp_path: Path) -> None:
    repo, old, new = _clone(tmp_path)
    bindir, _ = _git_logger(tmp_path)
    out = tmp_path / "pg-expect.txt"
    done = subprocess.run(
        [
            "python3",
            str(BIGBOY / "cut-contents-pg-api-posture.py"),
            "0" * 40,
            new,
            str(out),
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=_env(tmp_path, bindir, repo),
    )
    assert done.returncode != 0, done.stdout + done.stderr
    assert "unparsable" in done.stdout, done.stdout


def test_cut_contents_fails_when_a_posture_manifest_is_unparsable(
    tmp_path: Path,
) -> None:
    repo, old, new = _clone(tmp_path)
    bindir, _ = _git_logger(tmp_path)
    done = subprocess.run(
        [
            "bash",
            str(BIGBOY / "cut-contents.sh"),
            "0" * 40,
            new,
            str(tmp_path / "o.txt"),
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=_env(tmp_path, bindir, repo),
        stdin=subprocess.DEVNULL,
    )
    assert "unparsable" in done.stdout, done.stdout
    assert done.returncode != 0, done.stdout + done.stderr


def test_grants_check_reads_the_manifest_from_the_given_clone(tmp_path: Path) -> None:
    repo, old, new = _clone(tmp_path)
    bindir, log = _git_logger(tmp_path)
    root = tmp_path / "root"
    rec = root / "_records" / "bigboy-test"
    rec.mkdir(parents=True)
    (rec / "sha.txt").write_text(new + "\n")
    (root / "ops").mkdir()
    (root / "ops" / ".env").write_text("")
    (root / "_records" / "images.env").write_text("")
    stub = bindir / "docker"
    stub.write_text("#!/usr/bin/env bash\nexit 0\n")
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    env = _env(tmp_path, bindir, repo)
    env["BIGBOY_ROOT"] = str(root)
    done = subprocess.run(
        ["bash", str(BIGBOY / "bigboy-grants-check.sh"), str(rec)],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        stdin=subprocess.DEVNULL,
    )
    assert "no authorization.go" not in done.stdout + done.stderr, done.stdout
    assert log.exists() and "-C " + str(repo) in log.read_text()
    _assert_no_operator_git(log)


def test_without_the_variable_the_default_is_the_tools_own_checkout_never_the_operator_checkout(
    tmp_path: Path,
) -> None:
    bindir, log = _git_logger(tmp_path)
    done = subprocess.run(
        [
            "bash",
            str(BIGBOY / "cut-contents.sh"),
            "HEAD",
            "HEAD",
            str(tmp_path / "o.txt"),
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=_env(tmp_path, bindir, None),
        stdin=subprocess.DEVNULL,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    _assert_no_operator_git(log)
    assert "-C " + str(ROOT) in log.read_text()


def test_check_dho_api_ch_user_posture_default_follows_the_ops_repo_variable() -> None:
    text = (BIGBOY / "check-dho-api-ch-user.sh").read_text()
    assert "BIGBOY_OPS_REPO" in text
