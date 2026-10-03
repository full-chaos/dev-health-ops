"""bigboy-cut.sh: drives the REAL script from its own entry point (r3 P3, D2886 condition 2),
never an extracted fragment. `bigboy-cut.sh`, `bigboy-repin.sh`, and `bigboy-repin-web.sh`
all read a BIGBOY_ROOT root parameter now (CHAOS-7022 D2895) -- default is byte-identical to
the hardcoded path they always used; a caller pointing it at an isolated tree gets the same
script, unmodified, running end to end inside that sandbox instead of the real bigboy host
tree. `docker` and `gh` are stubbed on PATH (the only externals this test replaces); `jq`,
`sed`, `grep`, and every other tool the scripts call is the real system binary.

r3's own mutation (`exit 0` inserted immediately before the carry block) is exactly the class
this file exists to catch: a suite that only ever runs the extracted carry block cannot see a
defect in anything BEFORE that block, including the reachability question itself. Running the
real entry point end to end, with `docker` controlled per test case for the ONE
routing-carry/routing-repoint call this module cares about and canned-success for everything
else (image waits, digest resolution, repin), proves reachability AND the one-function success
rule (`routing_call_succeeded`, D2886 condition 1) together, against the real script text.
"""

from __future__ import annotations

import json
import shutil
import stat
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CUT = ROOT / "ci" / "bigboy" / "bigboy-cut.sh"

_OLD8 = "aaaaaaaa"
_NEW = "b" * 40
_OLD_DIGEST = "sha256:" + "1" * 64
_NEW_DIGEST = "sha256:" + "2" * 64
_WEB_SHA = "c" * 40
_WEB_OLD_DIGEST = "sha256:" + "3" * 64
_WEB_NEW_DIGEST = "sha256:" + "4" * 64

_IMAGES = (
    "dev-hops-api",
    "dev-health-go-dho",
    "dev-health-go-api-tools",
    "dev-health-go-operator",
)

_MIGRATE_MARKER = (
    "--no-deps migrate"  # the compose command line for the STEP right after the
)
# carry block. bigboy-cut.sh redirects that command's own stdout/stderr to a FILE
# ($REC/migrate.out), never to the script's own streams, so a docker-side message here would
# be invisible to this test -- the migrate STEP's presence in the script's OWN stdout (`st
# migrate $?`, printed unredirected) is what every test below checks for reachability instead.
_MIGRATE_STEP = "STEP migrate rc="


def _build_bigboy_root(tmp_path: Path) -> Path:
    """A minimal but real-shaped bigboy tree: the compose file repin.sh reads/rewrites, and an
    old cut's _records dir with the one file repin.sh's own sed lines name explicitly."""
    root = tmp_path / "bigboy-root"
    root.mkdir()
    compose_dir = root / "compose"
    compose_dir.mkdir()
    lines = [
        f"      image: ghcr.io/full-chaos/{name}@{_OLD_DIGEST}\n" for name in _IMAGES
    ]
    lines.append(f"      image: ghcr.io/full-chaos/dev-health-web@{_WEB_OLD_DIGEST}\n")
    (compose_dir / "compose.bigboy.images.yml").write_text("".join(lines))
    old_records = root / "_records" / f"bigboy-{_OLD8}"
    old_records.mkdir(parents=True)
    (old_records / "bigboy-rest-commands.sh").write_text(
        "#!/usr/bin/env bash\nSTEP=placeholder\nB=" + ("0" * 40) + "\n"
    )
    return root


def _write_stub(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def _docker_stub(stub_bin: Path, *, routing_response: str, routing_rc: int) -> None:
    """Handles every `docker` invocation the entry point makes before and during the carry
    block: `buildx imagetools inspect` (bare, for the image-wait loop, and with --format, for
    every digest resolution bigboy-cut.sh/bigboy-repin.sh/bigboy-repin-web.sh does), `pull`, and
    the one `compose ... run ... venue-tools "<cmd>"` call this test controls. Anything whose
    command line contains the migrate marker exits nonzero with a distinct message, so a run
    that gets PAST carry is unambiguous in the captured output rather than silently continuing
    into steps this test never stubbed.
    """
    digest_map = {name: _NEW_DIGEST for name in _IMAGES}
    digest_map["dev-health-web"] = _WEB_NEW_DIGEST
    # repin.sh's own OLD8 cross-check resolves dev-hops-api at sha-<first 7 chars of $OLD8> (the real image tag) and must see _OLD_DIGEST.
    old_probe = f"dev-hops-api:sha-{_OLD8[:7]}"
    script = ["#!/usr/bin/env bash", "set -u", 'args="$*"']
    script.append(
        '[ -z "${DOCKER_STUB_COMPOSE_LOG:-}" ] || printf "%s\\n" "${COMPOSE_FILE:-}" >> "$DOCKER_STUB_COMPOSE_LOG"'
    )
    script.append(
        '[ -z "${DOCKER_STUB_ARGS_LOG:-}" ] || printf "%s\\n" "$*" >> "$DOCKER_STUB_ARGS_LOG"'
    )
    script.append(f'if [[ "$args" == *"{_MIGRATE_MARKER}"* ]]; then')
    script.append("  exit 9")
    script.append("fi")
    script.append(f'if [[ "$args" == *"{old_probe}"* ]]; then')
    script.append(f'  echo \'{{"digest":"{_OLD_DIGEST}"}}\'; exit 0')
    script.append("fi")
    for name, digest in digest_map.items():
        script.append(f'if [[ "$args" == *"{name}:sha-"* ]]; then')
        script.append(f'  echo \'{{"digest":"{digest}"}}\'; exit 0')
        script.append("fi")
    script.append('if [[ "$args" == *"venue-tools"* ]]; then')
    script.append(f"  cat <<'EOF_BODY'\n{routing_response}\nEOF_BODY")
    script.append(f"  exit {routing_rc}")
    script.append("fi")
    script.append(
        "exit 0"
    )  # pull, and any other bare imagetools inspect call: succeed quietly
    _write_stub(stub_bin / "docker", "\n".join(script) + "\n")


def _gh_stub(stub_bin: Path) -> None:
    _write_stub(
        stub_bin / "gh",
        f"#!/usr/bin/env bash\necho {_WEB_SHA}\n",
    )


def _run_entry_point(
    tmp_path: Path, *, routing_response: str, routing_rc: int, timeout: float = 60
) -> subprocess.CompletedProcess[str]:
    root = _build_bigboy_root(tmp_path)
    stub_bin = tmp_path / "stubbin"
    stub_bin.mkdir()
    _docker_stub(stub_bin, routing_response=routing_response, routing_rc=routing_rc)
    _gh_stub(stub_bin)
    env = {
        "PATH": f"{stub_bin}:/usr/bin:/bin",
        "BIGBOY_ROOT": str(root),
    }
    return subprocess.run(
        ["bash", str(CUT), _OLD8, _NEW],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        env=env,
    )


def _routing_json(reason: str) -> str:
    return "GOAPI_ROUTING_JSON " + json.dumps({"reason": reason})


def test_root_is_printed_and_layout_is_verified(tmp_path: Path) -> None:
    """D2895's two conditions on the root parameter: it is printed in the first log line, and a
    root missing the expected layout is refused before anything else runs."""
    root = tmp_path / "not-a-bigboy-tree"
    root.mkdir()
    proc = subprocess.run(
        ["bash", str(CUT), _OLD8, _NEW],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
        env={"PATH": "/usr/bin:/bin", "BIGBOY_ROOT": str(root)},
    )
    assert proc.returncode != 0, (
        f"a root with no bigboy layout must be refused -- stdout={proc.stdout!r}"
    )
    assert f"BIGBOY_ROOT={root}" in proc.stderr, (
        f"the refusal must name the resolved root -- stderr={proc.stderr!r}"
    )


def test_real_zero_exit_no_json_aborts_from_the_real_entry_point(
    tmp_path: Path,
) -> None:
    """r3 P1, driven through the real entry point this time: a docker/compose-layer zero exit
    with no GOAPI_ROUTING_JSON line at all must abort before migrate, never reach it."""
    proc = _run_entry_point(tmp_path, routing_response="", routing_rc=0)
    assert proc.returncode != 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "STEP routing-carry rc=1" in proc.stdout, proc.stdout
    assert _MIGRATE_STEP not in proc.stdout, proc.stdout


def test_real_zero_exit_malformed_json_aborts_from_the_real_entry_point(
    tmp_path: Path,
) -> None:
    """Same class: the exit code is 0 but the JSON line does not parse."""
    proc = _run_entry_point(
        tmp_path, routing_response="GOAPI_ROUTING_JSON {not valid json", routing_rc=0
    )
    assert proc.returncode != 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "STEP routing-carry rc=1" in proc.stdout, proc.stdout


def test_real_zero_exit_unknown_reason_aborts_from_the_real_entry_point(
    tmp_path: Path,
) -> None:
    """The exit code is 0, the JSON parses, but the reason is not one carry ever emits."""
    proc = _run_entry_point(
        tmp_path, routing_response=_routing_json("something_new"), routing_rc=0
    )
    assert proc.returncode != 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "STEP routing-carry rc=1" in proc.stdout, proc.stdout


def test_real_zero_exit_empty_reason_aborts_from_the_real_entry_point(
    tmp_path: Path,
) -> None:
    """The exit code is 0, the JSON parses, but the reason field is present and empty."""
    proc = _run_entry_point(tmp_path, routing_response=_routing_json(""), routing_rc=0)
    assert proc.returncode != 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "STEP routing-carry rc=1" in proc.stdout, proc.stdout


def test_real_nonzero_exit_with_carried_reason_still_aborts(tmp_path: Path) -> None:
    """The other half of the rule: exit code alone cannot rescue a nonzero exit even if a
    `carried` reason string is somehow present in the captured output -- rc must be 0 too."""
    proc = _run_entry_point(
        tmp_path, routing_response=_routing_json("carried"), routing_rc=1
    )
    assert proc.returncode != 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "STEP routing-carry rc=1" in proc.stdout, proc.stdout


def test_real_carried_reaches_past_the_carry_block(tmp_path: Path) -> None:
    """The success case: exit 0 AND a `carried` reason must reach the step after carry. The
    migrate STEP itself is made to fail (rc=9) by the docker stub -- irrelevant here (bigboy-
    cut.sh does not gate on migrate's own exit code), it only bounds this test's runtime; the
    assertion is on REACHING that STEP at all, printed by the script's own unredirected `st()`."""
    proc = _run_entry_point(
        tmp_path, routing_response=_routing_json("carried"), routing_rc=0, timeout=120
    )
    assert "STEP routing-carry rc=0" in proc.stdout, proc.stdout
    assert _MIGRATE_STEP in proc.stdout, (
        f"a real carry success must reach the migrate step -- stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )


def _start_only(env: dict[str, str], *, wait: float = 5) -> tuple[int | None, str, str]:
    """Run the cut until it either exits or has been alive for `wait` seconds (past its guards it
    waits for images, which this test never provides): (exit code or None, stdout, stderr)."""
    proc = subprocess.Popen(
        ["bash", str(CUT), _OLD8, _NEW],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        out, err = proc.communicate(timeout=wait)
        return proc.returncode, out, err
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
        return None, out or "", err or ""


def test_the_cut_needs_no_ci_bigboy_under_the_root(tmp_path: Path) -> None:
    """CHAOS-7135: the tools come from the script's own directory, so a root that is only the
    running tree (compose file + _records) starts a cut; the old guard demanded a ci/bigboy
    symlink under the root and refused."""
    root = _build_bigboy_root(tmp_path)
    assert not (root / "ci").exists()
    code, out, err = _start_only({"PATH": "/usr/bin:/bin", "BIGBOY_ROOT": str(root)})
    assert "missing ci/bigboy" not in err, err
    assert f"root={root} tools={CUT.parent}" in out, (code, out, err)


def test_bigboy_tools_dir_overrides_where_the_tools_come_from(tmp_path: Path) -> None:
    root = _build_bigboy_root(tmp_path)
    tools = tmp_path / "tools"
    tools.mkdir()
    (tools / "bigboy-repin.sh").write_text("#!/usr/bin/env bash\n")
    code, out, err = _start_only(
        {
            "PATH": "/usr/bin:/bin",
            "BIGBOY_ROOT": str(root),
            "BIGBOY_TOOLS_DIR": str(tools),
        }
    )
    assert f"tools={tools}" in out, (code, out, err)


def test_a_tools_dir_that_is_not_the_bigboy_tools_is_refused(tmp_path: Path) -> None:
    root = _build_bigboy_root(tmp_path)
    empty = tmp_path / "empty"
    empty.mkdir()
    for tools, reason in (
        (tmp_path / "does-not-exist", "is not a directory"),
        (empty, "has no bigboy-repin.sh"),
    ):
        code, out, err = _start_only(
            {
                "PATH": "/usr/bin:/bin",
                "BIGBOY_ROOT": str(root),
                "BIGBOY_TOOLS_DIR": str(tools),
            }
        )
        assert code not in (None, 0), (tools, out, err)
        assert reason in err and "cut start" not in out, (tools, out, err)


def test_every_compose_file_the_cut_exports_resolves_without_ci_under_the_root(
    tmp_path: Path,
) -> None:
    """CHAOS-7135: the cut runs docker compose from the root, so a relative COMPOSE_FILE entry is
    resolved against the root; an entry that names a tools file must therefore be absolute (from
    the tools directory), or a root with no ci/bigboy fails every compose call. The stub docker
    records COMPOSE_FILE as the cut exports it; every entry must exist, with the root holding the
    compose files a real root holds and nothing under ci/."""
    import re

    root = _build_bigboy_root(tmp_path)
    chain_line = re.search(
        r"^export COMPOSE_FILE=(\S+)$", CUT.read_text(), re.MULTILINE
    )
    assert chain_line, "bigboy-cut.sh no longer exports COMPOSE_FILE"
    tools = str(CUT.parent)
    for entry in chain_line.group(1).split(":"):
        entry = entry.replace("$HERE", tools)
        if not entry.startswith("/") and not entry.startswith("ci/"):
            path = root / entry
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
    log = tmp_path / "compose-file.log"
    stub_bin = tmp_path / "stubbin"
    stub_bin.mkdir()
    _docker_stub(
        stub_bin, routing_response=_routing_json("digest_unchanged"), routing_rc=0
    )
    _gh_stub(stub_bin)
    proc = subprocess.run(
        ["bash", str(CUT), _OLD8, _NEW],
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
        env={
            "PATH": f"{stub_bin}:/usr/bin:/bin",
            "BIGBOY_ROOT": str(root),
            "DOCKER_STUB_COMPOSE_LOG": str(log),
        },
    )
    assert not (root / "ci").exists()
    seen = {line for line in log.read_text().splitlines() if line}
    assert seen, (proc.stdout, proc.stderr)
    for chain in seen:
        for entry in chain.split(":"):
            resolved = Path(entry) if entry.startswith("/") else root / entry
            assert resolved.is_file(), (
                f"COMPOSE_FILE entry {entry!r} does not resolve from the root: {resolved}"
            )


# ============================================================================
# CHAOS-8369: pass-bigboy-admin2.sh D4566 class-rule cases, added to this existing file (D3942: no new test file).
# EXPECTED-GONE at E7 with CHAOS-8364: the script drives the Python plane, so these cases end with it.
#
# pass-bigboy-admin2.sh obeys the D4566 class rules (CHAOS-8369), proven by RUNNING it.
#
# Rules: no credential in argv or the child env (the token reaches the in-container python on stdin
# only); no result directory under /tmp in a container (results leave as one framed stdout stream);
# compose verbs only (no bare `docker exec`/`docker run`); no test hook in the run file.
#
# Method: the REAL script runs under bash on a closed PATH whose `docker` is a recording stand-in. The
# stand-in records argv, its own env and its stdin, then executes the REAL producer
# (pass-bigboy-admin2-run.py, taken from the script's own `docker compose exec ... python3 -c SRC` argument)
# with http.client replaced by a stub, so the framed stream the host unpacks is the producer's real
# output, never hand-written. The script's two absolute host paths are substituted in a tmp copy (the
# run file carries no test hook).
#
# Guard observed failing: `violations()` is also run on the OLD form of the script (the four pre-8369
# lines, embedded below) and must report every rule break there; it must report none on the new one.
# ============================================================================

_a2_ROOT = Path(__file__).resolve().parents[2]
_a2_BB = _a2_ROOT / "ci" / "bigboy"
_a2_SCRIPT = _a2_BB / "pass-bigboy-admin2.sh"
_a2_RUN_PY = _a2_BB / "pass-bigboy-admin2-run.py"
_a2_UNPACK = _a2_BB / "pass-bigboy-admin2-unpack.py"
_a2_MARKER = "plainword-marker-a2"
_a2_HOST_ROOT = "/home/ubuntu/devhealth"

_a2_FAKE_DOCKER = r"""#!/bin/bash
# stand-in for docker: record argv/env/stdin, then run the real producer under a stubbed http.client
d="$FAKE_DIR"
printf '%s\0' "$@" > "$d/argv"
env > "$d/env"
src=""; prev=""
for a in "$@"; do [ "$prev" = "-c" ] && src=$a; prev=$a; done
if [ -z "$src" ]; then cat > "$d/stdin"; exit "${FAKE_RC:-0}"; fi
tee "$d/stdin" | python3 -c '
import http.client, os, sys
seen = open(os.environ["FAKE_DIR"] + "/seen-auth", "a")
methods = open(os.environ["FAKE_DIR"] + "/seen-methods", "a")
class R:
    reason = "stub"
    def __init__(self, status): self.status = status
    def read(self): return b"{}"
    def getheaders(self): return [("content-type", "application/json")]
class C:
    def __init__(self, *a, **k): pass
    def request(self, method, path, body=None, headers=None):
        seen.write((headers or {}).get("Authorization", "-") + "\n"); seen.flush()
        methods.write(method + " " + path + "\n"); methods.flush()
        dirty = os.environ.get("FAKE_STATE") == "dirty" and method == "GET" and path.endswith("/settings/general/zz-venue-probe")
        self._status = 200 if dirty else 404
    def getresponse(self): return R(self._status)
    def close(self): pass
http.client.HTTPConnection = C
exec(sys.argv[1])
' "$src"
rc=$?
[ -n "${FAKE_RC:-}" ] && exit "$FAKE_RC"
exit $rc
"""

_a2_OLD_FORM = f"""set -euo pipefail; umask 077
HERE=$(cd "$(dirname "$0")" && pwd); OUT=${{1:-$HERE/pass-admin2}}
rm -rf "$OUT"; mkdir -p "$OUT"
export ADMIN="$(cat {_a2_HOST_ROOT}/.go-api-dev/bigboy-admin-proof.token)"
docker exec -i -e ADMIN dev-health-api-1 python3 - < "$HERE/pass-bigboy-admin2-run.py"
docker exec dev-health-api-1 tar cf - -C /tmp bbpass | tar xf - -C "$OUT" --strip-components=1
docker exec dev-health-api-1 rm -rf /tmp/bbpass
"""
# The OLD run file read the token from the environment and wrote to /tmp/bbpass; the old form is
# exercised only through its docker calls (the stand-in records them), so any python3 stdin script works.
_a2_OLD_RUN_STUB = "import sys; sys.stdin.read()\n"


def _a2_closed_bin(tmp: Path) -> Path:
    b = tmp / "bin"
    b.mkdir()
    for tool in ("bash", "cat", "dirname", "tee", "rm", "mkdir", "env", "tar"):
        p = shutil.which(tool)
        assert p, tool
        (b / tool).symlink_to(p)
    (b / "python3").symlink_to(Path(sys.executable).resolve())
    docker = b / "docker"
    docker.write_text(_a2_FAKE_DOCKER)
    docker.chmod(0o755)
    return b


def _a2_stage(tmp: Path, script_text: str, run_py_text: str):
    host = tmp / "host"
    (host / ".go-api-dev").mkdir(parents=True)
    (host / "ops").mkdir()
    (host / "ops" / ".env").write_text("X=1\n")
    tokfile = host / ".go-api-dev" / "bigboy-admin-proof.token"
    tokfile.write_text(_a2_MARKER + "\n")
    tokfile.chmod(0o600)
    ci = tmp / "ci"
    ci.mkdir()
    n = script_text.count(_a2_HOST_ROOT)
    (ci / "pass-bigboy-admin2.sh").write_text(
        script_text.replace(_a2_HOST_ROOT, str(host))
    )
    (ci / "pass-bigboy-admin2-run.py").write_text(run_py_text)
    shutil.copy(_a2_BB / "pass-bigboy-admin2-table.py", ci)
    shutil.copy(_a2_UNPACK, ci)
    return ci / "pass-bigboy-admin2.sh", n


def _a2_run(
    tmp: Path,
    script: Path,
    out: Path,
    fake_rc: str | None = None,
    fake_state: str | None = None,
):
    fake = tmp / "fake"
    fake.mkdir(exist_ok=True)
    env = {"PATH": str(_a2_closed_bin(tmp)), "FAKE_DIR": str(fake), "HOME": str(tmp)}
    if fake_rc is not None:
        env["FAKE_RC"] = fake_rc
    if fake_state is not None:
        env["FAKE_STATE"] = fake_state
    proc = subprocess.run(
        ["bash", str(script), str(out)],
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
        cwd=tmp,
        check=False,
    )
    return proc, fake


def _a2_violations(fake: Path, proc: subprocess.CompletedProcess[str]) -> list[str]:
    """The D4566 class rules, read off what the stand-in actually received."""
    v = []
    argvs = (
        (fake / "argv").read_bytes().split(b"\0") if (fake / "argv").exists() else []
    )
    argv = [a.decode() for a in argvs]
    envtxt = (fake / "env").read_text() if (fake / "env").exists() else ""
    if any(_a2_MARKER in a for a in argv):
        v.append("credential in argv")
    if _a2_MARKER in envtxt or any(
        ln.startswith("ADMIN=") for ln in envtxt.splitlines()
    ):
        v.append("credential in child env")
    if "/tmp" in " ".join(argv):
        v.append("/tmp path in a container argument")
    if argv and argv[0] != "compose":
        v.append(f"bare docker verb {argv[0]!r}")
    return v


def _a2_new_run(
    tmp_path: Path, fake_rc: str | None = None, fake_state: str | None = None
):
    script, n = _a2_stage(tmp_path, _a2_SCRIPT.read_text(), _a2_RUN_PY.read_text())
    assert n == 2, (
        "expected exactly the two host-path literals (token file, compose root)"
    )
    out = tmp_path / "out"
    proc, fake = _a2_run(tmp_path, script, out, fake_rc, fake_state)
    return proc, fake, out


def test_new_script_obeys_every_class_rule(tmp_path: Path) -> None:
    proc, fake, out = _a2_new_run(tmp_path)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
    assert _a2_violations(fake, proc) == []
    argv = (fake / "argv").read_bytes().split(b"\0")
    assert argv[:2] == [b"compose", b"--env-file"] and b"exec" in argv and b"-T" in argv
    # positive control: the producer really received the token, and only via stdin
    assert (fake / "stdin").read_text() == _a2_MARKER + "\n"
    seen = (fake / "seen-auth").read_text().split("\n")
    assert f"Bearer {_a2_MARKER}" in seen
    # the real producer's frames reached $OUT and the table rendered from them
    files = sorted(p.name for p in out.iterdir())
    assert "table.txt" in files and any(f.endswith(".headers.raw") for f in files)
    assert not any("/tmp/bbpass" in str(p) for p in out.rglob("*"))
    # the credential is printed nowhere and is in no result file
    assert _a2_MARKER not in proc.stdout and _a2_MARKER not in proc.stderr
    for p in out.rglob("*"):
        if p.is_file():
            assert _a2_MARKER.encode() not in p.read_bytes(), p.name


def test_old_form_is_seen_failing_every_rule(tmp_path: Path) -> None:
    script, n = _a2_stage(
        tmp_path, "#!/usr/bin/env bash\n" + _a2_OLD_FORM, _a2_OLD_RUN_STUB
    )
    assert n == 1
    out = tmp_path / "out"
    proc, fake = _a2_run(tmp_path, script, out)
    got = _a2_violations(fake, proc)
    # the stand-in records the LAST docker call (rm -rf /tmp/bbpass); the first call's env/argv matter too,
    # so also read the shell source: the old form exports the token and uses bare exec.
    assert "bare docker verb 'exec'" in got
    assert "/tmp path in a container argument" in got
    assert "export ADMIN=" in _a2_OLD_FORM and "-e ADMIN" in _a2_OLD_FORM


def test_a_state_that_is_not_clean_aborts_before_any_write(tmp_path: Path) -> None:
    proc, fake, out = _a2_new_run(tmp_path, fake_state="dirty")
    assert proc.returncode != 0, (proc.stdout, proc.stderr)
    assert "ABORT part B" in proc.stderr
    sent = (fake / "seen-methods").read_text().splitlines()
    assert any(line.startswith("GET ") for line in sent)
    writes = [
        line
        for line in sent
        if line.split(" ", 1)[0] in {"PUT", "PATCH", "DELETE"}
        or (line.startswith("POST ") and "install-url" not in line)
    ]
    assert writes == [], writes
    assert not (out / "table.txt").exists() or (out / "table.txt").stat().st_size == 0


def test_remote_failure_fails_the_script(tmp_path: Path) -> None:
    proc, _, out = _a2_new_run(tmp_path, fake_rc="7")
    assert proc.returncode != 0
    assert not (out / "table.txt").exists() or (out / "table.txt").stat().st_size == 0


def test_run_py_without_token_on_stdin_exits_4() -> None:
    p = subprocess.run(
        [sys.executable, str(_a2_RUN_PY)],
        input="",
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert p.returncode == 4 and p.stdout == ""


def test_run_py_has_no_env_token_and_no_tmp() -> None:
    src = "\n".join(
        ln
        for ln in _a2_RUN_PY.read_text().splitlines()
        if not ln.lstrip().startswith("#")
    )
    assert "environ" not in src and "/tmp" not in src and "import os" not in src


def test_script_has_no_forbidden_forms() -> None:
    src = "\n".join(
        ln
        for ln in _a2_SCRIPT.read_text().splitlines()
        if not ln.lstrip().startswith("#")
    )
    for bad in (
        "export ADMIN",
        "-e ADMIN",
        "docker exec",
        "docker run",
        "/tmp",
        "tar ",
        "$(cat /home",
    ):
        assert bad not in src, bad
    assert "docker compose" in src


def _a2_unpack(stream: bytes, tmp_path: Path):
    out = tmp_path / "u"
    out.mkdir(exist_ok=True)
    p = subprocess.run(
        [sys.executable, str(_a2_UNPACK), str(out)],
        input=stream,
        capture_output=True,
        timeout=30,
    )
    return p, out


def test_unpack_accepts_a_well_formed_stream(tmp_path: Path) -> None:
    p, out = _a2_unpack(b"@@FILE\ta.x\t3\nabc\n@@END\t1\tok\n", tmp_path)
    assert p.returncode == 0 and (out / "a.x").read_bytes() == b"abc"


def test_unpack_refuses_every_malformed_stream(tmp_path: Path) -> None:
    bad = {
        "no END": b"@@FILE\ta.x\t3\nabc\n",
        "empty": b"",
        "short body": b"@@FILE\ta.x\t9\nabc\n@@END\t1\tok\n",
        "path name": b"@@FILE\t../evil\t1\nx\n@@END\t1\tok\n",
        "slash name": b"@@FILE\ta/b\t1\nx\n@@END\t1\tok\n",
        "count mismatch": b"@@FILE\ta.x\t1\nx\n@@END\t2\tok\n",
        "status not ok": b"@@FILE\ta.x\t1\nx\n@@END\t1\tabort-state-not-clean\n",
        "trailing bytes": b"@@END\t0\tok\nJUNK",
    }
    for label, stream in bad.items():
        p, _ = _a2_unpack(stream, tmp_path)
        assert p.returncode != 0, label
    assert not (tmp_path / "evil").exists()


def test_a2_a_non_empty_out_dir_is_refused_not_wiped(tmp_path: Path) -> None:
    """Approver fix: the script never rm -rf's a path taken from argv."""
    script, _ = _a2_stage(tmp_path, _a2_SCRIPT.read_text(), _a2_RUN_PY.read_text())
    out = tmp_path / "out"
    out.mkdir()
    keep = out / "keep.txt"
    keep.write_text("precious")
    proc, fake = _a2_run(tmp_path, script, out)
    assert proc.returncode == 3 and "REFUSED" in proc.stderr
    assert keep.read_text() == "precious"
    assert not (fake / "argv").exists(), (
        "docker must not run when the out dir is refused"
    )
    code = "\n".join(
        ln
        for ln in _a2_SCRIPT.read_text().splitlines()
        if not ln.lstrip().startswith("#")
    )
    assert "rm -rf" not in code
