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
import stat
import subprocess
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
