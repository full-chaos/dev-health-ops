"""CHAOS-10137 items 1 and 2: the repin scripts pin the linux/arm64 CHILD digest (what _records/images.env
pins), never the multi-arch index digest, and bigboy-cut.sh stops when a repin step fails."""

from __future__ import annotations

import hashlib
import os
import stat
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BIGBOY = ROOT / "ci" / "bigboy"
OLD8 = "aaaaaaaa"
NEW = "b" * 40
WEB_SHA = "c" * 40
IMAGES = ("dev-health-go-dho", "dev-health-go-api-tools", "dev-health-go-operator")


def _h(*parts: str) -> str:
    return "sha256:" + hashlib.sha256("-".join(parts).encode()).hexdigest()


def index_digest(image: str, sha7: str) -> str:
    return _h("index", image, sha7)


def arm64_digest(image: str, sha7: str) -> str:
    return _h("arm64", image, sha7)


def _raw_index(image: str, sha7: str) -> str:
    return (
        '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":['
        f'{{"digest":"{_h("amd64", image, sha7)}","platform":{{"architecture":"amd64","os":"linux"}}}},'
        f'{{"digest":"{arm64_digest(image, sha7)}","platform":{{"architecture":"arm64","os":"linux"}}}},'
        f'{{"digest":"{_h("att", image, sha7)}","platform":{{"architecture":"unknown","os":"unknown"}}}}'
        "]}"
    )


def _docker_stub(bindir: Path, refs: list[tuple[str, str]]) -> None:
    """Index images: `--raw` prints the index manifest, `--format` prints the index descriptor only."""
    lines = ["#!/usr/bin/env bash", 'args="$*"']
    for image, sha7 in refs:
        lines.append(f'if [[ "$args" == *"{image}:sha-{sha7}"* ]]; then')
        lines.append('  if [[ "$args" == *"--raw"* ]]; then')
        lines.append(f"    cat <<'EOF_RAW'\n{_raw_index(image, sha7)}\nEOF_RAW")
        lines.append("  else")
        lines.append(f"    echo '{{\"digest\":\"{index_digest(image, sha7)}\"}}'")
        lines.append("  fi; exit 0")
        lines.append("fi")
    lines.append("exit 1")
    stub = bindir / "docker"
    stub.write_text("\n".join(lines) + "\n")
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)


def _root(tmp_path: Path, pinned_dho_old: str) -> Path:
    root = tmp_path / "root"
    rec = root / "_records"
    (rec / f"bigboy-{OLD8}").mkdir(parents=True)
    (rec / f"bigboy-{OLD8}" / "bigboy-rest-commands.sh").write_text(
        "#!/usr/bin/env bash\nSTEP=x\nB=" + "0" * 40 + "\n"
    )
    old = "sha256:" + "1" * 64
    (rec / "images.env").write_text(
        f"BIGBOY_DHO_IMAGE=ghcr.io/full-chaos/dev-health-go-dho@{pinned_dho_old}\n"
        f"BIGBOY_API_TOOLS_IMAGE=ghcr.io/full-chaos/dev-health-go-api-tools@{old}\n"
        f"BIGBOY_OPERATOR_IMAGE=ghcr.io/full-chaos/dev-health-go-operator@{old}\n"
        f"BIGBOY_WEB_IMAGE=ghcr.io/full-chaos/dev-health-web@{old}\n"
    )
    return root


def _run(script: str, root: Path, bindir: Path, *args: str, **env: str):
    return subprocess.run(
        ["bash", str(BIGBOY / script), *args],
        capture_output=True,
        text=True,
        timeout=60,
        stdin=subprocess.DEVNULL,
        env={
            "PATH": f"{bindir}:{os.environ['PATH']}",
            "BIGBOY_ROOT": str(root),
            "WEB_REPIN_WAIT_TRIES": "1",
            "WEB_REPIN_WAIT_SECS": "0",
            **env,
        },
    )


def test_repin_pins_arm64_children_and_the_old8_check_compares_children(
    tmp_path: Path,
) -> None:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    refs = [(i, NEW[:7]) for i in IMAGES] + [("dev-health-go-dho", OLD8[:7])]
    _docker_stub(bindir, refs)
    root = _root(tmp_path, arm64_digest("dev-health-go-dho", OLD8[:7]))
    done = _run("bigboy-repin.sh", root, bindir, OLD8, NEW)
    assert done.returncode == 0, done.stdout + done.stderr
    pins = (root / "_records" / "images.env").read_text()
    for image in IMAGES:
        assert arm64_digest(image, NEW[:7]) in pins, pins
        assert index_digest(image, NEW[:7]) not in pins, pins


def test_repin_rejects_an_old8_whose_child_is_not_the_pinned_child(
    tmp_path: Path,
) -> None:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    refs = [(i, NEW[:7]) for i in IMAGES] + [("dev-health-go-dho", OLD8[:7])]
    _docker_stub(bindir, refs)
    root = _root(tmp_path, "sha256:" + "9" * 64)
    done = _run("bigboy-repin.sh", root, bindir, OLD8, NEW)
    assert done.returncode == 4, done.stdout + done.stderr


def test_repin_fails_when_the_index_has_no_arm64_child(tmp_path: Path) -> None:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    refs = [(i, NEW[:7]) for i in IMAGES] + [("dev-health-go-dho", OLD8[:7])]
    _docker_stub(bindir, refs)
    stub = bindir / "docker"
    stub.write_text(stub.read_text().replace('"architecture":"arm64"', '"architecture":"s390x"'))
    root = _root(tmp_path, arm64_digest("dev-health-go-dho", OLD8[:7]))
    before = (root / "_records" / "images.env").read_text()
    done = _run("bigboy-repin.sh", root, bindir, OLD8, NEW)
    assert done.returncode != 0, done.stdout + done.stderr
    assert (root / "_records" / "images.env").read_text() == before


def test_repin_web_pins_the_arm64_child(tmp_path: Path) -> None:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    _docker_stub(bindir, [("dev-health-web", WEB_SHA[:7])])
    root = _root(tmp_path, "sha256:" + "1" * 64)
    done = _run("bigboy-repin-web.sh", root, bindir, WEB_REPIN=WEB_SHA)
    assert done.returncode == 0, done.stdout + done.stderr
    pins = (root / "_records" / "images.env").read_text()
    assert arm64_digest("dev-health-web", WEB_SHA[:7]) in pins, pins
    assert index_digest("dev-health-web", WEB_SHA[:7]) not in pins, pins


def test_cut_resolves_the_operator_digest_through_the_arm64_helper() -> None:
    cut = (BIGBOY / "bigboy-cut.sh").read_text()
    line = next(
        ln for ln in cut.splitlines() if ln.startswith("BIGBOY_OPERATOR_DIGEST=")
    )
    assert "arm64_child_digest" in line, line
    assert "{{json .Manifest}}" not in line


def _cut_harness():
    import importlib.util

    path = Path(__file__).with_name("test_bigboy_cut_entry_point.py")
    spec = importlib.util.spec_from_file_location("_cut_ep_harness_10137", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_the_cut_stops_before_migrate_when_the_repin_fails(tmp_path: Path) -> None:
    ep = _cut_harness()
    root = ep._build_bigboy_root(tmp_path)
    # a record dir that already exists, so the old "no record dir" guard cannot stop the cut by accident
    (root / "_records" / f"bigboy-{ep._NEW[:8]}").mkdir()
    stub_bin = tmp_path / "stubbin"
    stub_bin.mkdir()
    ep._docker_stub(stub_bin, routing_response=ep._routing_json("carried"), routing_rc=0)
    ep._gh_stub(stub_bin)
    docker = stub_bin / "docker"
    wrong_old = "sha256:" + "9" * 64
    docker.write_text(
        docker.read_text().replace(
            f"echo '{{\"digest\":\"{ep._OLD_DIGEST}\"}}'",
            f"echo '{{\"digest\":\"{wrong_old}\"}}'",
        )
    )
    proc = subprocess.run(
        ["bash", str(BIGBOY / "bigboy-cut.sh"), ep._OLD8, ep._NEW],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={
            "PATH": f"{stub_bin}:/usr/bin:/bin",
            "BIGBOY_ROOT": str(root),
            "WEB_REPIN_WAIT_TRIES": "1",
            "WEB_REPIN_WAIT_SECS": "0",
        },
        stdin=subprocess.DEVNULL,
    )
    assert "STEP repin rc=4" in proc.stdout, proc.stdout
    assert proc.returncode != 0, (proc.returncode, proc.stdout)
    assert "STEP repin-web" not in proc.stdout, proc.stdout
    assert "STEP migrate" not in proc.stdout, proc.stdout
