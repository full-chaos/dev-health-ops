"""CHAOS-8693: stack.files is the one list of the bigboy compose chain.

Golden: every set in stack.files equals, file for file and in order, the chain its legacy script builds today.
Guard: no new bigboy overlay file and no new raw `-f` / COMPOSE_FILE chain appears outside stack.files.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BIGBOY = ROOT / "ci" / "bigboy"
LIST = BIGBOY / "stack.files"

# Legacy scripts that still build their own chain. Switching one to stack.sh removes it from this set;
# nothing may be added to it.
LEGACY_CHAIN_SCRIPTS = {
    "bigboy-cut.sh",
    "bigboy-graphql-prove.sh",
    "bigboy-hook-check.sh",
    "bigboy-river-apply.sh",
    "web-path-smoke.sh",
}
# compose.bigboy.images.yml is the tracked copy of the host file compose/compose.bigboy.images.yml, which the
# chain names under the bigboy root.
UNLISTED_OVERLAYS = {"compose.bigboy.images.yml"}


def _sets() -> dict[str, dict[str, list[str]]]:
    out: dict[str, dict[str, list[str]]] = {}
    for line in LIST.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, kind, value = line.split()[:3]
        out.setdefault(name, {}).setdefault(kind, []).append(value)
    return out


def _files(name: str) -> list[str]:
    """Ordered files of a set; tools files are written as `$HERE/<name>` like the legacy scripts."""
    entries: list[tuple[str, str]] = []
    for line in LIST.read_text().splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[0] == name and parts[1] in ("root", "tools"):
            entries.append((parts[1], parts[2]))
    return [f"$HERE/{v}" if k == "tools" else v for k, v in entries]


def _dash_f(text: str) -> list[str]:
    return [m.strip('"') for m in re.findall(r"-f\s+(\"?[^\s\")]+\"?)", text)]


def _block(script: str, start: str) -> str:
    text = (BIGBOY / script).read_text()
    i = text.index(start)
    return text[i : text.index("\n\n", i) if "\n\n" in text[i:] else len(text)]


def test_stack_set_equals_bigboy_cut_chain() -> None:
    match = re.search(
        r"^export COMPOSE_FILE=(\S+)$", (BIGBOY / "bigboy-cut.sh").read_text(), re.M
    )
    assert match
    assert _files("stack") == match.group(1).split(":")


def test_smoke_set_equals_web_path_smoke_chain() -> None:
    block = _block("web-path-smoke.sh", "BASE_COMPOSE_ARGS=(")
    block = block[: block.index(")") + 1]
    assert _files("smoke") == _dash_f(block)


def test_prove_set_equals_graphql_prove_chain() -> None:
    text = (BIGBOY / "bigboy-graphql-prove.sh").read_text()
    base = text[text.index("BASE=(") :]
    base = base[: base.index(")\n") + 1]
    run = [ln for ln in text.splitlines() if 'docker compose "${BASE[@]}" -f' in ln][0]
    assert _files("prove") == _dash_f(base) + _dash_f(run)
    assert "--profile venue" in base and _sets()["prove"]["profile"] == ["venue"]


def test_hook_check_and_river_apply_sets_equal_their_scripts() -> None:
    for script, name, overlay in (
        ("bigboy-hook-check.sh", "hook-check", "compose.bigboy.hook-check.yml"),
        ("bigboy-river-apply.sh", "river-apply", "compose.bigboy.river-apply.yml"),
    ):
        m = re.search(
            r'^export COMPOSE_FILE="\$\{COMPOSE_FILE:-(\S+)\}:(\S+)"',
            (BIGBOY / script).read_text(),
            re.M,
        )
        assert m, script
        assert (
            _files(name) == [m.group(1), m.group(2)]
            and m.group(2) == f"$HERE/{overlay}"
        )
        assert _sets()[name]["profile"] == ["venue"]


def test_every_set_names_project_and_env_file() -> None:
    for name, kinds in _sets().items():
        assert kinds["project"] == ["dev-health"], name
        assert kinds["env-file"] == ["ops/.env"], name


def test_no_unlisted_bigboy_overlay_file() -> None:
    listed = {Path(v).name for s in _sets().values() for v in s.get("tools", [])}
    on_disk = {p.name for p in BIGBOY.glob("compose*.y*ml")}
    assert on_disk - listed == UNLISTED_OVERLAYS, (
        "a compose overlay in ci/bigboy is not in stack.files; add it there: "
        f"{sorted(on_disk - listed - UNLISTED_OVERLAYS)}"
    )
    assert listed <= on_disk, sorted(listed - on_disk)


def test_no_new_raw_chain_outside_stack_files() -> None:
    raw = re.compile(
        r"COMPOSE_FILE=|docker compose[^|\n]*\s-f\s|compose-config-redacted\.sh[^|\n]*\s-f\s"
    )
    offenders = {
        p.name
        for p in BIGBOY.glob("*.sh")
        if p.name != "stack.sh"
        and any(
            raw.search(ln)
            for ln in p.read_text().splitlines()
            if not ln.lstrip().startswith("#")
        )
    }
    assert offenders <= LEGACY_CHAIN_SCRIPTS, (
        f"raw compose -f / COMPOSE_FILE chain in {sorted(offenders - LEGACY_CHAIN_SCRIPTS)}: "
        "add the files to ci/bigboy/stack.files and call ci/bigboy/stack.sh"
    )
