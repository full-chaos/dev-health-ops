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

# Legacy scripts that still build their own chain, with the number of raw-chain lines each has today. Switching one
# to stack.sh lowers its number; no number may rise and no script may be added.
LEGACY_CHAIN_LINES = {
    "bigboy-cut.sh": 2,
    "bigboy-graphql-prove.sh": 3,
    "bigboy-hook-check.sh": 1,
    "bigboy-river-apply.sh": 1,
    "web-path-smoke.sh": 3,
}
# compose.bigboy.images.yml is the tracked copy of the host file compose/compose.bigboy.images.yml, which the
# chain names under the bigboy root.
UNLISTED_OVERLAYS = {"compose.bigboy.images.yml"}


def _raw() -> dict[str, list[tuple[str, str]]]:
    out: dict[str, list[tuple[str, str]]] = {}
    for line in LIST.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, kind, value = line.split()[:3]
        out.setdefault(name, []).append((kind, value))
    return out


def _sets() -> dict[str, dict[str, list[str]]]:
    """Each set with its `include`d sets resolved first, as stack.sh reads it."""
    raw = _raw()

    def resolve(name: str) -> list[tuple[str, str]]:
        lines: list[tuple[str, str]] = []
        for kind, value in raw[name]:
            if kind == "include":
                lines += resolve(value)
        return lines + [(k, v) for k, v in raw[name] if k != "include"]

    out: dict[str, dict[str, list[str]]] = {}
    for name in raw:
        for kind, value in resolve(name):
            out.setdefault(name, {}).setdefault(kind, []).append(value)
    return out


def _files(name: str) -> list[str]:
    """Ordered files of a set; tools files are written as `$HERE/<name>` like the legacy scripts."""
    entries: list[tuple[str, str]] = []
    for kind, value in _resolved_lines(name):
        if kind in ("root", "tools"):
            entries.append((kind, value))
    return [f"$HERE/{v}" if k == "tools" else v for k, v in entries]


def _resolved_lines(name: str) -> list[tuple[str, str]]:
    raw = _raw()
    lines: list[tuple[str, str]] = []
    for kind, value in raw[name]:
        if kind == "include":
            lines += _resolved_lines(value)
    return lines + [(k, v) for k, v in raw[name] if k != "include"]


# The legacy scripts still name the two static host files; stack.files names their tracked copies (content equality
# with the host files is check-host-copies.sh's job: the host files are not in this repo).
_TRACKED_COPY = {
    "compose.yml": "$HERE/compose.bigboy.base.yml",
    "compose/compose.go.workers.yml": "$HERE/compose.bigboy.go-workers.yml",
}
# What a root entry may still be: per-cut state and the lane override, never static configuration.
ROOT_STATE_FILES = {
    "compose/compose.bigboy.images.yml",
    ".remember/lanes/team-lead/reconciler-sweep-override.yml",
}


def _tracked(files: list[str]) -> list[str]:
    return [_TRACKED_COPY.get(f, f) for f in files]


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
    assert _files("stack") == _tracked(match.group(1).split(":"))


def test_smoke_set_equals_web_path_smoke_chain() -> None:
    block = _block("web-path-smoke.sh", "BASE_COMPOSE_ARGS=(")
    block = block[: block.index(")") + 1]
    assert _files("smoke") == _tracked(_dash_f(block))


def test_prove_set_equals_graphql_prove_chain() -> None:
    text = (BIGBOY / "bigboy-graphql-prove.sh").read_text()
    base = text[text.index("BASE=(") :]
    base = base[: base.index(")\n") + 1]
    run = [ln for ln in text.splitlines() if 'docker compose "${BASE[@]}" -f' in ln][0]
    assert _files("prove") == _tracked(_dash_f(base) + _dash_f(run))
    assert "--profile venue" in base and _sets()["prove"]["profile"] == ["venue"]


def test_hook_check_and_river_apply_sets_equal_the_chain_they_run_in_the_cut() -> None:
    cut = re.search(
        r"^export COMPOSE_FILE=(\S+)$", (BIGBOY / "bigboy-cut.sh").read_text(), re.M
    )
    assert cut
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
        assert m.group(2) == f"$HERE/{overlay}"
        # the script appends its overlay to the COMPOSE_FILE the cut exported
        assert _files(name) == _tracked(cut.group(1).split(":")) + [m.group(2)]
        assert _sets()[name]["profile"] == ["venue"]


def test_needs_cover_every_required_variable_of_each_set() -> None:
    derived = {
        "BIGBOY_OPERATOR_IMAGE",
        "RIVER_DHO_IMAGE",
        "DHO_SMOKE_DIR",
        "DHO_API_CH_USERS_XML",
    }
    env_file = {
        "JWT_SECRET_KEY",
        "DHO_SMOKE_ADMIN_EMAIL",
        "DHO_SMOKE_ADMIN_PASSWORD_FILE",
    }
    for name, kinds in _sets().items():
        required = set()
        for f in kinds.get("tools", []):
            required |= set(re.findall(r"\$\{([A-Z_]+):\?", (BIGBOY / f).read_text()))
        assert required <= derived | env_file | set(kinds.get("needs", [])), (
            name,
            sorted(required - derived - env_file - set(kinds.get("needs", []))),
        )


def test_root_entries_are_only_per_cut_state_and_the_lane_override() -> None:
    for name in _raw():
        roots = {v for k, v in _resolved_lines(name) if k == "root"}
        assert roots <= ROOT_STATE_FILES, (name, sorted(roots - ROOT_STATE_FILES))


def test_every_set_names_project_and_env_file() -> None:
    for name, kinds in _sets().items():
        assert kinds["project"] == ["dev-health"], name
        assert kinds["env-file"] == ["ops/.env"], name


def _overlays(directory: Path) -> set[str]:
    return {str(p.relative_to(directory)) for p in directory.rglob("compose*.y*ml")}


def test_overlay_scan_sees_nested_files(tmp_path: Path) -> None:
    (tmp_path / "a" / "b").mkdir(parents=True)
    (tmp_path / "a" / "b" / "compose.x.yml").write_text("")
    assert _overlays(tmp_path) == {"a/b/compose.x.yml"}


def test_no_unlisted_bigboy_overlay_file() -> None:
    listed = {v for s in _sets().values() for v in s.get("tools", [])}
    on_disk = _overlays(BIGBOY)
    assert on_disk - listed == UNLISTED_OVERLAYS, (
        "a compose overlay in ci/bigboy is not in stack.files; add it there: "
        f"{sorted(on_disk - listed - UNLISTED_OVERLAYS)}"
    )
    assert listed <= on_disk, sorted(listed - on_disk)


_RAW_CHAIN = re.compile(
    r"COMPOSE_FILE=|(?<!\[)\s-f\s*\"?(\$\{?\w*(COMPOSE|FILE|OV)\w*|\S*compose\S*\.ya?ml)|docker compose[^|\n]*\s-f\s"
)


def _raw_chain_lines(directory: Path) -> dict[str, int]:
    out: dict[str, int] = {}
    for p in directory.rglob("*.sh"):
        if p.name == "stack.sh":
            continue
        lines = (
            ln for ln in p.read_text().splitlines() if not ln.lstrip().startswith("#")
        )
        n = sum(1 for ln in lines if _RAW_CHAIN.search(ln))
        if n:
            out[p.name] = n
    return out


def test_no_new_raw_chain_outside_stack_files() -> None:
    found = _raw_chain_lines(BIGBOY)
    grown = {k: v for k, v in found.items() if v > LEGACY_CHAIN_LINES.get(k, 0)}
    assert not grown, (
        f"more raw compose -f / COMPOSE_FILE chain lines than today in {grown}: "
        "add the files to ci/bigboy/stack.files and call ci/bigboy/stack.sh"
    )
    assert found == LEGACY_CHAIN_LINES, (
        f"a legacy script changed its raw-chain line count: lower the number in LEGACY_CHAIN_LINES to {found}"
    )


def test_raw_chain_guard_detects_nested_and_array_built_chains(tmp_path: Path) -> None:
    nested = tmp_path / "sub" / "deep"
    nested.mkdir(parents=True)
    (nested / "a.sh").write_text(
        'ARGS=(--env-file x -f "$ROOT/compose.yml" -f "$OV")\ndocker compose "${ARGS[@]}" up\n'
    )
    (nested / "b.sh").write_text("export COMPOSE_FILE=a.yml:b.yml\n")
    (nested / "c.sh").write_text("docker compose ps\n# docker compose -f x.yml up\n")
    assert set(_raw_chain_lines(tmp_path)) == {"a.sh", "b.sh"}
    (nested / "d.sh").write_text('docker compose -f"$X/compose.y.yml" up\n')
    assert "d.sh" in _raw_chain_lines(tmp_path)
