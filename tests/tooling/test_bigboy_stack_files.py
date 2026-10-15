"""CHAOS-8693: stack.files is the one list of the bigboy compose chain.

Structure: the one-off sets are the stack chain plus their overlay; smoke and prove are prefixes of it; the only
root-relative entry is the per-cut image pin file. Guard: no new bigboy overlay file and no new raw `-f` / COMPOSE_FILE chain appears outside stack.files.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BIGBOY = ROOT / "ci" / "bigboy"
LIST = BIGBOY / "stack.files"

# compose.bigboy.images.yml is the tracked copy of the per-cut pin file, which the
# chain names under the bigboy root as _records/compose.bigboy.images.yml.
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


# What a root entry may still be: per-cut state, never static configuration.
ROOT_STATE_FILES = {"_records/compose.bigboy.images.yml"}


def test_the_cut_and_the_checks_call_stack_sh_and_build_no_chain() -> None:
    cut = (BIGBOY / "bigboy-cut.sh").read_text()
    assert "COMPOSE_FILE" not in "\n".join(
        ln for ln in cut.splitlines() if not ln.lstrip().startswith("#")
    )
    for verb in (
        '"$HERE/stack.sh" run migrate',
        '"$HERE/stack.sh" up query-api go-api web',
        '"$HERE/stack.sh" config',
    ):
        assert verb in cut, verb
    assert '"$HERE/stack.sh" up go-worker go-worker-heavy' in cut
    assert (
        'stack.sh" exec clickhouse clickhouse-client'
        in (BIGBOY / "bigboy-grants-check.sh").read_text()
    )
    assert (
        'stack.sh" exec postgres sh -c' in (BIGBOY / "pg-grants-check.sh").read_text()
    )
    assert (
        'stack.sh" exec "$CH" clickhouse-client'
        in (BIGBOY / "check-dho-api-ch-user.sh").read_text()
    )


def test_smoke_and_prove_scripts_call_stack_sh_for_their_sets() -> None:
    smoke = (BIGBOY / "web-path-smoke.sh").read_text()
    assert (
        'STACK=("$HERE/stack.sh" -s smoke)' in smoke
        and '"${STACK[@]}" run web-smoke' in smoke
    )
    assert '"${STACK[@]}" config' in smoke
    prove = (BIGBOY / "bigboy-graphql-prove.sh").read_text()
    assert '"$HERE/stack.sh" -s prove-base exec postgres psql' in prove
    assert '"$HERE/stack.sh" -s prove run venue-prove' in prove


def test_smoke_chain_is_the_stack_chain_minus_the_worker_overlays_plus_smoke() -> None:
    # web-path-smoke ran: base, go-workers, images, router, smoke (no workers/clickhouse-users/billing-edge)
    stack = _files("stack")
    assert _files("smoke") == stack[:3] + [
        "$HERE/compose.bigboy.router.yml",
        "$HERE/compose.bigboy.smoke.yml",
    ]


def test_prove_base_is_the_first_four_stack_files_with_the_venue_profile() -> None:
    # bigboy-graphql-prove's BASE chain (used for the psql exec) and prove = BASE + the prove overlay
    assert _files("prove-base") == _files("stack")[:3]
    assert _sets()["prove-base"]["profile"] == ["venue"]
    assert _files("prove") == _files("prove-base") + ["$HERE/compose.bigboy.prove.yml"]


def test_hook_check_and_river_apply_sets_are_the_cut_chain_plus_their_overlay() -> None:
    """They run inside the cut, on its whole chain; the scripts now call stack.sh -s <set> (CHAOS-8693)."""
    for script, name, overlay in (
        ("bigboy-hook-check.sh", "hook-check", "compose.bigboy.hook-check.yml"),
        ("bigboy-river-apply.sh", "river-apply", "compose.bigboy.river-apply.yml"),
    ):
        assert _files(name) == _files("stack") + [f"$HERE/{overlay}"]
        assert _sets()[name]["profile"] == ["venue"]
        assert f'"$HERE/stack.sh" -s {name} run' in (
            BIGBOY / script
        ).read_text().replace("${HC[@]}", "")


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


def test_root_entries_are_only_per_cut_state() -> None:
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
    assert not found, (
        f"raw compose -f / COMPOSE_FILE chain in {found}: "
        "add the files to ci/bigboy/stack.files and call ci/bigboy/stack.sh"
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
