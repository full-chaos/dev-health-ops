"""CHAOS-9962: the bigboy stack is ONE compose file, ci/bigboy/compose.bigboy.yml, started with plain `docker compose`.

chris (board, 2026-10-16): "I already told gwc-backlog that I wanted a COMPOSE file to start bigboy service, and you still
went with a script." stack.sh, stack.files and the twelve overlay files are retired. These tests hold the shape:

- one compose file and no chain script in ci/bigboy; no script names a second -f or a COMPOSE_* variable;
- the service list per profile is what the old stack.sh chains rendered (the render of the old `stack` chain and of this file
  was byte-identical when the file was made; the lists below are the durable half of that gate);
- the image digests live in images.env only, and the file renders with nothing but the pins file and the two variables
  the live stack needs (a one-off service must never need its own variables to render, because compose interpolates the
  services of an inactive profile too).
"""

from __future__ import annotations

import re

from tests.tooling import _bigboy_compose as bb

STACK_SERVICES = """bugsink clickhouse go-api go-contractcheck go-reconciler go-river-provision go-scheduler go-stream-external
go-stream-ingest go-stream-pagerduty go-sync-dispatch-route-activate go-sync-finalize-route-activate go-sync-post-route-activate
go-sync-reference-route-activate go-worker go-worker-consumers-ready go-worker-heavy go-worker-migrate go-worker-ops go-worker-ready
go-worker-route-activate mailpit migrate pgbouncer pgbouncer-river-coordinator pgbouncer-river-queue postgres query-api riverui
traefik valkey web""".split()


def test_one_compose_file_and_no_chain_script() -> None:
    files = sorted(
        p.name for p in bb.TOOLS.iterdir() if p.name.startswith("compose.bigboy")
    )
    assert files == ["compose.bigboy.yml"], files
    assert (
        not (bb.TOOLS / "stack.sh").exists() and not (bb.TOOLS / "stack.files").exists()
    )


def test_no_script_builds_its_own_file_chain() -> None:
    """Every `docker compose` of the scripts is the DC array: the one file, the two env files, -p dev-health."""
    offenders = []
    for script in sorted(bb.TOOLS.glob("*.sh")):
        for number, line in enumerate(script.read_text().splitlines(), 1):
            code = line.split("#", 1)[0] if line.lstrip().startswith("#") else line
            if (
                re.search(r"COMPOSE_FILE|\bCOMPOSE_PROFILES\b", code)
                and "grep '^COMPOSE_'" not in code
                and "compgen" not in code
            ):
                offenders.append(f"{script.name}:{number}: {line.strip()}")
            if "docker compose" in code and not line.lstrip().startswith("#"):
                if " -f " in code and 'compose.bigboy.yml"' not in code:
                    offenders.append(f"{script.name}:{number}: {line.strip()}")
    assert offenders == [], offenders


def test_the_live_stack_runs_exactly_the_services_of_the_old_chain(tmp_path) -> None:
    root = bb.scratch_root(tmp_path)
    assert sorted(bb.services(root)) == sorted(STACK_SERVICES)


def test_profiles_add_only_their_own_services(tmp_path) -> None:
    root = bb.scratch_root(tmp_path)
    base = set(bb.services(root))
    assert set(bb.services(root, "smoke")) - base == {"web-smoke"}
    assert set(bb.services(root, "venue")) - base == {"venue-prove", "venue-tools"}
    assert set(bb.services(root, "venue-hook")) - base == {"venue-hook"}
    assert set(bb.services(root, "venue-river")) - base == {"venue-river"}
    # retired services stay out of every default and one-off render (the Python api, the Python billing edge)
    assert not {"api", "billing-edge", "metrics-api"} & set(
        bb.services(root, "smoke", "venue", "venue-hook", "venue-river")
    )


def test_the_digests_are_in_the_pins_file_only(tmp_path) -> None:
    compose = bb.COMPOSE_FILE.read_text()
    assert "@sha256:" not in compose
    pins = dict(
        line.split("=", 1)
        for line in bb.PINS_FILE.read_text().splitlines()
        if line and not line.startswith("#")
    )
    assert sorted(pins) == [
        "BIGBOY_API_TOOLS_IMAGE",
        "BIGBOY_DHO_IMAGE",
        "BIGBOY_WEB_IMAGE",
    ], sorted(pins)
    for name, ref in pins.items():
        assert re.fullmatch(r"ghcr\.io/full-chaos/[a-z-]+@sha256:[0-9a-f]{64}", ref), (
            name,
            ref,
        )
        assert "${" + name + ":?" in compose, name
    rendered = bb.services(
        bb.scratch_root(tmp_path), "smoke", "venue", "venue-hook", "venue-river"
    )
    assert rendered["query-api"]["image"] == pins["BIGBOY_DHO_IMAGE"]
    assert rendered["web"]["image"] == pins["BIGBOY_WEB_IMAGE"]
    assert rendered["venue-river"]["image"] == pins["BIGBOY_DHO_IMAGE"]
    assert rendered["venue-prove"]["image"] == pins["BIGBOY_API_TOOLS_IMAGE"]


def test_a_one_off_needs_none_of_its_own_variables_to_render(tmp_path) -> None:
    """Only the pins file, BIGBOY_OPERATOR_IMAGE and DHO_API_CH_USERS_XML: a `:?` inside a one-off service would break every
    other command (compose interpolates an inactive profile too)."""
    root = bb.scratch_root(tmp_path)
    for profile in (
        (),
        ("smoke",),
        ("venue",),
        ("venue-hook",),
        ("venue-river",),
        ("smoke", "venue", "venue-hook", "venue-river"),
    ):
        proc = bb.config(root, *profile)
        assert proc.returncode == 0, (profile, proc.stderr[-400:])
    required = sorted(
        set(re.findall(r"\$\{([A-Za-z_]+):\?", bb.COMPOSE_FILE.read_text()))
    )
    assert required == [
        "BIGBOY_API_TOOLS_IMAGE",
        "BIGBOY_DHO_IMAGE",
        "BIGBOY_OPERATOR_IMAGE",
        "BIGBOY_WEB_IMAGE",
        "DHO_API_CH_USERS_XML",
    ], required


def test_the_one_off_scripts_check_their_own_variables() -> None:
    """The `:?` they dropped for the compose file is a check in the script that starts them."""
    smoke = (bb.TOOLS / "web-path-smoke.sh").read_text()
    assert "resolves BLANK" in smoke and "DHO_SMOKE_ADMIN_EMAIL" in smoke
    prove = (bb.TOOLS / "bigboy-graphql-prove.sh").read_text()
    assert (
        "JWT_SECRET_KEY has no value in ops/.env" in prove
        and "exactly one org" in prove
    )


# The six old stack.sh sets, as the profiles that select the same services in the one file (CHAOS-9962 gate). `stack`, `hook-check`
# and `river-apply` rendered exactly these services before; `smoke`, `prove-base` and `prove` were short chains (no workers, no
# ClickHouse users mount, no billing override) and also rendered the Python `billing-edge`, which the live chain retires: the one
# file keeps it retired. Each of those three was only ever run by service name (`run --rm --no-deps -T web-smoke|venue-prove`).
OLD_SETS = {
    "stack": ((), set()),
    "smoke": (("smoke",), {"web-smoke"}),
    "prove-base": (("venue",), {"venue-prove", "venue-tools"}),
    "prove": (("venue",), {"venue-prove", "venue-tools"}),
    "hook-check": (
        ("venue", "venue-hook"),
        {"venue-prove", "venue-tools", "venue-hook"},
    ),
    "river-apply": (
        ("venue", "venue-river"),
        {"venue-prove", "venue-tools", "venue-river"},
    ),
}


def test_each_old_set_renders_the_services_it_rendered(tmp_path) -> None:
    root = bb.scratch_root(tmp_path)
    for name, (profiles, extra) in OLD_SETS.items():
        assert set(bb.services(root, *profiles)) == set(STACK_SERVICES) | extra, name
