"""CHAOS-7055: bigboy serves the billing host from go-api's billing-edge listener and retires the Python service.

The override is part of the cut's compose chain (so go-api is never recreated without the listener and the
router labels), carries the three Stripe/license values only as name-only pass-through (no value is written in
the file), and moves the Python `billing-edge` service to a profile nothing enables. Nothing here reads a secret.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "ci" / "bigboy"
OVERRIDE = TOOLS / "compose.bigboy.billing-edge.yml"
CUT = TOOLS / "bigboy-cut.sh"


class _Loader(yaml.SafeLoader):
    """Reads compose's `!override` tag as the value it wraps."""


_Loader.add_constructor(
    "!override",
    lambda loader, node: (
        loader.construct_sequence(node, deep=True)
        if isinstance(node, yaml.SequenceNode)
        else loader.construct_mapping(node, deep=True)
    ),
)


def _services() -> dict:
    return yaml.load(OVERRIDE.read_text(), Loader=_Loader)["services"]  # noqa: S506


def test_go_api_gets_the_billing_listener_and_the_router_labels() -> None:
    go_api = _services()["go-api"]
    command = go_api["command"]
    assert command == ["api", "--api-billing-edge-addr=:8010"], command
    labels = go_api["labels"]
    assert labels["traefik.http.routers.billing.rule"] == "Host(`billing.localhost`)"
    assert labels["traefik.http.routers.billing.entrypoints"] == "web"
    assert labels["traefik.http.routers.billing.service"] == "billing"
    assert labels["traefik.http.services.billing.loadbalancer.server.port"] == "8010"
    assert labels["traefik.enable"] == "true"
    assert labels["traefik.scope"] == "dev-health"


def test_the_three_values_are_name_only_pass_through() -> None:
    env = _services()["go-api"]["environment"]
    assert set(env) == {
        "STRIPE_SECRET_KEY",
        "STRIPE_WEBHOOK_SECRET",
        "LICENSE_PRIVATE_KEY",
    }
    for name, value in env.items():
        assert value == f"${{{name}:-}}", (name, value)


def test_the_python_billing_edge_service_is_retired_by_profile() -> None:
    edge = _services()["billing-edge"]
    assert edge == {"profiles": ["retired-billing-edge"]}, edge
    # nothing in the tracked bigboy tooling enables that profile
    for path in TOOLS.iterdir():
        if path.is_file() and path != OVERRIDE:
            assert "retired-billing-edge" not in path.read_text(errors="ignore"), path


def test_the_override_is_in_the_cut_chain_before_the_router() -> None:
    chain = next(
        line
        for line in CUT.read_text().splitlines()
        if line.startswith("export COMPOSE_FILE=")
    )
    files = chain.split("=", 1)[1].split(":")
    assert "$HERE/compose.bigboy.billing-edge.yml" in files, files
    assert files.index("$HERE/compose.bigboy.billing-edge.yml") < files.index(
        "$HERE/compose.bigboy.router.yml"
    )
    assert files[-1] == "$HERE/compose.bigboy.router.yml"


def test_no_secret_value_is_written_in_the_file() -> None:
    text = OVERRIDE.read_text()
    assert not re.search(r"sk_(live|test)_|whsec_|-----BEGIN", text)
