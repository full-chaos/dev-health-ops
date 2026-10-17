"""CHAOS-10004: the acr venue stack is part of the ONE bigboy compose file (profiles `acr` and `acr-venue`).

These tests hold what the move must not change: the host paths of the data, the container names, the networks the acr
services reach (and the one they must not), the secret files (paths only, never a value), the image pins, and that the
default stack renders exactly as before.
"""

from __future__ import annotations

import json
import re

from tests.tooling.test_bigboy_one_compose_file import (
    COMPOSE_FILE,
    PINS_FILE,
    STACK_SERVICES,
    config,
    scratch_root,
    services,
)

ACR_ALL = {
    "acr-api",
    "acr-db-acl",
    "acr-db-init",
    "acr-entitlement-standin",
    "acr-falkordb",
    "acr-mcp",
    "acr-migrate",
    "acr-postgres",
    "acr-projector",
}
ACR_VENUE = {
    "acr-db-acl",
    "acr-db-init",
    "acr-entitlement-standin",
    "acr-migrate",
    "acr-postgres",
}
SECRETS_DIR = "/var/lib/oci-cache/mcp-venue/secrets/"


def test_the_default_stack_does_not_start_an_acr_service(tmp_path) -> None:
    root = scratch_root(tmp_path)
    assert sorted(services(root)) == sorted(STACK_SERVICES)


def test_each_acr_profile_renders_alone_and_adds_only_acr_services(tmp_path) -> None:
    root = scratch_root(tmp_path)
    base = set(services(root))
    assert set(services(root, "acr")) - base == ACR_ALL
    assert set(services(root, "acr-venue")) - base == ACR_VENUE
    assert set(services(root, "acr", "acr-venue")) - base == ACR_ALL


def test_the_data_stays_on_the_host_paths_of_the_old_project(tmp_path) -> None:
    root = scratch_root(tmp_path)
    svc = services(root, "acr")
    binds = {
        (name, v["target"]): v["source"]
        for name, s in svc.items()
        for v in s.get("volumes", [])
        if v["type"] == "bind" and name.startswith("acr-")
    }
    assert (
        binds[("acr-postgres", "/var/lib/postgresql")]
        == "/var/lib/oci-cache/mcp-venue/postgres-data"
    )
    assert (
        binds[("acr-falkordb", "/var/lib/falkordb/data")]
        == "/var/lib/oci-cache/mcp-venue/falkordb-data"
    )
    proc = config(root, "acr")
    assert proc.returncode == 0, proc.stderr
    volumes = json.loads(proc.stdout)["volumes"]
    assert volumes["acr_entitlement_tmp"]["name"] == "dev-health-acr-entitlement-tmp"


def test_container_names_and_aliases_keep_every_acr_hostname(tmp_path) -> None:
    root = scratch_root(tmp_path)
    svc = services(root, "acr")
    assert {n: s["container_name"] for n, s in svc.items() if n in ACR_ALL} == {
        "acr-api": "dev-health-acr-mcp-acr-api-1",
        "acr-db-acl": "dev-health-acr-mcp-acr-db-acl-1",
        "acr-db-init": "dev-health-acr-mcp-acr-db-init-1",
        "acr-entitlement-standin": "dev-health-acr-mcp-acr-entitlement-standin-1",
        "acr-falkordb": "dev-health-acr-mcp-falkordb-1",
        "acr-mcp": "dev-health-acr-mcp-acr-mcp-1",
        "acr-migrate": "dev-health-acr-mcp-acr-migrate-1",
        "acr-postgres": "dev-health-acr-mcp-postgres-1",
        "acr-projector": "dev-health-acr-mcp-acr-projector-1",
    }
    # `postgres` and `falkordb` answer on the acr network only; the dev-health `postgres` is a different service
    assert svc["acr-postgres"]["networks"]["acr"]["aliases"] == ["postgres"]
    assert svc["acr-falkordb"]["networks"]["acr"]["aliases"] == ["falkordb"]
    assert "acr" not in svc["postgres"]["networks"]


def test_the_acr_services_reach_the_networks_they_always_did(tmp_path) -> None:
    root = scratch_root(tmp_path)
    svc = services(root, "acr")
    nets = {n: set(svc[n].get("networks", {})) for n in ACR_ALL}
    assert nets["acr-api"] == {"acr", "dho-query-internal", "dho-clickhouse-readers"}
    assert nets["acr-projector"] == {"acr", "dho-clickhouse-readers"}
    for name in ACR_ALL - {"acr-api", "acr-projector"}:
        assert nets[name] == {"acr"}, name
    # the acr stack is never attached to the ops main network
    assert not any("dev-health" in n for n in set().union(*nets.values()))
    env = svc["acr-api"]["environment"]
    for key in ("ACR_DATA_QUERY_URL", "ACR_DATA_GRAPHQL_URL"):
        assert re.fullmatch(r"http://query-api:\d+", env[key]), env[key]


def test_only_the_mcp_server_publishes_a_port_and_only_on_loopback(tmp_path) -> None:
    root = scratch_root(tmp_path)
    svc = services(root, "acr")
    published = {
        n: s["ports"] for n, s in svc.items() if n in ACR_ALL and s.get("ports")
    }
    assert list(published) == ["acr-mcp"]
    (port,) = published["acr-mcp"]
    assert (port["host_ip"], port["published"], port["target"]) == (
        "127.0.0.1",
        "8081",
        8081,
    )


def test_no_secret_value_is_in_the_file_only_paths_of_secret_files(tmp_path) -> None:
    root = scratch_root(tmp_path)
    proc = config(root, "acr")
    assert proc.returncode == 0, proc.stderr
    cfg = json.loads(proc.stdout)
    for name, secret in cfg["secrets"].items():
        if name.startswith("acr_"):
            assert secret["file"].startswith(SECRETS_DIR), (name, secret)
    for name in ACR_ALL:
        for key, value in cfg["services"][name].get("environment", {}).items():
            assert not re.search(r"://[^/\s]*:[^/\s@]*@", str(value)), (name, key)
            if re.search(r"(PASSWORD|KEY|TOKEN|DSN|KEYS)$", key):
                assert str(value).startswith("/run/secrets/"), (name, key)


def test_the_acr_images_are_pinned_by_registry_digest_and_a_missing_pin_fails_the_render(
    tmp_path,
) -> None:
    root = scratch_root(tmp_path)
    svc = services(root, "acr")
    pins = dict(
        line.split("=", 1)
        for line in PINS_FILE.read_text().splitlines()
        if line and not line.startswith("#")
    )
    for var, names in (
        ("BIGBOY_ACR_API_IMAGE", ("acr-api", "acr-migrate", "acr-projector")),
        ("BIGBOY_ACR_MCP_IMAGE", ("acr-mcp",)),
    ):
        assert re.fullmatch(
            r"ghcr\.io/full-chaos/dev-health-acr/acr-(api|mcp)@sha256:[0-9a-f]{64}",
            pins[var],
        ), var
        for name in names:
            assert svc[name]["image"] == pins[var], name
    for var, names in (
        ("BIGBOY_ACR_POSTGRES_IMAGE", ("acr-postgres", "acr-db-init", "acr-db-acl")),
        ("BIGBOY_ACR_NGINX_IMAGE", ("acr-entitlement-standin",)),
        ("BIGBOY_ACR_FALKORDB_IMAGE", ("acr-falkordb",)),
    ):
        assert re.search(r"@sha256:[0-9a-f]{64}$", pins[var]), var
        for name in names:
            assert svc[name]["image"] == pins[var], name
    # a missing pin FAILS THE RENDER, naming the variable, for each of the five: no default tag can start a wrong image
    pinned = PINS_FILE.read_text().splitlines()
    for var in (
        "BIGBOY_ACR_API_IMAGE",
        "BIGBOY_ACR_MCP_IMAGE",
        "BIGBOY_ACR_POSTGRES_IMAGE",
        "BIGBOY_ACR_NGINX_IMAGE",
        "BIGBOY_ACR_FALKORDB_IMAGE",
    ):
        (root / "_records" / "images.env").write_text(
            "\n".join(line for line in pinned if not line.startswith(var + "=")) + "\n"
        )
        for profiles in ((), ("acr",)):
            proc = config(root, *profiles)
            assert proc.returncode != 0 and var in proc.stderr, (
                var,
                profiles,
                proc.returncode,
            )
            assert "is required" in proc.stderr, proc.stderr[-300:]


def test_the_start_order_of_the_acr_stack_is_kept(tmp_path) -> None:
    root = scratch_root(tmp_path)
    svc = services(root, "acr")
    cond = {
        n: {d: v["condition"] for d, v in s.get("depends_on", {}).items()}
        for n, s in svc.items()
        if n in ACR_ALL
    }
    assert cond["acr-db-init"] == {"acr-postgres": "service_healthy"}
    assert cond["acr-migrate"] == {
        "acr-postgres": "service_healthy",
        "acr-db-init": "service_completed_successfully",
    }
    assert cond["acr-db-acl"] == {
        "acr-postgres": "service_healthy",
        "acr-migrate": "service_completed_successfully",
    }
    assert cond["acr-api"] == {
        "acr-postgres": "service_healthy",
        "acr-db-acl": "service_completed_successfully",
    }
    assert cond["acr-projector"] == {
        "acr-postgres": "service_healthy",
        "acr-db-acl": "service_completed_successfully",
    }
    assert cond["acr-mcp"] == {"acr-api": "service_started"}


def test_acr_api_talks_to_the_real_ops_and_the_stand_in_is_off_its_path(
    tmp_path,
) -> None:
    root = scratch_root(tmp_path)
    svc = services(root, "acr")
    env = svc["acr-api"]["environment"]
    # the go-api internal listener by alias, never an address; the bearer is a file-backed secret named acr_ops_svc_token
    assert env["ACR_DEV_HEALTH_ENTITLEMENT_URL"] == "http://go-api:8091"
    assert (
        env["ACR_DEV_HEALTH_ENTITLEMENT_TOKEN_FILE"] == "/run/secrets/acr_ops_svc_token"
    )
    assert "acr_ops_svc_token" in svc["acr-api"]["secrets"] or any(
        s["source"] == "acr_ops_svc_token" for s in svc["acr-api"]["secrets"]
    )
    assert "acr-entitlement-standin" not in svc["acr-api"].get("depends_on", {})
    # the removed setting stays removed
    assert "ACR_CONTEXT_FABRIC_MODEL_MAX_TRANSPORT_RETRIES" not in env
    proc = config(root, "acr")
    secret = json.loads(proc.stdout)["secrets"]["acr_ops_svc_token"]
    assert secret["file"] == "/var/lib/oci-cache/mcp-venue/secrets/ops_svc_acr_token"
    # acr-api reaches go-api on a network go-api is on
    assert "dho-query-internal" in svc["acr-api"]["networks"]
    assert "dho-query-internal" in services(root)["go-api"]["networks"]


def test_the_org_llm_config_source_is_passed_through_and_no_file_sets_it(
    tmp_path,
) -> None:
    """chris: a persistent BYO setting on bigboy bills his key. The compose file passes the variable through; the tracked env
    file does not set it; an environment can turn it on for one run."""
    root = scratch_root(tmp_path)
    key = "ACR_CONTEXT_FABRIC_ORG_LLM_CONFIG_SOURCE"
    assert "ORG_LLM_CONFIG_SOURCE: ops" not in COMPOSE_FILE.read_text()
    assert not any(line.startswith(key) for line in PINS_FILE.read_text().splitlines())
    assert services(root, "acr")["acr-api"]["environment"][key] == ""
    on = services(root, "acr", **{key: "ops"})
    assert on["acr-api"]["environment"][key] == "ops"
    # only acr-api reads it
    assert key not in services(root, "acr")["acr-projector"]["environment"]


def test_the_switch_procedure_removes_the_old_containers_and_keeps_the_data() -> None:
    """The new file fixes the old project's container names, so a stopped old container makes the new `up` fail with a name conflict
    (executed in a scratch project): the documented step is `down` of the old project, before the new `up`, never `-v`, never `--rmi`."""
    readme = (COMPOSE_FILE.parent / "README.md").read_text()
    section = readme[readme.index("The one-time move from the old project") :]
    section = section[: section.index("\n## ")]
    commands = re.findall(r"`([^`]*docker [^`]+)`", section)
    downs = [c for c in commands if re.search(r"\bdown\b", c)]
    assert len(downs) == 1, commands
    (down,) = downs
    assert "-p dev-health-acr-mcp " in down and "-f compose.yml" in down, down
    assert not re.search(r"\s-v\b|--volumes|--rmi", down), down
    assert not [c for c in commands if re.search(r"\bstop\b|\bkill\b", c)], commands
    assert section.index(down) < section.index("--profile acr up -d")
    assert "REMOVED, not only stopped" in section
    # MCP: the graph is saved before the down, the images are checked after the up, the repository count is read back
    assert section.index("BGSAVE") < section.index(down)
    assert "falkordb-data.backup-" in section
    assert section.index("--profile acr up -d") < section.index(
        "docker inspect --format"
    )
    assert "MATCH (r:Repository) RETURN count(r)" in section
    assert "docker volume rm dev-health-acr-mcp_acr_entitlement_tmp" in section
