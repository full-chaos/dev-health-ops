"""The bigboy ClickHouse readers user: SQL host restriction matches the compose network."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "ci" / "bigboy" / "compose.bigboy.yml"
READERS_SQL = ROOT / "ci" / "bigboy" / "prepare-clickhouse-readonly-user.sql"


class _ComposeLoader(yaml.SafeLoader):
    """Compose merge tags (`!override`, `!reset`) carry no meaning for these assertions."""


def _ignore_compose_tag(
    loader: yaml.SafeLoader, tag_suffix: str, node: yaml.Node
) -> object:
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node, deep=True)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node, deep=True)
    return loader.construct_scalar(node)


_ComposeLoader.add_multi_constructor("!", _ignore_compose_tag)


def _compose() -> dict:
    return yaml.load(COMPOSE.read_text(), Loader=_ComposeLoader)  # noqa: S506 - SafeLoader subclass


def test_readers_user_host_ip_is_the_readers_network_subnet() -> None:
    """A CIDR that drifts from the network the readers connect from locks them out."""
    subnet = _compose()["networks"]["dho-clickhouse-readers"]["ipam"]["config"][0][
        "subnet"
    ]
    match = re.search(r"HOST IP '([^']+)'", READERS_SQL.read_text())
    assert match is not None, (
        "prepare-clickhouse-readonly-user.sql has no HOST IP clause"
    )
    assert match.group(1) == subnet


def test_clickhouse_enables_access_management_so_the_readers_sql_can_apply() -> None:
    """The image writes access_management=0 for the default user; CREATE USER needs 1."""
    env = _compose()["services"]["clickhouse"]["environment"]
    if isinstance(env, list):
        env = dict(item.split("=", 1) for item in env)
    assert env["CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT"] == "1"


def test_readers_profile_forbids_writes_but_allows_per_query_settings_within_pinned_ceilings() -> (
    None
):
    """readonly = 1 refuses the per-query SETTINGS the MCP caller sends; readonly = 2 allows them,
    and only the MAX pins keep the ceilings from being raised."""
    sql = READERS_SQL.read_text()
    assert re.search(r"readonly\s*=\s*2\b", sql)
    assert re.search(r"max_execution_time\s*=\s*30\s+MAX\s+30\b", sql)
    assert re.search(r"max_memory_usage\s*=\s*4000000000\s+MAX\s+4000000000\b", sql)


def test_readers_user_is_granted_select_on_the_default_database_and_two_system_tables_only() -> (
    None
):
    """GWC 2026-10-17: prod's acr reads through acr_ro with database-wide SELECT, bigboy rehearses that shape; the per-table list
    broke the acr projector (497). Nothing but SELECT is granted, and the profile keeps every write out."""
    sql = READERS_SQL.read_text()
    grants = re.findall(r"^GRANT (.+?) TO (\w+);", sql, re.MULTILINE)
    assert grants == [
        ("SELECT ON default.*", "mcp_readonly"),
        ("SELECT ON system.tables", "mcp_readonly"),
        ("SELECT ON system.columns", "mcp_readonly"),
    ]
    assert not re.search(r"^GRANT (?!SELECT ON )", sql, re.MULTILINE)
    assert not re.search(r"^GRANT SELECT ON default\.\w+ ", sql, re.MULTILINE)
    assert re.search(r"readonly\s*=\s*2\b", sql)
