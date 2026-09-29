#!/usr/bin/env python3
"""render-dho-api-ch-users.py <authorization.go> <out-file> -- CHAOS-7162.

Writes ClickHouse's users.d declaration of dho_api_ch for bigboy. It takes NO XML as input: the file is
built from a fixed element table, and only two values vary.

  inputs   API_CH_PASSWORD   environment only (never argv); becomes its SHA-256 in <password_sha256_hex>.
           <authorization.go> the Go source holding func APIPosture (internal/storage/clickhouse/
                              authorization.go at the ops sha being cut); its table manifest becomes the
                              <grants> (the statements clickhouse.GrantStatements would issue, without the
                              role, which users.d implies).
  output   a fresh tree of exactly: clickhouse/users/dho_api_ch/{networks/ip, profile, quota,
           access_management, password_sha256_hex, grants/query...}. No attribute, comment, processing
           instruction or text from any input reaches it; the SHA-256 of the password is the only derived
           secret. It is validated against the shared grammar before anything is written, and refused
           (nothing written) if the password itself would appear in it.

The file is written 0644 because clickhouse-server runs as uid 101 in its container and exits on a mounted
users.d file it cannot read. Prints names and counts only.
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import re
import sys
import tempfile
import xml.etree.ElementTree as ET
from collections.abc import Callable
from pathlib import Path

DATABASE = "default"
_TABLE = re.compile(r'\{Database: database, Table: "(\w+)",([^}]*)\}')
_PRIVILEGES = (
    ("AllowSelect", "SELECT"),
    ("AllowInsert", "INSERT"),
    ("AllowDelete", "ALTER DELETE"),
)


def _load_validate() -> Callable[[str], None]:
    """The shape module is a sibling file, loaded by path so the script runs from any directory."""
    path = Path(__file__).resolve().parent / "dho_api_ch_users_shape.py"
    spec = importlib.util.spec_from_file_location("dho_api_ch_users_shape", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    validator: Callable[[str], None] = module.validate
    return validator


validate = _load_validate()


def grants_from_manifest(source: str, database: str = DATABASE) -> list[str]:
    """The GRANT statements of func APIPosture, in manifest order (the same extraction
    bigboy-grants-check-compare.py uses; an unparsable manifest yields nothing and is refused)."""
    if "func APIPosture(" not in source:
        return []
    body = source.split("func APIPosture(", 1)[1].split("\n}\n", 1)[0]
    statements: list[str] = []
    for line in body.splitlines():
        match = _TABLE.search(line)
        if not match:
            continue
        privileges = [
            name for flag, name in _PRIVILEGES if f"{flag}: true" in match.group(2)
        ]
        if privileges:
            statements.append(
                f"GRANT {', '.join(privileges)} ON {database}.{match.group(1)}"
            )
    return statements


def render(grants: list[str], password: str) -> str:
    """Build the file from the fixed table; the only variable inputs are the hash and the grant list."""
    if not grants:
        raise SystemExit("the posture manifest yielded no grants, nothing written")
    root = ET.Element("clickhouse")
    user = ET.SubElement(ET.SubElement(root, "users"), "dho_api_ch")
    ET.SubElement(ET.SubElement(user, "networks"), "ip").text = "::/0"
    ET.SubElement(user, "profile").text = "default"
    ET.SubElement(user, "quota").text = "default"
    ET.SubElement(user, "access_management").text = "0"
    ET.SubElement(user, "password_sha256_hex").text = hashlib.sha256(
        password.encode()
    ).hexdigest()
    block = ET.SubElement(user, "grants")
    for statement in grants:
        ET.SubElement(block, "query").text = statement
    rendered = ET.tostring(root, encoding="unicode") + "\n"
    try:
        validate(rendered)
    except ValueError as error:
        raise SystemExit(
            f"the rendered file is refused, nothing written: {error}"
        ) from None
    if password in rendered:
        raise SystemExit(
            "the rendered file is refused, nothing written: "
            "the password would appear in the world-readable output"
        )
    return rendered


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(
            "usage: render-dho-api-ch-users.py <authorization.go> <out-file>",
            file=sys.stderr,
        )
        return 2
    password = os.environ.get("API_CH_PASSWORD", "")
    if not password:
        print("API_CH_PASSWORD is not set in the environment", file=sys.stderr)
        return 2
    manifest = Path(argv[1])
    if not manifest.is_file():
        print(f"{manifest} is not a file", file=sys.stderr)
        return 2
    xml = render(grants_from_manifest(manifest.read_text()), password)
    out = Path(argv[2])
    fd, tmp = tempfile.mkstemp(dir=out.parent, prefix=f".{out.name}.")
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(xml)
        os.chmod(tmp, 0o644)
        os.replace(tmp, out)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    print(f"wrote {out} mode=644 grants={xml.count('<query>')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
