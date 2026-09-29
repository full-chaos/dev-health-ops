#!/usr/bin/env python3
"""The exact shape of bigboy's dho_api_ch users.d file (CHAOS-7162), shared by the renderer and the checker.

The file is mounted into ClickHouse world-readable and ClickHouse EXITS on a users.d file it cannot parse, so
it is validated by parsing it, not by matching text: well-formed XML, root `clickhouse` holding one `users`
holding exactly one user `dho_api_ch`, whose only children are the allowed elements below, with exactly one
64-hex `password_sha256_hex` and no other authentication element (no plaintext `<password>`), and grants that
are `<query>GRANT ...` statements only. Nothing here prints a value: reasons name elements, never contents.

CLI: dho_api_ch_users_shape.py <file>  -> exit 0 when valid; exit 1 printing one reason on stderr.
"""

from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

USER = "dho_api_ch"
#: Children a dho_api_ch declaration may have. Anything else -- above all any other authentication element
#: (password, password_double_sha1_hex, ssl_certificates, ldap, kerberos, ...) -- is refused.
ALLOWED = {
    "access_management",
    "grants",
    "networks",
    "password_sha256_hex",
    "profile",
    "quota",
}
_HEX64 = re.compile(r"[0-9a-f]{64}")
#: The whole tree, as an allow-list (CHAOS-7162): no element may carry an attribute, containers hold only
#: element children (no text at all), leaves hold only text that matches their pattern. Nothing outside this
#: table is a valid element, so a value cannot ride in an attribute, a stray node or a comment-like place.
_NAME = re.compile(r"[A-Za-z0-9_.\-]{1,64}")
_NET = re.compile(r"[A-Za-z0-9_.:/*+?|()\[\]^$\\\-]{1,255}")
_CONTAINERS = {
    "clickhouse": {"users"},
    "users": {USER},
    USER: ALLOWED,
    "networks": {"ip", "host", "host_regexp", "name"},
    "grants": {"query"},
}
_LEAVES: dict[str, re.Pattern[str]] = {
    "password_sha256_hex": _HEX64,
    "access_management": re.compile(r"[01]"),
    "profile": _NAME,
    "quota": _NAME,
    "ip": _NET,
    "host": _NET,
    "host_regexp": _NET,
    "name": _NET,
    "query": re.compile(r"GRANT [^<>]{1,2000}", re.IGNORECASE),
}


def _check_node(node: ET.Element) -> None:
    if node.attrib:
        raise ValueError(f"<{node.tag}> has an attribute: attributes are not allowed")
    if node.tag in _CONTAINERS:
        if (node.text or "").strip():
            raise ValueError(f"<{node.tag}> holds text: it may hold only elements")
        for child in node:
            if child.tag not in _CONTAINERS[node.tag]:
                raise ValueError(f"<{child.tag}> is not allowed inside <{node.tag}>")
            if (child.tail or "").strip():
                raise ValueError(f"text after <{child.tag}> is not allowed")
            _check_node(child)
        return
    pattern = _LEAVES.get(node.tag)
    if pattern is None:
        raise ValueError(f"<{node.tag}> is not an allowed element")
    if len(node):
        raise ValueError(f"<{node.tag}> may not hold elements")
    if not pattern.fullmatch((node.text or "").strip()):
        raise ValueError(f"<{node.tag}> holds text that is not an allowed value")


def validate(text: str) -> None:
    """Raise ValueError naming the first problem; return None when the file has the exact shape."""
    try:
        root = ET.fromstring(text)
    except ET.ParseError as error:
        raise ValueError(
            f"not well-formed XML ({error.__class__.__name__}): ClickHouse would exit on it"
        ) from None
    if root.tag != "clickhouse" or [child.tag for child in root] != ["users"]:
        raise ValueError("the root must be <clickhouse> holding exactly one <users>")
    users = list(root[0])
    if [user.tag for user in users] != [USER]:
        raise ValueError(f"<users> must hold exactly one user, <{USER}>")
    declaration = users[0]
    tags = [child.tag for child in declaration]
    unexpected = sorted({tag for tag in tags if tag not in ALLOWED})
    if unexpected:
        raise ValueError(
            f"<{USER}> has element(s) that are not allowed: {', '.join(unexpected)} (only a hash may authenticate it)"
        )
    if tags.count("password_sha256_hex") != 1:
        raise ValueError(f"<{USER}> must have exactly one <password_sha256_hex>")
    for tag in ALLOWED:
        if tags.count(tag) > 1:
            raise ValueError(f"<{USER}> has more than one <{tag}>")
    digest = declaration.find("password_sha256_hex")
    if digest is None or not _HEX64.fullmatch((digest.text or "").strip()):
        raise ValueError("<password_sha256_hex> is not a 64-hex SHA-256")
    grants = declaration.find("grants")
    if grants is None or not len(grants):
        raise ValueError(f"<{USER}> must declare its <grants>")
    for query in grants:
        if (
            query.tag != "query"
            or len(query)
            or not (query.text or "").strip().upper().startswith("GRANT ")
        ):
            raise ValueError(
                "<grants> may hold only <query>GRANT ...</query> statements"
            )
    _check_node(root)
    if "://" in text:
        raise ValueError('the file contains a URI ("://")')


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: dho_api_ch_users_shape.py <file>", file=sys.stderr)
        return 2
    try:
        validate(Path(argv[1]).read_text())
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
