#!/usr/bin/env python3
"""Validate structural invariants in the documentation IA v2 manifest."""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Any

URL_RE = re.compile(r"^/(?:[a-z0-9]+(?:-[a-z0-9]+)*/)*$")
EXPECTED_TOP_LEVEL = {
    "/get-started/",
    "/use/",
    "/admin/",
    "/operate/",
    "/integrate/",
    "/reference/",
    "/contribute/",
}
EXPECTED_FILES = {
    "home.tsv",
    "get-started.tsv",
    "use.tsv",
    "admin.tsv",
    "operate.tsv",
    "integrate.tsv",
    "reference.tsv",
    "contribute.tsv",
}
ALLOWED_PUBLIC_STATES = {"public", "internal", "reserved"}
ALLOWED_LIFECYCLES = {"planned", "active", "deprecated", "archived"}
ALLOWED_KINDS = {
    "landing",
    "section",
    "tutorial",
    "task-guide",
    "workflow-guide",
    "concept",
    "reference",
    "api-reference",
    "cli-reference",
    "configuration-reference",
    "generated-reference",
    "architecture",
    "troubleshooting-index",
    "troubleshooting",
    "runbook",
}
TRUTHY = {"1", "true", "yes"}


def _as_bool(value: Any) -> bool:
    return str(value).strip().lower() in TRUTHY


def validate_nodes(nodes: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    ids: dict[str, dict[str, Any]] = {}
    urls: dict[str, str] = {}

    for index, node in enumerate(nodes):
        node_id = str(node.get("id") or "").strip()
        url = str(node.get("url") or "").strip()
        label = str(node.get("label") or "").strip()
        parent_id = str(node.get("parent_id") or "").strip() or None
        kind = str(node.get("kind") or "").strip()
        public_state = str(node.get("public_state") or "").strip()
        lifecycle = str(node.get("lifecycle") or "").strip()

        if not node_id:
            errors.append(f"nodes[{index}].id is required")
            continue
        if node_id in ids:
            errors.append(f"duplicate node id: {node_id}")
        ids[node_id] = node

        if not label:
            errors.append(f"{node_id}: label is required")
        if not URL_RE.match(url):
            errors.append(f"{node_id}: invalid canonical URL {url!r}")
        elif url in urls:
            errors.append(f"duplicate canonical URL {url}: {urls[url]} and {node_id}")
        else:
            urls[url] = node_id

        if kind not in ALLOWED_KINDS:
            errors.append(f"{node_id}: unsupported kind {kind!r}")
        if public_state not in ALLOWED_PUBLIC_STATES:
            errors.append(f"{node_id}: unsupported public_state {public_state!r}")
        if lifecycle not in ALLOWED_LIFECYCLES:
            errors.append(f"{node_id}: unsupported lifecycle {lifecycle!r}")
        if public_state == "internal" and _as_bool(node.get("nav")):
            errors.append(f"{node_id}: internal node may not appear in public navigation")

        lowered = url.lower()
        if lowered != url:
            errors.append(f"{node_id}: URL must be lowercase")
        if "first-10-minutes" in lowered or "first-ten-minutes" in lowered:
            errors.append(f"{node_id}: current onboarding title may not be preserved")
        if "context-fabric" in lowered:
            errors.append(f"{node_id}: Context Fabric is reserved, not a live IA node")
        segments = [segment for segment in url.strip("/").split("/") if segment]
        if len(segments) > 4:
            errors.append(f"{node_id}: URL exceeds the default four-segment budget")

    for node_id, node in ids.items():
        parent_id = str(node.get("parent_id") or "").strip() or None
        if parent_id is not None and parent_id not in ids:
            errors.append(f"{node_id}: missing parent {parent_id}")
        if parent_id == node_id:
            errors.append(f"{node_id}: node may not parent itself")

    home = ids.get("home")
    if not home or home.get("url") != "/" or (home.get("parent_id") or ""):
        errors.append("home must exist at / with no parent")

    actual_top = {
        str(node["url"])
        for node in nodes
        if str(node.get("parent_id") or "").strip() == "home"
    }
    missing_top = EXPECTED_TOP_LEVEL - actual_top
    extra_top = actual_top - EXPECTED_TOP_LEVEL
    if missing_top:
        errors.append(f"missing top-level domains: {sorted(missing_top)}")
    if extra_top:
        errors.append(f"unexpected top-level domains: {sorted(extra_top)}")

    get_started = ids.get("get-started")
    if not get_started or not _as_bool(get_started.get("provisional")):
        errors.append("/get-started/ must remain explicitly provisional")

    for node_id in ids:
        seen: set[str] = set()
        current: str | None = node_id
        while current is not None:
            if current in seen:
                errors.append(f"parent cycle detected from {node_id}")
                break
            seen.add(current)
            parent = str(ids.get(current, {}).get("parent_id") or "").strip()
            current = parent or None

    return errors


def load_nodes(path: Path) -> list[dict[str, str]]:
    if path.is_file():
        files = [path]
    else:
        files = sorted(path.glob("*.tsv"))
        actual = {item.name for item in files}
        if actual != EXPECTED_FILES:
            missing = sorted(EXPECTED_FILES - actual)
            extra = sorted(actual - EXPECTED_FILES)
            raise ValueError(f"IA manifest files mismatch; missing={missing}, extra={extra}")

    nodes: list[dict[str, str]] = []
    for manifest_file in files:
        with manifest_file.open(encoding="utf-8", newline="") as handle:
            nodes.extend(csv.DictReader(handle, delimiter="\t"))
    return nodes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "manifest",
        nargs="?",
        type=Path,
        default=Path(".github/documentation-program/ia"),
    )
    args = parser.parse_args()

    try:
        nodes = load_nodes(args.manifest)
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 1

    errors = validate_nodes(nodes)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print(f"IA manifest valid: {args.manifest} ({len(nodes)} nodes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
