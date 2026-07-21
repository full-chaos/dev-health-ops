#!/usr/bin/env python3
"""Validate the committed documentation inventory and reviewed disposition.

This checker keeps the Phase 1 evidence reproducible without granting CI write
access. It regenerates review formats from the current factual JSON inventory,
checks that committed snapshots are current, and verifies that the frozen
Phase 1 review has complete dispositions consistent with the locked IA.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

FACTUAL_FIELDS = [
    "source_repo",
    "source_path",
    "artifact_type",
    "publication_classification",
    "current_url",
    "current_nav_location",
    "content_type",
    "primary_audience",
    "secondary_audiences",
    "product_area",
    "owner",
    "last_meaningful_review",
    "generated",
    "public_today",
    "duplicate_group",
    "known_accuracy_risk",
    "known_usability_risk",
    "build_dependencies",
    "links_in",
    "links_out",
    "notes",
]
LIST_FIELDS = {
    "current_nav_location",
    "secondary_audiences",
    "build_dependencies",
    "links_in",
    "links_out",
}
REQUIRED_DISPOSITION_FIELDS = {
    "proposed_disposition",
    "target_section",
    "target_page_type",
    "canonical_owner",
    "source_of_truth",
    "migration_phase",
    "reason",
    "reviewer",
}
PUBLIC_CLASSIFICATIONS = {"public-nav", "public-reference"}
EXPLICIT_NONPUBLIC = {
    "archive",
    "internal-only",
    "internal-source-evidence",
    "remove",
    "remove-or-replace",
    "retain-internal",
    "archive-or-recapture",
}
REVIEWED_DISPOSITION_ROWS = 449
REVIEWED_OPS_ROWS = 313
REVIEWED_WEB_ROWS = 136


def _load_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _write_factual_tsv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=FACTUAL_FIELDS,
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            normalized = dict(row)
            for field in LIST_FIELDS:
                normalized[field] = " | ".join(
                    str(value) for value in (normalized.get(field) or [])
                )
            writer.writerow({field: normalized.get(field) for field in FACTUAL_FIELDS})


def _summary(rows: list[dict[str, Any]]) -> str:
    classifications = Counter(row["publication_classification"] for row in rows)
    artifact_types = Counter(row["artifact_type"] for row in rows)
    unclassified = [
        row for row in rows if row["publication_classification"] == "unclassified"
    ]
    gaps = [
        row for row in rows if row["publication_classification"] == "gap-unverified"
    ]
    public_pages = [
        row
        for row in rows
        if row.get("public_today") and row["artifact_type"] == "markdown-page"
    ]

    lines = [
        "# Documentation inventory review summary",
        "",
        "Generated deterministically by `scripts/docs_inventory_review.py`.",
        "",
        f"- Inventory rows: **{len(rows)}**",
        f"- Current-site Markdown pages: **{artifact_types.get('markdown-page', 0)}**",
        f"- Prototype pages: **{artifact_types.get('prototype-page', 0)}**",
        f"- Pages treated as public today: **{len(public_pages)}**",
        f"- Unclassified pages: **{len(unclassified)}**",
        f"- Explicit external/runtime gaps: **{len(gaps)}**",
        "",
        "## Publication classifications",
        "",
    ]
    lines.extend(
        f"- `{name}`: {count}" for name, count in sorted(classifications.items())
    )
    lines.extend(["", "## Artifact types", ""])
    lines.extend(
        f"- `{name}`: {count}" for name, count in sorted(artifact_types.items())
    )
    lines.extend(["", "## Explicit gaps", ""])
    if gaps:
        lines.extend(
            f"- `{row['source_path']}` — {row.get('notes') or ''}" for row in gaps
        )
    else:
        lines.append("- None")
    return "\n".join(lines) + "\n"


def _approved_urls(ia_dir: Path) -> set[str]:
    urls: set[str] = set()
    for path in sorted(ia_dir.glob("*.tsv")):
        for row in _load_tsv(path):
            url = row.get("url", "").strip()
            if url:
                urls.add(url)
    return urls


def _assert_equal_file(actual: Path, expected: Path) -> None:
    if actual.read_bytes() != expected.read_bytes():
        raise ValueError(
            f"Committed snapshot is stale: {expected}. "
            "Regenerate the inventory and review the resulting diff."
        )


def validate(
    generated_json: Path,
    inventory_dir: Path,
    ia_dir: Path,
) -> None:
    factual = json.loads(generated_json.read_text(encoding="utf-8"))
    factual_rows = factual["rows"]
    committed_json = inventory_dir / "documentation-inventory.json"
    committed = json.loads(committed_json.read_text(encoding="utf-8"))
    expected_factual_rows = committed["row_count"]
    if len(committed["rows"]) != expected_factual_rows:
        raise ValueError("Committed factual inventory row_count is inconsistent")
    if (
        factual["row_count"] != expected_factual_rows
        or len(factual_rows) != expected_factual_rows
    ):
        raise ValueError(
            f"Expected {expected_factual_rows} dev-health-ops rows, "
            f"found {factual['row_count']}"
        )

    generated_tsv = generated_json.with_suffix(".tsv")
    generated_summary = generated_json.with_name("documentation-inventory-summary.md")
    _write_factual_tsv(generated_tsv, factual_rows)
    generated_summary.write_text(_summary(factual_rows), encoding="utf-8")

    _assert_equal_file(
        generated_json,
        committed_json,
    )
    _assert_equal_file(
        generated_tsv,
        inventory_dir / "documentation-inventory.tsv",
    )
    _assert_equal_file(
        generated_summary,
        inventory_dir / "generated-summary.md",
    )

    disposition = _load_tsv(inventory_dir / "disposition-matrix.tsv")
    if len(disposition) != REVIEWED_DISPOSITION_ROWS:
        raise ValueError(
            f"Expected {REVIEWED_DISPOSITION_ROWS} disposition rows, "
            f"found {len(disposition)}"
        )

    ops_rows = [
        row for row in disposition if row["source_repo"] == "full-chaos/dev-health-ops"
    ]
    web_rows = [
        row for row in disposition if row["source_repo"] == "full-chaos/dev-health-web"
    ]
    if len(ops_rows) != REVIEWED_OPS_ROWS:
        raise ValueError(
            f"Expected {REVIEWED_OPS_ROWS} ops disposition rows, found {len(ops_rows)}"
        )
    if len(web_rows) != REVIEWED_WEB_ROWS:
        raise ValueError(
            f"Expected {REVIEWED_WEB_ROWS} web disposition rows, found {len(web_rows)}"
        )

    reviewed_ops_rows = [
        row
        for path in sorted(inventory_dir.glob("ops-*.tsv"))
        for row in _load_tsv(path)
    ]
    ops_by_path = {row["source_path"]: row for row in ops_rows}
    reviewed_ops_by_path = {row["source_path"]: row for row in reviewed_ops_rows}
    if len(reviewed_ops_by_path) != REVIEWED_OPS_ROWS or (
        reviewed_ops_by_path != ops_by_path
    ):
        raise ValueError("Ops disposition matrix differs from its reviewed split files")

    web_snapshot = _load_tsv(inventory_dir / "dev-health-web-snapshot.tsv")
    if web_snapshot != web_rows:
        raise ValueError("dev-health-web-snapshot.tsv differs from the reviewed matrix")

    approved_urls = _approved_urls(ia_dir)
    errors: list[str] = []
    for row in disposition:
        key = f"{row['source_repo']}:{row['source_path']}"
        for field in REQUIRED_DISPOSITION_FIELDS:
            if not row.get(field, "").strip():
                errors.append(f"{key} missing {field}")

        target_url = row.get("target_url", "").strip()
        if target_url and target_url not in approved_urls:
            errors.append(f"{key} targets non-IA URL {target_url}")
        if (
            row.get("publication_classification") in PUBLIC_CLASSIFICATIONS
            and not target_url
            and row.get("proposed_disposition") not in EXPLICIT_NONPUBLIC
        ):
            errors.append(f"{key} lacks target or explicit non-public disposition")

    if any(row["publication_classification"] == "unclassified" for row in factual_rows):
        errors.append("The factual inventory contains unclassified current sources")

    gaps = [
        row
        for row in factual_rows
        if row["publication_classification"] == "gap-unverified"
    ]
    if [row["source_path"] for row in gaps] != [
        "external://dev-health-docs.fullchaos.workers.dev"
    ]:
        errors.append("The only deferred runtime gap must be the Workers preview crawl")

    if errors:
        raise ValueError("Inventory review failed:\n" + "\n".join(errors[:50]))

    print(
        f"Validated {expected_factual_rows} factual ops rows and "
        f"{REVIEWED_DISPOSITION_ROWS} reviewed dispositions "
        "against the locked IA."
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--generated-json",
        type=Path,
        default=Path(".build/documentation-inventory.json"),
    )
    parser.add_argument(
        "--inventory-dir",
        type=Path,
        default=Path(".github/documentation-program/inventory"),
    )
    parser.add_argument(
        "--ia-dir",
        type=Path,
        default=Path(".github/documentation-program/ia"),
    )
    args = parser.parse_args()
    validate(args.generated_json, args.inventory_dir, args.ia_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
