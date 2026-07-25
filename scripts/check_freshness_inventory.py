#!/usr/bin/env python3
"""Validate the archived legacy freshness inventory.

Every tracked page has an explicit disposition (``refresh`` or ``retire``),
an owner, and a ``last-reviewed`` date. Retired pages must name a
``replacement`` page that exists in the archived documentation tree. Every
tracked page must still exist on disk. Entries older than the review cadence
fail the guard rather than silently aging out of review.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INVENTORY = ROOT / ".github" / "docs-legacy" / "freshness-inventory.yml"
DEFAULT_DOCS_ROOT = ROOT / ".github" / "docs-legacy"
REVIEW_CADENCE_DAYS = 180


@dataclass(frozen=True, slots=True)
class FreshnessEntry:
    page: str
    disposition: str
    owner: str
    last_reviewed: date
    replacement: str | None


class FreshnessInventoryError(Exception):
    pass


def load_inventory(path: Path) -> list[FreshnessEntry]:
    if not path.is_file():
        raise FreshnessInventoryError(f"missing freshness inventory: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or []
    if not isinstance(raw, list):
        raise FreshnessInventoryError(f"{path} must be a YAML list of entries")
    entries: list[FreshnessEntry] = []
    for item in raw:
        if not isinstance(item, dict):
            raise FreshnessInventoryError(
                f"{path} entries must be mappings, got {item!r}"
            )
        page = item.get("page")
        disposition = item.get("disposition")
        owner = item.get("owner")
        last_reviewed_raw = item.get("last-reviewed")
        replacement = item.get("replacement")
        if (
            not isinstance(page, str)
            or not isinstance(owner, str)
            or not isinstance(last_reviewed_raw, str)
        ):
            raise FreshnessInventoryError(
                f"{path} entry missing required string fields: {item!r}"
            )
        if disposition not in ("refresh", "retire"):
            raise FreshnessInventoryError(
                f"{path} entry for {page} has invalid disposition {disposition!r}"
            )
        if replacement is not None and not isinstance(replacement, str):
            raise FreshnessInventoryError(
                f"{path} entry for {page} has a non-string replacement {replacement!r}"
            )
        entries.append(
            FreshnessEntry(
                page=page,
                disposition=disposition,
                owner=owner,
                last_reviewed=date.fromisoformat(last_reviewed_raw),
                replacement=replacement,
            )
        )
    return entries


def check_freshness_inventory(
    entries: list[FreshnessEntry], docs_root: Path, today: date
) -> list[str]:
    errors: list[str] = []
    for entry in entries:
        source_path = docs_root / entry.page
        if not source_path.is_file():
            errors.append(
                f"{entry.page}: freshness inventory references a missing page"
            )
            continue
        age_days = (today - entry.last_reviewed).days
        if age_days > REVIEW_CADENCE_DAYS:
            errors.append(
                f"{entry.page}: last-reviewed {entry.last_reviewed.isoformat()} is "
                f"{age_days} days old, exceeding the {REVIEW_CADENCE_DAYS}-day review cadence"
            )
        if entry.disposition == "retire":
            if not entry.replacement:
                errors.append(
                    f"{entry.page}: retired pages must name a replacement page"
                )
                continue
            replacement_path = docs_root / entry.replacement
            if not replacement_path.is_file():
                errors.append(
                    f"{entry.page}: replacement page {entry.replacement} does not exist"
                )
    return errors


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the archived legacy freshness inventory"
    )
    parser.add_argument("--inventory", type=Path, default=DEFAULT_INVENTORY)
    parser.add_argument("--docs-root", type=Path, default=DEFAULT_DOCS_ROOT)
    args = parser.parse_args(argv)

    try:
        entries = load_inventory(args.inventory)
    except FreshnessInventoryError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    errors = check_freshness_inventory(entries, args.docs_root, date.today())
    if errors:
        for issue in errors:
            print(f"ERROR: {issue}")
        return 1
    print("Archived legacy freshness inventory check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
