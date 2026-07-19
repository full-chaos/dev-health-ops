#!/usr/bin/env python3
"""Expand the reviewed Phase 1 disposition and validate it against the locked IA.

The payload chunks are a one-time transport. The inventory workflow expands
these into ordinary reviewable TSV files and removes the transport files.
"""

from __future__ import annotations

import argparse
import base64
import csv
import io
import json
import zlib
from pathlib import Path

EXPECTED_ROWS = 448
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


def decode_files(payload_dir: Path) -> dict[str, str]:
    chunks = sorted(payload_dir.glob(".disposition-payload-*.b64"))
    if not chunks:
        raise FileNotFoundError("No reviewed disposition payload chunks found")
    encoded = "".join(path.read_text(encoding="utf-8").strip() for path in chunks)
    raw = zlib.decompress(base64.b64decode(encoded.encode("ascii")))
    files = json.loads(raw.decode("utf-8"))
    if not isinstance(files, dict):
        raise TypeError("Disposition payload must decode to a file mapping")
    return {str(name): str(content) for name, content in files.items()}


def load_approved_urls(ia_dir: Path) -> set[str]:
    approved: set[str] = set()
    for path in sorted(ia_dir.glob("*.tsv")):
        with path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle, delimiter="\t"):
                url = str(row.get("url") or "").strip()
                if url:
                    approved.add(url)
    return approved


def write_table(path: Path, rows: list[dict[str, str]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--payload-dir",
        type=Path,
        default=Path("scripts"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(".github/documentation-program/inventory"),
    )
    parser.add_argument(
        "--ia-dir",
        type=Path,
        default=Path(".github/documentation-program/ia"),
    )
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    encoded_files = decode_files(args.payload_dir)
    all_rows: list[dict[str, str]] = []
    fields: list[str] | None = None

    for name, content in sorted(encoded_files.items()):
        destination = args.output_dir / name
        destination.write_text(content, encoding="utf-8")
        reader = csv.DictReader(io.StringIO(content), delimiter="\t")
        if fields is None:
            fields = list(reader.fieldnames or [])
        elif list(reader.fieldnames or []) != fields:
            raise ValueError(f"Field mismatch in {name}")
        all_rows.extend(dict(row) for row in reader)

    if fields is None:
        raise ValueError("No disposition rows were decoded")

    all_rows.sort(key=lambda row: (row["source_repo"], row["source_path"]))
    if len(all_rows) != EXPECTED_ROWS:
        raise ValueError(
            f"Expected {EXPECTED_ROWS} disposition rows, found {len(all_rows)}"
        )

    approved_urls = load_approved_urls(args.ia_dir)
    errors: list[str] = []
    for row in all_rows:
        key = f"{row['source_repo']}:{row['source_path']}"
        for required in (
            "proposed_disposition",
            "target_section",
            "target_page_type",
            "canonical_owner",
            "source_of_truth",
            "migration_phase",
            "reason",
            "reviewer",
        ):
            if not str(row.get(required) or "").strip():
                errors.append(f"{key} missing {required}")

        target_url = str(row.get("target_url") or "").strip()
        if target_url and target_url not in approved_urls:
            errors.append(f"{key} targets non-IA URL {target_url}")

        classification = str(row.get("publication_classification") or "")
        disposition = str(row.get("proposed_disposition") or "")
        if (
            classification in PUBLIC_CLASSIFICATIONS
            and not target_url
            and disposition not in EXPLICIT_NONPUBLIC
        ):
            errors.append(
                f"{key} has neither a locked-IA target nor an explicit "
                "non-public disposition"
            )

    if errors:
        preview = "\n".join(f"- {error}" for error in errors[:50])
        raise ValueError(f"Disposition validation failed:\n{preview}")

    write_table(args.output_dir / "disposition-matrix.tsv", all_rows, fields)

    web_rows = [
        row for row in all_rows
        if row["source_repo"] == "full-chaos/dev-health-web"
    ]
    write_table(args.output_dir / "dev-health-web-snapshot.tsv", web_rows, fields)

    page_rows = [
        row for row in all_rows
        if row["artifact_type"] in {"markdown-page", "prototype-page"}
    ]
    write_table(args.output_dir / "page-current-to-target.tsv", page_rows, fields)

    print(
        f"Wrote {len(all_rows)} reviewed rows; "
        f"{len(web_rows)} web rows; {len(page_rows)} page rows."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
