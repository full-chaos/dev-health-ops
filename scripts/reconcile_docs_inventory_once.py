#!/usr/bin/env python3
"""Reconcile the reviewed Phase 1 disposition with the current source tree once."""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

from validate_docs_inventory_review import _summary, _write_factual_tsv, validate

FIELDS = [
    "source_repo",
    "source_path",
    "artifact_type",
    "publication_classification",
    "proposed_disposition",
    "target_section",
    "target_url",
    "target_page_type",
    "canonical_owner",
    "source_of_truth",
    "duplicate_group",
    "redirect_required",
    "asset_status",
    "migration_phase",
    "reason",
    "reviewer",
]

WEB_FILES = [
    "web-canonical-and-entry-points.tsv",
    "web-internal-docs.tsv",
    "web-visual-assets.tsv",
]


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _write_tsv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=FIELDS,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(
            sorted(rows, key=lambda row: (row["source_repo"], row["source_path"]))
        )


def _base_decision(row: dict[str, Any]) -> dict[str, str]:
    return {
        "source_repo": str(row["source_repo"]),
        "source_path": str(row["source_path"]),
        "artifact_type": str(row["artifact_type"]),
        "publication_classification": str(row["publication_classification"]),
        "target_url": "",
        "duplicate_group": "",
        "redirect_required": "no",
        "asset_status": "",
        "reviewer": "documentation-remediation-audit",
    }


def _new_decision(row: dict[str, Any]) -> dict[str, str]:
    path = str(row["source_path"])
    base = _base_decision(row)

    if path == ".github/workflows/docs-release.yml":
        return base | {
            "proposed_disposition": "replace-and-simplify",
            "target_section": "Documentation system",
            "target_page_type": "publication-workflow",
            "canonical_owner": "documentation",
            "source_of_truth": "Phase 11 ADR",
            "migration_phase": "Phase 11",
            "reason": "Replace only after Pages-versus-Workers, trust-boundary, redirect, and rollback decisions are approved.",
        }

    if path.startswith("docs-qa/"):
        return base | {
            "proposed_disposition": "remove-or-replace",
            "target_section": "Documentation system",
            "target_page_type": "browser-qa",
            "canonical_owner": "documentation",
            "source_of_truth": "lean quality gate",
            "migration_phase": "Phase 10",
            "reason": "Review-only QA machinery; preserve only a small representative task and accessibility smoke suite when justified.",
        }

    if path.startswith("docs/overrides/"):
        return base | {
            "proposed_disposition": "remove-or-replace",
            "target_section": "Documentation system",
            "target_page_type": "theme-override",
            "canonical_owner": "design",
            "source_of_truth": "approved v2 shell",
            "migration_phase": "Phase 5",
            "reason": "Replace the current custom shell and page-type overrides with the restrained Material-first v2 implementation.",
        }

    script_rules = {
        "scripts/check_built_site_links.py": (
            "retain-and-simplify",
            "documentation",
            "lean quality gate",
            "Phase 10",
            "Keep internal link, anchor, and asset validation with a small maintained checker.",
        ),
        "scripts/check_investment_docs_drift.py": (
            "retain-and-simplify",
            "product-analytics",
            "canonical Investment code and taxonomy",
            "Phase 5",
            "Retain focused source-drift checks for exact Investment facts used by the vertical slice.",
        ),
        "scripts/docs_publication.py": (
            "replace-and-simplify",
            "documentation",
            "approved IA manifest",
            "Phase 9",
            "Validate explicit public/internal state and canonical URLs; remove broad catch-all publication assumptions.",
        ),
        "scripts/gen_taxonomy_docs.py": (
            "retain",
            "product-analytics",
            "canonical taxonomy source",
            "Phase 5",
            "Retain the exact generated Investment taxonomy reference pipeline.",
        ),
        "scripts/user_guide_evidence_contract.py": (
            "remove-or-replace",
            "documentation",
            "lean quality gate",
            "Phase 10",
            "Remove generic evidence and prose contracts; retain only objective source-fact validation where justified.",
        ),
        "scripts/user_guide_evidence_validation.py": (
            "remove-or-replace",
            "documentation",
            "lean quality gate",
            "Phase 10",
            "Remove generic evidence and prose contracts; retain only objective source-fact validation where justified.",
        ),
        "scripts/validate_user_guide_evidence.py": (
            "remove-or-replace",
            "documentation",
            "lean quality gate",
            "Phase 10",
            "Remove generic evidence and prose contracts; retain only objective source-fact validation where justified.",
        ),
    }
    if path in script_rules:
        disposition, owner, source, phase, reason = script_rules[path]
        return base | {
            "proposed_disposition": disposition,
            "target_section": "Documentation system",
            "target_page_type": "quality-script",
            "canonical_owner": owner,
            "source_of_truth": source,
            "migration_phase": phase,
            "reason": reason,
        }

    raise ValueError(f"No reviewed disposition rule for new source: {path}")


def _readme(dispositions: list[dict[str, str]]) -> str:
    counts = Counter(row["proposed_disposition"] for row in dispositions)
    named = [
        ("Move and rewrite", "move-and-rewrite"),
        ("Archive or recapture visual evidence", "archive-or-recapture"),
        ("Internal only", "internal-only"),
        ("Remove or replace", "remove-or-replace"),
        ("Merge and rewrite", "merge-and-rewrite"),
        ("Retain internal", "retain-internal"),
        ("Archive", "archive"),
        ("Internal source evidence", "internal-source-evidence"),
    ]
    represented = sum(counts[key] for _, key in named)
    table = "\n".join(f"| {label} | {counts[key]} |" for label, key in named)
    other = len(dispositions) - represented
    return f"""# Documentation inventory and disposition

This directory is the reviewed Phase 1 inventory for the **User Guides & Development Documentation** remediation.

It covers the current `dev-health-ops` documentation system, the isolated v2 prototype, internal remediation artifacts, relevant `dev-health-web` documentation/help/publication sources, and the live Workers preview as an explicit runtime baseline.

## Source snapshots

- `full-chaos/dev-health-ops` main: `dde247972ea9d798b4a56809b7efb172861203f4`
- `full-chaos/dev-health-web` main: `a2ffbcb9afea26bbf7e4f2b2b93220deb259bb2a`

The web snapshot was exported through now-closed review PR `full-chaos/dev-health-web#794`. No temporary export workflow is retained.

## Inventory result

- Total reviewed rows: **449**
- `dev-health-ops` rows: **313**
- `dev-health-web` rows: **136**
- Current-site Markdown pages: **216**
- Prototype pages: **12**
- Web documentation pages: **38**
- Visual/static assets reviewed: **90**
- Current pages classified as public today: **165**
- Rows assigned to duplicate groups: **72**
- Unclassified current sources: **0**

## Validation

- Current public pages with a canonical locked-IA target: **136**
- Current public pages explicitly archived, internalized, or removed: **29**
- Current public pages without a target or explicit non-public disposition: **0**
- Target URLs outside the locked 198-node IA: **0**
- Rows missing a disposition, reason, migration phase, or reviewer: **0**

## Disposition totals

| Disposition | Rows |
| --- | ---: |
{table}
| Other explicit dispositions | {other} |

## Durable source and review output

`docs_inventory_review.py`, `validate_docs_inventory_review.py`, and the read-only `Documentation inventory review` workflow generate the factual JSON/TSV inventory and validate the complete row-level disposition on demand.

The reviewed disposition is split by source and target domain so GitHub can render and review ordinary TSV diffs:

- `ops-home.tsv`
- `ops-get-started.tsv`
- `ops-use-dev-health.tsv`
- `ops-administer-dev-health.tsv`
- `ops-install-and-operate.tsv`
- `ops-integrate-and-extend.tsv`
- `ops-reference.tsv`
- `ops-contribute.tsv`
- `ops-documentation-system.tsv`
- `ops-documentation-system-supporting.tsv`
- `ops-internal-project-records.tsv`
- `ops-internal-project-records-supporting.tsv`
- `web-canonical-and-entry-points.tsv`
- `web-internal-docs.tsv`
- `web-visual-assets.tsv`

The directory also contains the generated factual inventory, a consolidated disposition matrix, the cross-repository web snapshot, and a page-only current-to-target report.

Every disposition row includes its source, current classification, proposed outcome, locked-IA target where public, canonical owner, source of truth, duplicate group, redirect requirement, migration phase, reason, and reviewer.

## Interpretation

The matrix does not make the current site canonical. It says what is retained, rewritten, merged, generated, internalized, archived, removed, or used as source evidence. Every public target URL is a node in the locked IA; the matrix creates no new navigation branches.

The live Workers preview remains a non-canonical baseline. Its host-level crawl, headers, indexing, redirects, and final retirement or redirect behavior are intentionally handled in Phase 11.

With the validation counts above at zero, this source and disposition inventory satisfies the Phase 1 inventory gate and is the source map for Phase 5 and the gated Phase 6 migration.
"""


def reconcile(generated_json: Path, inventory_dir: Path, ia_dir: Path) -> None:
    factual = json.loads(generated_json.read_text(encoding="utf-8"))
    factual_rows: list[dict[str, Any]] = factual["rows"]
    factual_by = {
        (str(row["source_repo"]), str(row["source_path"])): row
        for row in factual_rows
    }

    ops_files = sorted(inventory_dir.glob("ops-*.tsv"))
    old_decisions: dict[tuple[str, str], dict[str, str]] = {}
    membership: dict[tuple[str, str], str] = {}
    for path in ops_files:
        for row in _read_tsv(path):
            key = (row["source_repo"], row["source_path"])
            if key in old_decisions:
                raise ValueError(f"Duplicate reviewed source across ops splits: {key}")
            old_decisions[key] = row
            membership[key] = path.name

    web_rows: list[dict[str, str]] = []
    for name in WEB_FILES:
        web_rows.extend(_read_tsv(inventory_dir / name))
    if len(web_rows) != 136:
        raise ValueError(f"Expected 136 web review rows, found {len(web_rows)}")

    updated_ops: list[dict[str, str]] = []
    groups = {path.name: [] for path in ops_files}
    for key, factual_row in sorted(factual_by.items(), key=lambda item: item[0][1]):
        if key in old_decisions:
            decision = dict(old_decisions[key])
            decision["artifact_type"] = str(factual_row["artifact_type"])
            decision["publication_classification"] = str(
                factual_row["publication_classification"]
            )
        else:
            decision = _new_decision(factual_row)
        updated_ops.append(decision)
        groups[membership.get(key, "ops-documentation-system-supporting.tsv")].append(
            decision
        )

    if len(updated_ops) != 313:
        raise ValueError(f"Expected 313 current ops decisions, found {len(updated_ops)}")

    for name, rows in groups.items():
        _write_tsv(inventory_dir / name, rows)

    dispositions = sorted(
        updated_ops + web_rows,
        key=lambda row: (row["source_repo"], row["source_path"]),
    )
    _write_tsv(inventory_dir / "disposition-matrix.tsv", dispositions)
    _write_tsv(
        inventory_dir / "page-current-to-target.tsv",
        [
            row
            for row in dispositions
            if row["artifact_type"] in {"markdown-page", "prototype-page"}
        ],
    )

    shutil.copyfile(generated_json, inventory_dir / "documentation-inventory.json")
    _write_factual_tsv(inventory_dir / "documentation-inventory.tsv", factual_rows)
    (inventory_dir / "generated-summary.md").write_text(
        _summary(factual_rows), encoding="utf-8"
    )
    (inventory_dir / "README.md").write_text(
        _readme(dispositions), encoding="utf-8"
    )

    validate(generated_json, inventory_dir, ia_dir)


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
        "--ia-dir", type=Path, default=Path(".github/documentation-program/ia")
    )
    args = parser.parse_args()
    reconcile(args.generated_json, args.inventory_dir, args.ia_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
