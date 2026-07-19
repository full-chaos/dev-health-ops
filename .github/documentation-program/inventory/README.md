# Documentation inventory and disposition

This directory is the reviewed Phase 1 inventory for the **User Guides & Development Documentation** remediation.

It covers the current `dev-health-ops` documentation system, the isolated v2 prototype, internal remediation artifacts, relevant `dev-health-web` documentation/help/publication sources, and the live Workers preview as an explicit runtime baseline.

## Source snapshots

- `full-chaos/dev-health-ops`: `30176cd030349db67c9c55df7a8a6c67f4102299`
- `full-chaos/dev-health-web`: `429023aee5525129353592354febbe57c88c31d2`

## Inventory result

- Total reviewed rows: **448**
- `dev-health-ops` rows: **312**
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
| Move and rewrite | 108 |
| Archive or recapture visual evidence | 90 |
| Internal only | 80 |
| Remove or replace | 27 |
| Merge and rewrite | 25 |
| Retain internal | 21 |
| Archive | 13 |
| Internal source evidence | 10 |
| Other explicit dispositions | 74 |

## Durable source and review output

`docs_inventory_review.py` and the `Documentation inventory review` workflow generate the factual JSON/TSV inventory and validate the complete row-level disposition on demand.

The reviewed disposition is split here by source and target domain so GitHub can render and review ordinary TSV diffs:

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

With the validation counts above at zero, this source and disposition inventory satisfies the Phase 1 inventory gate and can be used as the input to Phase 5.
