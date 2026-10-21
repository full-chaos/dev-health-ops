# Documentation tooling data

This directory holds machine-consumed inputs for the documentation build and its
guards. It is **not** published: it sits outside `docs/` so MkDocs never copies it
into the site, and nothing here is reachable from `mkdocs.yml` navigation.

Reader-facing documentation guidance lives under
[`docs/contribute/documentation/`](../docs/contribute/documentation/index.md).

## Contents and consumers

| Path | Consumed by |
| --- | --- |
| `ia/*.tsv` | `scripts/validate_docs_ia_v2.py`, `cigate docs validate-publication`, `cigate docs validate-inventory-review` |
| `redirects.tsv` | `cigate docs validate-publication`, `scripts/build_docs_cloudflare.py`, `.github/workflows/docs-cloudflare.yml` |
| `search-acceptance.json` | `cigate docs check-docs-search` (`internal/cigates/docstools`) via `.github/workflows/docs-guards.yml`, `scripts/build_docs_cloudflare.py` |
| `inventory/` | `cigate docs validate-inventory-review` via `.github/workflows/docs-inventory-review.yml` (a frozen Phase 1 snapshot: its generator is deleted) |
| `freshness-inventory.yml` | `scripts/check_freshness_inventory.py` (`make docs:check-freshness`) |
| `external-link-allowlist.yml` | `scripts/check_external_links.py` (`make docs:check-external-links`, `.github/workflows/docs-health-report.yml`) |
