# Documentation tooling data

This directory holds machine-consumed inputs for the documentation build and its
guards. It is **not** published: it sits outside `docs/` so MkDocs never copies it
into the site, and nothing here is reachable from `mkdocs.yml` navigation.

Reader-facing documentation guidance lives under
[`docs/contribute/documentation/`](../docs/contribute/documentation/index.md).

## Contents and consumers

| Path | Consumed by |
| --- | --- |
| `ia/*.tsv` | `scripts/validate_docs_ia_v2.py`, `scripts/validate_docs_v2_publication.py`, `scripts/validate_docs_inventory_review.py`, `tests/docs/test_docs_ia_v2.py`, `tests/docs/test_built_site_links.py` |
| `redirects.tsv` | `scripts/validate_docs_v2_publication.py`, `scripts/build_docs_cloudflare.py`, `.github/workflows/docs-cloudflare.yml` |
| `search-acceptance.json` | `scripts/check_docs_candidate_search.py` via `.github/workflows/docs-guards.yml`, `scripts/build_docs_cloudflare.py` |
| `inventory/` | `scripts/docs_inventory_review.py`, `scripts/validate_docs_inventory_review.py`, `.github/workflows/docs-inventory-review.yml` |
| `freshness-inventory.yml` | `scripts/check_freshness_inventory.py` (`make docs:check-freshness`) |
| `external-link-allowlist.yml` | `scripts/check_external_links.py` (`make docs:check-external-links`, `.github/workflows/docs-health-report.yml`) |
