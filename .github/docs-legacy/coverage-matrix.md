# Unified documentation coverage matrix

This matrix is the authoritative issue-level scope for the unified
documentation initiative. Every row names a disposition, owning source path,
automated proof, and explicit completion action.

| Issue | Disposition | Owner path | Automated proof | Completion action |
| --- | --- | --- | --- | --- |
| CHAOS-2310 | Regression baseline | `docs/llm/categorization-contract.md` | Taxonomy and schema drift test | Retain regression coverage |
| CHAOS-2311 | Regression baseline | `docs/user-guide/journeys/investment-view.md` | Strict build and canonical-link crawl | Retain regression coverage |
| CHAOS-2312 | Regression baseline | `docs/architecture/database-architecture.md` | ClickHouse wording guard | Retain regression coverage |
| CHAOS-2313 | Regression baseline | `docs/ops/cli-reference.md` | Command and reference link check | Retain regression coverage |
| CHAOS-2314 | Regression baseline | `docs/api/` | Effort-weighting terminology test | Retain regression coverage |
| CHAOS-2315 | Regression baseline | `scripts/check_docs_links.py` | Internal-link suite | Retain regression coverage |
| CHAOS-2316 | Reconcile and close | `docs/product/investment-taxonomy.md` | Generated taxonomy diff and Linear evidence | Close after evidence review |
| CHAOS-2317 | Regression baseline | `docs/user-guide/how-to-read-dev-health.md` | Language and terminology guard | Retain regression coverage |
| CHAOS-2318 | Regression baseline | `docs/user-guide/journeys/investment-view.md` | Screenshot and link crawl | Retain regression coverage |
| CHAOS-2319 | Regression baseline | `docs/publication.yml` | Publication manifest classification test | Retain regression coverage |
| CHAOS-2320 | Regression baseline | `scripts/gen_taxonomy_docs.py` | Idempotent generator test | Retain regression coverage |
| CHAOS-2321 | Regression baseline | `scripts/check_investment_docs_drift.py` | Stale-key negative test | Retain regression coverage |
| CHAOS-2322 | Regression baseline | `scripts/check_docs_links.py` | Built-site and cross-repository link tests | Retain regression coverage |
| CHAOS-2323 | Regression baseline | `docs/publication.yml` | Canonical terminology guard | Retain regression coverage |
| CHAOS-2324 | Regression baseline | `docs/llm/categorization-contract.md` | Documentation/config parity test | Retain regression coverage |
| CHAOS-2325 | Regression baseline | `docs/api/` | Existing API regression suite | Retain regression coverage |
| CHAOS-2326 | Reconcile and close | `docs/architecture/adr/002-investment-period-components.md` | ADR and current-code verification | Close after evidence review |
| CHAOS-2329 | Implement | `docs/user-guide/first-10-minutes.md` | Browser onboarding journey | Verify and close |
| CHAOS-2330 | Implement | `docs/index.md` | Navigation CTA browser test | Verify and close |
| CHAOS-2331 | Implement | `docs/user-guide/how-to-read-dev-health.md` | Plain-language guardrail test | Verify and close |
| CHAOS-2332 | Implement | `docs/user-guide/glossary.md` | Term and link coverage test | Verify and close |
| CHAOS-2333 | Implement | `docs/user-guide/views/quadrants.md` | Raw-value and no-ranking assertion | Verify and close |
| CHAOS-2334 | Implement | `docs/user-guide/views/flame-diagrams.md` | Evidence-drilldown browser check | Verify and close |
| CHAOS-2335 | Implement | `docs/user-guide/views/code-hotspots.md` | Interpretation and anti-blame assertion | Verify and close |
| CHAOS-2336 | Implement | `docs/user-guide/views/pr-flow.md` | Stage and latency browser check | Verify and close |
| CHAOS-2337 | Implement | `docs/user-guide/views/capacity-planning.md` | Caveat and derivation assertion | Verify and close |
| CHAOS-2338 | Implement | `docs/user-guide/views/work-graph.md` | Relationship and evidence cross-link test | Verify and close |
| CHAOS-2339 | Implement | `docs/user-guide/views/ai-*.md` | AI-label and calibrated-language guard | Verify and close |
| CHAOS-2340 | Implement | `docs/user-guide/reports.md` | Reports Center browser journey | Verify and close |
| CHAOS-2341 | Implement | `docs/user-guide/metrics-interpretation.md` | Null-not-zero and trends guard | Verify and close |
| CHAOS-2342 | Implement | `docs/contributing/platform-contract.md` | Canonical-source hash and link test | Verify and close |
| CHAOS-2343 | Implement | `scripts/check_docs_links.py` | Cross-tree link checker test | Verify and close |
| CHAOS-2344 | Implement | `docs/publication.yml` | Audience navigation manifest test | Verify and close |
| CHAOS-2345 | Implement | `docs/freshness-inventory.yml` | Owner and last-reviewed expiry test | Verify and close |
| CHAOS-2346 | Implement | `docs/user-guide/` | Journey and QA-spec classification test | Verify and close |
| CHAOS-2883 | Implement | `docs/decisions/unified-docs-cloudflare.md` | Hosting-source-of-truth ADR test | Verify and close |
| CHAOS-2884 | Implement | `wrangler.jsonc` and `.github/workflows/` | Preview and production deployment evidence | Verify and close |
| CHAOS-2885 | Implement | `docs/user-guide/` and web runtime config | Runtime config and canonical-link smoke | Verify and close |
| CHAOS-2886 | Implement | `web/wrangler.jsonc` and workflows | Root-export and preview/production smoke | Verify and close |
| CHAOS-2887 | Implement | `docs/redirects.yml` and headers | HTTP/TLS/header/rollback evidence | Verify and close |
