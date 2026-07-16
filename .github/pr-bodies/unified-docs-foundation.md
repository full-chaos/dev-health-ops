## Scope

Foundation wave (plan Todos 1-6) for the unified Cloudflare documentation
initiative: governance and Linear traceability, a deterministic publication
manifest, Investment taxonomy correctness, the docs design system and
primitive showcase, audience-first navigation, and cross-tree publication
quality guards.

- `docs/decisions/unified-docs-cloudflare.md`, `docs/coverage-matrix.md`,
  `docs/contributing/platform-contract.md`, and
  `scripts/render_workspace_agents.py` establish the versioned platform
  contract and the exact 40-issue Linear scope.
- `docs/publication.yml` + `scripts/docs_publication.py` classify every
  `docs/**/*.md` file as `public-nav`, `public-reference`, or
  `excluded-internal`; `mkdocs build --strict` is warning-free.
- `tests/docs/test_investment_drift.py` extends the Investment drift guard
  and reconciles CHAOS-2316 and CHAOS-2326 with posted Linear evidence.
- `ops/DESIGN.md`, Material overrides, and `ops/docs-qa/` add the
  evidence-led documentation design system and primitive showcase.
- Rebuilt `mkdocs.yml` navigation and `docs/freshness-inventory.yml`
  introduce the six fixed audience sections and page-freshness tracking.
- `scripts/check_built_site_links.py`, `scripts/check_external_links.py`,
  `scripts/check_freshness_inventory.py`, and
  `scripts/check_code_prerequisites.py` extend cross-tree publication
  quality guards; `.github/workflows/docs-guards.yml` wires strict build,
  built-site link/anchor/asset crawling, and external-link checking into
  CI, and fixes an aggregate-job defect where a failed `changes` job could
  mask required guards as "skipped" (treated as a pass).

## Tests

- `.venv/bin/pytest tests/docs -q`
- `make docs:check`
- `.venv/bin/mkdocs build --strict --site-dir .build/site`
- `make docs:check-built-site`
- `make docs:check-external-links`
- `bash ci/local_validate.sh`

## Risk

Low-to-medium: documentation-only and CI-guard changes, no application
runtime behavior changes. The `docs-guards.yml` aggregate-job fix changes
CI pass/fail semantics for a masked-skip edge case; verified with a
parametrized bash-script test proving both the pre-fix false-pass and the
post-fix correct failure.

`docs/external-link-allowlist.yml` carries six bounded, owned exceptions
(90-day expiry) for pre-existing broken/flaky external links discovered
while wiring the external-link guard; these are content follow-ups, not
new regressions introduced by this PR.

## Screenshot waiver

SCREENSHOT-WAIVER: backend/tooling-only change (governance docs, CI guard
scripts, and workflow YAML); no rendered product UI changed. Rendered docs
site screenshots are captured separately by the design-system and
navigation todos in this same PR.
