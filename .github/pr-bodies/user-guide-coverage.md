## Scope

Completes the evidence-led user-guide coverage packet while preserving calibrated,
non-ranking interpretation guidance and the existing AI evidence paths.

## Issue mapping

- CHAOS-2329 — First 10 minutes walkthrough: Cockpit context, navigation, chart reading,
  confidence, evidence, help, operating modes, trends over absolutes, and signals not judgment.
- CHAOS-2331 — How to read Dev Health: context, trends, caveats, evidence quality, and
  plain-language interpretation guardrails.
- CHAOS-2332 — Glossary: WorkUnit, taxonomy, flow, capacity, resilience, and evidence terms.
- CHAOS-2333 — Quadrants guide: raw-value, no-ranking interpretation and evidence path.
- CHAOS-2334 — Flame diagrams guide: single-item diagnosis and evidence drill-down.
- CHAOS-2335 — Code Hotspots guide: churn/complexity context without blame.
- CHAOS-2336 — PR Flow guide: current versus planned state-flow behavior and follow-up path.
- CHAOS-2337 — Capacity Planning guide: backlog derivation, forecast caveats, and scenario use.
- CHAOS-2338 — Work Graph guide: relationship inspection and Theme → Subcategory → Evidence.
- CHAOS-2339 — AI Impact, AI Attribution, AI Review Load, and AI Risk: labeled estimates,
  calibrated language, and persisted evidence without browser recomputation or ranking.
- CHAOS-2340 — Reports Center guide: create, clone, None/Weekly/Monthly schedules, Run Now,
  rendered output, AI labeling, and operator follow-up.
- CHAOS-2341 — Metrics interpretation: cycle/lead time, throughput, WIP, after-hours/weekend
  ratios, bus factor, trends, and null-is-not-zero guidance.

## Validation

- `pytest tests/docs -q`
- `make docs:check`
- `python -m mkdocs build --strict --site-dir .build/site`
- `python scripts/check_docs_links.py`
- `python scripts/check_built_site_links.py --site-dir .build/site`
- `python scripts/check_freshness_inventory.py`
- `pnpm --dir docs-qa run typecheck`
- `pnpm --dir docs-qa run test:visual`
- `pnpm --dir docs-qa run test:a11y`
- `pnpm --dir docs-qa run test:journeys`
- `pnpm --dir docs-qa run test:search`
- `EVIDENCE_ROOT="$HOME/projects/full-chaos/dev-health/.omo/evidence"; .venv/bin/python scripts/validate_user_guide_evidence.py --evidence-root "$EVIDENCE_ROOT"`
- `bash ci/local_validate.sh`

### Required receipts before opening the PR

- [ ] `FINAL-GATE-RECEIPT`: `<!-- paste the final local gate receipt after push -->`
- [ ] `EVIDENCE-VALIDATOR-RECEIPT`: `<!-- paste the 5-manifest / 48-artifact local validator receipt after fresh capture -->`

The evidence-validator command is a required local pre-PR gate. CI does not depend on
uncommitted workspace evidence.

## Visual evidence

<!-- Add hosted GitHub attachment URLs only after the branch is pushed and the PR exists. -->

- [ ] First 10 minutes — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] First 10 minutes — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] First 10 minutes — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] How to read Dev Health — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] How to read Dev Health — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] How to read Dev Health — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] Glossary — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Glossary — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Glossary — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] Investment journey — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Investment journey — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Investment journey — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] Quadrants — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Quadrants — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Quadrants — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] Flame diagrams — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Flame diagrams — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Flame diagrams — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] Code Hotspots — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Code Hotspots — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Code Hotspots — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] PR Flow — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] PR Flow — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] PR Flow — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] Capacity Planning — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Capacity Planning — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Capacity Planning — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] Work Graph — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Work Graph — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Work Graph — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] AI Impact — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] AI Impact — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] AI Impact — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] AI Attribution — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] AI Attribution — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] AI Attribution — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] AI Review Load — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] AI Review Load — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] AI Review Load — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] AI Risk — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] AI Risk — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] AI Risk — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] Report Center — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Report Center — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Report Center — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] Metrics interpretation — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Metrics interpretation — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Metrics interpretation — desktop (1280 × 900): `<!-- screenshot-url -->`

## Risk

Low: documentation templates, coverage contracts, and local evidence validation only. Guides
remain evidence-led: AI-generated narrative is labeled, missing context is not represented as
zero, and no output is framed as a person-level conclusion.
