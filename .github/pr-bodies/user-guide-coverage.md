## Scope

Aligns user-guide source truth with the current State Flow, Report Center, and fixture-backed
surfaces while preserving calibrated, non-ranking interpretation guidance.

## Issue mapping

- CHAOS-2340 — Reports Center guide: create, clone, fixed None/Weekly/Monthly schedules,
  Run Now, rendered output, AI labeling, and operator follow-up.
- CHAOS-2341 — Metrics interpretation guide: cycle and lead time, throughput, WIP,
  after-hours and weekend ratios, and the 80% bus-factor threshold with evidence-first
  guardrails.
- User-guide remediation — State Flow’s work-item state-transition Sankey, canonical Work
  Graph search relevance, and disclosed Cockpit/Investment fixture states.

## Validation

- `pytest tests/docs -q`, strict site build, source and built-site links, and freshness guards.
- Docs QA typecheck plus State Flow, Report Center, metric, fixture, and search browser
  journeys.
- Final local-validation and sanitized command receipts are maintained in the local evidence
  manifest.

## Visual evidence

<!-- Replace each placeholder with its hosted GitHub attachment before opening the PR. -->

- [ ] Report Center guide — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Report Center guide — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Report Center guide — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] Metrics interpretation guide — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Metrics interpretation guide — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Metrics interpretation guide — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] State Flow guide — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] State Flow guide — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] State Flow guide — desktop (1280 × 900): `<!-- screenshot-url -->`

## Risk

Low: documentation, navigation polish, and documentation-QA coverage only. The guides
deliberately avoid implementation internals and treat unavailable values as missing context
rather than zero. Report output remains evidence-led: **AI-generated** narrative is labeled,
the current rendered surface does not promise a provenance panel, and no output is framed as
a person-level conclusion.
