## Scope

Completes the user-guide coverage batch for Reports Center and shared metric interpretation.

## Issue mapping

- CHAOS-2340 — Reports Center guide: create, clone, schedule, Run Now, completed output,
  provenance, AI labeling, and operator follow-up.
- CHAOS-2341 — Metrics interpretation guide: cycle and lead time, throughput, WIP,
  after-hours and weekend ratios, and bus factor with evidence-first guardrails.

## Validation

- `pytest tests/docs -q` (95 passed), strict site build, source and built-site links, and
  freshness guards.
- Docs QA typecheck plus the Report Center browser journey, accessibility coverage, and
  responsive visual coverage for both new guides.
- `bash ci/local_validate.sh` is recorded in the local evidence manifest.
- Sanitized command receipts and responsive browser captures: `.omo/evidence/task-11-unified-cloudflare-documentation/`.

## Visual evidence

<!-- Replace each placeholder with its hosted GitHub attachment before opening the PR. -->

- [ ] Report Center guide — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Report Center guide — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Report Center guide — desktop (1280 × 900): `<!-- screenshot-url -->`
- [ ] Metrics interpretation guide — mobile (375 × 900): `<!-- screenshot-url -->`
- [ ] Metrics interpretation guide — tablet (768 × 900): `<!-- screenshot-url -->`
- [ ] Metrics interpretation guide — desktop (1280 × 900): `<!-- screenshot-url -->`

## Risk

Low: documentation, navigation polish, and documentation-QA coverage only. The guides
deliberately avoid implementation internals and treat unavailable values as missing context
rather than zero. Report output remains evidence-led: **AI-generated** narrative is labeled
and never framed as a person-level conclusion.
