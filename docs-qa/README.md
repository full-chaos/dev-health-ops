# Documentation QA

Run the browser suites from this directory after a strict static build:

```bash
pnpm run build:docs
pnpm run typecheck
pnpm run test:visual
pnpm run test:a11y
pnpm run test:journeys
pnpm run test:search
```

## Required local evidence gate before opening the user-guide PR

The five canonical workspace evidence manifests and their 48 screenshots are local release
evidence, not committed source. After capturing a fresh packet for the current commit, run:

```bash
EVIDENCE_ROOT="$HOME/projects/full-chaos/dev-health/.omo/evidence"
../.venv/bin/python ../scripts/validate_user_guide_evidence.py --evidence-root "$EVIDENCE_ROOT"
```

The validator requires the five task-7 through task-11 manifests to enumerate exactly 48
sanitized `375×900`, `768×900`, and `1280×900` Chrome artifacts with source-head, freshness,
digest, route/state/browser/timestamp, console, network, accessibility, and sanitization receipts.
CI deliberately does not run this local-evidence command.
