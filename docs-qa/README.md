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
EVIDENCE_ROOT="/path/to/user-guide-coverage-evidence"
../.venv/bin/python ../scripts/validate_user_guide_evidence.py --evidence-root "$EVIDENCE_ROOT"
```

The validator recursively inventories PNGs only inside the five canonical task-7 through task-11
directories, where it requires exactly 48 sanitized `375×900`, `768×900`, and `1280×900` Chrome
artifacts. Historical evidence elsewhere under the explicit root is ignored, except the known
noncanonical `task-3-final` orphan, which this wave rejects and the capture operator must remove.
Every manifest records the final source HEAD and a capture start after that commit; every artifact
records a post-start capture timestamp, matching post-start file mtime, actual SHA-256,
route/state/browser, console, network, accessibility, and sanitization receipts. CI deliberately
does not run this local-evidence command.
