# Phase 5 acceptance scenarios

These scenarios are human-review inputs for CHAOS-2995. Automated checks may verify URLs, links, builds, redirects, and objective facts; they do not replace uncoached reader completion.

## Test conditions

Run the primary scenarios with reviewers who did not author the pages. Record:

- entry point and device/viewport;
- whether navigation or search was used;
- time to the first relevant page;
- wrong turns and misleading labels;
- whether the reader completed the task without a direct URL or coaching;
- confidence in the result and next action;
- source-accuracy, IA, content, design, accessibility, search, redirect, and platform defects.

Repeat representative scenarios at desktop, tablet, mobile, keyboard-only, 200% zoom, reduced motion, and with screen-reader landmark/heading review.

## Scenario 1 — Direct Investment route

> You want to understand where the selected team's effort appears to be going for the current time window. Starting at the documentation home, find the supported procedure and explain the first result you would inspect.

Pass conditions:

- The reader selects **Use Dev Health** without learning an audience taxonomy.
- The reader reaches `/use/investment/investigate-effort/` without a direct URL.
- Scope, time window, coverage, required role, and source prerequisites are visible before interpretation.
- The guide distinguishes an observed or derived result from a verdict about a person.
- The next action links to Investment Mix, evidence, troubleshooting, or exact reference based on the reader's need.

## Scenario 2 — Exact calculation lookup

> A displayed Investment distribution is unexpected. Find the exact weighting, aggregation, rounding, and null/zero behavior without reading a second walkthrough.

Pass conditions:

- Search or the task page reaches `/reference/metrics/weighting-and-aggregation/`.
- Exact facts are separated from workflow prose.
- The page identifies applicability and source-of-truth code.
- The reader can return to the task without a reciprocal-link dump.

## Scenario 3 — Category definition

> You need the canonical meaning and boundary of an Investment category shown in the product.

Pass conditions:

- Search returns `/reference/taxonomies/investment/` as the canonical result.
- The taxonomy is generated or checked from the canonical source.
- Deprecated or alternate labels do not create another canonical page.

## Scenario 4 — No or incomplete data

> The Investment view has no result or appears incomplete. Determine whether the value is a measured zero, unavailable, partial, stale, or delayed, and choose the correct next check.

Pass conditions:

- The reader reaches `/use/troubleshooting/no-or-incomplete-data/` from the task or search.
- The diagnostic sequence starts with safe user-visible checks.
- Zero, unavailable, incomplete, stale, delayed, estimated, unsupported, and no detected association are not conflated.
- The reader is sent to administration only for source/sync/coverage checks and to operations only for a verified platform failure.
- Each escalation preserves the original scope, time window, and evidence needed for support.

## Scenario 5 — Optional Get Started route

> You are new to Dev Health and do not know whether you have the access and data needed for the Investment task.

Run two variants:

1. Start from `/` and choose the task directly.
2. Start from `/` and use the provisional Get Started prerequisite route.

Pass conditions:

- The optional route contains only prerequisites and task routing that reduce uncertainty.
- No old “first ten minutes,” quick-start, Start Here, or feature-tour sequence is present.
- Both variants reach the same canonical task page.
- The comparison records completion time, wrong turns, confidence, and whether Get Started added or removed reader effort.

Required decision:

- retain `/get-started/` as a small router;
- collapse it into `/`;
- collapse it into `/use/`; or
- remove it.

## Scenario 6 — Natural-language search

Test queries derived from reader language rather than exact titles:

- “where is our effort going”
- “investment mix looks wrong”
- “how are investment percentages weighted”
- “what does this investment category mean”
- “investment data missing”
- “data has not refreshed”

Pass conditions:

- The canonical task, reference, taxonomy, or troubleshooting page is in the first useful result set.
- Duplicate legacy pages do not compete with the canonical result.
- Result context communicates the domain and page purpose.

## Scenario 7 — Responsive and accessible reading

Verify the home, task, workflow, reference, and troubleshooting pages at the approved breakpoints and modes.

Pass conditions:

- Navigation, search, theme controls, breadcrumbs, local contents, edit/source, and feedback are keyboard and touch operable.
- Landmark and heading order remains logical when navigation collapses.
- Content reflows at 200% zoom without hiding procedures, code, tables, or callouts.
- Focus is visible and not communicated by color alone.
- Reduced motion is respected.
- Code and tables remain usable on narrow screens.

## Scenario 8 — Redirect and canonical behavior

Open each legacy Investment and onboarding URL in the approved disposition slice.

Pass conditions:

- Moved or merged public pages redirect to the exact canonical destination.
- Removed onboarding pages do not redirect to a misleading equivalent.
- Canonical metadata, navigation, search, and edit/source links agree on one page.
- The current Workers preview remains explicitly non-canonical until Phase 11.

## Scale decision record

CHAOS-2995 must record one of:

- **Approve** — Phase 6–9 public migration may begin.
- **Approve with owned debt** — every accepted defect has an issue, owner, priority, and exit criterion.
- **Reject and return** — name the exact IA, content, design, accessibility, search, or platform gate that must be corrected.

Passing CI alone is not an approval.
