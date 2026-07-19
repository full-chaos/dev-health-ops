# Phase 10 — Search, accessibility, and lean quality controls

Phase 10 protects reader-critical behavior without recreating the WIP screenshot, exact-prose, or browser-evidence framework.

## Required pull-request gate

The single `Docs Guards` workflow performs only deterministic checks that belong on every documentation change:

1. validate the explicit publication inventory, canonical URLs, IA placement, redirects, and local source links;
2. build the candidate with strict MkDocs warnings;
3. crawl the built site for broken internal links, anchors, scripts, styles, and assets;
4. validate a controlled set of natural-language reader queries against the generated search index;
5. verify structural accessibility invariants across every built page;
6. compare exact Investment taxonomy keys with their canonical code registry.

No pull-request check depends on external network availability, screenshots, prose hashes, decorative composition, exact heading copy, or a broad browser matrix.

## Search evidence

`search-acceptance.json` contains representative task, symptom, provider, error, and technical lookup language. The checker deduplicates section results to canonical page URLs and requires each expected destination within the first five unique results. The query set should change only when reader language, product terminology, or canonical destinations change.

Manual review still verifies the real Material search dialog, keyboard behavior, suggestions, no-result recovery, and search wording. The deterministic check is intentionally a stable approximation over the same generated index, not a second search product.

## Accessibility evidence

The required static audit checks:

- page language and document title;
- one main landmark and one content H1;
- the Material skip link;
- accessible content images, links, buttons, inputs, and tables;
- no positive `tabindex` values;
- visible focus and reduced-motion CSS protections.

The human WCAG 2.2 AA review remains responsible for:

- keyboard-only operation and focus order;
- screen-reader landmarks, names, state changes, and search announcements;
- 200% and 400% zoom and reflow;
- desktop, tablet, and mobile navigation;
- light and dark contrast, forced colors, and non-text contrast;
- code, table, Mermaid, and long-token overflow;
- reduced motion and touch target behavior.

Critical and high findings block Phase 11 production delivery. Accepted lower-severity debt requires an owner and exit issue.

## Removed WIP concepts

The following are not quality gates:

- screenshot hashes or exact screenshot counts;
- visual regression of ordinary prose composition;
- exact-prose or exact-heading assertions;
- primitive showcase pages;
- evidence rails or metadata panel composition;
- test count as a quality measure;
- external-link requests on every pull request;
- age-only freshness failures.

External links and risk-based freshness belong in scheduled reports. Browser testing, when needed for a regression, should be a small issue-specific test rather than a permanent matrix.
