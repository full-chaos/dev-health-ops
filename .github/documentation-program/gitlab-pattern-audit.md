# GitLab documentation pattern audit

The benchmark is structural, not visual copying.

| Pattern | GitLab reader benefit | Decision | Full Chaos rule |
| --- | --- | --- | --- |
| Stable product/task hierarchy in a persistent left navigation | Readers can browse without knowing repository layout | Adopt | Seven task domains with one canonical location per page |
| Section landing pages | A section explains scope and exposes concrete child tasks | Adopt | Every top-level and dense level-two branch has a concise index |
| Human-readable URLs | Links remain meaningful outside the site | Adopt | Lowercase, hyphenated, task/concept URLs; no repository or sprint names |
| Breadcrumbs plus local table of contents | Readers retain global and local context | Adopt | Breadcrumbs are always available; TOC appears when a page has enough headings |
| Dense reference separated from task guidance | Procedures stay usable while exact facts remain findable | Adopt | Tasks link to canonical Reference pages rather than copying tables |
| Search with hierarchy context | Result relevance is easier to judge | Adopt | Result title, summary, breadcrumb, applicability, and deprecation context |
| Edit/source affordances | Readers can report or repair errors quickly | Adopt | Use maintained Material actions; no custom feedback framework at launch |
| Multiple navigation placements for the same page | Can expose content from several routes | Reject | Contextual links are allowed; canonical navigation placement is unique |
| Marketing-card wall as documentation home | Strong visual entry points | Reject | Root is a concise task router, not a product marketing page |
| Deeply nested category trees | Accommodates a large product | Adapt | Prefer three visible nav levels and four path segments after the domain |
| Public architecture history and internal plans | Can aid contributors | Adapt | Publish only durable supported decisions; keep delivery evidence internal |
| Broad per-page visual regression | Detects layout changes | Reject | Use representative accessibility/browser smoke tests and human design review |
| Exact prose assertions | Can protect terminology | Reject | Generate or validate objective facts only; prose remains human-reviewed |
| Version selector | Useful for multiple supported releases | Defer | Express applicability now; add versioned docs only when support policy requires it |
| Full dark-mode parity | Helps reader preference | Adopt | Both schemes use the logo-derived palette and meet WCAG 2.2 AA |
