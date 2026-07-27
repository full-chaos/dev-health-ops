# Provider budget route families v1

`route-families.json` is the language-neutral catalog of every route family the
per-provider budget estimators (`providers/<provider>/budget.py`) are allowed to
reserve against, together with the quota dimension(s) each family charges, what
it covers, and the calibration confidence of its estimate. Its exact Draft
2020-12 shape is defined by `route-families.schema.json`.

This is a **code contract, not documentation**. Its rows cite internal class
names (`GitHubCodeClient`, `GitLabFeatureFlagsClient`) and CHAOS ticket IDs, and
its only consumer is a test. It previously lived as Markdown tables inside a
prose page under `.github/docs-legacy/providers/rate-limit-policy.md`, scoped by
`<!-- route-families:<provider> -->` HTML comments; that page was deleted in
`e23ede618`, and the catalog was recovered here rather than re-authored as prose.

`tests/providers/test_route_family_contract.py` is the differential oracle over
it: it runs the real estimators through `estimate_provider_budget` for every
supported dataset, with every flag-gated family enabled, and fails when code
emits a `route_family` this catalog does not list.

The check is deliberately **one-directional** (emitted ⊆ catalogued). The reverse
is not asserted, because the catalog intentionally carries modeled-but-not-yet-
emitted families — LaunchDarkly `projects`, `segments`, and `members` are
declared in `LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES` and documented here for
completeness while no client fetches them yet. Adding a reverse assertion would
fail on those rows by design.

`pagerduty` is dispatched by `estimate_provider_budget` but has no rows here: the
catalog predates that estimator. See the coverage note in the test module.
