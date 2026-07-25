# CHAOS-105: Fixture validation for WorkUnit investment records

## Context

Fixture validation previously treated Work Graph connected components as the proxy
for WorkUnits and required at least `max(2, repo_count)` components. That check
was useful while the graph builder and fixtures were immature, but it now conflicts
with the product contract: WorkUnits are evidence containers for issues, PRs,
commits, incidents, and related artifacts, not a category or a topology target.
The production materializer still starts from connected graph components, but the
observable contract for the Investment View is the persisted
`WorkUnitInvestmentRecord` distribution rendered by the visualization layer.

## Decision

Replace the component-count threshold with validation that inspects the persisted
investment output. Fixture validation should continue to require non-empty graph
edges and evidence bundles with at least `MIN_EVIDENCE_CHARS`, but it should judge
Investment View readiness by three persisted-data signals: a non-trivial count of
`work_unit_investment_records`, repository coverage across expected fixture repos,
and team coverage against expected fixture teams. Repository and team checks are
coverage checks, not topology checks; they assert that generated evidence reaches
the scopes the UI can filter and aggregate by.

## Alternatives

One option was to keep the connected-component threshold and tune it again. That
would preserve the current CI behavior but keep encoding topology as a product
semantic. Another option was to delete the validation entirely and rely only on
frontend smoke tests. That would avoid false failures, but it would allow fixture
runs with empty or scope-less investment records to pass until a visualization
breaks. A third option was to validate only raw graph prerequisites. That confirms
source data exists, but not that the materialized Investment View contract exists.

## Consequences

Existing fixture databases may fail validation if they were generated without
`--with-metrics`/materialized investment records, or if their records lack repo or
team evidence. This is intentional: such fixtures are not sufficient for the
Investment View. The validation no longer fails merely because graph topology
collapses into fewer connected components than repos, which better matches the
many-to-many relationships among teams, projects, and repositories. Future changes
should adjust density or coverage thresholds only when the persisted Investment
View contract changes, not when graph shape changes.
