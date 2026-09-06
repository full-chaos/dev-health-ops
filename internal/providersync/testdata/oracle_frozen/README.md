# Frozen oracle snapshots

Two independent deletion PRs landed the same freezing mechanism
(`compareRowsAgainstFrozenOracle`/`frozenOracleDivergences` in
`oracle_compare_test.go`) against different sets of retired Python producers.
Both sections below apply the SAME contract: `compareRowsAgainstFrozenOracle`
reads a frozen JSON file instead of shelling out to
`python_generic_row_oracle.py`, then runs the EXACT same field-by-field diff
`compareRowsAgainstPythonOracle` does -- this is a regression guard against
the frozen snapshot, not a live-drift guard: it proves providersync's Go
derivation hasn't changed since capture. It does NOT prove Python still
agrees, because Python no longer exists to ask.

## CHAOS-5329 (parent CHAOS-3092): Jira work-items ingestion pairs

The Python Jira ingestion path (`src/dev_health_ops/providers/jira/normalize.py`,
`src/dev_health_ops/providers/jira/provider.py`'s `JiraProvider`, and
`src/dev_health_ops/metrics/work_items.py`'s `fetch_jira_work_items_with_extras`)
is deleted -- providersync's `JiraAtlassianRouteHandler` and
`JiraWorkItemsRouteHandler`'s underlying normalizers are the only producers of
Jira work-items/dependencies/transitions/interactions/reopen-events/sprints
now. Every automatic trigger for the Python path was already gone before this
PR (the `syncJira` Helm CronJob ships `enabled: false` in every values file in
this repo; the sync-provider registry's `get_provider("jira")` had zero
callers outside its own docstring).

The 3 pairs here (of 14 total `jira_*` oracle pairs -- the other 11 test
different, unrelated Python functions and are untouched) used to run
`python_generic_row_oracle.py` live on every test run; with the Python
producer deleted, there is nothing left to shell out to, so each pair's LAST
live comparison output was captured once and frozen into a JSON file with the
same shape `python_generic_row_oracle.py` itself emits (`{"cases": [{"id",
"row"}], "excluded_fields": {...}}`).

- `jira_work-items_atlassian.json` -- `TestJiraAtlassianSurfacesMatchLivePythonProducer` (`jira_atlassian_oracle_test.go`), pair `jira/work-items/atlassian`.
- `jira_work-items_batch.json` -- `TestJiraProducerBatchMatchesLivePython` (`jira_work_items_batch_oracle_test.go`), pair `jira/work-items/batch`.
- `jira_work-items_issue.json` -- `TestJiraWorkItemMatchesLivePythonProductionRow` (`jira_work_items_oracle_prep_test.go`), pair `jira/work-items/issue`.

Captured 2026-09-06 on bigboy, from the pushed tip of
`lane-5055-jira-python-delete` (based on main `16e824942ed697a1aa2f55f95f3d757f699e4e4c`),
by temporarily instrumenting `oracleDivergences` to dump its own decoded
`output` bytes for these 3 pairIDs while running the then-still-live
`TestJiraAtlassianSurfacesMatchLivePythonProducer` /
`TestJiraProducerBatchMatchesLivePython` /
`TestJiraWorkItemMatchesLivePythonProductionRow` tests with
`DEV_HEALTH_LIVE_PYTHON_ORACLES=1` against the SAME case sets those tests
already use (`jiraAtlassianOracleCases`, `jiraWorkItemsBatchOracleCases`,
`jiraWorkItemOraclePrepCases`) -- never a hand-reconstructed case set, so the
frozen bytes are exactly what those tests would have produced. The
instrumentation itself was never committed; capturing again (a future
provider/field addition to these three pairs' Go code, requiring a new frozen
fixture) means re-adding the same temporary hook, one test run per pair, then
discarding it.

Negative control run before installing: with these 3 files removed, all 3
tests fail cleanly on `open ...: no such file or directory` -- the mechanism
discriminates real absence, not a vacuous pass.

## CHAOS-5310/CHAOS-5321/CHAOS-3092 (R6): metrics-daily / team-attributions / state-durations pairs

`compute_work_item_metrics_daily`, `compute_work_item_team_attributions`, and
`compute_work_item_state_durations_daily` are deleted from the Python
codebase -- native Go executors (daily-partition path) and providersync's own
ingest-time derivation (this package) are the only producers of
`work_item_metrics_daily`/`work_item_user_metrics_daily`/
`work_item_cycle_times`/`work_item_team_attributions`/
`work_item_state_durations_daily` now. The pairs here (4 providers x
{metrics-daily, team-attributions, state-durations, cycle-times,
user-metrics-daily}, plus a github team-attributions backstop and a jira
metrics-daily route variant) used to run `python_generic_row_oracle.py` live
on every test run; with the Python producers gone, there is nothing left to
shell out to, so each pair's LAST live comparison output was captured once
and frozen the same way as the section above.

Captured 2026-09-06 on bigboy, from the pushed tip of
`delete-work-item-families-python` (commit `c914b485429a9784af8d8ac9ee9acff44d75db2f`),
by temporarily instrumenting `oracleDivergences` to dump its own decoded
`output` bytes for each pairID while running the then-still-live
`Test*MatchesLivePythonProduction` tests with `DEV_HEALTH_LIVE_PYTHON_ORACLES=1`
against the SAME case sets those tests already use
(`githubWorkItemMetricTripletOracleCases`, `gitlabOracleCases`,
`jiraDerivedOracleCases`, `linearizeWorkItemOracleCases`, and their
provider-specific metrics-daily variants) -- never a hand-reconstructed case
set, so the frozen bytes are exactly what those tests would have produced.
The instrumentation itself was never committed; capturing again (a future
provider/field addition to these families' Go code, requiring a new frozen
fixture) means re-adding the same temporary hook, one test run per pair, then
discarding it.
