# Frozen oracle snapshots

CHAOS-5329 (parent CHAOS-3092): the Python Jira ingestion path
(`src/dev_health_ops/providers/jira/normalize.py`,
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

`compareRowsAgainstFrozenOracle`/`frozenOracleDivergences` (oracle_compare_
test.go) read these files instead of shelling out, then run the EXACT same
field-by-field diff `compareRowsAgainstPythonOracle` does -- this is a
regression guard against the frozen snapshot, not a live-drift guard: it
proves providersync's Go derivation hasn't changed since capture. It does NOT
prove Python still agrees, because Python no longer exists to ask.

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

## Merge note

This file/directory may also be created independently by
CHAOS-5310/CHAOS-5321/CHAOS-3092 (R6, `delete-work-item-families-python`,
different pairs -- metrics-daily/team-attributions/state-durations across all
4 providers). Whichever PR lands second takes a mechanical union merge of this
README and the shared `oracle_compare_test.go` loader (both PRs add the same
`compareRowsAgainstFrozenOracle`/`frozenOracleDivergences` machinery
independently; keep one copy, union the snapshot files).
