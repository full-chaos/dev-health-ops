# Frozen oracle snapshots

CHAOS-5310/CHAOS-5321/CHAOS-3092 (R6): `compute_work_item_metrics_daily`,
`compute_work_item_team_attributions`, and `compute_work_item_state_
durations_daily` are deleted from the Python codebase -- native Go executors
(daily-partition path) and providersync's own ingest-time derivation (this
package) are the only producers of `work_item_metrics_daily`/
`work_item_user_metrics_daily`/`work_item_cycle_times`/
`work_item_team_attributions`/`work_item_state_durations_daily` now. The
12 pairs here (4 providers x {metrics-daily, team-attributions,
state-durations}) used to run `python_generic_row_oracle.py` live on every
test run; with the Python producers gone, there is nothing left to shell out
to, so each pair's LAST live comparison output was captured once and frozen
into a JSON file with the same shape `python_generic_row_oracle.py` itself
emits (`{"cases": [{"id", "row"}], "excluded_fields": {...}}`).

`compareRowsAgainstFrozenOracle`/`frozenOracleDivergences` (oracle_compare_
test.go) read these files instead of shelling out, then run the EXACT same
field-by-field diff `compareRowsAgainstPythonOracle` does -- this is a
regression guard against the frozen snapshot, not a live-drift guard: it
proves providersync's Go derivation hasn't changed since capture, the same
way `TestComputeDailyTripletMatchesPythonGolden` proves the daily-partition
Go executor against `tests/fixtures/daily_work_item_python_golden.json`. It
does NOT prove Python still agrees, because Python no longer exists to ask.

Captured 2026-09-06 on bigboy, from the pushed tip of
`delete-work-item-families-python` (commit `c914b485429a9784af8d8ac9ee9acff44d75db2f`),
by temporarily instrumenting `oracleDivergences` to dump its own decoded
`output` bytes for each of the 12 pairIDs while running the then-still-live
`Test*MatchesLivePythonProduction` tests with `DEV_HEALTH_LIVE_PYTHON_ORACLES=1`
against the SAME case sets those tests already use
(`githubWorkItemMetricTripletOracleCases`, `gitlabOracleCases`,
`jiraDerivedOracleCases`, `linearizeWorkItemOracleCases`, and their
provider-specific metrics-daily variants) -- never a hand-reconstructed case
set, so the frozen bytes are exactly what those tests would have produced.
The instrumentation itself was never committed; capturing again (a future
provider/field addition to these three families' Go code, requiring a new
frozen fixture) means re-adding the same temporary hook, one test run per
pair, then discarding it.
