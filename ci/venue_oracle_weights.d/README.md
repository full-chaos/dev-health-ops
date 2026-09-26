# Venue-oracle weights

Measured seconds of one venue-oracles registry `run` row, one file per package (`<package dir with "/" written "__">.tsv`, the same names as `ci/venue_oracle_registry.d/`), so two PRs that add venue rows in different packages touch different files and never conflict (CHAOS-6926). It replaces the single file `ci/venue_oracle_weights.tsv`.

Rows: `<package dir><TAB><test><TAB><seconds>[<TAB>unmeasured]`.

The rationale that used to head the single file follows.

Measured seconds of one venue-oracles registry `run` row (CHAOS-6891): the weight
ci/venue_oracle_shard.awk balances the venue legs by (longest-processing-time
assignment). Columns: package dir, test, seconds. Regenerate from a saved run log:
  gh run view <RUN_ID> --log > run.log && python3 ci/venue_oracle_weights.py run.log
A stale weight costs balance, never coverage (the legs always partition the run
rows); tests/tooling/test_venue_oracle_shards.py fails a PR whose registry has a run
row with no weight here, a weight for a test that left the registry, or a heaviest
leg over the leg's test-time budget. A row no log has measured carries the
provisional weight (600, above the slowest row ever measured) and the marker
`unmeasured` in a fourth column, until a run log measures it. Sorted (LC_ALL=C)
by package, then test.
