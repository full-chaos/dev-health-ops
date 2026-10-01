package providersync

import "testing"

// This pair executed LinearProvider.iter_ingest with an injected typed client
// when its answer was recorded on the pinned build. The test compares that
// recorded WorkItem dataclass against the same Go-normalized production row;
// it runs in every package test run and executes no Python.
func TestLinearProviderIterIngestMatchesFrozenPythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"linear/work-items/provider",
		linearWorkItemOracleCases(),
		buildLinearWorkItemOracleRow,
		linearWorkItemWriteStampGoOnly,
	)
}
