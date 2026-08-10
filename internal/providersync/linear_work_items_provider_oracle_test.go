package providersync

import "testing"

// This pair executes LinearProvider.iter_ingest with an injected typed client
// and compares the resulting WorkItem dataclass against the same Go-normalized
// production row. It is opt-in through the shared live-oracle gate; ordinary
// package tests never claim Python execution evidence.
func TestLinearProviderIterIngestMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"linear/work-items/provider",
		linearWorkItemOracleCases(),
		buildLinearWorkItemOracleRow,
		linearWorkItemWriteStampGoOnly,
	)
}
