package main

import (
	"strings"
	"testing"
)

// TestMountedRouteLogMessage_ListsExactlyRegisteredOperations is
// CHAOS-4710 deliverable 3's direct proof. Before this fix, main.go's
// mount-confirmation line was a hand-typed literal naming six operations
// ("featureFlags, reviewEdges, cognitiveLoad, complexityTimeseries,
// hotspots, operatingReview") while queryRouteDigestByOperation actually
// registers twelve -- an operator enabling workGraphEdges, flowMatrix, or
// either investment operation would read that line during enablement (the
// worst moment) and reasonably conclude their document was not mounted.
//
// This test parses the ACTUAL produced message (not a hand-maintained
// expectation) and asserts its operation set is exactly
// queryRouteDigestByOperation's key set -- so a thirteenth operation
// added to that map without a matching edit here cannot desync the log
// line again: the message is DERIVED from the map, not maintained
// alongside it.
func TestMountedRouteLogMessage_ListsExactlyRegisteredOperations(t *testing.T) {
	digestByOperation := queryRouteDigestByOperation()
	msg := mountedRouteLogMessage(digestByOperation)

	open := strings.Index(msg, "(")
	closeIdx := strings.LastIndex(msg, ")")
	if open < 0 || closeIdx < 0 || closeIdx <= open {
		t.Fatalf("mounted route log message %q has no operation list", msg)
	}
	loggedNames := strings.Split(msg[open+1:closeIdx], ", ")

	logged := make(map[string]bool, len(loggedNames))
	for _, name := range loggedNames {
		logged[name] = true
	}

	if len(logged) != len(loggedNames) {
		t.Fatalf("mounted route log message %q names a duplicate operation: %v", msg, loggedNames)
	}
	if len(logged) != len(digestByOperation) {
		t.Fatalf("mounted route log message names %d operations, want %d (msg=%q)",
			len(logged), len(digestByOperation), msg)
	}
	for operation := range digestByOperation {
		if !logged[operation] {
			t.Fatalf("queryRouteDigestByOperation registers %q but the mounted-route log message %q does not name it",
				operation, msg)
		}
	}
}

// TestMountedRouteLogMessage_NamesAllTwelveCurrentOperations pins the
// CURRENT registered-operation count as a tripwire, same spirit as the
// go-api-enablement-runbook's "12 documents, EXECUTED at <sha>" table: if
// this test's count ever needs to change, that is real news (an operation
// was added or removed) that deserves a look, not a silent pass. The
// SET-equality test above is what actually prevents drift; this one just
// makes a count regression loud.
func TestMountedRouteLogMessage_NamesAllTwelveCurrentOperations(t *testing.T) {
	digestByOperation := queryRouteDigestByOperation()
	const wantOperationCount = 12
	if len(digestByOperation) != wantOperationCount {
		t.Fatalf("queryRouteDigestByOperation has %d operations, want %d -- "+
			"if this is a deliberate addition/removal, update this pin",
			len(digestByOperation), wantOperationCount)
	}

	msg := mountedRouteLogMessage(digestByOperation)
	for operation := range digestByOperation {
		if !strings.Contains(msg, operation) {
			t.Fatalf("mounted route log message %q does not mention operation %q", msg, operation)
		}
	}
}
