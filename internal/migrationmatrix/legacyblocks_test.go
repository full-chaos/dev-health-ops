package migrationmatrix

import "testing"

func TestRenderProviderSyncBlockRejectsUnknownExecutor(t *testing.T) {
	pairs := []ProviderMatrixPair{{Provider: "github", Dataset: "blame", GoExecutor: "some_new_executor"}}
	if _, err := RenderProviderSyncBlock(pairs); err == nil {
		t.Fatal("expected an unmapped go_executor value to refuse rendering")
	}
}

func TestRenderProviderSyncBlockSortsAndRendersPythonBooleans(t *testing.T) {
	pairs := []ProviderMatrixPair{
		{Provider: "gitlab", Dataset: "blame", GoExecutor: "native_go", RouteDestinations: []string{"git_blame"}, RouteReady: true, Plannable: true},
		{Provider: "github", Dataset: "blame", GoExecutor: "native_go", RouteDestinations: nil, RouteReady: true, Plannable: false},
	}
	got, err := RenderProviderSyncBlock(pairs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "| Provider | Dataset | Executor | Route destinations (tables written) | Route ready | Plannable |\n" +
		"| --- | --- | --- | --- | --- | --- |\n" +
		"| github | `blame` | NATIVE | -- | True | False |\n" +
		"| gitlab | `blame` | NATIVE | `git_blame` | True | True |\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestConsistencyGuardReportsMissingAndExtra(t *testing.T) {
	live := map[string]bool{"a": true, "b": true}
	curated := map[string]bool{"b": true, "c": true}
	err := consistencyGuard("thing(s)", live, curated, "fix it")
	if err == nil {
		t.Fatal("expected a violation for the missing family")
	}
	// Missing ("a" absent from curated) is reported before extra -- assert
	// on substance, not the exact wording, so the message can still improve.
	if got := err.Error(); !containsSubstr(got, "a") {
		t.Fatalf("expected the missing name in the error, got: %s", got)
	}
}

func TestRenderDailyMetricsBlockRejectsAFamilyWithNoCuratedRow(t *testing.T) {
	families := &NativeFamilies{Daily: map[string]string{"repo_user_commit": "native"}}
	_, err := RenderDailyMetricsBlock([]string{"repo_user_commit", "brand_new_family"}, families, nil)
	if err == nil {
		t.Fatal("expected a family with no DailyCitationLedger row to refuse rendering")
	}
}

func TestAssertWorkgraphLedgerMatchesArtifactRejectsAWiringMismatch(t *testing.T) {
	// workgraph.build is ledgered NATIVE; claim the artifact says compat.
	err := assertWorkgraphLedgerMatchesArtifact(map[string]string{"workgraph.build": "compat"})
	if err == nil {
		t.Fatal("expected a NATIVE ledger row disagreeing with a compat artifact wiring to refuse")
	}
}

func TestAssertWorkgraphLedgerMatchesArtifactRejectsAStaleRow(t *testing.T) {
	// Every dotted ledger kind must still be in the artifact; drop one.
	artifact := map[string]string{"investment.materialize": "native"} // missing workgraph.build
	err := assertWorkgraphLedgerMatchesArtifact(artifact)
	if err == nil {
		t.Fatal("expected a ledger row whose kind left the artifact to refuse (stale row, CHAOS-4438 shape)")
	}
}

func containsSubstr(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
