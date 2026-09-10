package goapiproof

import "testing"

// TestInvestmentFullRepoCoverage_CapturedBodiesMatchOnlyWithDeclaration is
// the durable regression pin for the sankey.coverage.repoCoverage /
// .teamCoverage FloatTierB declarations added to investmentFull in
// operations.go.
//
// The two captured bodies (baseline 773418f7..., candidate 7ae7cb5b...,
// build e4a9fabff, run 5167b7de-18da-4747-addf-16c65a2858c8) are the
// deployed-executed proof's own JOB 5 evidence: comparing them under the
// REGISTERED investmentFull Parity (sankey.nodes/edges value + order
// already declared per CHAOS-5451/CHAOS-5546) reported exactly one
// finding outside any declaration, `repoCoverage`
// (0.9931181145418517 vs 0.9931181145418516, 1 ULP). That is what earns
// the fix here: repoCoverage/teamCoverage are both ratios of
// sum()/sumIf(repoEffortCol, ...) ClickHouse float sums whenever
// coverageUseInvestment is true -- unconditionally on this document's
// committed useInvestment=true payload, per sankeycoverage.go's
// resolveSankeyCoverage/compileSankeyCoverage.
func TestInvestmentFullRepoCoverage_CapturedBodiesMatchOnlyWithDeclaration(t *testing.T) {
	baseline := snapshotFromFile(t, "testdata/investmentfull_baseline_job5_773418f7.json")
	candidate := snapshotFromFile(t, "testdata/investmentfull_candidate_job5_7ae7cb5b.json")

	spec, err := SpecFor("investmentFull")
	if err != nil {
		t.Fatalf("SpecFor(investmentFull): %v", err)
	}
	const teamPath = "data.analytics.sankey.coverage.teamCoverage"
	const repoPath = "data.analytics.sankey.coverage.repoCoverage"
	if _, ok := spec.Parity.FloatTierB[teamPath]; !ok {
		t.Fatalf("investmentFull's registered Parity carries no FloatTierB declaration for %s", teamPath)
	}
	if _, ok := spec.Parity.FloatTierB[repoPath]; !ok {
		t.Fatalf("investmentFull's registered Parity carries no FloatTierB declaration for %s", repoPath)
	}

	// WITH the declarations (the registered spec, as committed): 0
	// findings outside any declaration, and the vacuity guard must not
	// fire for either new entry -- both fields are present in both
	// captured bodies, so a declaration that "matched nothing" would mean
	// this test's own fixtures are wrong, not that the guard is overly
	// strict.
	withDeclaration := Compare(baseline, candidate, spec.Parity)
	if !withDeclaration.IsMatch() {
		t.Fatalf("with the registered FloatTierB declarations: terminal_state = %q, findings = %d: %+v",
			withDeclaration.TerminalState, len(withDeclaration.Findings), withDeclaration.Findings)
	}
	for _, unused := range withDeclaration.UnusedTierB {
		if unused == teamPath || unused == repoPath {
			t.Fatalf("vacuity guard fired for %s -- the declaration matched nothing, but the field is present in both captured bodies", unused)
		}
	}

	// WITHOUT the two new declarations (everything else registered stays,
	// including the sankey.nodes/.edges FloatTierB and OrderInsensitiveLists
	// entries the same two bodies also need): reproduces exactly the one
	// finding investmentFull's job5 proof run actually reported --
	// repoCoverage. teamCoverage happens to match bit-for-bit in this
	// particular capture (both bodies read 0.997591340089648), so removing
	// its declaration alone reproduces no finding; it stays declared
	// because the rule -- ratio of float sums -- does not depend on one
	// run's summation order landing the same way twice.
	withoutNewDeclarations := spec.Parity
	trimmedTierB := make(map[string]string, len(spec.Parity.FloatTierB))
	for path, reason := range spec.Parity.FloatTierB {
		if path == teamPath || path == repoPath {
			continue
		}
		trimmedTierB[path] = reason
	}
	withoutNewDeclarations.FloatTierB = trimmedTierB

	positional := Compare(baseline, candidate, withoutNewDeclarations)
	if positional.IsMatch() {
		t.Fatal("without the repoCoverage/teamCoverage declarations, the same two bodies unexpectedly matched -- the fixtures no longer exercise the ULP divergence")
	}
	var mismatches []Finding
	for _, finding := range positional.Findings {
		if finding.Kind == FindingMismatch {
			mismatches = append(mismatches, finding)
		}
	}
	if len(mismatches) != 1 {
		t.Fatalf("without the two new declarations: got %d mismatch findings, want 1 (repoCoverage): %+v", len(mismatches), mismatches)
	}
	if got := tieredPath(mismatches[0].Path); got != repoPath {
		t.Fatalf("without the two new declarations: mismatch at %q, want %q", got, repoPath)
	}
}
