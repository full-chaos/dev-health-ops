package goapiproof

import (
	"context"
	"errors"
	"testing"
)

// The opt-out default only applies to an entry that sets
// NumericLeavesDeclared -- a route with no marker keeps FloatTierB's
// plain opt-in, unchanged. This is the same 1e-10/1e-5 shape
// TestCompareFloatTiers already pins for the unmarked case; here the
// marker is set, and a leaf named in FloatTierB alone (no
// FloatExactLeaves entry) still gets Tier-B tolerance.
func TestCompareFloatLeafTolerantByDefaultWhenDeclared(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"hotspots":{"score":1.0000000004}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"hotspots":{"score":1.0000000005}}}`)

	result := Compare(baseline, candidate, Options{
		NumericLeavesDeclared: true,
		FloatTierB:            map[string]string{"data.hotspots.score": "CHAOS-5451 merged Float64 aggregate"},
	})
	if !result.IsMatch() {
		t.Fatalf("a leaf declared float (FloatTierB) under the marker must still tolerate 1e-10, got %v", findingPaths(result))
	}
	if len(result.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("a declared leaf must never read as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}

// FloatExactLeaves opts a float-declared leaf back to exact WITHOUT
// reclassifying it as integer: the leaf stays in FloatTierB (it IS
// float-provenance) and also appears in FloatExactLeaves, and the pair
// together mean "float, but compare it exactly, here is why".
func TestCompareFloatExactLeavesOptsBackToExact(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"hotspots":{"score":1.0000000004}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"hotspots":{"score":1.0000000005}}}`)

	opts := Options{
		NumericLeavesDeclared: true,
		FloatTierB:            map[string]string{"data.hotspots.score": "CHAOS-5451 merged Float64 aggregate"},
		FloatExactLeaves:      map[string]string{"data.hotspots.score": "test: opted back to exact"},
	}
	result := Compare(baseline, candidate, opts)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("FloatExactLeaves must compare exactly even though the leaf is also in FloatTierB, got %s (%v)", result.TerminalState, findingPaths(result))
	}
	if len(result.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("a leaf in FloatExactLeaves must still count as declared, got %v", result.UndeclaredNumericLeaves)
	}

	// The identical pair still matches -- FloatExactLeaves makes the
	// comparison exact, not "always mismatch".
	same := snapshotFromJSON(t, `{"data":{"hotspots":{"score":1.0000000004}}}`)
	if !Compare(same, same, opts).IsMatch() {
		t.Fatal("FloatExactLeaves over two identical values must still match")
	}
}

// IntegerLeaves declares a leaf's type for the enforcement check, but
// changes nothing about HOW it compares: integer leaves are Tier A
// (exact) whether or not they are declared, so declaring one must not
// accidentally grant it tolerance.
func TestCompareIntegerLeavesStayExact(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"cells":{"value":41}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"cells":{"value":42}}}`)

	result := Compare(baseline, candidate, Options{
		NumericLeavesDeclared: true,
		IntegerLeaves:         map[string]string{"data.cells.value": "test: bare count"},
	})
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("an integer-declared leaf must still mismatch on a real 1-count difference, got %s", result.TerminalState)
	}
	if len(result.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("a leaf in IntegerLeaves must count as declared, got %v", result.UndeclaredNumericLeaves)
	}
}

// The core guarantee: a numeric leaf reached under NumericLeavesDeclared
// that appears in none of FloatTierB/FloatExactLeaves/IntegerLeaves is
// reported via Result.UndeclaredNumericLeaves. Without the marker, the
// exact same leaf/body reports nothing here -- an unmarked route keeps
// today's behaviour, no leaf is ever required to appear anywhere.
func TestCompareUndeclaredNumericLeafReportedOnlyWhenMarked(t *testing.T) {
	body := `{"data":{"cells":{"value":42}}}`

	unmarked := Compare(snapshotFromJSON(t, body), snapshotFromJSON(t, body), Options{})
	if len(unmarked.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("a route with no marker must never report an undeclared leaf, got %v", unmarked.UndeclaredNumericLeaves)
	}

	marked := Compare(snapshotFromJSON(t, body), snapshotFromJSON(t, body), Options{NumericLeavesDeclared: true})
	if len(marked.UndeclaredNumericLeaves) != 1 || marked.UndeclaredNumericLeaves[0] != "data.cells.value" {
		t.Fatalf("a marked route reaching an undeclared numeric leaf must report it, got %v", marked.UndeclaredNumericLeaves)
	}
}

// An undeclared leaf still compares -- Tier A, the same default an
// unmarked route gets -- so a real difference is never swallowed while
// waiting on Result.UndeclaredNumericLeaves to be read. Undeclared-ness
// is an ADDITIONAL signal, not a substitute for the ordinary finding.
func TestCompareUndeclaredNumericLeafStillComparesTierA(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"cells":{"value":41}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"cells":{"value":42}}}`)

	result := Compare(baseline, candidate, Options{NumericLeavesDeclared: true})
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("an undeclared leaf must still compare (Tier A) and mismatch on a real difference, got %s", result.TerminalState)
	}
	if len(result.UndeclaredNumericLeaves) != 1 || result.UndeclaredNumericLeaves[0] != "data.cells.value" {
		t.Fatalf("the same leaf must also be reported as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}

// TestRunRefusesOnAnUndeclaredNumericLeaf pins the enforcement guard: the
// guard (run.go's `if len(result.UndeclaredNumericLeaves) > 0
// { return refuse(...) }`) must actually terminate the run, not just
// populate a Result field nothing reads -- the same shape
// TestRunRefusesOnAnOrderInsensitiveListDeclarationMatchingNothing pins
// for its own declared-list guard. A version of the marker that passed a run with an
// undeclared leaf would be worse than not having the marker at all.
func TestRunRefusesOnAnUndeclaredNumericLeaf(t *testing.T) {
	withOverriddenParity(t, "investmentFull", Options{
		NumericLeavesDeclared: true,
		// Deliberately empty: data.analytics.breakdowns.items.value below
		// is a numeric leaf named in none of FloatTierB/FloatExactLeaves/
		// IntegerLeaves.
	})

	body := `{"data":{"analytics":{"breakdowns":{"items":[{"value":1}]}}}}`
	edge := &fakeEdge{goBody: body, pythonBody: body}
	runner := newRunner(t, edge, "canary")
	edge.goBuild = runner.Registry.BuildIdentity
	runner.Documents = map[string]string{"investmentFull": "query InvestmentFull { analytics { breakdowns { items { value } } } }"}
	runner.Registry.DocumentDigest = map[string]string{"investmentFull": "ff77aa88"}
	runner.Routing = map[string]RoutingRow{"investmentFull": {Mode: "canary", CandidateBuild: runner.Registry.BuildIdentity}}

	outcomes, _, err := runner.Run(context.Background())
	if !errors.Is(err, ErrNothingMeasured) {
		t.Fatalf("an undeclared numeric leaf must refuse the run, got %v", err)
	}
	if outcomes[0].RefusalReason != RefusalUndeclaredNumericLeaf {
		t.Fatalf("expected %s, got %s (%s)", RefusalUndeclaredNumericLeaf, outcomes[0].RefusalReason, outcomes[0].RefusalDetail)
	}
}

// validateNumericLeaves catches a contradictory corpus declaration
// before any comparison runs: a leaf is float or integer, never both.
func TestValidateNumericLeavesRejectsContradiction(t *testing.T) {
	if err := validateNumericLeaves(Options{
		IntegerLeaves: map[string]string{"data.x": "a count"},
		FloatTierB:    map[string]string{"data.x": "a float aggregate"},
	}); err == nil {
		t.Fatal("a path in both IntegerLeaves and FloatTierB must be rejected")
	}
	if err := validateNumericLeaves(Options{
		IntegerLeaves:    map[string]string{"data.x": "a count"},
		FloatExactLeaves: map[string]string{"data.x": "opted to exact"},
	}); err == nil {
		t.Fatal("a path in both IntegerLeaves and FloatExactLeaves must be rejected")
	}
}

// FloatExactLeaves opts a FloatTierB path back to exact; it cannot
// declare a leaf float on its own -- a path here with no matching
// FloatTierB entry is not an opt-out of anything.
func TestValidateNumericLeavesRejectsOrphanedFloatExact(t *testing.T) {
	if err := validateNumericLeaves(Options{
		FloatExactLeaves: map[string]string{"data.x": "opted to exact"},
	}); err == nil {
		t.Fatal("a FloatExactLeaves entry with no matching FloatTierB entry must be rejected")
	}
}

// The ordinary, non-contradictory shapes must all validate clean.
func TestValidateNumericLeavesAcceptsOrdinaryDeclarations(t *testing.T) {
	for name, opts := range map[string]Options{
		"float tolerant":       {FloatTierB: map[string]string{"data.x": "reason"}},
		"float opted to exact": {FloatTierB: map[string]string{"data.x": "reason"}, FloatExactLeaves: map[string]string{"data.x": "reason"}},
		"integer":              {IntegerLeaves: map[string]string{"data.x": "reason"}},
		"empty":                {},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateNumericLeaves(opts); err != nil {
				t.Fatalf("an ordinary declaration must validate, got %v", err)
			}
		})
	}
}

// A dynamically-keyed JSON object (a Go map[string]<number> with
// data-dependent keys, e.g. theme_distribution keyed by an org's own
// theme names) cannot be named leaf-by-leaf in advance. Declaring the
// map's OWN path (its parent, one level up from any one value) covers
// every value beneath it -- the same "one declaration, every element"
// shape FloatTierB already gives a LIST via tieredPath's index
// stripping, extended here to the one JSON shape that has no syntactic
// index to strip.
func TestCompareDynamicMapFloatLeafDeclaredAtParent(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"dist":{"engineering":1.0000000004,"quality":2.0000000004}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"dist":{"engineering":1.0000000005,"quality":2.0000000006}}}`)

	result := Compare(baseline, candidate, Options{
		NumericLeavesDeclared: true,
		FloatTierB:            map[string]string{"data.dist": "test: dynamically-keyed float map"},
	})
	if !result.IsMatch() {
		t.Fatalf("both dynamically-keyed values must tolerate 1e-10 under the parent's own FloatTierB declaration, got %v", findingPaths(result))
	}
	if len(result.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("a value under a declared dynamic map must never read as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
	if len(result.UnusedTierB) != 0 {
		t.Fatalf("the parent declaration matched real leaves and must not read as stale, got %v", result.UnusedTierB)
	}

	// A real difference beyond tolerance still mismatches.
	tooFar := Compare(
		snapshotFromJSON(t, `{"data":{"dist":{"engineering":1.0}}}`),
		snapshotFromJSON(t, `{"data":{"dist":{"engineering":1.1}}}`),
		Options{NumericLeavesDeclared: true, FloatTierB: map[string]string{"data.dist": "test"}},
	)
	if tooFar.TerminalState != TerminalStateMismatch {
		t.Fatalf("a dynamic-map Tier-B leaf must still catch a real difference, got %s", tooFar.TerminalState)
	}
}

// The same parent-path fallback, for IntegerLeaves: a dynamically-keyed
// count map (evidence_quality_distribution, unassigned_reasons) declares
// its type once at the map's own path, and every value beneath it stays
// Tier A (exact) -- declaring a leaf integer must never accidentally
// grant it tolerance.
func TestCompareDynamicMapIntegerLeafDeclaredAtParent(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"counts":{"high":3,"low":5}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"counts":{"high":3,"low":6}}}`)

	result := Compare(baseline, candidate, Options{
		NumericLeavesDeclared: true,
		IntegerLeaves:         map[string]string{"data.counts": "test: dynamically-keyed count map"},
	})
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("a dynamic-map integer leaf must still mismatch on a real 1-count difference, got %s", result.TerminalState)
	}
	if len(result.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("a value under a declared dynamic integer map must never read as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}

// A leaf's OWN exact declaration takes precedence over its parent's --
// the parent fallback only fires when the leaf itself is undeclared.
func TestCompareLeafExactDeclarationTakesPrecedenceOverParent(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"dist":{"engineering":1}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"dist":{"engineering":2}}}`)

	result := Compare(baseline, candidate, Options{
		NumericLeavesDeclared: true,
		FloatTierB:            map[string]string{"data.dist": "test: parent declared float"},
		IntegerLeaves:         map[string]string{"data.dist.engineering": "test: this ONE leaf is declared integer, overriding the parent"},
	})
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("the leaf's own exact IntegerLeaves declaration must win over the parent's FloatTierB, got %s", result.TerminalState)
	}
}

// TestTrancheNumericLeafDeclarationsAreReachable runs Compare() with a
// minimal, representative body for EVERY route this ticket's tranche
// wired (sankey, heatmap, investment family) against itself, under the
// REAL registered corpus Options -- not a hand-built stand-in. A typo in
// a declared dotted path (a field renamed, a wrong nesting level) would
// compile clean and pass every other test in this file, since those all
// construct their own Options; this is the one test that would catch it,
// the same way a live prove run against a real response would, before
// a run against the real edge ever sees it.
func TestTrancheNumericLeafDeclarationsAreReachable(t *testing.T) {
	for name, body := range map[string]string{
		"heatmap review_wait_density": `{"data":{"cells":[{"x":"a","y":"b","value":1.5}]}}`,
		"heatmap repo_touchpoints":    `{"data":{"cells":[{"x":"a","y":"b","value":3}]}}`,
		"heatmap hotspot_risk":        `{"data":{"cells":[{"x":"a","y":"b","value":2.5}]}}`,
		"heatmap active_hours":        `{"data":{"cells":[{"x":"a","y":"b","value":4}]}}`,

		"sankey investment mode": `{"data":{"nodes":[{"name":"n","group":"g","value":null}],"links":[{"source":"a","target":"b","value":1.5}]}}`,
		"sankey hotspot mode":    `{"data":{"nodes":[{"name":"n","group":"g","value":null}],"links":[{"source":"a","target":"b","value":3}]}}`,
		"sankey expense mode":    `{"data":{"links":[{"source":"a","target":"b","value":1.5}]}}`,
		"sankey state mode":      `{"data":{"links":[{"source":"a","target":"b","value":3}]}}`,

		"investment main":     `{"data":{"theme_distribution":{"feature":1.5},"subcategory_distribution":{"feature.bug":2.5},"evidence_quality_distribution":{"high":3},"evidence_quality_stats":{"mean":0.5,"stddev":0.1,"total":10,"band_counts":{"high":3}}}}`,
		"investment sunburst": `{"data":[{"theme":"t","subcategory":"s","scope":"repo","value":1.5}]}`,
		"investment explain":  `{"data":{"confidence":{"quality_mean":0.5,"quality_stddev":0.1,"band_mix":{"high":3}},"top_findings":[{"evidence":{"share_pct":10.0,"delta_pct_points":1.0,"evidence_quality_mean":0.5}}]}}`,

		"investment flow dynamic":   `{"data":{"nodes":[{"name":"n","group":"g","value":1.5}],"links":[{"source":"a","target":"b","value":1.5}],"team_coverage":0.5,"repo_coverage":0.5,"distinct_team_targets":3,"distinct_repo_targets":4}}`,
		"investment flow mode":      `{"data":{"nodes":[{"name":"n","group":"g","value":1.5}],"links":[{"source":"a","target":"b","value":1.5}],"team_coverage":0.5,"repo_coverage":0.5,"distinct_team_targets":3,"distinct_repo_targets":4,"coverage":{"team_coverage":0.5,"repo_coverage":0.5},"unassigned_reasons":{"missing_team":1,"missing_repo":2},"top_n_repos":12}}`,
		"investment flow repo-team": `{"data":{"nodes":[{"name":"n","group":"g","value":1.5}],"links":[{"source":"a","target":"b","value":1.5}]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			opts, ok := trancheOptionsFor(name)
			if !ok {
				t.Fatalf("no Options registered in this test for %q", name)
			}
			snap := snapshotFromJSON(t, body)
			result := Compare(snap, snap, opts)
			if len(result.UndeclaredNumericLeaves) != 0 {
				t.Fatalf("%s: numeric leaf reached with no declared type, got %v", name, result.UndeclaredNumericLeaves)
			}
			if len(result.UnusedTierB) != 0 {
				t.Fatalf("%s: a declared FloatTierB entry matched nothing in this body, got %v", name, result.UnusedTierB)
			}
			if !result.IsMatch() {
				t.Fatalf("%s: comparing the body against itself must match, got %v", name, findingPaths(result))
			}
		})
	}
}

// trancheOptionsFor maps this test's own scenario names to the REAL
// registered corpus Options -- kept as a small local switch rather than
// importing restcorpus.go's map-shaped registry, since several of these
// (heatmap's four) are not keyed by anything this package exposes for
// direct lookup by scenario name.
func trancheOptionsFor(name string) (Options, bool) {
	switch name {
	case "heatmap review_wait_density":
		return heatmapReviewWaitDensityParity, true
	case "heatmap repo_touchpoints":
		return heatmapRepoTouchpointsParity, true
	case "heatmap hotspot_risk":
		return heatmapHotspotRiskParity, true
	case "heatmap active_hours":
		return heatmapActiveHoursParity, true
	case "sankey investment mode":
		return sankeyInvestmentParity, true
	case "sankey hotspot mode":
		return sankeyRepoDedupParity, true
	case "sankey expense mode":
		return sankeyCycleTimesDedupParity, true
	case "sankey state mode":
		return sankeyStateFlowParity, true
	case "investment main":
		return investmentParity, true
	case "investment sunburst":
		return investmentSunburstParity, true
	case "investment explain":
		return Options{NumericLeavesDeclared: true, FloatTierB: investmentExplainDeterministicFloats, IntegerLeaves: investmentExplainIntegerLeaves}, true
	case "investment flow dynamic":
		return investmentFlowDynamicParity, true
	case "investment flow mode":
		return investmentFlowModeParity, true
	case "investment flow repo-team":
		return investmentFlowRepoTeamParity, true
	default:
		return Options{}, false
	}
}

// A dynamically-keyed map's own key can itself contain a "." (subcategory_
// distribution's real keys are compound, "<theme>.<subcategory>"), which
// pushes the declared ancestor two levels up from the leaf, not one.
// The climb must not stop at the first miss.
func TestCompareDynamicMapFloatLeafClimbsPastKeyWithEmbeddedDot(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"dist":{"feature_delivery.new_feature":1.0000000004}}}`)
	candidate := snapshotFromJSON(t, `{"data":{"dist":{"feature_delivery.new_feature":1.0000000005}}}`)

	result := Compare(baseline, candidate, Options{
		NumericLeavesDeclared: true,
		FloatTierB:            map[string]string{"data.dist": "test: compound-keyed float map"},
	})
	if !result.IsMatch() {
		t.Fatalf("a compound key's embedded dot must not defeat the parent climb, got %v", findingPaths(result))
	}
	if len(result.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("a value under a declared dynamic map must never read as undeclared regardless of how many dots its own key carries, got %v", result.UndeclaredNumericLeaves)
	}
}
