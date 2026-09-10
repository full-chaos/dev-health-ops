package goapiproof

import (
	"os"
	"strings"
	"testing"
)

// --- CHAOS-5546 red-first pins for OrderInsensitiveList ----------------

// snapshotFromFile loads one captured go-api-prove response body as a
// Snapshot, failing the test rather than skipping if the fixture is
// missing -- an absent fixture must never read as "nothing to compare".
func snapshotFromFile(t *testing.T, path string) Snapshot {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	snapshot, err := DecodeSnapshot(body)
	if err != nil {
		t.Fatalf("decode fixture %s: %v", path, err)
	}
	return snapshot
}

// TestInvestmentFullSankeyOrderRelaxation_CapturedBodiesMatchOnlyWithDeclaration
// is the durable regression pin for CHAOS-5546's rule-5 escape: the two
// job5 proof-run bodies (baseline a5e7c5d3..., candidate fe29a0ab...,
// run 7e7b0e44-4fef-4885-a460-f421485b9f0b) disagree ONLY on
// sankey.nodes/edges order (same 15 node ids, same 50 edges -- verified
// by hand when these fixtures were captured). Comparing them under
// investmentFull's REGISTERED Parity (which now carries the
// OrderInsensitiveLists declaration) must find nothing; comparing them
// under the SAME Parity minus that one field must reproduce the original
// 46 findings exactly.
func TestInvestmentFullSankeyOrderRelaxation_CapturedBodiesMatchOnlyWithDeclaration(t *testing.T) {
	baseline := snapshotFromFile(t, "testdata/investmentfull_baseline_job5_a5e7c5d3.json")
	candidate := snapshotFromFile(t, "testdata/investmentfull_candidate_job5_fe29a0ab.json")

	spec, err := SpecFor("investmentFull")
	if err != nil {
		t.Fatalf("SpecFor(investmentFull): %v", err)
	}
	if len(spec.Parity.OrderInsensitiveLists) == 0 {
		t.Fatal("investmentFull's registered Parity carries no OrderInsensitiveLists -- this test would pass vacuously against the un-relaxed comparator")
	}

	withDeclaration := Compare(baseline, candidate, spec.Parity)
	if !withDeclaration.IsMatch() {
		t.Fatalf("with the registered OrderInsensitiveLists declaration: terminal_state = %q, findings = %d: %+v",
			withDeclaration.TerminalState, len(withDeclaration.Findings), withDeclaration.Findings)
	}
	if len(withDeclaration.UnusedOrderInsensitiveLists) != 0 {
		t.Fatalf("declaration matched nothing (stale): %v", withDeclaration.UnusedOrderInsensitiveLists)
	}
	if len(withDeclaration.OrderInsensitiveListRefusals) != 0 {
		t.Fatalf("declaration could not pair elements: %v", withDeclaration.OrderInsensitiveListRefusals)
	}

	// Same Parity, minus the one field under test -- everything else
	// (FloatTierB) stays, so the only variable is the relaxation itself.
	withoutDeclaration := spec.Parity
	withoutDeclaration.OrderInsensitiveLists = nil
	positional := Compare(baseline, candidate, withoutDeclaration)
	if positional.IsMatch() {
		t.Fatal("without the declaration, the same two bodies unexpectedly matched -- the fixtures no longer exercise an order-only divergence")
	}
	const wantFindings = 46 // go-api-prove run 7e7b0e44-4fef-4885-a460-f421485b9f0b, proof-report-r2.json
	if len(positional.Findings) != wantFindings {
		t.Fatalf("positional comparison of the captured bodies: got %d findings, want %d (job5's original count) -- got: %+v",
			len(positional.Findings), wantFindings, positional.Findings)
	}
	for _, finding := range positional.Findings {
		if finding.Kind != FindingMismatch {
			continue
		}
		if tiered := tieredPath(finding.Path); tiered != "data.analytics.sankey.nodes" && !strings.HasPrefix(tiered, "data.analytics.sankey.nodes.") {
			t.Fatalf("positional finding outside sankey.nodes: %+v", finding)
		}
	}
}

// TestOrderInsensitiveList_ReorderedListOnAnUndeclaredOperationStillMismatches
// proves the relaxation is per-operation, not global: a list at the SAME
// shape and path, reordered, mismatches when nothing declares it
// order-insensitive -- exactly parity rule 5's unchanged default for
// every operation other than the one that declared an escape.
func TestOrderInsensitiveList_ReorderedListOnAnUndeclaredOperationStillMismatches(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"id":"a","value":1},{"id":"b","value":2}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"id":"b","value":2},{"id":"a","value":1}]}}`)

	result := Compare(baseline, candidate, Options{})
	if result.IsMatch() {
		t.Fatal("a reordered list with no OrderInsensitiveList declaration must still mismatch (parity rule 5 default)")
	}
}

// TestOrderInsensitiveList_DeclaredListToleratesReorderingByKey is the
// same fixture as above, WITH the declaration -- proving the escape
// actually neutralises a pure reordering.
func TestOrderInsensitiveList_DeclaredListToleratesReorderingByKey(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"id":"a","value":1},{"id":"b","value":2}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"id":"b","value":2},{"id":"a","value":1}]}}`)

	opts := Options{OrderInsensitiveLists: []OrderInsensitiveList{
		{Path: "data.items", KeyFields: []string{"id"}, Reason: "test", Ticket: "CHAOS-0000"},
	}}
	result := Compare(baseline, candidate, opts)
	if !result.IsMatch() {
		t.Fatalf("declared order-insensitive list should tolerate a pure reordering: %+v", result.Findings)
	}
	if len(result.UnusedOrderInsensitiveLists) != 0 {
		t.Fatalf("declaration matched the list, must not report as unused: %v", result.UnusedOrderInsensitiveLists)
	}
}

// TestOrderInsensitiveList_MissingElementIsStillAFinding proves pairing
// by key does not hide a genuinely missing/extra element behind "it's
// just reordered" -- a key present on only one side must still surface.
func TestOrderInsensitiveList_MissingElementIsStillAFinding(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"id":"a","value":1},{"id":"b","value":2}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"id":"a","value":1}]}}`) // "b" dropped, not just reordered

	opts := Options{OrderInsensitiveLists: []OrderInsensitiveList{
		{Path: "data.items", KeyFields: []string{"id"}, Reason: "test", Ticket: "CHAOS-0000"},
	}}
	result := Compare(baseline, candidate, opts)
	if result.IsMatch() {
		t.Fatal("a genuinely missing element (not a reordering) must still mismatch under the declaration")
	}
	found := false
	for _, finding := range result.Findings {
		if finding.Kind == FindingMismatch && finding.Detail == "present in baseline, absent in candidate" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a 'present in baseline, absent in candidate' finding for key %q, got: %+v", "b", result.Findings)
	}
}

// TestOrderInsensitiveList_MissingKeyFieldRefuses proves the vacuity
// guard: an element that does not carry every declared key field means
// the declaration does not describe this data, and the comparison
// refuses rather than guessing a partial pairing.
func TestOrderInsensitiveList_MissingKeyFieldRefuses(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"id":"a","value":1},{"value":2}]}}`) // second element has no "id"
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"id":"a","value":1},{"id":"b","value":2}]}}`)

	opts := Options{OrderInsensitiveLists: []OrderInsensitiveList{
		{Path: "data.items", KeyFields: []string{"id"}, Reason: "test", Ticket: "CHAOS-0000"},
	}}
	result := Compare(baseline, candidate, opts)
	if len(result.OrderInsensitiveListRefusals) == 0 {
		t.Fatalf("expected OrderInsensitiveListRefusals to be non-empty when an element lacks the declared key field, got result: %+v", result)
	}
}

// TestOrderInsensitiveList_DeclarationMatchingNoListIsReportedUnused pins
// the other vacuity guard: a declared Path that never occurs in the
// response is reported so a stale/misspelled declaration cannot pass
// silently -- same discipline as UnusedTierB/UnusedExclusions.
func TestOrderInsensitiveList_DeclarationMatchingNoListIsReportedUnused(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"id":"a","value":1}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"id":"a","value":1}]}}`)

	opts := Options{OrderInsensitiveLists: []OrderInsensitiveList{
		{Path: "data.nonexistent", KeyFields: []string{"id"}, Reason: "test", Ticket: "CHAOS-0000"},
	}}
	result := Compare(baseline, candidate, opts)
	if len(result.UnusedOrderInsensitiveLists) != 1 || result.UnusedOrderInsensitiveLists[0] != "data.nonexistent" {
		t.Fatalf("expected UnusedOrderInsensitiveLists = [data.nonexistent], got %v", result.UnusedOrderInsensitiveLists)
	}
}

// TestOrderInsensitiveList_KeyValueContainingADotDoesNotCorruptTieredPath
// pins the tieredPath fix this feature required: a SUBCATEGORY-shaped key
// value ("feature_delivery.build", a real production node id) must not
// be misread as a path-segment boundary when a nested field inside that
// element (here, "value") is looked up against FloatTierB -- a dotted key
// silently defeating a Tier-B declaration would turn a real float-summation
// tolerance into a false Tier-A mismatch.
func TestOrderInsensitiveList_KeyValueContainingADotDoesNotCorruptTieredPath(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"items":[{"id":"feature_delivery.build","value":1.0000000001}]}}`)
	candidate := snapshotFromJSON(t, `{"data":{"items":[{"id":"feature_delivery.build","value":1.0000000002}]}}`)

	opts := Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: "data.items", KeyFields: []string{"id"}, Reason: "test", Ticket: "CHAOS-0000"},
		},
		FloatTierB: map[string]string{"data.items.value": "test tolerance"},
	}
	result := Compare(baseline, candidate, opts)
	if !result.IsMatch() {
		t.Fatalf("a Tier-B float within tolerance, reached through a dotted pairing key, must still match: %+v", result.Findings)
	}
	if len(result.UnusedTierB) != 0 {
		t.Fatalf("FloatTierB declaration for data.items.value must be seen as used even through a dotted key: unused = %v", result.UnusedTierB)
	}
}
