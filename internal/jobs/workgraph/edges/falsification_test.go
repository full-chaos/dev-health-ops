package edges

import (
	"encoding/json"
	"math"
	"testing"
)

// The tests in this file check the CHECKS. A golden suite that cannot fail is
// indistinguishable from one that passes, and this port's whole safety argument
// rests on the golden — so every assertion in golden_full_test.go gets a planted
// defect here that it must reject.
//
// This is not ceremony. The map-keyed-by-intern-index defect that
// TestEdgeIDMatchesPythonForEveryFrozenEdge caught during development silently
// blanked source_type on every row and mis-decoded five edge_types; it looked
// exactly like a passing decoder until an assertion with real teeth ran.

// TestEdgeIDComparisonRejectsATamperedEndpoint proves the id check is sensitive
// to each field of the hash input, not just to the id string.
func TestEdgeIDComparisonRejectsATamperedEndpoint(t *testing.T) {
	const (
		sourceType = NodeTypeIssue
		sourceID   = "linear:CHAOS-4766"
		edgeType   = EdgeTypeRelates
		targetType = NodeTypeIssue
		targetID   = "linear:CHAOS-4758"
	)
	baseline := EdgeID(sourceType, sourceID, edgeType, targetType, targetID)

	for name, tampered := range map[string]string{
		"source type":  EdgeID(NodeTypePR, sourceID, edgeType, targetType, targetID),
		"source id":    EdgeID(sourceType, sourceID+"x", edgeType, targetType, targetID),
		"edge type":    EdgeID(sourceType, sourceID, EdgeTypeBlocks, targetType, targetID),
		"target type":  EdgeID(sourceType, sourceID, edgeType, NodeTypePR, targetID),
		"target id":    EdgeID(sourceType, sourceID, edgeType, targetType, targetID+"x"),
		"swapped ends": EdgeID(targetType, targetID, edgeType, sourceType, sourceID),
	} {
		if tampered == baseline {
			t.Errorf("changing the %s did not change the edge id", name)
		}
	}

}

// TestEdgeIDIsAmbiguousAcrossDelimiters PINS a real, latent property of the
// Python id scheme rather than pretending it is absent.
//
// The canonical string is "{st}:{sid}|{et}|{tt}:{tid}", concatenated with no
// escaping, so a value containing a delimiter can be re-parsed at a different
// boundary and two DIFFERENT edges can hash to the same id:
//
//	("a", "b|c", "d", "e", "f")  ->  "a:b|c|d|e:f"
//	("a", "b",  "c|d", "e", "f") ->  "a:b|c|d|e:f"
//
// This is NOT fixed here, and must not be: the id is the key the cleanup step
// deletes by and the twin of the ClickHouse dedup identity, so changing it
// would orphan every stored row and destroy the only parity instrument this
// port has. It is recorded as a test so the property is a known, asserted fact
// instead of a surprise to whoever hits it.
//
// What bounds it in practice: the two type positions are a closed enum
// (NodeType members are lowercase identifiers with no ':' or '|'), so a
// collision needs a delimiter inside a work item ID. Real ids do contain colons
// ("linear:CHAOS-4314", "jira:API-1"), which is why the colon case is the live
// one — but a colon inside an ID can only collide with a type token that also
// contains a colon, and none does. The pipe case needs a pipe in an ID, which no
// provider emits today and nothing validates.
func TestEdgeIDIsAmbiguousAcrossDelimiters(t *testing.T) {
	if EdgeID("a", "b|c", "d", "e", "f") != EdgeID("a", "b", "c|d", "e", "f") {
		t.Error(
			"the pipe ambiguity is gone: the id scheme changed, which means every stored " +
				"edge_id and every delete key in the cleanup step just changed too",
		)
	}
	if EdgeID("a", "b:c", "d", "e", "f") != EdgeID("a:b", "c", "d", "e", "f") {
		t.Error("the colon ambiguity is gone: see above, the id scheme changed")
	}
	// The bound that makes it harmless: no node type token carries a delimiter.
	for _, nodeType := range []string{NodeTypeIssue, NodeTypePR} {
		for _, delimiter := range []string{":", "|"} {
			if indexOf(nodeType, delimiter) >= 0 {
				t.Errorf("node type %q contains %q, which unbounds the id ambiguity", nodeType, delimiter)
			}
		}
	}
}

// TestRotGuardComparisonCatchesPlantedDrift proves the byte comparison the rot
// guard performs is not vacuous — a canonicalising comparison that ignored
// values would pass every drift.
func TestRotGuardComparisonCatchesPlantedDrift(t *testing.T) {
	frozen := json.RawMessage(`[[1,2,3,4,5,6,7,1.0,8,9,10,11,12,13]]`)
	for name, planted := range map[string]string{
		"confidence lowered":   `[[1,2,3,4,5,6,7,0.9,8,9,10,11,12,13]]`,
		"endpoint swapped":     `[[1,4,3,2,5,6,7,1.0,8,9,10,11,12,13]]`,
		"row dropped":          `[]`,
		"row duplicated":       `[[1,2,3,4,5,6,7,1.0,8,9,10,11,12,13],[1,2,3,4,5,6,7,1.0,8,9,10,11,12,13]]`,
		"string index shifted": `[[1,2,3,4,5,6,7,1.0,8,9,10,11,12,14]]`,
	} {
		if string(canonicalJSON(t, frozen)) == string(canonicalJSON(t, json.RawMessage(planted))) {
			t.Errorf("the rot-guard comparison does not detect %q", name)
		}
	}
	// ...and does not report a false positive on insignificant formatting.
	spaced := json.RawMessage("[ [1, 2, 3, 4, 5, 6, 7, 1.0, 8, 9, 10, 11, 12, 13] ]")
	if string(canonicalJSON(t, frozen)) != string(canonicalJSON(t, spaced)) {
		t.Error("the rot-guard comparison reports whitespace as drift")
	}
}

// TestQuantizeIsNotAnIdentityOnFloat64 is the guard against the single most
// likely silent regression in this package: someone "simplifying" Quantize away,
// or storing confidence as float64. Either would reproduce 1,833 components
// where production has 1,832, with a different component set.
func TestQuantizeIsNotAnIdentityOnFloat64(t *testing.T) {
	if float64(Quantize(0.9)) == 0.9 {
		t.Fatal(
			"Quantize(0.9) round-trips to the float64 literal, so the Float32 narrowing " +
				"this package depends on is not happening",
		)
	}
	// It must, however, be idempotent: narrowing an already-narrow value is a
	// no-op, which is what makes a re-read equal to a write.
	once := Quantize(0.9)
	if twice := Quantize(float64(once)); twice != once {
		t.Fatalf("Quantize is not idempotent: %v then %v", once, twice)
	}
}

// TestDependencyConfidenceSeparatesTheTiers pins the ordering the CHAOS-2775
// split relies on. A policy that returned the same value for both families would
// satisfy every "is it 0.9" assertion and still reproduce the defect.
func TestDependencyConfidenceSeparatesTheTiers(t *testing.T) {
	for edgeType := range AssociativeEdgeTypes {
		associative := DependencyConfidence(edgeType)
		for _, delivery := range []string{EdgeTypeImplements, EdgeTypeParentOf, EdgeTypeChildOf} {
			if !(associative < DependencyConfidence(delivery)) {
				t.Fatalf(
					"%s (%v) does not rank strictly below %s (%v); the split's edge-drop phase "+
						"only drops edges BELOW the component max, so equal tiers reinstate the "+
						"node-deleting behaviour this policy exists to remove",
					edgeType, associative, delivery, DependencyConfidence(delivery),
				)
			}
		}
	}
}

// TestValidateConfidenceIsReachedBeforeGrouping documents WHY the validator
// exists by pinning the arithmetic that makes a NaN dangerous, rather than
// trusting a comment. Both comparisons against NaN are false, so a NaN belongs
// to neither partition of a >=/< split and vanishes from grouping.
func TestValidateConfidenceIsReachedBeforeGrouping(t *testing.T) {
	nan := math.NaN()
	if nan >= 1.0 || nan < 1.0 {
		t.Fatal("NaN compared true against a bound; the premise of the validator is wrong")
	}
	if got := math.Max(nan, 1.0); !math.IsNaN(got) && got != 1.0 {
		t.Fatalf("unexpected max(NaN, 1.0) = %v", got)
	}
	if err := ValidateConfidence(float32(nan)); err == nil {
		t.Fatal("the writer would mint an ungroupable NaN confidence")
	}
}
