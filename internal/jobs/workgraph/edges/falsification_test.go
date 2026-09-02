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
	quantized, err := Quantize(0.9)
	if err != nil {
		t.Fatalf("0.9 is a valid confidence: %v", err)
	}
	if float64(quantized) == 0.9 {
		t.Fatal(
			"Quantize(0.9) round-trips to the float64 literal, so the Float32 narrowing " +
				"this package depends on is not happening",
		)
	}
	// It must, however, be idempotent: narrowing an already-narrow value is a
	// no-op, which is what makes a re-read equal to a write.
	once := quantized
	twice, err := Quantize(float64(once))
	if err != nil {
		t.Fatalf("re-quantizing a narrowed value must stay valid: %v", err)
	}
	if twice != once {
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

// TestEventTimeBindingRejectsARotatedGolden reproduces adversarial review round
// 2's attack and requires it to fail now.
//
// Round 2 rotated event_ts across the frozen edges: 2,053 of 3,548 values
// changed, yet every one remained "some dependency's last_synced", so the
// then-current assertion (membership in the fixture-wide set) accepted it. A Go
// derivation that assigned another valid dependency's timestamp to an edge would
// have passed while writing the wrong freshness — and event_ts is what readers
// filter time windows on, and what the blocker projection's watermark maximises.
func TestEventTimeBindingRejectsARotatedGolden(t *testing.T) {
	document := loadGolden(t)

	type pair struct{ low, high string }
	key := func(a, b string) pair {
		if a > b {
			a, b = b, a
		}
		return pair{a, b}
	}
	permitted := map[pair]map[int64]struct{}{}
	for index, dependency := range document.Dependencies {
		source, err := document.String(dependency[0])
		if err != nil {
			t.Fatalf("dependency %d: %v", index, err)
		}
		target, err := document.String(dependency[1])
		if err != nil {
			t.Fatalf("dependency %d: %v", index, err)
		}
		instant, err := document.Instant(dependency[5])
		if err != nil {
			t.Fatalf("dependency %d: %v", index, err)
		}
		if permitted[key(source, target)] == nil {
			permitted[key(source, target)] = map[int64]struct{}{}
		}
		permitted[key(source, target)][instant.UnixNano()] = struct{}{}
	}

	rows := make([]Row, 0, len(document.Edges))
	for index, edge := range document.Edges {
		row, err := document.EdgeRow(edge)
		if err != nil {
			t.Fatalf("edge %d: %v", index, err)
		}
		rows = append(rows, row)
	}

	// The attack: give every edge the NEXT edge's event_ts. Each value is still a
	// genuine dependency instant from this very fixture.
	rotated, rejected := 0, 0
	for index, row := range rows {
		donor := rows[(index+1)%len(rows)]
		if donor.EventTs.Equal(row.EventTs) {
			continue
		}
		rotated++
		allowed := permitted[key(row.SourceID, row.TargetID)]
		if _, ok := allowed[donor.EventTs.UnixNano()]; !ok {
			rejected++
		}
	}
	if rotated == 0 {
		t.Fatal("the rotation changed no timestamps, so it cannot test the binding")
	}
	// Not every rotated value can be caught: 167 edges share an endpoint pair
	// carrying more than one distinct last_synced (CHAOS-4788's unmerged
	// duplicates), so a donor could coincidentally be permitted. The binding must
	// still catch the overwhelming majority, or it is decoration.
	if rejected*20 < rotated*19 {
		t.Fatalf(
			"the per-edge binding rejected only %d of %d rotated timestamps; round 2's "+
				"attack would still pass",
			rejected, rotated,
		)
	}
	t.Logf("rotation attack: %d of %d moved timestamps rejected by the per-edge binding", rejected, rotated)
}

// TestProvenanceAndEvidenceOracleRejectsACorruptedRegeneration is the falsifying
// half of round 4's finding: it plants the exact regression that used to
// regenerate green and requires the derived oracle to reject it.
//
// The scenario the finding described: someone changes the producer so `evidence`
// carries the wrong string (or `provenance` stops being native), regenerates the
// golden, and every test passes because the golden now agrees with the changed
// Python and nothing derives either field independently.
func TestProvenanceAndEvidenceOracleRejectsACorruptedRegeneration(t *testing.T) {
	document := loadGolden(t)

	type binding struct{ low, high, edgeType string }
	key := func(a, b, edgeType string) binding {
		if a > b {
			a, b = b, a
		}
		return binding{a, b, edgeType}
	}
	derived := map[binding]map[string]struct{}{}
	for index, dependency := range document.Dependencies {
		source, err := document.String(dependency[0])
		if err != nil {
			t.Fatalf("dependency %d: %v", index, err)
		}
		target, err := document.String(dependency[1])
		if err != nil {
			t.Fatalf("dependency %d: %v", index, err)
		}
		relationship, err := document.String(dependency[2])
		if err != nil {
			t.Fatalf("dependency %d: %v", index, err)
		}
		raw, err := document.String(dependency[3])
		if err != nil {
			t.Fatalf("dependency %d: %v", index, err)
		}
		evidence := raw
		if evidence == "" {
			evidence = relationship
		}
		if evidence == "" {
			evidence = "dependency"
		}
		bindingKey := key(source, target, dependencyEdgeType(relationship))
		if derived[bindingKey] == nil {
			derived[bindingKey] = map[string]struct{}{}
		}
		derived[bindingKey][evidence] = struct{}{}
	}

	// Plant three regressions a regeneration would otherwise freeze.
	corruptions := map[string]func(Row) Row{
		"evidence replaced with a constant": func(row Row) Row {
			row.Evidence = "dependency"
			return row
		},
		"evidence taken from another kind": func(row Row) Row {
			row.Evidence = "linear_relation:related"
			return row
		},
		"evidence truncated": func(row Row) Row {
			if len(row.Evidence) > 3 {
				row.Evidence = row.Evidence[:3]
			}
			return row
		},
	}
	for name, corrupt := range corruptions {
		moved, rejected := 0, 0
		for index, edge := range document.Edges {
			row, err := document.EdgeRow(edge)
			if err != nil {
				t.Fatalf("edge %d: %v", index, err)
			}
			bad := corrupt(row)
			if bad.Evidence == row.Evidence {
				continue
			}
			moved++
			if _, ok := derived[key(bad.SourceID, bad.TargetID, bad.EdgeType)][bad.Evidence]; !ok {
				rejected++
			}
		}
		if moved == 0 {
			t.Fatalf("%s: the corruption changed nothing, so it cannot test the oracle", name)
		}
		// "evidence taken from another kind" legitimately matches for the edges
		// that ALREADY carry that value, so require the overwhelming majority
		// rather than all.
		if rejected*20 < moved*19 {
			t.Errorf(
				"%s: the derived oracle rejected only %d of %d corrupted values; a "+
					"regeneration carrying this regression would still go green",
				name, rejected, moved,
			)
			continue
		}
		t.Logf("%s: %d of %d corrupted values rejected", name, rejected, moved)
	}

	// And provenance, which is unconditional.
	if ProvenanceNative == ProvenanceHeuristic || ProvenanceNative == ProvenanceExplicitText {
		t.Fatal("the provenance constants collapsed; the assertion cannot distinguish them")
	}
}

// TestConfidenceAcceptSetIsPythonsNotOurs pins the validator against the
// measured behaviour of `WorkGraphEdge.__post_init__`, not against what a
// reasonable validator would do.
//
// The distinction is the point. A stricter Go check rejects rows Python writes;
// a looser one mints rows Python refuses. Both are divergences, and neither
// fails a test that asserts only the author's intuition.
func TestConfidenceAcceptSetIsPythonsNotOurs(t *testing.T) {
	// Measured 2026-09-02 against the deployed dataclass, see confidence.go.
	pythonRejects := map[string]float32{
		"NaN":  float32(math.NaN()),
		"+Inf": float32(math.Inf(1)),
		"1.5":  1.5,
		"-0.5": -0.5,
	}
	pythonAccepts := map[string]float32{
		"0.9": 0.9,
		"0.0": 0.0,
		// The two tiers this port actually writes must both be inside the
		// reference's accept-set, or the policy mints rows Python would refuse.
		"delivery tier":    DeliveryConfidence,
		"associative tier": AssociativeConfidence,
	}
	for name, value := range pythonRejects {
		if err := ValidateConfidence(value); err == nil {
			t.Errorf("Python rejects %s but this port accepts it — it would mint a row "+
				"the reference refuses", name)
		}
	}
	for name, value := range pythonAccepts {
		if err := ValidateConfidence(value); err != nil {
			t.Errorf("Python accepts %s but this port rejects it (%v) — it would drop a row "+
				"the reference writes", name, err)
		}
	}
	// The boundaries are inclusive in Python (`0.0 <= c <= 1.0`), so the exact
	// endpoints must pass; an exclusive Go comparison would silently drop them.
	for name, value := range map[string]float32{"exactly 0": 0, "exactly 1": 1} {
		if err := ValidateConfidence(value); err != nil {
			t.Errorf("%s must be accepted — Python's bounds are inclusive: %v", name, err)
		}
	}
}

// TestNarrowingCannotLaunderAnInvalidConfidence pins codex round 3's P3.
//
// Float32 narrowing is lossy in the direction that HIDES a violation:
// 1.00000001 becomes exactly 1, and -1e-50 becomes -0. Both then satisfy every
// downstream range check, while Python's `WorkGraphEdge.__post_init__` raises on
// the originals. Validating after narrowing therefore accepts values the
// reference refuses — so the check has to be on the float64, and Quantize is the
// only way to narrow.
func TestNarrowingCannotLaunderAnInvalidConfidence(t *testing.T) {
	for _, value := range []float64{1.00000001, -1e-50, 1.5, -0.5, math.NaN(), math.Inf(1), math.Inf(-1)} {
		narrowed, err := Quantize(value)
		if err == nil {
			t.Errorf("Quantize(%v) returned %v with no error; Python raises ValueError on this "+
				"value, and after narrowing nothing downstream can tell", value, narrowed)
		}
	}
	// The boundary values Python accepts must still pass, including negative
	// zero, which `0.0 <= -0.0` admits.
	for _, value := range []float64{0, 1, 0.9, math.Copysign(0, -1)} {
		if _, err := Quantize(value); err != nil {
			t.Errorf("Quantize(%v) was refused, but Python accepts it: %v", value, err)
		}
	}
}

// TestEveryDivergenceIsImplemented keeps the fidelity contract from going
// stale the way its predecessor did.
//
// The previous contract was a sentence claiming "exactly ONE enumerated
// divergence" while a second file separately claimed to hold "THE ONE
// deliberate divergence". Both were true when written and neither was revisited
// as divergences accumulated. A list with a probe per entry cannot drift that
// way: remove the code and the entry fails.
//
// What this proves and does not: every LISTED divergence is real. It cannot
// prove no UNLISTED divergence exists, and saying otherwise would repeat the
// overclaim it replaces.
func TestEveryDivergenceIsImplemented(t *testing.T) {
	if len(Divergences) == 0 {
		t.Fatal("the divergence list is empty; the port has at least the variant-C policy")
	}
	goldenCanSee := 0
	for _, divergence := range Divergences {
		if divergence.Authority == "" {
			t.Errorf("%q has no authority; a divergence without a ruling or ticket is a defect",
				divergence.Name)
		}
		if divergence.implemented == nil {
			t.Errorf("%q has no probe, so the list cannot detect its removal", divergence.Name)
			continue
		}
		if !divergence.implemented() {
			t.Errorf("%q is LISTED but not implemented — either the code lost it or the "+
				"list went stale, and both are the failure this test exists to catch",
				divergence.Name)
		}
		if divergence.GoldenCanSee {
			goldenCanSee++
		}
	}
	// Pinned rather than asserted loosely: if a second divergence ever becomes
	// golden-visible that is a real improvement, and it should be a deliberate
	// edit here rather than a silent change in what the oracle covers.
	if goldenCanSee != 1 {
		t.Errorf("%d divergences claim the golden can see them, expected exactly 1 (variant-C); "+
			"the golden holds no malformed id and is a scoped run", goldenCanSee)
	}
}
