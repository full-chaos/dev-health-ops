package edges

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"
)

// TestTheDerivationIsByteIdenticalAcrossRuns pins emission ORDER, not just
// content.
//
// Python's builder emits edges in a deterministic order: it accumulates into a
// dict and iterates it, and since 3.7 that is insertion order. Go's map
// iteration order is deliberately RANDOMISED per range statement, so a port
// that ranges a map anywhere on the path to ordered output produces a different
// byte sequence on each run while every set-based assertion stays green.
//
// That failure is invisible to the golden comparison, which compares edges BY
// ID. It surfaces downstream instead: the write path pages rows in fixed-size
// batches, so a reordering redistributes rows across pages, and any consumer
// that diffs pages or relies on stable batch boundaries sees spurious churn on
// every run with no input change at all.
//
// Two derivations in one process is a real test because the randomisation is
// per-range, not per-process: if any map were ranged into this output, two
// consecutive runs would almost certainly disagree.
func TestTheDerivationIsByteIdenticalAcrossRuns(t *testing.T) {
	document := loadGolden(t)
	buildClock := parseFrozenInstant(t, document.FrozenNow)
	rows := goldenDependencyRows(t, document)

	digest := func(label string) string {
		result, err := DeriveIssueIssueEdges(rows, buildClock)
		if err != nil {
			t.Fatalf("%s: derive: %v", label, err)
		}
		// The whole ordered result, not a set: Edges in emission order and
		// Outcomes in input order. Counts is a map and is compared separately
		// by value, since a map's own iteration order is not part of any
		// contract.
		encoded, err := json.Marshal(struct {
			Edges    []Row     `json:"edges"`
			Outcomes []Outcome `json:"outcomes"`
		}{result.Edges, result.Outcomes})
		if err != nil {
			t.Fatalf("%s: encode: %v", label, err)
		}
		sum := sha256.Sum256(encoded)
		return hex.EncodeToString(sum[:])
	}

	first, second := digest("first run"), digest("second run")
	if first != second {
		t.Fatalf("two derivations over identical input disagree:\n  %s\n  %s\n"+
			"something on the path to ordered output ranges a Go map", first, second)
	}

	// The cleanup plan pages ids into fixed-size batches, so its ordering is
	// load-bearing in exactly the same way.
	planDigest := func(label string) string {
		plan := BuildCleanupPlan(rows, nil)
		encoded, err := json.Marshal(plan)
		if err != nil {
			t.Fatalf("%s: encode plan: %v", label, err)
		}
		sum := sha256.Sum256(encoded)
		return hex.EncodeToString(sum[:])
	}
	if firstPlan, secondPlan := planDigest("first plan"), planDigest("second plan"); firstPlan != secondPlan {
		t.Fatalf("two cleanup plans over identical input disagree:\n  %s\n  %s", firstPlan, secondPlan)
	}
	t.Logf("stable across two runs: %d rows in, %d edges out, digest %s",
		len(rows), len(mustDerive(t, rows, buildClock).Edges), first[:16])
}

func mustDerive(t *testing.T, rows []DependencyRow, buildClock time.Time) DeriveResult {
	t.Helper()
	result, err := DeriveIssueIssueEdges(rows, buildClock)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	return result
}

// TestTheDeterminismPinDetectsMapOrdering demonstrates that the pin above is
// not passing vacuously. It builds the same edges through a Go map — the exact
// mistake the pin exists to catch — and asserts the digests DO disagree.
//
// Without this, a derivation that became trivially deterministic for some
// unrelated reason (say, always producing zero edges) would keep the pin green
// while it had stopped testing anything.
func TestTheDeterminismPinDetectsMapOrdering(t *testing.T) {
	document := loadGolden(t)
	buildClock := parseFrozenInstant(t, document.FrozenNow)
	edges := mustDerive(t, goldenDependencyRows(t, document), buildClock).Edges

	byID := make(map[string]Row, len(edges))
	for _, edge := range edges {
		byID[edge.EdgeID] = edge
	}
	digest := func() string {
		ordered := make([]Row, 0, len(byID))
		for _, edge := range byID { // the defect, on purpose
			ordered = append(ordered, edge)
		}
		encoded, err := json.Marshal(ordered)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		sum := sha256.Sum256(encoded)
		return hex.EncodeToString(sum[:])
	}
	if digest() == digest() {
		t.Fatal("ranging a map twice produced the same order, so the pin above " +
			"cannot distinguish ordered output from map order and proves nothing")
	}
}
