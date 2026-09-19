package goapiproof

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// The go-only class, enumerated through Admit, the only door.
//
// Invariant: relative to an Admit that has no go-only class, the class adds
// exactly one admission -- ledger names the operation, the baseline is exactly
// its deletion error, the candidate is a 2xx from the go plane with a non-null,
// non-empty root -- and removes none. Every cell below is generated from the
// alphabets; the expectation is computed from the axes, not from Admit.

type goOnlyLedgerKind string

const (
	ledgerNil       goOnlyLedgerKind = "nil ledger"
	ledgerNamesOp   goOnlyLedgerKind = "ledger names the operation"
	ledgerOtherOnly goOnlyLedgerKind = "ledger names another operation only"
)

type baselineClass string

const (
	baselineData      baselineClass = "data"
	baselineDeletion  baselineClass = "deletion"
	baselineErrored   baselineClass = "other errors"
	baselineBadStatus baselineClass = "non-2xx"
	baselineBadPlane  baselineClass = "plane wrong or absent"
)

type baselineCell struct {
	name   string
	class  baselineClass
	status int
	plane  string
	body   func(op, root, message string) string
}

func deletionErrorJSON(op, message string, overrides map[string]any) map[string]any {
	object := map[string]any{"message": message, "locations": []any{map[string]any{"line": 1, "column": 9}}, "path": []any{op}}
	for key, value := range overrides {
		if value == nil {
			delete(object, key)
		} else {
			object[key] = value
		}
	}
	return object
}

func envelope(data any, errs ...any) string {
	document := map[string]any{"data": data}
	if len(errs) > 0 {
		document["errors"] = errs
	}
	encoded, _ := json.Marshal(document)
	return string(encoded)
}

func baselineAlphabet() []baselineCell {
	ok, py := http.StatusOK, "python"
	return []baselineCell{
		{"data", baselineData, ok, py, func(_, root, _ string) string {
			return envelope(map[string]any{root: map[string]any{"a": 1}})
		}},
		{"exact deletion, data null", baselineDeletion, ok, py, func(op, _, m string) string { return envelope(nil, deletionErrorJSON(op, m, nil)) }},
		{"exact deletion, root null", baselineDeletion, ok, py, func(op, root, m string) string {
			return envelope(map[string]any{root: nil}, deletionErrorJSON(op, m, nil))
		}},
		{"deletion of another operation", baselineErrored, ok, py, func(op, _, _ string) string {
			return envelope(nil, deletionErrorJSON(op, "otherOperation is served by query-api and has no Python implementation.", nil))
		}},
		{"deletion message with an extra key", baselineErrored, ok, py, func(op, _, m string) string {
			return envelope(nil, deletionErrorJSON(op, m, map[string]any{"extensions": map[string]any{"code": "X"}}))
		}},
		{"deletion error with a wrong path", baselineErrored, ok, py, func(op, _, m string) string {
			return envelope(nil, deletionErrorJSON(op, m, map[string]any{"path": []any{"elsewhere"}}))
		}},
		{"two errors", baselineErrored, ok, py, func(op, _, m string) string {
			return envelope(nil, deletionErrorJSON(op, m, nil), deletionErrorJSON(op, m, nil))
		}},
		{"unrelated error", baselineErrored, ok, py, func(op, _, _ string) string {
			return envelope(nil, deletionErrorJSON(op, "boom", nil))
		}},
		{"data next to the deletion error", baselineErrored, ok, py, func(op, root, m string) string {
			return envelope(map[string]any{root: map[string]any{"a": 1}}, deletionErrorJSON(op, m, nil))
		}},
		{"deletion error then trailing bytes", baselineErrored, ok, py, func(op, _, m string) string {
			return envelope(nil, deletionErrorJSON(op, m, nil)) + "\n{}"
		}},
		{"exact deletion, HTTP 500", baselineBadStatus, http.StatusInternalServerError, py, func(op, _, m string) string {
			return envelope(nil, deletionErrorJSON(op, m, nil))
		}},
		{"exact deletion, served by go", baselineBadPlane, ok, "go", func(op, _, m string) string { return envelope(nil, deletionErrorJSON(op, m, nil)) }},
		{"exact deletion, no plane header", baselineBadPlane, ok, "", func(op, _, m string) string { return envelope(nil, deletionErrorJSON(op, m, nil)) }},
	}
}

type candidateClass string

const (
	candidateObject candidateClass = "non-null object"
	candidateNull   candidateClass = "null root"
	candidateFail   candidateClass = "refused by every leg rule"
)

type candidateCell struct {
	name   string
	class  candidateClass
	status int
	plane  string
	build  string
	body   func(root string) string
}

func candidateAlphabet() []candidateCell {
	ok, goPlane := http.StatusOK, "go"
	object := func(root string) string { return envelope(map[string]any{root: map[string]any{"a": 1}}) }
	return []candidateCell{
		{"object, build bound", candidateObject, ok, goPlane, "b", object},
		{"object, build absent (edge)", candidateObject, ok, goPlane, "", object},
		{"null root", candidateNull, ok, goPlane, "b", func(root string) string { return envelope(map[string]any{root: nil}) }},
		{"root missing", candidateFail, ok, goPlane, "b", func(string) string { return envelope(map[string]any{"other": map[string]any{"a": 1}}) }},
		{"empty object root", candidateFail, ok, goPlane, "b", func(root string) string { return envelope(map[string]any{root: map[string]any{}}) }},
		{"typename-only root", candidateFail, ok, goPlane, "b", func(root string) string {
			return envelope(map[string]any{root: map[string]any{"__typename": "T"}})
		}},
		{"scalar root", candidateFail, ok, goPlane, "b", func(root string) string { return envelope(map[string]any{root: 0}) }},
		{"data null", candidateFail, ok, goPlane, "b", func(string) string { return envelope(nil) }},
		{"errors beside data", candidateFail, ok, goPlane, "b", func(root string) string {
			return envelope(map[string]any{root: map[string]any{"a": 1}}, map[string]any{"message": "x"})
		}},
		{"HTTP 500", candidateFail, http.StatusInternalServerError, goPlane, "b", object},
		{"served by python", candidateFail, ok, "python", "b", object},
		{"no plane header", candidateFail, ok, "", "b", object},
		{"other build", candidateFail, ok, goPlane, "other", object},
		{"trailing bytes", candidateFail, ok, goPlane, "b", func(root string) string { return object(root) + "\n{}" }},
	}
}

func ledgerFor(t *testing.T, kind goOnlyLedgerKind, operation string) *GoServedLedger {
	t.Helper()
	switch kind {
	case ledgerNil:
		return nil
	case ledgerNamesOp:
		full := defaultLedgerForTest(t)
		entries := append([]GoServedEntry(nil), full.Entries...)
		if _, ok := full.Entry(operation); !ok {
			// An operation the shipped ledger does not name, added so the
			// cell can ask what happens when the ledger names it.
			entries = append(entries, GoServedEntry{Operation: operation, TwoPlaneOpsSHA: full.Entries[0].TwoPlaneOpsSHA, Guards: full.Entries[0].Guards})
		}
		return &GoServedLedger{MessageTemplate: full.MessageTemplate, Entries: entries}
	default:
		full := defaultLedgerForTest(t)
		var entries []GoServedEntry
		for _, entry := range full.Entries {
			if entry.Operation != operation {
				entries = append(entries, entry)
			}
		}
		if len(entries) == 0 {
			entries = []GoServedEntry{{Operation: "unrelatedOperation", TwoPlaneOpsSHA: full.Entries[0].TwoPlaneOpsSHA, Guards: full.Entries[0].Guards}}
		}
		return &GoServedLedger{MessageTemplate: full.MessageTemplate, Entries: entries}
	}
}

func TestGoOnlyClassEnumerationThroughAdmit(t *testing.T) {
	operations := []string{"capacityForecast", "capacityForecasts", "throughputForecast", "featureFlags"}
	ledgerKinds := []goOnlyLedgerKind{ledgerNil, ledgerNamesOp, ledgerOtherOnly}
	cells := 0
	goOnlyAdmissions := 0
	for _, operation := range operations {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%s): %v", operation, err)
		}
		for _, kind := range ledgerKinds {
			ledger := ledgerFor(t, kind, operation)
			message := defaultLedgerForTest(t).ExpectedMessage(operation)
			for _, baseline := range baselineAlphabet() {
				for _, candidate := range candidateAlphabet() {
					cells++
					name := fmt.Sprintf("%s/%s/%s/%s", operation, kind, baseline.name, candidate.name)
					baselineBody := baseline.body(operation, spec.ResponseRoot, message)
					candidateBody := candidate.body(spec.ResponseRoot)
					baselineSnapshot, err := DecodeSnapshot([]byte(baselineBody))
					if err != nil {
						t.Fatalf("%s: decode baseline: %v", name, err)
					}
					candidateSnapshot, err := DecodeSnapshot([]byte(candidateBody))
					if err != nil {
						t.Fatalf("%s: decode candidate: %v", name, err)
					}
					input := AdmissionInput{
						Route: RouteEdge, NamedBuild: "b", ResponseRoot: spec.ResponseRoot, RootNullable: spec.RootNullable,
						Operation: operation, GoServed: ledger,
						Candidate:     Observation{StatusCode: candidate.status, Plane: candidate.plane, Build: candidate.build},
						Baseline:      Observation{StatusCode: baseline.status, Plane: baseline.plane},
						CandidateSnap: candidateSnapshot, BaselineSnap: baselineSnapshot,
					}
					got := Admit(input)

					// Expectation from the axes.
					goOnlyApplies := kind == ledgerNamesOp && baseline.class == baselineDeletion
					legsFine := candidate.status == http.StatusOK && candidate.plane == "go" &&
						(candidate.build == "" || candidate.build == "b") &&
						baseline.status == http.StatusOK && baseline.plane == "python" &&
						candidate.class != candidateFail
					var wantAdmitted, wantGoOnly bool
					switch {
					case !legsFine:
					case baseline.class == baselineData:
						wantAdmitted = candidate.class == candidateObject || (candidate.class == candidateNull && spec.RootNullable)
					case goOnlyApplies:
						wantAdmitted = candidate.class == candidateObject
						wantGoOnly = wantAdmitted
					}
					if got.Admitted != wantAdmitted || got.GoOnly != wantGoOnly {
						t.Fatalf("%s: admitted=%v goOnly=%v (%s / %s), want admitted=%v goOnly=%v",
							name, got.Admitted, got.GoOnly, got.Reason, got.Detail, wantAdmitted, wantGoOnly)
					}
					if wantGoOnly {
						goOnlyAdmissions++
						want, _ := NewGoOnlyCitation(ledger, operation)
						if got.GoOnlyCitation != want || got.EdgeBuildBinding == "" {
							t.Fatalf("%s: citation %q binding %q", name, got.GoOnlyCitation, got.EdgeBuildBinding)
						}
					} else if got.GoOnlyCitation != "" {
						t.Fatalf("%s: a citation on a non-go-only verdict", name)
					}

					// No admission of main is lost and every new one is the class:
					// what the class admits, the same input without a ledger
					// refuses; what it does not, the same input without a ledger
					// decides identically.
					input.GoServed = nil
					without := Admit(input)
					if wantGoOnly {
						if without.Admitted {
							t.Fatalf("%s: the class admitted what a ledger-less Admit also admits", name)
						}
					} else if without.Admitted != got.Admitted || (!goOnlyApplies && without.Reason != got.Reason) {
						t.Fatalf("%s: the class changed a verdict it does not own: with=%v/%s without=%v/%s",
							name, got.Admitted, got.Reason, without.Admitted, without.Reason)
					}
				}
			}
		}
	}
	// The table must have exercised the class, and its size is pinned so a
	// new alphabet entry is a deliberate edit.
	if want := len(operations) * len(ledgerKinds) * len(baselineAlphabet()) * len(candidateAlphabet()); cells != want {
		t.Fatalf("enumerated %d cells, want %d", cells, want)
	}
	// 3 ledgered operations plus featureFlags added to the ledger by the
	// "ledger names the operation" axis, each admitting two baseline shapes
	// against the two object candidates.
	if want := 4 * 2 * 2; goOnlyAdmissions != want {
		t.Fatalf("the class admitted %d cells, want %d", goOnlyAdmissions, want)
	}
}

// An unledgered operation whose baseline is exactly the deletion error is
// refused as an errored response, and the refusal names why.
func TestUnledgeredDeletionErrorIsRefusedByName(t *testing.T) {
	ledger := ledgerFor(t, ledgerOtherOnly, "capacityForecast")
	message := defaultLedgerForTest(t).ExpectedMessage("capacityForecast")
	baseline, _ := DecodeSnapshot([]byte(envelope(nil, deletionErrorJSON("capacityForecast", message, nil))))
	candidate, _ := DecodeSnapshot([]byte(envelope(map[string]any{"capacityForecast": map[string]any{"a": 1}})))
	got := Admit(AdmissionInput{
		Route: RouteEdge, NamedBuild: "b", ResponseRoot: "capacityForecast", RootNullable: true,
		Operation: "capacityForecast", GoServed: ledger,
		Candidate:     Observation{StatusCode: 200, Plane: "go", Build: "b"},
		Baseline:      Observation{StatusCode: 200, Plane: "python"},
		CandidateSnap: candidate, BaselineSnap: baseline,
	})
	if got.Admitted || got.Reason != RefusalErroredResponse {
		t.Fatalf("admitted=%v reason=%s", got.Admitted, got.Reason)
	}
	if want := "the go-served ledger does not name capacityForecast"; !strings.Contains(got.Detail, want) {
		t.Fatalf("detail %q does not say %q", got.Detail, want)
	}
}
