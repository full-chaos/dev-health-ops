package goapiproof

import (
	"reflect"
	"strings"
	"testing"
)

// resultFieldClassification documents, for every field Result declares,
// whether Result.Acceptance (compare.go) reads it -- an acceptance input
// -- or the reason it is not one. TestResultFieldsAreAllClassified walks
// Result's fields via reflection and fails when one is missing here: the
// ONLY thing that makes "a new Result field must be classified" a
// guarantee rather than a hope, the way the GraphQL and REST provers'
// hand-written, independently-maintained if-chains used to let a new
// field (UndeclaredNumericLeaves) go unread by one of the two for as long
// as nobody happened to notice.
var resultFieldClassification = map[string]string{
	"TerminalState":                    "not an acceptance input: this IS the verdict Acceptance's checks protect, not a signal that gates it",
	"Findings":                         "not an acceptance input: the raw observations a verdict is built from; DifferencesOutsideBaselineDefect is the count a caller reads",
	"CoveredByShape":                   "not an acceptance input: reporting bookkeeping only",
	"OutsideByShape":                   "not an acceptance input: reporting bookkeeping only",
	"UnusedExclusions":                 "acceptance input: read by Acceptance",
	"UnusedTierB":                      "acceptance input: read by Acceptance",
	"UndeclaredNumericLeaves":          "acceptance input: read by Acceptance",
	"BaselineDefectsMatched":           "not an acceptance input: recorded on the receipt as evidence, never a refusal signal itself",
	"StaleBaselineDefects":             "acceptance input: read by Acceptance",
	"LiveBaselineDefectsUnexplained":   "acceptance input: read by Acceptance",
	"IdleIntermittentBaselineDefects":  "not an acceptance input: the true idle case is expected and recorded, never refused -- see the field's own doc comment",
	"DifferencesOutsideBaselineDefect": "not an acceptance input: read by the CALLER's own match/mismatch accounting (e.g. enablement eligibility), never by Acceptance",
	"UnusedOrderInsensitiveLists":      "acceptance input: read by Acceptance",
	"OrderInsensitiveListRefusals":     "acceptance input: read by Acceptance",
	"StructuralRefusal":                "checked by the CALLER before Acceptance is ever invoked, never one of Acceptance's own return values: Compare returns immediately on it, leaving every field Acceptance reads at its zero value",
	"StructuralDetail":                 "not an acceptance input: detail text for StructuralRefusal",
	"StochasticLeafCitation":           "not an acceptance input: recorded on the receipt when StochasticLeaves applied cleanly; carries no refusal itself",
	"StochasticLeafRefusals":           "acceptance input: read by Acceptance",
}

// TestResultFieldsAreAllClassified is this ticket's pin: a new Result
// field must be classified -- acted on by Acceptance, or stated "not an
// acceptance input because ..." -- or this test goes red. Symmetric: a
// classification entry naming a field Result no longer declares is
// stale and must be removed, not left to describe nothing.
func TestResultFieldsAreAllClassified(t *testing.T) {
	typ := reflect.TypeOf(Result{})
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		classification, ok := resultFieldClassification[name]
		if !ok {
			t.Errorf("Result.%s is not classified in resultFieldClassification -- state whether Acceptance reads it, or why it is not an acceptance input", name)
			continue
		}
		if strings.TrimSpace(classification) == "" {
			t.Errorf("Result.%s's classification is empty -- state whether Acceptance reads it, or why it is not an acceptance input", name)
		}
	}
	if len(resultFieldClassification) == 0 {
		t.Fatal("resultFieldClassification is empty")
	}
	for name := range resultFieldClassification {
		if _, ok := typ.FieldByName(name); !ok {
			t.Errorf("resultFieldClassification names %q, which Result no longer declares -- remove the stale entry", name)
		}
	}
}

// resultWithOneStringSliceField builds a Result with exactly one named
// []string field populated -- every acceptance-input field Result
// declares is []string, so this one helper drives every case below.
func resultWithOneStringSliceField(t *testing.T, field string, value []string) Result {
	t.Helper()
	var r Result
	fv := reflect.ValueOf(&r).Elem().FieldByName(field)
	if !fv.IsValid() {
		t.Fatalf("Result has no field %q", field)
	}
	fv.Set(reflect.ValueOf(value))
	return r
}

// TestEveryAcceptanceInputFieldIsReachable proves resultFieldClassification's
// "acceptance input" entries are true, not just asserted: populating ONLY
// that field must produce exactly one Acceptance refusal, naming that
// same field. A classification claiming "read by Acceptance" for a field
// Acceptance's own add(...) calls do not actually list would pass
// TestResultFieldsAreAllClassified (the field IS classified) while still
// being silently unread -- the exact shape of this ticket's bug, one
// level up.
func TestEveryAcceptanceInputFieldIsReachable(t *testing.T) {
	if len(resultFieldClassification) == 0 {
		t.Fatal("resultFieldClassification is empty")
	}
	for name, classification := range resultFieldClassification {
		if !strings.HasPrefix(classification, "acceptance input") {
			continue
		}
		t.Run(name, func(t *testing.T) {
			result := resultWithOneStringSliceField(t, name, []string{"probe"})
			refusals := result.Acceptance()
			if len(refusals) != 1 {
				t.Fatalf("Result{%s: [probe]}.Acceptance() = %+v, want exactly one refusal", name, refusals)
			}
			if refusals[0].Field != name {
				t.Fatalf("Result{%s: [probe]}.Acceptance()[0].Field = %q, want %q", name, refusals[0].Field, name)
			}
			if refusals[0].Code == "" {
				t.Fatalf("Result{%s: [probe]}.Acceptance()[0].Code is empty", name)
			}
			if !strings.Contains(refusals[0].Detail, "probe") {
				t.Fatalf("Result{%s: [probe]}.Acceptance()[0].Detail = %q, want it to name the value", name, refusals[0].Detail)
			}
		})
	}
}

// TestAcceptancePriorityOrder pins the fixed order both provers must
// never reorder (Acceptance's own doc comment states why): the GraphQL
// prover refuses on the first entry, so if two fields are non-empty at
// once, WHICH one is reported first is itself a tested contract, not an
// implementation detail.
func TestAcceptancePriorityOrder(t *testing.T) {
	result := Result{
		UndeclaredNumericLeaves:        []string{"a"},
		UnusedTierB:                    []string{"b"},
		LiveBaselineDefectsUnexplained: []string{"c"},
		StaleBaselineDefects:           []string{"d"},
		UnusedExclusions:               []string{"e"},
		UnusedOrderInsensitiveLists:    []string{"f"},
		OrderInsensitiveListRefusals:   []string{"g"},
		StochasticLeafRefusals:         []string{"h"},
	}
	wantOrder := []string{
		"UndeclaredNumericLeaves", "UnusedTierB", "LiveBaselineDefectsUnexplained",
		"StaleBaselineDefects", "UnusedExclusions", "UnusedOrderInsensitiveLists",
		"OrderInsensitiveListRefusals", "StochasticLeafRefusals",
	}
	refusals := result.Acceptance()
	if len(refusals) != len(wantOrder) {
		t.Fatalf("got %d refusals, want %d: %+v", len(refusals), len(wantOrder), refusals)
	}
	for i, want := range wantOrder {
		if refusals[i].Field != want {
			t.Fatalf("refusals[%d].Field = %q, want %q (full: %+v)", i, refusals[i].Field, want, refusals)
		}
	}
}

// TestAcceptanceHardFieldsMatchTheDocumentedSet pins WHICH three fields
// are Hard (AcceptanceRefusal.Hard's own doc comment: a receipt built
// under one of these could read as a genuine match despite something
// never actually being checked) against a change silently widening or
// narrowing that set -- the REST prover's immediate, no-receipt refusal
// path (cmd/go-api-rest-prove/main.go) only ever sees the Hard ones, so a
// field moving in or out of this set changes whether a live corpus-
// completeness gap on it is caught per-request or only in the batch.
func TestAcceptanceHardFieldsMatchTheDocumentedSet(t *testing.T) {
	wantHard := map[string]bool{
		"UndeclaredNumericLeaves":      true,
		"OrderInsensitiveListRefusals": true,
		"StochasticLeafRefusals":       true,
	}
	if len(resultFieldClassification) == 0 {
		t.Fatal("resultFieldClassification is empty")
	}
	for name := range resultFieldClassification {
		if !strings.HasPrefix(resultFieldClassification[name], "acceptance input") {
			continue
		}
		t.Run(name, func(t *testing.T) {
			result := resultWithOneStringSliceField(t, name, []string{"probe"})
			refusals := result.Acceptance()
			if len(refusals) != 1 {
				t.Fatalf("Result{%s: [probe]}.Acceptance() = %+v, want exactly one refusal", name, refusals)
			}
			if refusals[0].Hard != wantHard[name] {
				t.Fatalf("Result{%s: [probe]}.Acceptance()[0].Hard = %v, want %v", name, refusals[0].Hard, wantHard[name])
			}
		})
	}
}
