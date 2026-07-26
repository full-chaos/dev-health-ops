package providersync

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// oracleCase is one input case for a generic Python<->Go oracle comparison
// (CHAOS-3162). Input must be JSON-serializable exactly as the target
// pair's Python build_row(case) expects it (see
// testdata/oracle_pairs/<pair>.py).
type oracleCase struct {
	ID    string
	Input map[string]any
}

// compareRowsAgainstPythonOracle is the generic whole-row comparator this
// package's pairs share (CHAOS-3162's actual deliverable: the COMPARISON is
// generic, not just the module-loading plumbing python_oracle_loader.py
// already solved). For each case it:
//
//  1. Shells out ONCE to testdata/python_generic_row_oracle.py <pairID>
//     with all cases, getting back the real, live Python row for each case
//     plus the pair's own declared excluded_fields (with required reasons).
//  2. Calls goRowBuilder(input) to get the Go-side row for the SAME case,
//     round-tripped through JSON so both sides compare as
//     map[string]any (the same representation, so a Go int and a Python
//     int that both decode through encoding/json as float64 compare equal
//     without a bespoke numeric-type dance).
//  3. Fails on ANY undeclared divergence: a key present on one side and
//     absent on the other, or a value that differs -- UNLESS the field is
//     declared in the Python pair's excluded_fields OR the caller's own
//     goOnlyFields (for Go-side bookkeeping fields the Python side
//     structurally cannot have an opinion about, e.g. org_id). Every
//     exclusion requires a written reason at the call site, mirroring
//     expected_survivor_reason in scripts/mutation_harness.py -- an
//     omission must be declared, never silently missing.
//
// This is the single comparator every pair reuses; a pair difference is a
// difference in WHAT gets compared (the pair id, the cases, the Go row
// builder), never in HOW comparison happens.
//
// Use this form (which fails t directly) when asserting the CURRENT,
// production code matches Python. To instead prove the comparator would
// CATCH a specific pre-fix/buggy variant (without corrupting the enclosing
// test's pass/fail state -- a subtest's t.Errorf always propagates failure
// to every ancestor test, so "run it and check t.Run's bool" does not work
// for that), use oracleDivergences directly.
func compareRowsAgainstPythonOracle(
	t *testing.T,
	pairID string,
	cases []oracleCase,
	goRowBuilder func(t *testing.T, input map[string]any) map[string]any,
	goOnlyFields map[string]string,
) {
	t.Helper()
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.ID, func(t *testing.T) {
			t.Helper()
			for _, message := range oracleDivergencesForCase(t, pairID, testCase, goRowBuilder, goOnlyFields) {
				t.Error(message)
			}
		})
	}
}

// oracleDivergencesForCase runs ONE case through oracleDivergences. Kept
// separate so compareRowsAgainstPythonOracle can still get one t.Run per
// case (clear per-case pass/fail in `go test -v` output) while probes that
// need every case in a single Python subprocess call use oracleDivergences
// directly.
func oracleDivergencesForCase(
	t *testing.T,
	pairID string,
	testCase oracleCase,
	goRowBuilder func(t *testing.T, input map[string]any) map[string]any,
	goOnlyFields map[string]string,
) []string {
	t.Helper()
	return oracleDivergences(t, pairID, []oracleCase{testCase}, goRowBuilder, goOnlyFields)
}

// oracleDivergences is the reusable core: it does the Python shellout, the
// Go row build, and the field-by-field diff, and returns every divergence
// message found -- WITHOUT calling t.Error/t.Fatal for the divergences
// themselves (setup failures -- the Python process erroring, its output not
// parsing, an undeclared-reason exclusion -- still fail t immediately,
// since those indicate the comparison could not run at all, not that it ran
// and found something).
//
// An empty return means the generic comparator found the two sides
// identical (module declared exclusions). This is the function
// CHAOS-3162's acceptance test calls directly: "does the comparator
// rediscover a known defect" is exactly "is this slice non-empty when
// pointed at a pre-fix Go row builder".
func oracleDivergences(
	t *testing.T,
	pairID string,
	cases []oracleCase,
	goRowBuilder func(t *testing.T, input map[string]any) map[string]any,
	goOnlyFields map[string]string,
) []string {
	t.Helper()
	for name, reason := range goOnlyFields {
		if reason == "" {
			t.Fatalf("goOnlyFields[%q] needs a non-empty written reason", name)
		}
	}

	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)

	payload := make([]map[string]any, 0, len(cases))
	for _, c := range cases {
		entry := map[string]any{"id": c.ID}
		for key, value := range c.Input {
			entry[key] = value
		}
		payload = append(payload, entry)
	}
	casesFile, err := os.CreateTemp(t.TempDir(), "oracle-cases-*.json")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := casesFile.Write(encoded); err != nil {
		t.Fatal(err)
	}
	if err := casesFile.Close(); err != nil {
		t.Fatal(err)
	}

	output, err := exec.Command(
		python,
		filepath.Join(packageDir, "testdata", "python_generic_row_oracle.py"),
		pairID,
		casesFile.Name(),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python generic row oracle for %s: %v: %s", pairID, err, output)
	}

	var decoded struct {
		Cases []struct {
			ID  string         `json:"id"`
			Row map[string]any `json:"row"`
		} `json:"cases"`
		ExcludedFields map[string]string `json:"excluded_fields"`
	}
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode Python generic row oracle output for %s: %v: %s", pairID, err, output)
	}
	for name, reason := range decoded.ExcludedFields {
		if reason == "" {
			t.Fatalf("pair %s: excluded_fields[%q] has no reason -- oracle_registry.register "+
				"should have rejected this", pairID, name)
		}
	}

	pythonRows := make(map[string]map[string]any, len(decoded.Cases))
	for _, entry := range decoded.Cases {
		pythonRows[entry.ID] = entry.Row
	}

	var messages []string
	for _, testCase := range cases {
		pythonRow, ok := pythonRows[testCase.ID]
		if !ok {
			t.Fatalf("oracle output missing case %q", testCase.ID)
		}
		goRow := jsonRoundTripToMap(t, goRowBuilder(t, testCase.Input))
		messages = append(messages, diffRows(
			testCase.ID, pythonRow, goRow, decoded.ExcludedFields, goOnlyFields,
		)...)
	}
	return messages
}

// jsonRoundTripToMap marshals then unmarshals a value into map[string]any
// so it compares against the Python oracle's own JSON-decoded output using
// the identical Go representation for every JSON type (crucially: a JSON
// number is always float64 on both sides, not int on one and float64 on
// the other).
func jsonRoundTripToMap(t *testing.T, value any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal Go row: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal Go row: %v", err)
	}
	return decoded
}

// diffRows is CHAOS-3162's actual comparison logic: every key present on
// EITHER side must either match on both, or be declared excluded
// (Python-declared via excluded_fields, or Go-only via goOnlyFields) with a
// written reason. A key present on one side and absent on the other is
// exactly as much a divergence as a key with different values on each side
// -- neither is treated as "probably fine". Returns one message per
// divergence found (nil/empty = the rows matched under the declared
// exclusions).
func diffRows(
	caseID string,
	pythonRow, goRow map[string]any,
	pythonExcluded, goOnlyFields map[string]string,
) []string {
	allKeys := map[string]struct{}{}
	for key := range pythonRow {
		allKeys[key] = struct{}{}
	}
	for key := range goRow {
		allKeys[key] = struct{}{}
	}
	sortedKeys := make([]string, 0, len(allKeys))
	for key := range allKeys {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	var messages []string
	for _, key := range sortedKeys {
		if _, excluded := pythonExcluded[key]; excluded {
			continue
		}
		if _, excluded := goOnlyFields[key]; excluded {
			continue
		}
		pythonValue, pythonHas := pythonRow[key]
		goValue, goHas := goRow[key]
		switch {
		case pythonHas && !goHas:
			messages = append(messages, fmt.Sprintf(
				"case %q, field %q: present in Python's row (%v) but absent from "+
					"Go's -- declare an exclusion with a reason if this is intentional, "+
					"don't leave it silently missing", caseID, key, pythonValue))
		case !pythonHas && goHas:
			messages = append(messages, fmt.Sprintf(
				"case %q, field %q: present in Go's row (%v) but absent from "+
					"Python's -- declare an exclusion with a reason if this is intentional, "+
					"don't leave it silently missing", caseID, key, goValue))
		case !reflect.DeepEqual(pythonValue, goValue):
			messages = append(messages, fmt.Sprintf(
				"case %q, field %q: python=%#v go=%#v", caseID, key, pythonValue, goValue))
		}
	}
	return messages
}

// TestDiffRowsClauseCoverage is a fast, synthetic-data unit test of
// diffRows's own logic -- the core comparator every pair (Python-shellout
// or ClickHouse-readback alike) reuses -- isolating each of its four
// clauses (matching value, differing value, present-in-Python-only,
// present-in-Go-only) plus its two independent exclusion mechanisms
// (Python-declared, Go-only-declared) as its own case, so a mutation to any
// one clause is caught by a case that exercises ONLY that clause, not by
// coincidence from a case exercising several at once. This is the mutation
// harness's own "mutate compound predicates clause by clause" rule applied
// to the framework code itself, not just to a pair's row-construction
// logic.
func TestDiffRowsClauseCoverage(t *testing.T) {
	tests := []struct {
		name           string
		pythonRow      map[string]any
		goRow          map[string]any
		pythonExcluded map[string]string
		goOnlyFields   map[string]string
		wantMessages   int
		// wantSubstring, when non-empty, must appear in messages[0] -- this
		// is what actually distinguishes "the present-in-Python-only clause
		// fired" from "the value-mismatch clause ALSO fired, coincidentally
		// producing one message, because a missing map key decodes as a nil
		// value that fails reflect.DeepEqual against anything". A
		// mutation-harness run against an earlier version of this test
		// (which only asserted len(messages)) proved that gap: disabling
		// the present-in-Python-only/present-in-Go-only clauses SURVIVED,
		// because the fallthrough value-mismatch clause still produced
		// exactly one message for the same two cases, for the wrong reason.
		wantSubstring string
	}{
		{
			name:         "identical single field: no divergence",
			pythonRow:    map[string]any{"state": "open"},
			goRow:        map[string]any{"state": "open"},
			wantMessages: 0,
		},
		{
			name:          "differing value: exactly one divergence",
			pythonRow:     map[string]any{"state": "open"},
			goRow:         map[string]any{"state": "closed"},
			wantMessages:  1,
			wantSubstring: `python="open" go="closed"`,
		},
		{
			name:          "present in Python only: exactly one divergence",
			pythonRow:     map[string]any{"state": "open"},
			goRow:         map[string]any{},
			wantMessages:  1,
			wantSubstring: "present in Python's row (open) but absent from Go's",
		},
		{
			name:          "present in Go only: exactly one divergence",
			pythonRow:     map[string]any{},
			goRow:         map[string]any{"state": "open"},
			wantMessages:  1,
			wantSubstring: "present in Go's row (open) but absent from Python's",
		},
		{
			name:           "declared Python-side exclusion suppresses a value mismatch",
			pythonRow:      map[string]any{"reviews_count": float64(1)},
			goRow:          map[string]any{"reviews_count": float64(0)},
			pythonExcluded: map[string]string{"reviews_count": "owned by github/pr-reviews"},
			wantMessages:   0,
		},
		{
			name:           "declared Python-side exclusion suppresses a present-or-absent divergence",
			pythonRow:      map[string]any{"reviews_count": float64(1)},
			goRow:          map[string]any{},
			pythonExcluded: map[string]string{"reviews_count": "owned by github/pr-reviews"},
			wantMessages:   0,
		},
		{
			name:         "declared Go-only exclusion suppresses a present-or-absent divergence",
			pythonRow:    map[string]any{},
			goRow:        map[string]any{"org_id": "org-acme"},
			goOnlyFields: map[string]string{"org_id": "Go-side tenant bookkeeping"},
			wantMessages: 0,
		},
		{
			name: "one excluded field plus one real divergence: only the real one is reported",
			pythonRow: map[string]any{
				"reviews_count": float64(1), "state": "open",
			},
			goRow: map[string]any{
				"reviews_count": float64(0), "state": "closed",
			},
			pythonExcluded: map[string]string{"reviews_count": "owned by github/pr-reviews"},
			wantMessages:   1,
			wantSubstring:  `python="open" go="closed"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			messages := diffRows("case", tt.pythonRow, tt.goRow, tt.pythonExcluded, tt.goOnlyFields)
			if tt.wantSubstring != "" {
				if len(messages) == 0 || !strings.Contains(messages[0], tt.wantSubstring) {
					t.Fatalf("diffRows() = %v, want a message containing %q", messages, tt.wantSubstring)
				}
			}
			if len(messages) != tt.wantMessages {
				t.Fatalf("diffRows() = %d message(s) %v, want %d", len(messages), messages, tt.wantMessages)
			}
		})
	}
}
