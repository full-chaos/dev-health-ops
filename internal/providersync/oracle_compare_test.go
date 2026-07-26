package providersync

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
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
// package's pairs share. For each case it:
//
//  1. Shells out ONCE to testdata/python_generic_row_oracle.py <pairID>
//     with all cases, getting back the real, live Python row for each case
//     plus the pair's own declared excluded_fields (with required reasons).
//     Python enforces its own completeness (oracle_registry.check_completeness,
//     codex finding #1's Python-side half) before this ever returns.
//  2. Calls goRowBuilder(input) to get the Go-side row for the SAME case AS
//     A CONCRETE, PRODUCTION-TYPED VALUE T (a real struct, e.g.
//     pullRequestRow -- never a hand-picked map). typedEncode then reflects
//     EVERY field T declares, exhaustively: unlike a Python dict (which can
//     omit a key with no compiler complaint), a Go struct return type makes
//     "silently expose only 3 of 20 fields" a type error, not a runtime
//     choice -- this is codex finding #1's Go-side half, enforced by the
//     type system rather than a second, parallel, hand-maintained manifest.
//  3. Fails on ANY undeclared divergence: a key present on one side and
//     absent on the other, a key with a different VALUE, or a key with a
//     different TYPE TAG (codex finding #2: every leaf on both sides is
//     encoded as {"t": "<type>", "v": "<string>"} -- see typedEncode --
//     specifically so an int and an integral float, or two integers that
//     would collide at float64 precision, or a datetime and a same-looking
//     plain string, never compare equal by accident) -- UNLESS the field is
//     declared in the Python pair's excluded_fields OR the caller's own
//     goOnlyFields. Every exclusion requires a written reason, mirroring
//     expected_survivor_reason in scripts/mutation_harness.py.
//
// This is the single comparator every pair reuses; a pair difference is a
// difference in WHAT gets compared (the pair id, the cases, the Go row
// builder, the Go row TYPE), never in HOW comparison happens.
//
// Use this form (which fails t directly) when asserting the CURRENT,
// production code matches Python. To instead prove the comparator would
// CATCH a specific pre-fix/buggy variant (without corrupting the enclosing
// test's pass/fail state -- a subtest's t.Errorf always propagates failure
// to every ancestor test), use oracleDivergences directly.
func compareRowsAgainstPythonOracle[T any](
	t *testing.T,
	pairID string,
	cases []oracleCase,
	goRowBuilder func(t *testing.T, input map[string]any) T,
	goOnlyFields map[string]string,
) {
	t.Helper()
	wrapped := func(t *testing.T, input map[string]any) any { return goRowBuilder(t, input) }
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.ID, func(t *testing.T) {
			t.Helper()
			for _, message := range oracleDivergencesForCase(t, pairID, testCase, wrapped, goOnlyFields) {
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
	goRowBuilder func(t *testing.T, input map[string]any) any,
	goOnlyFields map[string]string,
) []string {
	t.Helper()
	return oracleDivergences(t, pairID, []oracleCase{testCase}, goRowBuilder, goOnlyFields)
}

// oracleDivergences is the reusable core: it does the Python shellout, the
// Go row build (as a concrete typed value, then walked exhaustively by
// typedEncode), and the field-by-field diff, and returns every divergence
// message found -- WITHOUT calling t.Error/t.Fatal for the divergences
// themselves (setup failures still fail t immediately, since those
// indicate the comparison could not run at all, not that it ran and found
// nothing).
//
// codex findings #6/#7 (second review): an empty cases slice, or two cases
// sharing an id, would otherwise let a comparison "pass" without actually
// comparing anything meaningful, or let one case's Python result be
// diffed against a DIFFERENT case's Go result. Both are rejected here as
// hard setup failures, in addition to python_generic_row_oracle.py's own
// mirror checks on the Python side -- defense in depth, not redundant,
// since a caller could construct []oracleCase directly without ever
// reaching the Python CLI's own validation for cases that fail before exec.
func oracleDivergences(
	t *testing.T,
	pairID string,
	cases []oracleCase,
	goRowBuilder func(t *testing.T, input map[string]any) any,
	goOnlyFields map[string]string,
) []string {
	t.Helper()
	assertOracleSourcesUnchangedSinceBuild(t)
	if len(cases) == 0 {
		t.Fatalf("oracleDivergences: cases must be non-empty -- a comparison over " +
			"zero cases proves nothing and must not be allowed to look like a pass")
	}
	seenIDs := make(map[string]struct{}, len(cases))
	for _, testCase := range cases {
		if testCase.ID == "" {
			t.Fatalf("oracleDivergences: case has an empty id")
		}
		if _, duplicate := seenIDs[testCase.ID]; duplicate {
			t.Fatalf("oracleDivergences: duplicate case id %q -- results are keyed by "+
				"id, so a duplicate would let one case's Python result be compared "+
				"against a different case's Go result unnoticed", testCase.ID)
		}
		seenIDs[testCase.ID] = struct{}{}
	}
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

	// UseNumber, defensively: every leaf this oracle emits is type-tagged
	// (codex finding #2) so no bare JSON number should reach this decode at
	// all, but decoding any that slipped through (a malformed pair, a bug
	// in _encode) as json.Number rather than the default lossy float64 is a
	// second, independent layer against exactly the precision-collapse
	// finding #2 named.
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.UseNumber()
	var decoded struct {
		Cases []struct {
			ID  string         `json:"id"`
			Row map[string]any `json:"row"`
		} `json:"cases"`
		ExcludedFields map[string]string `json:"excluded_fields"`
	}
	if err := decoder.Decode(&decoded); err != nil {
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
		if _, duplicate := pythonRows[entry.ID]; duplicate {
			t.Fatalf("pair %s: Python oracle output has duplicate case id %q", pairID, entry.ID)
		}
		pythonRows[entry.ID] = entry.Row
	}

	var messages []string
	for _, testCase := range cases {
		pythonRow, ok := pythonRows[testCase.ID]
		if !ok {
			t.Fatalf("oracle output missing case %q", testCase.ID)
		}
		goValue := goRowBuilder(t, testCase.Input)
		goRow, ok := typedEncode(t, reflect.ValueOf(goValue)).(map[string]any)
		if !ok {
			t.Fatalf("case %q: Go row builder must return a struct or map, got %T", testCase.ID, goValue)
		}
		messages = append(messages, diffRows(
			testCase.ID, pythonRow, goRow, decoded.ExcludedFields, goOnlyFields,
		)...)
	}
	return messages
}

// typedEncode walks v exhaustively via reflection and produces the SAME
// type-tagged wire shape python_generic_row_oracle.py's _encode produces
// (codex finding #2): every leaf becomes {"t": "<tag>", "v": "<string>"},
// nil pointers/interfaces become bare JSON null (unambiguous, no tag
// needed), and structs/maps/slices recurse. Struct fields are named by
// their `json:"..."` tag (falling back to the Go field name, matching
// encoding/json's own default) so the wire field names match what the row
// actually persists as.
//
// Because this walks reflect.Type's fields EXHAUSTIVELY -- there is no
// mechanism to skip one -- a caller cannot construct a "narrowed" struct
// type that quietly hides a field the way a hand-built map[string]any
// could: the completeness guarantee this closes (codex finding #1) comes
// from the Go type system itself, not from a second, parallel, hand
// maintained field list that could drift from the struct it describes.
func typedEncode(t *testing.T, v reflect.Value) any {
	t.Helper()
	for v.IsValid() && (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return nil
	}
	if timeValue, ok := v.Interface().(time.Time); ok {
		return map[string]any{"t": "datetime", "v": timeValue.UTC().Format(time.RFC3339Nano)}
	}
	switch v.Kind() {
	case reflect.String:
		return map[string]any{"t": "str", "v": v.String()}
	case reflect.Bool:
		return map[string]any{"t": "bool", "v": strconv.FormatBool(v.Bool())}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return map[string]any{"t": "int", "v": strconv.FormatInt(v.Int(), 10)}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"t": "int", "v": strconv.FormatUint(v.Uint(), 10)}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"t": "float", "v": strconv.FormatFloat(v.Float(), 'g', -1, 64)}
	case reflect.Struct:
		result := make(map[string]any, v.NumField())
		structType := v.Type()
		for i := 0; i < v.NumField(); i++ {
			field := structType.Field(i)
			if field.PkgPath != "" {
				continue // unexported: not part of the persisted row.
			}
			name := jsonFieldName(field)
			if name == "-" {
				continue
			}
			result[name] = typedEncode(t, v.Field(i))
		}
		return result
	case reflect.Map:
		result := make(map[string]any, v.Len())
		for _, key := range v.MapKeys() {
			keyString, ok := key.Interface().(string)
			if !ok {
				t.Fatalf("typedEncode: unsupported non-string map key type %s", key.Type())
			}
			result[keyString] = typedEncode(t, v.MapIndex(key))
		}
		return result
	case reflect.Slice, reflect.Array:
		result := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			result[i] = typedEncode(t, v.Index(i))
		}
		return result
	default:
		t.Fatalf("typedEncode: unsupported kind %s for type %s -- every leaf type "+
			"this comparator can see must have an explicit, type-tagged encoding; "+
			"falling through untagged would silently reopen codex finding #2", v.Kind(), v.Type())
		return nil
	}
}

func jsonFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag == "" {
		return field.Name
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" {
		return field.Name
	}
	return name
}

// diffRows is CHAOS-3162's actual comparison logic: every key present on
// EITHER side must either match on both, or be declared excluded
// (Python-declared via excluded_fields, or Go-only via goOnlyFields) with a
// written reason. A key present on one side and absent on the other is
// exactly as much a divergence as a key with different values on each side
// -- neither is treated as "probably fine". Returns one message per
// divergence found (nil/empty = the rows matched under the declared
// exclusions).
//
// codex finding #6 (second review): if every key in the union is excluded,
// this returns zero divergences even though nothing was actually compared
// -- indistinguishable, from the caller's side, from a real match. Fail
// loudly instead: a case where every field is excluded is a caller
// configuration error (probably an over-broad goOnlyFields), not a pass.
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
	if len(allKeys) == 0 {
		return []string{fmt.Sprintf(
			"case %q: both rows are empty -- nothing was compared, which is a "+
				"setup error, not a pass", caseID)}
	}
	sortedKeys := make([]string, 0, len(allKeys))
	for key := range allKeys {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	var messages []string
	comparedFields := 0
	for _, key := range sortedKeys {
		if _, excluded := pythonExcluded[key]; excluded {
			continue
		}
		if _, excluded := goOnlyFields[key]; excluded {
			continue
		}
		comparedFields++
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
	if comparedFields == 0 {
		return []string{fmt.Sprintf(
			"case %q: every field in both rows is declared excluded -- nothing was "+
				"actually compared, which is a caller configuration error (an "+
				"over-broad exclusion list), not a pass", caseID)}
	}
	return messages
}

// embeddedOracleSources exists purely so `go:embed` makes the compiled test
// binary's content, and therefore Go's test result cache key, sensitive to
// every byte of these Python files (codex finding #4, second review).
// `go test` result caching keys off the compiled test binary; that binary
// has NO knowledge that oracleDivergences shells out to
// python_generic_row_oracle.py at run time, so a Python-only edit with no
// Go-file change reuses a stale cached PASS unless something makes the
// binary itself change too. Listing these files here does exactly that:
// any byte-level edit to any of them changes what go:embed bakes into the
// binary, which changes the binary's content hash, which busts the cache
// on its own -- no reliance on a developer remembering `-count=1`.
// assertOracleSourcesUnchangedSinceBuild is the second half: it re-reads
// the same files from disk at run time and fails loudly if they no longer
// match what was embedded, which would only happen if this embed
// directive's file list drifted out of sync with what oracleDivergences
// actually executes.
//
//go:embed testdata/python_generic_row_oracle.py testdata/oracle_registry.py testdata/python_oracle_loader.py testdata/field_reflection.py testdata/oracle_pairs/*.py
var embeddedOracleSources embed.FS

func assertOracleSourcesUnchangedSinceBuild(t *testing.T) {
	t.Helper()
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	err := fs.WalkDir(embeddedOracleSources, ".", func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		embedded, readErr := embeddedOracleSources.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		onDisk, readErr := os.ReadFile(filepath.Join(packageDir, path))
		if readErr != nil {
			return readErr
		}
		if !bytes.Equal(embedded, onDisk) {
			t.Fatalf("embedded copy of %s does not match the on-disk file -- this "+
				"should be impossible outside of a build-cache bug, since go:embed "+
				"reads the same file go test just compiled from; if you see this, "+
				"rebuild with -count=1 and report it", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk embedded oracle sources: %v", err)
	}
}

// TestDiffRowsClauseCoverage is a fast, synthetic-data unit test of
// diffRows's own logic -- the core comparator every pair (Python-shellout
// or ClickHouse-readback alike) reuses -- isolating each of its clauses
// (matching value, differing value, present-in-Python-only,
// present-in-Go-only, the two vacuity guards added for codex finding #6)
// plus its two independent exclusion mechanisms (Python-declared,
// Go-only-declared) as its own case, so a mutation to any one clause is
// caught by a case that exercises ONLY that clause, not by coincidence
// from a case exercising several at once. This is the mutation harness's
// own "mutate compound predicates clause by clause" rule applied to the
// framework code itself, not just to a pair's row-construction logic.
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
			pythonRow:      map[string]any{"reviews_count": float64(1), "state": "open"},
			goRow:          map[string]any{"reviews_count": float64(0), "state": "open"},
			pythonExcluded: map[string]string{"reviews_count": "owned by github/pr-reviews"},
			wantMessages:   0,
		},
		{
			name:           "declared Python-side exclusion suppresses a present-or-absent divergence",
			pythonRow:      map[string]any{"reviews_count": float64(1), "state": "open"},
			goRow:          map[string]any{"state": "open"},
			pythonExcluded: map[string]string{"reviews_count": "owned by github/pr-reviews"},
			wantMessages:   0,
		},
		{
			name:         "declared Go-only exclusion suppresses a present-or-absent divergence",
			pythonRow:    map[string]any{"state": "open"},
			goRow:        map[string]any{"org_id": "org-acme", "state": "open"},
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
		{
			name:          "both rows empty: fails as a setup error, not a silent pass",
			pythonRow:     map[string]any{},
			goRow:         map[string]any{},
			wantMessages:  1,
			wantSubstring: "both rows are empty",
		},
		{
			name:           "every field excluded: fails as a setup error, not a silent pass",
			pythonRow:      map[string]any{"reviews_count": float64(1)},
			goRow:          map[string]any{"reviews_count": float64(1)},
			pythonExcluded: map[string]string{"reviews_count": "owned by github/pr-reviews"},
			wantMessages:   1,
			wantSubstring:  "every field in both rows is declared excluded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			messages := diffRows("case", tt.pythonRow, tt.goRow, tt.pythonExcluded, tt.goOnlyFields)
			if len(messages) != tt.wantMessages {
				t.Fatalf("diffRows() = %d message(s) %v, want %d", len(messages), messages, tt.wantMessages)
			}
			if tt.wantSubstring != "" {
				if len(messages) == 0 || !strings.Contains(messages[0], tt.wantSubstring) {
					t.Fatalf("diffRows() = %v, want a message containing %q", messages, tt.wantSubstring)
				}
			}
		})
	}
}
