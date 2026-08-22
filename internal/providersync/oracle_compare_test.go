package providersync

import (
	"bytes"
	"embed"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/oraclecompare"
)

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
//     goOnlyFields. Every exclusion requires a written reason: an omission
//     must be declared in writing at registration time, never discovered by a
//     reader wondering why a field went untested.
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
	// One shellout for the WHOLE batch, not one per case: codex findings
	// (third review) about a stale/unused declared exclusion, and a
	// goOnlyFields entry that turns out to appear on the Python side after
	// all, are properties of the BATCH ("did this exclusion ever match
	// anything across every case"), not of any single case in isolation --
	// they cannot be checked correctly if each case only ever sees itself.
	// oracleDivergences returns messages case-prefixed by
	// caseDivergencePrefix; anything that doesn't match any case's prefix
	// is a batch-level finding, surfaced in its own subtest below.
	all := oracleDivergences(t, pairID, cases, wrapped, goOnlyFields)
	attributed := make(map[string]bool, len(all))
	for _, testCase := range cases {
		testCase := testCase
		prefix := caseDivergencePrefix(testCase.ID)
		t.Run(testCase.ID, func(t *testing.T) {
			t.Helper()
			for _, message := range all {
				if strings.HasPrefix(message, prefix) {
					attributed[message] = true
					t.Error(message)
				}
			}
		})
	}
	var batchLevel []string
	for _, message := range all {
		if !attributed[message] {
			batchLevel = append(batchLevel, message)
		}
	}
	if len(batchLevel) > 0 {
		t.Run("exclusion integrity", func(t *testing.T) {
			t.Helper()
			for _, message := range batchLevel {
				t.Error(message)
			}
		})
	}
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
	recordGenericOracleProof(t, packageDir, pairID)

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

	pythonRowsByCase := make(map[string]map[string]any, len(cases))
	goRowsByCase := make(map[string]map[string]any, len(cases))
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
		pythonRowsByCase[testCase.ID] = pythonRow
		goRowsByCase[testCase.ID] = goRow
		messages = append(messages, diffRows(
			testCase.ID, pythonRow, goRow, decoded.ExcludedFields, goOnlyFields,
		)...)
	}
	messages = append(messages, checkExclusionIntegrity(
		pythonRowsByCase, goRowsByCase, decoded.ExcludedFields, goOnlyFields,
	)...)
	return messages
}

// recordGenericOracleProof records one successfully executed checked-in pair.
// ci/check_go.sh derives the complete expected inventory from oracle_pairs/*.py
// and requires every corresponding marker after the package passes. Keeping
// this after CombinedOutput succeeds means selecting the interpreter, running
// an unrelated Python test, or failing to import a pair cannot satisfy the
// dedicated live-oracle gate.
func recordGenericOracleProof(t *testing.T, packageDir, pairID string) {
	t.Helper()
	pairFilename := strings.ReplaceAll(pairID, "/", "_") + ".py"
	if filepath.Base(pairFilename) != pairFilename {
		t.Fatalf("pair %q does not map to a safe oracle proof filename", pairID)
	}
	pairSource := filepath.Join(packageDir, "testdata", "oracle_pairs", pairFilename)
	if info, err := os.Stat(pairSource); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("pair %q does not map to a checked-in oracle source %s", pairID, pairSource)
	}
	proof := filepath.Join(os.Getenv(livePythonOracleProofDir), pairFilename)
	if err := os.WriteFile(proof, []byte("executed\n"), 0o600); err != nil {
		t.Fatalf("write live Python oracle proof for %s: %v", pairID, err)
	}
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
//go:embed testdata/python_generic_row_oracle.py testdata/oracle_registry.py testdata/python_oracle_loader.py testdata/field_reflection.py testdata/python_investment_call_site.py testdata/oracle_pairs/*.py
var embeddedOracleSources embed.FS

// The comparison vocabulary below moved to internal/testsupport/oraclecompare
// under CHAOS-3092 P0 so a second comparator cannot grow up beside it with its
// own subtly different rules. These thin aliases keep this package's existing
// call sites -- 77 oracle test files plus the readback integration pairs --
// calling the names they always did, so the extraction is a pure refactor at
// every use site rather than a 77-file rename.

type oracleCase = oraclecompare.Case

func caseDivergencePrefix(caseID string) string {
	return oraclecompare.CaseDivergencePrefix(caseID)
}

func typedEncode(t *testing.T, v reflect.Value) any {
	t.Helper()
	return oraclecompare.TypedEncode(t, v)
}

func typedValuesEqual(pythonValue, goValue any) bool {
	return oraclecompare.TypedValuesEqual(pythonValue, goValue)
}

func diffRows(
	caseID string,
	pythonRow, goRow map[string]any,
	pythonExcluded, goOnlyFields map[string]string,
) []string {
	return oraclecompare.DiffRows(caseID, pythonRow, goRow, pythonExcluded, goOnlyFields)
}

func checkExclusionIntegrity(
	pythonRowsByCase, goRowsByCase map[string]map[string]any,
	excludedFields, goOnlyFields map[string]string,
) []string {
	return oraclecompare.CheckExclusionIntegrity(
		pythonRowsByCase, goRowsByCase, excludedFields, goOnlyFields,
	)
}

// assertOracleSourcesUnchangedSinceBuild binds this package's own embedded
// pair sources to the shared check. The embed directive and the files it names
// stay here because go:embed paths are package-relative and the pairs are this
// package's testdata; only the verification logic is shared.
func assertOracleSourcesUnchangedSinceBuild(t *testing.T) {
	t.Helper()
	_, currentFile, _, _ := runtime.Caller(0)
	oraclecompare.AssertSourcesUnchangedSinceBuild(
		t, embeddedOracleSources, filepath.Dir(currentFile),
	)
}
