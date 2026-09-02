package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// These tests police the DIALECT the v1 wire schemas may be written in, so
// that the Go, Python and TypeScript validators enforce the same thing.
//
// Both rules below come from a MEASURED disagreement, not from caution:
//
//   - `format` is asserted by the Python (jsonschema + rfc3339-validator) and
//     TypeScript (ajv + ajv-formats) validators, and IGNORED outright by
//     github.com/google/jsonschema-go -- its own doc.go says so under
//     "Deviations from the specification": recorded, ignored during
//     validation, "does not even produce annotations", use `pattern` instead.
//     A constraint carried only by `format` is therefore enforced in two
//     languages out of three.
//
//   - `\d` is UNICODE in Python's `re` and ASCII in Go RE2 and ECMA-262.
//     Measured on this schema's own timestamp pattern: the Devanagari
//     timestamp in examples/principal/invalid-expires-at-unicode-digits.json
//     was ACCEPTED by the Python validator and REJECTED by the Go one while
//     the pattern spelled its digit class `\d`. Same hazard for `\w` and `\s`.
//
// Each guard carries a POSITIVE CONTROL asserting it can actually detect the
// violation it exists to catch. A guard that has quietly stopped inspecting
// anything passes every clean tree, which is indistinguishable from a guard
// that works.

// unicodeAmbiguousClasses are shorthand classes whose meaning differs between
// Python's `re` (Unicode) and Go RE2 / ECMA-262 (ASCII). Written as the
// two-character escape; the check below ignores an escaped backslash so that
// a literal `\\d` in a pattern is not a false positive.
var unicodeAmbiguousClasses = []string{`\d`, `\w`, `\s`, `\D`, `\W`, `\S`}

type schemaFinding struct {
	file    string
	pointer string
	problem string
}

func (f schemaFinding) String() string {
	return fmt.Sprintf("%s %s: %s", f.file, f.pointer, f.problem)
}

// stripEscapedBackslashes removes `\\` pairs so that a following `d` is not
// misread as the `\d` class. Without this, the pattern `\\د` (a literal
// backslash then a letter) would be flagged wrongly.
func stripEscapedBackslashes(pattern string) string {
	return strings.ReplaceAll(pattern, `\\`, "")
}

// inspectSchemaNode walks a decoded schema document and reports dialect
// violations. It recurses through every object and array, so a rule nested
// inside $defs, oneOf, items or properties is inspected exactly like one at
// the top level -- a walker that only looked at the top level is the
// top-level-only defect this contract family already paid for once.
func inspectSchemaNode(file, pointer string, node any, findings *[]schemaFinding) {
	switch typed := node.(type) {
	case map[string]any:
		_, hasFormat := typed["format"]
		_, hasPattern := typed["pattern"]
		if hasFormat && !hasPattern {
			format, _ := typed["format"].(string)
			*findings = append(*findings, schemaFinding{
				file:    file,
				pointer: pointer,
				problem: fmt.Sprintf(
					"has \"format\": %q with no sibling \"pattern\". google/jsonschema-go "+
						"ignores \"format\" during validation, so this constraint would be "+
						"enforced by Python and TypeScript and NOT by Go. Add a \"pattern\" "+
						"expressing the cross-language floor and keep \"format\" as the "+
						"stricter check the other two apply on top.", format),
			})
		}
		if raw, ok := typed["pattern"].(string); ok {
			scanned := stripEscapedBackslashes(raw)
			for _, class := range unicodeAmbiguousClasses {
				if strings.Contains(scanned, class) {
					*findings = append(*findings, schemaFinding{
						file:    file,
						pointer: pointer,
						problem: fmt.Sprintf(
							"pattern %q uses %s, whose meaning differs across the three "+
								"validators: Python's `re` treats it as Unicode, Go RE2 and "+
								"ECMA-262 as ASCII. Use an explicit class such as [0-9].",
							raw, class),
					})
				}
			}
			if _, err := regexp.Compile(raw); err != nil {
				*findings = append(*findings, schemaFinding{
					file:    file,
					pointer: pointer,
					problem: fmt.Sprintf(
						"pattern %q does not compile under Go RE2 (%v). RE2 has no "+
							"lookaround and no back-references; a pattern needing either "+
							"cannot mean the same thing in all three validators.", raw, err),
				})
			}
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			inspectSchemaNode(file, pointer+"/"+key, typed[key], findings)
		}
	case []any:
		for i, item := range typed {
			inspectSchemaNode(file, fmt.Sprintf("%s/%d", pointer, i), item, findings)
		}
	}
}

func wireSchemaFiles(t *testing.T) []string {
	t.Helper()
	dir := filepath.Join(ContractsDir(testRoot(t)), "jsonschema")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".schema.json") {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		// Without this the whole guard is vacuous and still exits 0.
		t.Fatalf("no *.schema.json found under %s -- the dialect guards would "+
			"inspect nothing and pass", dir)
	}
	return files
}

func TestEveryWireSchemaStaysInTheCrossLanguageDialect(t *testing.T) {
	var findings []schemaFinding
	for _, path := range wireSchemaFiles(t) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		var document any
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		inspectSchemaNode(filepath.Base(path), "", document, &findings)
	}
	for _, finding := range findings {
		t.Error(finding)
	}
}

func TestTheDialectGuardDetectsTheViolationsItExistsToCatch(t *testing.T) {
	// Positive controls. Each constructs the violation and asserts the walker
	// reports it -- proving the clean result above is a real pass and not a
	// walker that has stopped looking.
	cases := []struct {
		name   string
		schema string
		want   string
	}{
		{
			name:   "format with no sibling pattern",
			schema: `{"properties":{"when":{"type":"string","format":"date-time"}}}`,
			want:   "no sibling \"pattern\"",
		},
		{
			name:   "unicode-ambiguous digit class",
			schema: `{"properties":{"when":{"type":"string","pattern":"^\\d{4}$"}}}`,
			want:   "differs across the three validators",
		},
		{
			name:   "nested deep inside $defs, not at the top level",
			schema: `{"$defs":{"a":{"oneOf":[{"type":"null"},{"properties":{"b":{"pattern":"^\\w+$"}}}]}}}`,
			want:   "differs across the three validators",
		},
		{
			name:   "pattern RE2 cannot compile",
			schema: `{"properties":{"x":{"pattern":"^(?=foo)bar$"}}}`,
			want:   "does not compile under Go RE2",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var document any
			if err := json.Unmarshal([]byte(testCase.schema), &document); err != nil {
				t.Fatalf("control schema is not valid JSON: %v", err)
			}
			var findings []schemaFinding
			inspectSchemaNode("control.schema.json", "", document, &findings)
			if len(findings) == 0 {
				t.Fatalf("the guard reported nothing for a schema that violates the dialect")
			}
			var joined []string
			for _, finding := range findings {
				joined = append(joined, finding.String())
			}
			if !strings.Contains(strings.Join(joined, "\n"), testCase.want) {
				t.Fatalf("guard fired but not for the expected reason; want substring %q, got:\n%s",
					testCase.want, strings.Join(joined, "\n"))
			}
		})
	}
}

func TestTheDialectGuardDoesNotFireOnACleanSchema(t *testing.T) {
	// Negative control for the guard itself: a schema that obeys the dialect
	// must produce NO findings. Without this, a walker that flagged
	// everything would pass every positive control above.
	clean := `{"properties":{"when":{"type":"string","format":"date-time",` +
		`"pattern":"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},` +
		`"name":{"type":"string","pattern":"^[a-z][a-z0-9_]*$"},` +
		`"literal_backslash":{"type":"string","pattern":"^a\\\\d$"}}}`
	var document any
	if err := json.Unmarshal([]byte(clean), &document); err != nil {
		t.Fatalf("control schema is not valid JSON: %v", err)
	}
	var findings []schemaFinding
	inspectSchemaNode("clean.schema.json", "", document, &findings)
	for _, finding := range findings {
		t.Errorf("guard fired on a clean schema: %s", finding)
	}
}
