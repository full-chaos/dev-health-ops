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

// dialectSafeEscapes are the backslash escapes whose meaning is IDENTICAL in
// Python `re`, Go RE2 and ECMA-262: escaped punctuation, which is literal in
// all three.
//
// THE LIST IS INVERTED ON PURPOSE, and the previous version's shape is the
// reason. It enumerated the classes known to diverge -- `\d`, `\w`, `\s` and
// their complements -- and treated everything else as safe. Codex round 2
// walked straight through the gap with `\b`, whose word-boundary is Unicode-
// aware in Python and ASCII in the other two (Go's own regexp/syntax doc says
// "at ASCII word boundary"). Measured on all three engines with `^\bé\b$`:
// Python accepts "é", ECMA-262 rejects it, RE2's `\b` is ASCII by
// specification. That was the FOURTH instance of this family on this branch
// and the second the guard itself failed to catch.
//
// Enumerating today's known-bad shapes and defaulting the rest to safe is the
// exact mistake the brief records as "default unrecognised shapes to
// DANGEROUS, not safe" -- and this lane applied that rule to its own
// fail-closed pattern rewriting while leaving this guard fail-open. So: any
// backslash escape NOT in this list is reported, and adding one is a
// deliberate act that requires knowing all three dialects agree on it.
var dialectSafeEscapes = map[byte]bool{
	'.': true, '\\': true, '/': true, '+': true, '*': true, '?': true,
	'(': true, ')': true, '[': true, ']': true, '{': true, '}': true,
	'^': true, '$': true, '|': true, '-': true,
}

// unsafeEscapesIn returns every backslash escape in a pattern that is not on
// dialectSafeEscapes, walking the string so an escaped backslash cannot be
// misread as introducing an escape.
// unsafeGroupsIn reports every `(?…` group opener that is not the plain
// non-capturing `(?:`.
//
// The escape safe-list closed one family and left another open: `(?i)` and
// `(?i:…)` are INLINE FLAG groups, which are neither a backslash escape nor
// something RE2 refuses to compile, so the guard accepted them. Measured on
// two engines: Go RE2 compiles `(?i)^a$` and Python matches "A" with it, while
// ECMA-262 rejects the pattern outright —
// `SyntaxError: Invalid regular expression: /(?i)^a$/: Invalid group`. A
// schema carrying one would validate in two runtimes and fail to compile at
// all in the third (codex round 3).
//
// `(?:` is the only opener all three agree on and is therefore the whole
// allow-list. Lookarounds (`(?=`, `(?!`, `(?<=`, `(?<!`) are also reported
// here, and separately fail RE2 compilation — two independent detections of
// the same construct is deliberate, since the compile check is the one that
// would silently stop applying if the pattern were ever validated by
// something other than RE2.
func unsafeGroupsIn(pattern string) []string {
	var found []string
	seen := map[string]bool{}
	for i := 0; i+1 < len(pattern); i++ {
		if pattern[i] != '(' || pattern[i+1] != '?' {
			continue
		}
		if i > 0 && pattern[i-1] == '\\' {
			continue // an escaped literal parenthesis, not a group opener
		}
		end := i + 2
		for end < len(pattern) && pattern[end] != ':' && pattern[end] != ')' {
			end++
		}
		opener := pattern[i:min(end+1, len(pattern))]
		if opener == "(?:" || seen[opener] {
			continue
		}
		seen[opener] = true
		found = append(found, opener)
	}
	sort.Strings(found)
	return found
}

func unsafeEscapesIn(pattern string) []string {
	var found []string
	seen := map[byte]bool{}
	for i := 0; i < len(pattern); i++ {
		if pattern[i] != '\\' || i+1 >= len(pattern) {
			continue
		}
		next := pattern[i+1]
		i++ // consume the escaped byte, so `\\\\d` is a literal backslash then d
		if dialectSafeEscapes[next] || seen[next] {
			continue
		}
		seen[next] = true
		found = append(found, `\\`+string(next))
	}
	sort.Strings(found)
	return found
}

type schemaFinding struct {
	file    string
	pointer string
	problem string
}

func (f schemaFinding) String() string {
	return fmt.Sprintf("%s %s: %s", f.file, f.pointer, f.problem)
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
			for _, escape := range unsafeEscapesIn(raw) {
				*findings = append(*findings, schemaFinding{
					file:    file,
					pointer: pointer,
					problem: fmt.Sprintf(
						"pattern %q uses the escape %s, which is not on the list of escapes "+
							"all three validators agree on. Shorthand classes and boundaries "+
							"(\\d, \\w, \\s, \\b and complements) are Unicode-aware in Python's "+
							"`re` and ASCII in Go RE2 and ECMA-262. Use an explicit character "+
							"class such as [0-9], or add the escape to dialectSafeEscapes once "+
							"you have checked all three dialects agree.",
						raw, escape),
				})
			}
			for _, group := range unsafeGroupsIn(raw) {
				*findings = append(*findings, schemaFinding{
					file:    file,
					pointer: pointer,
					problem: fmt.Sprintf(
						"pattern %q opens a group with %s. The only group opener all three "+
							"validators accept is `(?:`. Inline flag groups compile in Go RE2 "+
							"and Python and are a SyntaxError in ECMA-262, so the schema would "+
							"fail to compile in one validator while passing in two; lookarounds "+
							"are unsupported by RE2 entirely.",
						raw, group),
				})
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
			want:   "not on the list of escapes",
		},
		{
			name:   "nested deep inside $defs, not at the top level",
			schema: `{"$defs":{"a":{"oneOf":[{"type":"null"},{"properties":{"b":{"pattern":"^\\w+$"}}}]}}}`,
			want:   "not on the list of escapes",
		},
		{
			// The escape the ENUMERATED version of this guard walked straight
			// past (codex round 2 P2). Python's `\b` is Unicode-aware, Go's is
			// "at ASCII word boundary" by its own regexp/syntax doc, and
			// ECMA-262 agrees with Go -- measured on all three with `^\bé\b$`:
			// Python accepts "é", node rejects it. It is here as a named case
			// rather than trusted to the safe-list, because it is the specific
			// input that proved the old shape wrong.
			name:   "unicode-sensitive word boundary",
			schema: `{"properties":{"x":{"pattern":"^\\bé\\b$"}}}`,
			want:   "not on the list of escapes",
		},
		{
			// Proves the inversion is what makes this guard general: \p{L} is
			// a perfectly ordinary escape that the enumerated version had no
			// row for and would have accepted. RE2 and Python support it,
			// ECMA-262 requires the /u flag, and ajv does not set one.
			name:   "an escape the enumerated guard had no row for",
			schema: `{"properties":{"x":{"pattern":"^\\p{L}+$"}}}`,
			want:   "not on the list of escapes",
		},
		{
			// The escape safe-list had no answer for this: not an escape, and
			// RE2 compiles it happily. Two engines accept, one rejects.
			name:   "inline flag group",
			schema: `{"properties":{"x":{"pattern":"(?i)^a$"}}}`,
			want:   "The only group opener all three validators accept",
		},
		{
			name:   "inline flag group with a body",
			schema: `{"properties":{"x":{"pattern":"^(?i:abc)$"}}}`,
			want:   "The only group opener all three validators accept",
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
			// Log EVERY finding, not only the one asserted on. A control that
			// reports selectively is a control you cannot read: this test
			// passes when the wanted finding is present, so if the guard ALSO
			// fires for an unintended reason the extra finding is invisible
			// and the control silently stops describing what it proves.
			// (Returned by lane-auth-wave1, who hit the same shape on #2143:
			// their six-way control logged only the regressions matching the
			// expectation and so hid whether two of the six were caught at
			// all.)
			for _, finding := range joined {
				t.Log("fired:", finding)
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
		`"literal_backslash":{"type":"string","pattern":"^a\\\\d$"},` +
		// (?: is the one group opener all three accept; the guard must not
		// flag it, or every grouped pattern in every future surface breaks.
		`"non_capturing_group":{"type":"string","pattern":"^(?:ab)+$"}}}`
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
