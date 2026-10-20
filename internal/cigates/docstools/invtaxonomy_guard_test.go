package docstools_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// invTxSeed builds a work directory holding a page whose generated block was just written by gen-taxonomy.
func invTxSeed(t *testing.T) {
	t.Helper()
	seedFromRoot(t, invTxDoc)
	if code, _, stderr := runDocs("gen-taxonomy"); code != 0 {
		t.Fatalf("seed gen-taxonomy rc %d: %s", code, stderr)
	}
}

func invTxEdit(t *testing.T, from, to string) {
	t.Helper()
	doc := mustRead(t, invTxDoc)
	if !strings.Contains(doc, from) {
		t.Fatalf("seed doc lacks %q", from)
	}
	writeTree(t, invTxDoc, strings.Replace(doc, from, to, 1), 0o644)
}

func TestGenTaxonomyWritesLiveSortedTaxonomyAndKeepsSurroundingText(t *testing.T) {
	invTxSeed(t)
	doc := mustRead(t, invTxDoc)
	block := realBlock(t, doc, invTxBegin, invTxEnd)
	want := []string{invTxBegin, "```text", "# THEMES"}
	want = append(want, units.SortedThemes[:]...)
	want = append(want, "", "# SUBCATEGORIES (theme.subcategory)")
	want = append(want, units.SortedSubcategories[:]...)
	want = append(want, "", "# SUBCATEGORY_TO_THEME")
	for _, s := range units.SortedSubcategories {
		want = append(want, s+" -> "+units.ThemeOf(s))
	}
	want = append(want, "```", invTxEnd)
	if block != strings.Join(want, "\n") {
		t.Fatalf("block differs from the live taxonomy:\n%s", block)
	}
	if !strings.HasPrefix(doc, "---\npage_id: ref-investment-taxonomy") || !strings.Contains(doc, "# Investment taxonomy") {
		t.Fatalf("text before the block changed")
	}
	if code, _, _ := runDocs("gen-taxonomy"); code != 0 || mustRead(t, invTxDoc) != doc {
		t.Fatalf("second run changed the page")
	}
}

func TestGenTaxonomyRefusals(t *testing.T) {
	invTxSeed(t)
	invTxEdit(t, invTxBegin, "<!-- BEGIN X -->")
	before := mustRead(t, invTxDoc)
	code, _, stderr := runDocs("gen-taxonomy")
	if code != 1 || !strings.Contains(stderr, "Generated taxonomy markers not found in ") {
		t.Fatalf("no markers: rc %d stderr %q", code, stderr)
	}
	if mustRead(t, invTxDoc) != before {
		t.Fatalf("refused run wrote the page")
	}
	invTxSeed(t)
	writeTree(t, invTxDoc, invTxEnd+"\n"+invTxBegin+"\n", 0o644)
	if code, _, stderr := runDocs("gen-taxonomy"); code != 1 || !strings.Contains(stderr, "markers not found") {
		t.Fatalf("end before begin: rc %d stderr %q", code, stderr)
	}
	t.Chdir(t.TempDir())
	if code, _, stderr := runDocs("gen-taxonomy"); code != 1 || !strings.Contains(stderr, "FileNotFoundError") {
		t.Fatalf("missing doc: rc %d stderr %q", code, stderr)
	}
}

func TestCheckInvestmentDriftDetectsPlantedDefects(t *testing.T) {
	cases := []struct{ name, from, to, want string }{
		{"theme dropped from block", "# THEMES\nfeature_delivery\n", "# THEMES\n", "ERROR: generated theme keys drift: expected ['feature_delivery', 'maintenance', "},
		{"subcategory dropped from block", "risk.vulnerability\n\n# SUBCATEGORY_TO_THEME", "\n# SUBCATEGORY_TO_THEME", "ERROR: generated subcategory keys drift"},
		{"mapping changed", "risk.security -> risk\n", "risk.security -> quality\n", "ERROR: generated subcategory mapping drift: expected {'feature_delivery.customer': 'feature_delivery', "},
		{"unknown subcategory documented", "Do not edit it by hand.", "Do not edit it by hand. See `operational.external`.", "ERROR: documented unknown subcategory keys: ['operational.external']"},
		{"theme undocumented", "| `risk` | Risk / Security |\n", "", "ERROR: documented theme keys drift"},
		{"subcategory undocumented", "`risk.security`, ", "", "ERROR: documented subcategory keys missing: ['risk.security']"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			invTxSeed(t)
			if code, out, _ := runDocs("check-investment-drift"); code != 0 {
				t.Fatalf("seed not clean: %q", out)
			}
			invTxEdit(t, c.from, c.to)
			code, out, _ := runDocs("check-investment-drift")
			if code != 1 || !strings.Contains(out, c.want) {
				t.Fatalf("rc %d out %q, want %q", code, out, c.want)
			}
		})
	}
}

func TestCheckInvestmentDriftJSONExamples(t *testing.T) {
	invTxSeed(t)
	writeTree(t, "docs/zz/nested.md", "```json\n{\"themes\":{\"risk\":1,\"bogus\":2},\"investment\":{\"themes\":{\"other\":1},\"subcategories\":{\"a.b\":1}}}\n```\n"+
		"```json\n{broken\n```\n```json\n[1]\n```\n```json\n{\"themes\":[\"x\"]}\n```\n", 0o644)
	code, out, _ := runDocs("check-investment-drift")
	want := "ERROR: zz/nested.md JSON example #1 unknown themes keys: ['bogus']\n" +
		"ERROR: zz/nested.md JSON example #1 unknown themes keys: ['other']\n" +
		"ERROR: zz/nested.md JSON example #1 unknown subcategories keys: ['a.b']\n"
	if code != 1 || out != want {
		t.Fatalf("rc %d out %q, want %q", code, out, want)
	}
}

func TestCheckInvestmentDriftRefusals(t *testing.T) {
	invTxSeed(t)
	invTxEdit(t, "risk.security -> risk\n", "risk.security risk\n")
	code, out, stderr := runDocs("check-investment-drift")
	if code != 1 || out != "" || !strings.Contains(stderr, "ValueError: bad mapping line in generated block: risk.security risk") {
		t.Fatalf("bad mapping line: rc %d out %q stderr %q", code, out, stderr)
	}
	invTxSeed(t)
	invTxEdit(t, invTxEnd, "")
	if code, _, stderr := runDocs("check-investment-drift"); code != 1 || !strings.Contains(stderr, "generated taxonomy markers missing in ") {
		t.Fatalf("no end marker: rc %d stderr %q", code, stderr)
	}
}

// Python's json.loads accepts NaN, Infinity and -Infinity; a block it cannot parse is skipped.
func TestCheckInvestmentDriftJSONExamplesAcceptPythonConstants(t *testing.T) {
	invTxSeed(t)
	writeTree(t, "docs/zz/nan.md", "```json\n{\"themes\":{\"bogus\": NaN},\"subcategories\":{\"a.b\": -Infinity,\"c.d\":[Infinity]}}\n```\n"+
		"```json\n{\"themes\":{\"NaN\": 1}}\n```\n"+ // a string is not a constant: the key NaN is unknown
		"```json\n{\"themes\":{NaN: 1}}\n```\n"+ // a constant as a key: Python rejects it, the block is skipped
		"```json\n{\"themes\":{\"x\": NaNNaN}}\n```\n"+
		"```json\n{\"themes\":{\"y\": -NaN}}\n```\n", 0o644)
	code, out, _ := runDocs("check-investment-drift")
	want := "ERROR: zz/nan.md JSON example #1 unknown themes keys: ['bogus']\n" +
		"ERROR: zz/nan.md JSON example #1 unknown subcategories keys: ['a.b', 'c.d']\n" +
		"ERROR: zz/nan.md JSON example #2 unknown themes keys: ['NaN']\n"
	if code != 1 || out != want {
		t.Fatalf("rc %d out %q, want %q", code, out, want)
	}
}

func TestInvestmentVerbsRefuseInvalidUTF8(t *testing.T) {
	invTxSeed(t)
	writeTree(t, "docs/bad.md", "x\xff\n", 0o644)
	code, out, stderr := runDocs("check-investment-drift")
	if code != 1 || out != "" || !strings.HasPrefix(stderr, "UnicodeDecodeError: ") || !strings.HasSuffix(stderr, "docs/bad.md: invalid UTF-8\n") || strings.Count(stderr, "\n") != 1 {
		t.Fatalf("page: rc %d out %q stderr %q", code, out, stderr)
	}
	invTxSeed(t)
	writeTree(t, invTxDoc, mustRead(t, invTxDoc)+"\xff", 0o644)
	before := mustRead(t, invTxDoc)
	for _, verb := range []string{"gen-taxonomy", "check-investment-drift"} {
		code, _, stderr := runDocs(verb)
		if code != 1 || !strings.HasPrefix(stderr, "UnicodeDecodeError: ") || !strings.HasSuffix(stderr, invTxDoc+": invalid UTF-8\n") {
			t.Fatalf("%s: rc %d stderr %q", verb, code, stderr)
		}
	}
	if mustRead(t, invTxDoc) != before {
		t.Fatalf("refused run wrote the page")
	}
}
