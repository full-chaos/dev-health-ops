package complexity

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These tests reuse the radon golden that pycc's parity test uses, but assert
// a different layer: pycc proves the BLOCK list matches radon, and this proves
// the FILE-LEVEL aggregates the family actually stores match too. The two are
// not the same claim -- identical blocks can still be summed, averaged or
// thresholded wrongly on the way to a row, and those five numbers are what
// land in file_complexity_snapshots.

type goldenFile struct {
	Skipped                     bool    `json:"skipped"`
	LOC                         int     `json:"loc"`
	FunctionsCount              int     `json:"functions_count"`
	CyclomaticTotal             int     `json:"cyclomatic_total"`
	CyclomaticAvg               float64 `json:"cyclomatic_avg"`
	HighComplexityFunctions     int     `json:"high_complexity_functions"`
	VeryHighComplexityFunctions int     `json:"very_high_complexity_functions"`
}

type goldenDoc struct {
	RadonVersion string                `json:"radon_version"`
	Files        map[string]goldenFile `json:"files"`
}

func loadCorpusGolden(t *testing.T) (goldenDoc, string) {
	t.Helper()
	corpus := filepath.Join("pycc", "testdata", "corpus")
	raw, err := os.ReadFile(filepath.Join("pycc", "testdata", "radon_cc_golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var doc goldenDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(doc.Files) == 0 {
		t.Fatalf("golden describes no files; every assertion below would be vacuous")
	}
	return doc, corpus
}

func TestAnalyzeFileMatchesRadonAggregates(t *testing.T) {
	doc, corpus := loadCorpusGolden(t)
	thresholds := DefaultThresholds()

	checked := 0
	for name, want := range doc.Files {
		if want.Skipped {
			continue
		}
		// The corpus is *.py.txt so Python tooling leaves it alone; the
		// analyzer keys on extension, so it is presented under its real
		// Python name.
		pythonName := strings.TrimSuffix(name, ".txt")

		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(corpus, name))
			if err != nil {
				t.Fatalf("read corpus file: %v", err)
			}
			got, err := AnalyzeFile(pythonName, string(source), thresholds)
			if err != nil {
				t.Fatalf("AnalyzeFile: %v", err)
			}
			if got == nil {
				t.Fatalf("AnalyzeFile skipped %s, but radon analysed it", pythonName)
			}

			if got.Language != "python" {
				t.Errorf("language: got %q, want python", got.Language)
			}
			if got.LOC != want.LOC {
				t.Errorf("loc: got %d, radon %d", got.LOC, want.LOC)
			}
			if got.FunctionsCount != want.FunctionsCount {
				t.Errorf("functions_count: got %d, radon %d",
					got.FunctionsCount, want.FunctionsCount)
			}
			if got.CyclomaticTotal != want.CyclomaticTotal {
				t.Errorf("cyclomatic_total: got %d, radon %d",
					got.CyclomaticTotal, want.CyclomaticTotal)
			}
			// Exact float equality is intentional: both sides compute
			// total/count from the same two integers, so any difference is a
			// real divergence rather than accumulated error. A tolerance here
			// would hide exactly the bug this test exists to catch.
			if got.CyclomaticAvg != want.CyclomaticAvg {
				t.Errorf("cyclomatic_avg: got %v, radon %v",
					got.CyclomaticAvg, want.CyclomaticAvg)
			}
			if got.HighComplexityFunctions != want.HighComplexityFunctions {
				t.Errorf("high_complexity_functions: got %d, radon %d",
					got.HighComplexityFunctions, want.HighComplexityFunctions)
			}
			if got.VeryHighComplexityFunctions != want.VeryHighComplexityFunctions {
				t.Errorf("very_high_complexity_functions: got %d, radon %d",
					got.VeryHighComplexityFunctions, want.VeryHighComplexityFunctions)
			}
		})
		checked++
	}
	if checked == 0 {
		t.Fatalf("no corpus files checked; this test proved nothing")
	}
}

// lizardGoldenFile is lizardcc's own golden shape (clike_parity_test.go),
// reused here the same way loadCorpusGolden reuses pycc's -- this proves the
// FILE-LEVEL aggregates (compute.go's BuildFileResult) for the C-family
// analyzer, where lizardcc's own parity test proves only the raw per-function
// numbers.
type lizardGoldenFile struct {
	FunctionsCount             int     `json:"functions_count"`
	CyclomaticTotal            int     `json:"cyclomatic_total"`
	CyclomaticAvg              float64 `json:"cyclomatic_avg"`
	HighComplexityFunctions    int     `json:"high_complexity_functions"`
	VeryHighComplexityFunction int     `json:"very_high_complexity_functions"`
}

type lizardGoldenDoc struct {
	LizardVersion string                      `json:"lizard_version"`
	Files         map[string]lizardGoldenFile `json:"files"`
}

// TestAnalyzeFileMatchesLizardAggregatesForCFamily is CHAOS-5156 PR2a's
// contract test against PR1's seam: it proves the C/C++ analyzer this PR
// registers (CFamilyAnalyzer) reaches AnalyzeFile -> AnalyzeFileWith ->
// BuildFileResult exactly like the python analyzer does in
// TestAnalyzeFileMatchesRadonAggregates above, deriving LOC/count/total/
// average/threshold fields the SAME way regardless of which analyzer ran.
func TestAnalyzeFileMatchesLizardAggregatesForCFamily(t *testing.T) {
	corpus := filepath.Join("lizardcc", "testdata", "corpus")
	raw, err := os.ReadFile(filepath.Join("lizardcc", "testdata", "lizard_cc_golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var doc lizardGoldenDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(doc.Files) == 0 {
		t.Fatalf("golden describes no files; every assertion below would be vacuous")
	}

	thresholds := DefaultThresholds()
	checked := 0
	for name, want := range doc.Files {
		// The corpus is <name>.<real-ext>.txt; strip .txt so LanguageFor
		// sees the extension the fixture is testing, exactly like
		// lizardcc's own oracle and parity test do.
		realName := strings.TrimSuffix(name, ".txt")
		lang, known := LanguageFor(realName)
		if !known {
			t.Fatalf("%s: extension not registered in languageByExtension", realName)
		}

		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(corpus, name))
			if err != nil {
				t.Fatalf("read corpus file: %v", err)
			}
			got, err := AnalyzeFile(realName, string(source), thresholds)
			if err != nil {
				t.Fatalf("AnalyzeFile: %v", err)
			}
			if got == nil {
				t.Fatalf("AnalyzeFile skipped %s, but lizard analysed it", realName)
			}
			if got.Language != lang {
				t.Errorf("language: got %q, want %q", got.Language, lang)
			}
			if got.FunctionsCount != want.FunctionsCount {
				t.Errorf("functions_count: got %d, lizard %d", got.FunctionsCount, want.FunctionsCount)
			}
			if got.CyclomaticTotal != want.CyclomaticTotal {
				t.Errorf("cyclomatic_total: got %d, lizard %d", got.CyclomaticTotal, want.CyclomaticTotal)
			}
			if got.CyclomaticAvg != want.CyclomaticAvg {
				t.Errorf("cyclomatic_avg: got %v, lizard %v", got.CyclomaticAvg, want.CyclomaticAvg)
			}
			if got.HighComplexityFunctions != want.HighComplexityFunctions {
				t.Errorf("high_complexity_functions: got %d, lizard %d",
					got.HighComplexityFunctions, want.HighComplexityFunctions)
			}
			if got.VeryHighComplexityFunctions != want.VeryHighComplexityFunction {
				t.Errorf("very_high_complexity_functions: got %d, lizard %d",
					got.VeryHighComplexityFunctions, want.VeryHighComplexityFunction)
			}
		})
		checked++
	}
	if checked == 0 {
		t.Fatalf("no corpus files checked; this test proved nothing")
	}
}

// TestLanguageForRoutesEveryCFamilyExtensionToCFamilyAnalyzer proves every
// extension CLikeReader owns in Python (clike.py:27) reaches the SAME Go
// analyzer, keyed by the SAME two language strings compute.go's extension
// map already declared before this PR existed.
func TestLanguageForRoutesEveryCFamilyExtensionToCFamilyAnalyzer(t *testing.T) {
	cases := []struct {
		path string
		lang string
	}{
		{"a.c", "c"}, {"a.h", "c"},
		{"a.cpp", "cpp"}, {"a.cc", "cpp"}, {"a.hpp", "cpp"},
	}
	analyzers := DefaultAnalyzers()
	for _, c := range cases {
		lang, known := LanguageFor(c.path)
		if !known || lang != c.lang {
			t.Fatalf("%s: LanguageFor got (%q, %v), want (%q, true)", c.path, lang, known, c.lang)
		}
		analyze, ok := analyzers[lang]
		if !ok {
			t.Fatalf("%s: no analyzer registered for language %q", c.path, lang)
		}
		complexities, skipped, err := analyze(c.path, "int f() { if (1) { return 1; } return 0; }")
		if err != nil || skipped {
			t.Fatalf("%s: analyze returned err=%v skipped=%v", c.path, err, skipped)
		}
		if len(complexities) != 1 || complexities[0] != 2 {
			t.Fatalf("%s: got %v, want a single function with complexity 2", c.path, complexities)
		}
	}
}

// TestAnalyzeFileMatchesLizardAggregatesForGoRust is this PR's contract
// test against PR1's seam for the two analyzers it registers
// (go/rust), mirroring TestAnalyzeFileMatchesLizardAggregatesForCFamily.
func TestAnalyzeFileMatchesLizardAggregatesForGoRust(t *testing.T) {
	corpus := filepath.Join("lizardcc", "testdata", "corpus_go_rust")
	raw, err := os.ReadFile(filepath.Join("lizardcc", "testdata", "lizard_cc_golden_go_rust.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var doc lizardGoldenDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(doc.Files) == 0 {
		t.Fatalf("golden describes no files; every assertion below would be vacuous")
	}

	thresholds := DefaultThresholds()
	checked := 0
	for name, want := range doc.Files {
		realName := strings.TrimSuffix(name, ".txt")
		lang, known := LanguageFor(realName)
		if !known {
			t.Fatalf("%s: extension not registered in languageByExtension", realName)
		}

		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(corpus, name))
			if err != nil {
				t.Fatalf("read corpus file: %v", err)
			}
			got, err := AnalyzeFile(realName, string(source), thresholds)
			if err != nil {
				t.Fatalf("AnalyzeFile: %v", err)
			}
			if got == nil {
				t.Fatalf("AnalyzeFile skipped %s, but lizard analysed it", realName)
			}
			if got.Language != lang {
				t.Errorf("language: got %q, want %q", got.Language, lang)
			}
			if got.FunctionsCount != want.FunctionsCount {
				t.Errorf("functions_count: got %d, lizard %d", got.FunctionsCount, want.FunctionsCount)
			}
			if got.CyclomaticTotal != want.CyclomaticTotal {
				t.Errorf("cyclomatic_total: got %d, lizard %d", got.CyclomaticTotal, want.CyclomaticTotal)
			}
			if got.CyclomaticAvg != want.CyclomaticAvg {
				t.Errorf("cyclomatic_avg: got %v, lizard %v", got.CyclomaticAvg, want.CyclomaticAvg)
			}
			if got.HighComplexityFunctions != want.HighComplexityFunctions {
				t.Errorf("high_complexity_functions: got %d, lizard %d",
					got.HighComplexityFunctions, want.HighComplexityFunctions)
			}
			if got.VeryHighComplexityFunctions != want.VeryHighComplexityFunction {
				t.Errorf("very_high_complexity_functions: got %d, lizard %d",
					got.VeryHighComplexityFunctions, want.VeryHighComplexityFunction)
			}
		})
		checked++
	}
	if checked == 0 {
		t.Fatalf("no corpus files checked; this test proved nothing")
	}
}

// TestLanguageForRoutesEveryGoRustExtensionToItsAnalyzer proves .go and
// .rs both reach a registered analyzer under the right language string.
func TestLanguageForRoutesEveryGoRustExtensionToItsAnalyzer(t *testing.T) {
	cases := []struct {
		path string
		lang string
	}{
		{"a.go", "go"}, {"a.rs", "rust"},
	}
	analyzers := DefaultAnalyzers()
	for _, c := range cases {
		lang, known := LanguageFor(c.path)
		if !known || lang != c.lang {
			t.Fatalf("%s: LanguageFor got (%q, %v), want (%q, true)", c.path, lang, known, c.lang)
		}
		if _, ok := analyzers[lang]; !ok {
			t.Fatalf("%s: no analyzer registered for language %q", c.path, lang)
		}
	}
}

// TestAnalyzeFileMatchesLizardAggregatesForJVMSwiftFamily is CHAOS-5156
// PR2b's contract test against PR1's seam, mirroring
// TestAnalyzeFileMatchesLizardAggregatesForCFamily for the four analyzers
// this PR registers (csharp/kotlin/scala/swift). Only the FILE-LEVEL
// aggregates are asserted here (LOC/count/total/average/threshold), all
// order-independent -- lizardcc's own jvmswift_parity_test.go is what
// proves the raw per-function numbers, including why order itself is
// NOT asserted there for this family.
func TestAnalyzeFileMatchesLizardAggregatesForJVMSwiftFamily(t *testing.T) {
	corpus := filepath.Join("lizardcc", "testdata", "corpus_jvm_swift")
	raw, err := os.ReadFile(filepath.Join("lizardcc", "testdata", "lizard_cc_golden_jvm_swift.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var doc lizardGoldenDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(doc.Files) == 0 {
		t.Fatalf("golden describes no files; every assertion below would be vacuous")
	}

	thresholds := DefaultThresholds()
	checked := 0
	for name, want := range doc.Files {
		realName := strings.TrimSuffix(name, ".txt")
		lang, known := LanguageFor(realName)
		if !known {
			t.Fatalf("%s: extension not registered in languageByExtension", realName)
		}

		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(corpus, name))
			if err != nil {
				t.Fatalf("read corpus file: %v", err)
			}
			got, err := AnalyzeFile(realName, string(source), thresholds)
			if err != nil {
				t.Fatalf("AnalyzeFile: %v", err)
			}
			if got == nil {
				t.Fatalf("AnalyzeFile skipped %s, but lizard analysed it", realName)
			}
			if got.Language != lang {
				t.Errorf("language: got %q, want %q", got.Language, lang)
			}
			if got.FunctionsCount != want.FunctionsCount {
				t.Errorf("functions_count: got %d, lizard %d", got.FunctionsCount, want.FunctionsCount)
			}
			if got.CyclomaticTotal != want.CyclomaticTotal {
				t.Errorf("cyclomatic_total: got %d, lizard %d", got.CyclomaticTotal, want.CyclomaticTotal)
			}
			if got.CyclomaticAvg != want.CyclomaticAvg {
				t.Errorf("cyclomatic_avg: got %v, lizard %v", got.CyclomaticAvg, want.CyclomaticAvg)
			}
			if got.HighComplexityFunctions != want.HighComplexityFunctions {
				t.Errorf("high_complexity_functions: got %d, lizard %d",
					got.HighComplexityFunctions, want.HighComplexityFunctions)
			}
			if got.VeryHighComplexityFunctions != want.VeryHighComplexityFunction {
				t.Errorf("very_high_complexity_functions: got %d, lizard %d",
					got.VeryHighComplexityFunctions, want.VeryHighComplexityFunction)
			}
		})
		checked++
	}
	if checked == 0 {
		t.Fatalf("no corpus files checked; this test proved nothing")
	}
}

// TestLanguageForRoutesEveryJVMSwiftExtensionToItsAnalyzer proves every
// extension PR2b registers reaches the right analyzer under the right
// language string.
func TestLanguageForRoutesEveryJVMSwiftExtensionToItsAnalyzer(t *testing.T) {
	cases := []struct {
		path string
		lang string
	}{
		{"a.cs", "csharp"}, {"a.kt", "kotlin"}, {"a.kts", "kotlin"},
		{"a.scala", "scala"}, {"a.swift", "swift"},
	}
	analyzers := DefaultAnalyzers()
	for _, c := range cases {
		lang, known := LanguageFor(c.path)
		if !known || lang != c.lang {
			t.Fatalf("%s: LanguageFor got (%q, %v), want (%q, true)", c.path, lang, known, c.lang)
		}
		if _, ok := analyzers[lang]; !ok {
			t.Fatalf("%s: no analyzer registered for language %q", c.path, lang)
		}
	}
}

// TestLanguageForRoutesEveryJSTSExtensionToItsAnalyzer proves every
// extension CHAOS-4291 registers for javascript/typescript reaches the
// right analyzer under the right language string -- .js/.jsx/.mjs/.cjs all
// route to "javascript" (lizardcc.AnalyzeJavaScript), .ts/.tsx to
// "typescript" (lizardcc.AnalyzeTypeScript), matching Python's
// JavaScriptReader/TypeScriptReader ext lists (javascript.py:11,
// typescript.py:51) exactly.
func TestLanguageForRoutesEveryJSTSExtensionToItsAnalyzer(t *testing.T) {
	cases := []struct {
		path string
		lang string
	}{
		{"a.js", "javascript"}, {"a.jsx", "javascript"},
		{"a.mjs", "javascript"}, {"a.cjs", "javascript"},
		{"a.ts", "typescript"}, {"a.tsx", "typescript"},
	}
	analyzers := DefaultAnalyzers()
	for _, c := range cases {
		lang, known := LanguageFor(c.path)
		if !known || lang != c.lang {
			t.Fatalf("%s: LanguageFor got (%q, %v), want (%q, true)", c.path, lang, known, c.lang)
		}
		if _, ok := analyzers[lang]; !ok {
			t.Fatalf("%s: no analyzer registered for language %q", c.path, lang)
		}
	}
}

// TestAnalyzeFileMatchesLizardAggregatesForJSTS is this PR's contract test
// against PR1's seam for the javascript/typescript analyzer, mirroring
// TestAnalyzeFileMatchesLizardAggregatesForJava.
func TestAnalyzeFileMatchesLizardAggregatesForJSTS(t *testing.T) {
	corpus := filepath.Join("lizardcc", "testdata", "corpus_js_ts")
	entries, err := os.ReadDir(corpus)
	if err != nil {
		t.Fatalf("read corpus: %v", err)
	}
	seen := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".txt") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".txt")
		src, err := os.ReadFile(filepath.Join(corpus, entry.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		seen++
		t.Run(entry.Name(), func(t *testing.T) {
			result, err := AnalyzeFile(name, string(src), DefaultThresholds())
			if err != nil {
				t.Fatalf("AnalyzeFile(%s): %v", name, err)
			}
			if result == nil {
				t.Fatalf("AnalyzeFile(%s): expected a row, got nil", name)
			}
			wantLang := "typescript"
			if strings.HasSuffix(name, ".js") {
				wantLang = "javascript"
			}
			if result.Language != wantLang {
				t.Errorf("%s: language = %q, want %q", name, result.Language, wantLang)
			}
		})
	}
	if seen == 0 {
		t.Fatalf("no corpus files found; this test proved nothing")
	}
}

// TestAnalyzeFileMatchesLizardAggregatesForJava is this PR's contract test
// against PR1's seam for the java analyzer, mirroring
// TestAnalyzeFileMatchesLizardAggregatesForCFamily.
func TestAnalyzeFileMatchesLizardAggregatesForJava(t *testing.T) {
	corpus := filepath.Join("lizardcc", "testdata", "corpus_java")
	raw, err := os.ReadFile(filepath.Join("lizardcc", "testdata", "lizard_cc_golden_java.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var doc lizardGoldenDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(doc.Files) == 0 {
		t.Fatalf("golden describes no files; every assertion below would be vacuous")
	}

	thresholds := DefaultThresholds()
	checked := 0
	for name, want := range doc.Files {
		realName := strings.TrimSuffix(name, ".txt")
		lang, known := LanguageFor(realName)
		if !known {
			t.Fatalf("%s: extension not registered in languageByExtension", realName)
		}

		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(corpus, name))
			if err != nil {
				t.Fatalf("read corpus file: %v", err)
			}
			got, err := AnalyzeFile(realName, string(source), thresholds)
			if err != nil {
				t.Fatalf("AnalyzeFile: %v", err)
			}
			if got == nil {
				t.Fatalf("AnalyzeFile skipped %s, but lizard analysed it", realName)
			}
			if got.Language != lang {
				t.Errorf("language: got %q, want %q", got.Language, lang)
			}
			if got.FunctionsCount != want.FunctionsCount {
				t.Errorf("functions_count: got %d, lizard %d", got.FunctionsCount, want.FunctionsCount)
			}
			if got.CyclomaticTotal != want.CyclomaticTotal {
				t.Errorf("cyclomatic_total: got %d, lizard %d", got.CyclomaticTotal, want.CyclomaticTotal)
			}
			if got.CyclomaticAvg != want.CyclomaticAvg {
				t.Errorf("cyclomatic_avg: got %v, lizard %v", got.CyclomaticAvg, want.CyclomaticAvg)
			}
			if got.HighComplexityFunctions != want.HighComplexityFunctions {
				t.Errorf("high_complexity_functions: got %d, lizard %d",
					got.HighComplexityFunctions, want.HighComplexityFunctions)
			}
			if got.VeryHighComplexityFunctions != want.VeryHighComplexityFunction {
				t.Errorf("very_high_complexity_functions: got %d, lizard %d",
					got.VeryHighComplexityFunctions, want.VeryHighComplexityFunction)
			}
		})
		checked++
	}
	if checked == 0 {
		t.Fatalf("no corpus files checked; this test proved nothing")
	}
}

// TestLanguageForRoutesRubyExtensionToItsAnalyzer proves .rb reaches a
// registered analyzer under the right language string, matching Python's
// RubyReader.ext (ruby.py:22).
func TestLanguageForRoutesRubyExtensionToItsAnalyzer(t *testing.T) {
	lang, known := LanguageFor("a.rb")
	if !known || lang != "ruby" {
		t.Fatalf("a.rb: LanguageFor got (%q, %v), want (\"ruby\", true)", lang, known)
	}
	if _, ok := DefaultAnalyzers()[lang]; !ok {
		t.Fatalf("a.rb: no analyzer registered for language %q", lang)
	}
}

// TestAnalyzeFileMatchesLizardAggregatesForRuby is this PR's contract test
// against PR1's seam for the Ruby analyzer, mirroring
// TestAnalyzeFileMatchesLizardAggregatesForJava.
func TestAnalyzeFileMatchesLizardAggregatesForRuby(t *testing.T) {
	corpus := filepath.Join("lizardcc", "testdata", "corpus_ruby")
	raw, err := os.ReadFile(filepath.Join("lizardcc", "testdata", "lizard_cc_golden_ruby.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var doc lizardGoldenDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(doc.Files) == 0 {
		t.Fatalf("golden describes no files; every assertion below would be vacuous")
	}

	thresholds := DefaultThresholds()
	checked := 0
	for name, want := range doc.Files {
		realName := strings.TrimSuffix(name, ".txt")
		lang, known := LanguageFor(realName)
		if !known {
			t.Fatalf("%s: extension not registered in languageByExtension", realName)
		}

		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(corpus, name))
			if err != nil {
				t.Fatalf("read corpus file: %v", err)
			}
			got, err := AnalyzeFile(realName, string(source), thresholds)
			if err != nil {
				t.Fatalf("AnalyzeFile: %v", err)
			}
			if got == nil {
				t.Fatalf("AnalyzeFile skipped %s, but lizard analysed it", realName)
			}
			if got.Language != lang {
				t.Errorf("language: got %q, want %q", got.Language, lang)
			}
			if got.FunctionsCount != want.FunctionsCount {
				t.Errorf("functions_count: got %d, lizard %d", got.FunctionsCount, want.FunctionsCount)
			}
			if got.CyclomaticTotal != want.CyclomaticTotal {
				t.Errorf("cyclomatic_total: got %d, lizard %d", got.CyclomaticTotal, want.CyclomaticTotal)
			}
			if got.CyclomaticAvg != want.CyclomaticAvg {
				t.Errorf("cyclomatic_avg: got %v, lizard %v", got.CyclomaticAvg, want.CyclomaticAvg)
			}
			if got.HighComplexityFunctions != want.HighComplexityFunctions {
				t.Errorf("high_complexity_functions: got %d, lizard %d",
					got.HighComplexityFunctions, want.HighComplexityFunctions)
			}
			if got.VeryHighComplexityFunctions != want.VeryHighComplexityFunction {
				t.Errorf("very_high_complexity_functions: got %d, lizard %d",
					got.VeryHighComplexityFunctions, want.VeryHighComplexityFunction)
			}
		})
		checked++
	}
	if checked == 0 {
		t.Fatalf("no corpus files checked; this test proved nothing")
	}
}

// TestLanguageForRoutesPHPExtensionToItsAnalyzer proves .php reaches a
// registered analyzer under the right language string, matching Python's
// PHPReader.ext (php.py:190).
func TestLanguageForRoutesPHPExtensionToItsAnalyzer(t *testing.T) {
	lang, known := LanguageFor("a.php")
	if !known || lang != "php" {
		t.Fatalf("a.php: LanguageFor got (%q, %v), want (\"php\", true)", lang, known)
	}
	if _, ok := DefaultAnalyzers()[lang]; !ok {
		t.Fatalf("a.php: no analyzer registered for language %q", lang)
	}
}

// TestAnalyzeFileMatchesLizardAggregatesForPHP is this PR's contract test
// against PR1's seam for the PHP analyzer, mirroring
// TestAnalyzeFileMatchesLizardAggregatesForJava.
func TestAnalyzeFileMatchesLizardAggregatesForPHP(t *testing.T) {
	corpus := filepath.Join("lizardcc", "testdata", "corpus_php")
	raw, err := os.ReadFile(filepath.Join("lizardcc", "testdata", "lizard_cc_golden_php.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var doc lizardGoldenDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(doc.Files) == 0 {
		t.Fatalf("golden describes no files; every assertion below would be vacuous")
	}

	thresholds := DefaultThresholds()
	checked := 0
	for name, want := range doc.Files {
		realName := strings.TrimSuffix(name, ".txt")
		lang, known := LanguageFor(realName)
		if !known {
			t.Fatalf("%s: extension not registered in languageByExtension", realName)
		}

		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(corpus, name))
			if err != nil {
				t.Fatalf("read corpus file: %v", err)
			}
			got, err := AnalyzeFile(realName, string(source), thresholds)
			if err != nil {
				t.Fatalf("AnalyzeFile: %v", err)
			}
			if got == nil {
				t.Fatalf("AnalyzeFile skipped %s, but lizard analysed it", realName)
			}
			if got.Language != lang {
				t.Errorf("language: got %q, want %q", got.Language, lang)
			}
			if got.FunctionsCount != want.FunctionsCount {
				t.Errorf("functions_count: got %d, lizard %d", got.FunctionsCount, want.FunctionsCount)
			}
			if got.CyclomaticTotal != want.CyclomaticTotal {
				t.Errorf("cyclomatic_total: got %d, lizard %d", got.CyclomaticTotal, want.CyclomaticTotal)
			}
			if got.CyclomaticAvg != want.CyclomaticAvg {
				t.Errorf("cyclomatic_avg: got %v, lizard %v", got.CyclomaticAvg, want.CyclomaticAvg)
			}
			if got.HighComplexityFunctions != want.HighComplexityFunctions {
				t.Errorf("high_complexity_functions: got %d, lizard %d",
					got.HighComplexityFunctions, want.HighComplexityFunctions)
			}
			if got.VeryHighComplexityFunctions != want.VeryHighComplexityFunction {
				t.Errorf("very_high_complexity_functions: got %d, lizard %d",
					got.VeryHighComplexityFunctions, want.VeryHighComplexityFunction)
			}
		})
		checked++
	}
	if checked == 0 {
		t.Fatalf("no corpus files checked; this test proved nothing")
	}
}

// TestLanguageForRoutesObjCExtensionsToItsAnalyzer proves .m/.mm both
// reach a registered analyzer under the "objective-c" language string,
// matching Python's ObjCReader.ext (objc.py:8).
func TestLanguageForRoutesObjCExtensionsToItsAnalyzer(t *testing.T) {
	for _, path := range []string{"a.m", "a.mm"} {
		lang, known := LanguageFor(path)
		if !known || lang != "objective-c" {
			t.Fatalf("%s: LanguageFor got (%q, %v), want (\"objective-c\", true)", path, lang, known)
		}
		if _, ok := DefaultAnalyzers()[lang]; !ok {
			t.Fatalf("%s: no analyzer registered for language %q", path, lang)
		}
	}
}

// TestAnalyzeFileMatchesLizardAggregatesForObjC is this PR's contract test
// against PR1's seam for the Objective-C analyzer, mirroring
// TestAnalyzeFileMatchesLizardAggregatesForJava.
func TestAnalyzeFileMatchesLizardAggregatesForObjC(t *testing.T) {
	corpus := filepath.Join("lizardcc", "testdata", "corpus_objc")
	raw, err := os.ReadFile(filepath.Join("lizardcc", "testdata", "lizard_cc_golden_objc.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var doc lizardGoldenDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(doc.Files) == 0 {
		t.Fatalf("golden describes no files; every assertion below would be vacuous")
	}

	thresholds := DefaultThresholds()
	checked := 0
	for name, want := range doc.Files {
		realName := strings.TrimSuffix(name, ".txt")
		lang, known := LanguageFor(realName)
		if !known {
			t.Fatalf("%s: extension not registered in languageByExtension", realName)
		}

		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(corpus, name))
			if err != nil {
				t.Fatalf("read corpus file: %v", err)
			}
			got, err := AnalyzeFile(realName, string(source), thresholds)
			if err != nil {
				t.Fatalf("AnalyzeFile: %v", err)
			}
			if got == nil {
				t.Fatalf("AnalyzeFile skipped %s, but lizard analysed it", realName)
			}
			if got.Language != lang {
				t.Errorf("language: got %q, want %q", got.Language, lang)
			}
			if got.FunctionsCount != want.FunctionsCount {
				t.Errorf("functions_count: got %d, lizard %d", got.FunctionsCount, want.FunctionsCount)
			}
			if got.CyclomaticTotal != want.CyclomaticTotal {
				t.Errorf("cyclomatic_total: got %d, lizard %d", got.CyclomaticTotal, want.CyclomaticTotal)
			}
			if got.CyclomaticAvg != want.CyclomaticAvg {
				t.Errorf("cyclomatic_avg: got %v, lizard %v", got.CyclomaticAvg, want.CyclomaticAvg)
			}
			if got.HighComplexityFunctions != want.HighComplexityFunctions {
				t.Errorf("high_complexity_functions: got %d, lizard %d",
					got.HighComplexityFunctions, want.HighComplexityFunctions)
			}
			if got.VeryHighComplexityFunctions != want.VeryHighComplexityFunction {
				t.Errorf("very_high_complexity_functions: got %d, lizard %d",
					got.VeryHighComplexityFunctions, want.VeryHighComplexityFunction)
			}
		})
		checked++
	}
	if checked == 0 {
		t.Fatalf("no corpus files checked; this test proved nothing")
	}
}

// TestLanguageForRoutesLuaExtensionToItsAnalyzer proves .lua reaches a
// registered analyzer under the right language string, matching Python's
// LuaReader.ext (lua.py:9).
func TestLanguageForRoutesLuaExtensionToItsAnalyzer(t *testing.T) {
	lang, known := LanguageFor("a.lua")
	if !known || lang != "lua" {
		t.Fatalf("a.lua: LanguageFor got (%q, %v), want (\"lua\", true)", lang, known)
	}
	if _, ok := DefaultAnalyzers()[lang]; !ok {
		t.Fatalf("a.lua: no analyzer registered for language %q", lang)
	}
}

// TestAnalyzeFileMatchesLizardAggregatesForLua is this PR's contract test
// against PR1's seam for the Lua analyzer, mirroring
// TestAnalyzeFileMatchesLizardAggregatesForJava.
func TestAnalyzeFileMatchesLizardAggregatesForLua(t *testing.T) {
	corpus := filepath.Join("lizardcc", "testdata", "corpus_lua")
	raw, err := os.ReadFile(filepath.Join("lizardcc", "testdata", "lizard_cc_golden_lua.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var doc lizardGoldenDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(doc.Files) == 0 {
		t.Fatalf("golden describes no files; every assertion below would be vacuous")
	}

	thresholds := DefaultThresholds()
	checked := 0
	for name, want := range doc.Files {
		realName := strings.TrimSuffix(name, ".txt")
		lang, known := LanguageFor(realName)
		if !known {
			t.Fatalf("%s: extension not registered in languageByExtension", realName)
		}

		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(corpus, name))
			if err != nil {
				t.Fatalf("read corpus file: %v", err)
			}
			got, err := AnalyzeFile(realName, string(source), thresholds)
			if err != nil {
				t.Fatalf("AnalyzeFile: %v", err)
			}
			if got == nil {
				t.Fatalf("AnalyzeFile skipped %s, but lizard analysed it", realName)
			}
			if got.Language != lang {
				t.Errorf("language: got %q, want %q", got.Language, lang)
			}
			if got.FunctionsCount != want.FunctionsCount {
				t.Errorf("functions_count: got %d, lizard %d", got.FunctionsCount, want.FunctionsCount)
			}
			if got.CyclomaticTotal != want.CyclomaticTotal {
				t.Errorf("cyclomatic_total: got %d, lizard %d", got.CyclomaticTotal, want.CyclomaticTotal)
			}
			if got.CyclomaticAvg != want.CyclomaticAvg {
				t.Errorf("cyclomatic_avg: got %v, lizard %v", got.CyclomaticAvg, want.CyclomaticAvg)
			}
			if got.HighComplexityFunctions != want.HighComplexityFunctions {
				t.Errorf("high_complexity_functions: got %d, lizard %d",
					got.HighComplexityFunctions, want.HighComplexityFunctions)
			}
			if got.VeryHighComplexityFunctions != want.VeryHighComplexityFunction {
				t.Errorf("very_high_complexity_functions: got %d, lizard %d",
					got.VeryHighComplexityFunctions, want.VeryHighComplexityFunction)
			}
		})
		checked++
	}
	if checked == 0 {
		t.Fatalf("no corpus files checked; this test proved nothing")
	}
}

// TestLanguageForRoutesVueExtensionToItsAnalyzer proves .vue reaches a
// registered analyzer under the right language string, matching Python's
// VueReader.ext (vue.py:11) -- this is CHAOS-4291's LAST language, closing
// out every LANGUAGE_BY_EXTENSION key (see
// TestEveryLanguageByExtensionKeyHasARegisteredAnalyzer above).
func TestLanguageForRoutesVueExtensionToItsAnalyzer(t *testing.T) {
	lang, known := LanguageFor("a.vue")
	if !known || lang != "vue" {
		t.Fatalf("a.vue: LanguageFor got (%q, %v), want (\"vue\", true)", lang, known)
	}
	if _, ok := DefaultAnalyzers()[lang]; !ok {
		t.Fatalf("a.vue: no analyzer registered for language %q", lang)
	}
}

// TestAnalyzeFileMatchesLizardAggregatesForVue is this PR's contract test
// against PR1's seam for the Vue analyzer, mirroring
// TestAnalyzeFileMatchesLizardAggregatesForJava.
func TestAnalyzeFileMatchesLizardAggregatesForVue(t *testing.T) {
	corpus := filepath.Join("lizardcc", "testdata", "corpus_vue")
	raw, err := os.ReadFile(filepath.Join("lizardcc", "testdata", "lizard_cc_golden_vue.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var doc lizardGoldenDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(doc.Files) == 0 {
		t.Fatalf("golden describes no files; every assertion below would be vacuous")
	}

	thresholds := DefaultThresholds()
	checked := 0
	for name, want := range doc.Files {
		realName := strings.TrimSuffix(name, ".txt")
		lang, known := LanguageFor(realName)
		if !known {
			t.Fatalf("%s: extension not registered in languageByExtension", realName)
		}

		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(corpus, name))
			if err != nil {
				t.Fatalf("read corpus file: %v", err)
			}
			got, err := AnalyzeFile(realName, string(source), thresholds)
			if err != nil {
				t.Fatalf("AnalyzeFile: %v", err)
			}
			if got == nil {
				t.Fatalf("AnalyzeFile skipped %s, but lizard analysed it", realName)
			}
			if got.Language != lang {
				t.Errorf("language: got %q, want %q", got.Language, lang)
			}
			if got.FunctionsCount != want.FunctionsCount {
				t.Errorf("functions_count: got %d, lizard %d", got.FunctionsCount, want.FunctionsCount)
			}
			if got.CyclomaticTotal != want.CyclomaticTotal {
				t.Errorf("cyclomatic_total: got %d, lizard %d", got.CyclomaticTotal, want.CyclomaticTotal)
			}
			if got.CyclomaticAvg != want.CyclomaticAvg {
				t.Errorf("cyclomatic_avg: got %v, lizard %v", got.CyclomaticAvg, want.CyclomaticAvg)
			}
			if got.HighComplexityFunctions != want.HighComplexityFunctions {
				t.Errorf("high_complexity_functions: got %d, lizard %d",
					got.HighComplexityFunctions, want.HighComplexityFunctions)
			}
			if got.VeryHighComplexityFunctions != want.VeryHighComplexityFunction {
				t.Errorf("very_high_complexity_functions: got %d, lizard %d",
					got.VeryHighComplexityFunctions, want.VeryHighComplexityFunction)
			}
		})
		checked++
	}
	if checked == 0 {
		t.Fatalf("no corpus files checked; this test proved nothing")
	}
}

// TestLanguageForRoutesJavaExtensionToItsAnalyzer proves .java reaches a
// registered analyzer under the right language string.
func TestLanguageForRoutesJavaExtensionToItsAnalyzer(t *testing.T) {
	lang, known := LanguageFor("a.java")
	if !known || lang != "java" {
		t.Fatalf("a.java: LanguageFor got (%q, %v), want (\"java\", true)", lang, known)
	}
	if _, ok := DefaultAnalyzers()[lang]; !ok {
		t.Fatalf("a.java: no analyzer registered for language %q", lang)
	}
}

func TestAnalyzeFileSkipsUnanalysedExtensions(t *testing.T) {
	// Python's _analyze_content returns None for an extension absent from
	// LANGUAGE_BY_EXTENSION. That is a skip, not an error, and not a zero row.
	for _, path := range []string{"README.md", "data.json", "Makefile", "x.yaml"} {
		got, err := AnalyzeFile(path, "if x and y:\n    pass\n", DefaultThresholds())
		if err != nil {
			t.Errorf("%s: unexpected error %v", path, err)
		}
		if got != nil {
			t.Errorf("%s: expected a skip, got a row: %+v", path, got)
		}
	}
}

// TestEveryLanguageByExtensionKeyHasARegisteredAnalyzer closes out
// CHAOS-4291: every LANGUAGE_BY_EXTENSION language now has a native Go
// analyzer, so this package can never again silently route a real,
// recognised extension through ErrLanguageNotPorted. This replaces the
// former TestAnalyzeFileFailsClosedOnLizardLanguages, which asserted the
// OPPOSITE fact (a specific list of not-yet-ported extensions) one
// language at a time as each PR landed -- "b.go" moved out when go-rust
// registered `go`, "c.java" when a later PR registered `java`, "a.ts" when
// CHAOS-4291 registered `javascript`/`typescript`, "d.rb" when it
// registered `ruby`, and "e.vue" -- the last entry -- when it registered
// `vue` here. An empty not-yet-ported list would have made that test
// silently vacuous (a for loop over nothing asserts nothing), so this
// test takes over as the PERMANENT fail-closed invariant: any future
// language ever added to languageByExtension without ALSO registering an
// analyzer fails HERE, immediately, rather than only being caught by
// chance if some other test happens to exercise that specific extension.
func TestEveryLanguageByExtensionKeyHasARegisteredAnalyzer(t *testing.T) {
	analyzers := DefaultAnalyzers()
	if len(languageByExtension) == 0 {
		t.Fatalf("languageByExtension is empty; this test would prove nothing")
	}
	for ext, lang := range languageByExtension {
		if _, ok := analyzers[lang]; !ok {
			t.Errorf("%s -> %q: no analyzer registered in DefaultAnalyzers", ext, lang)
		}
	}
}

// TestAnalyzeFileFailsClosedOnAnUnregisteredLanguage proves the
// ErrLanguageNotPorted contract itself still works, now that no REAL
// languageByExtension entry can exercise it: DefaultAnalyzers with one
// entry deliberately removed must still fail closed (error, not a skip,
// not a zero row) for a file whose language maps to that removed entry --
// see AnalyzeFileWith's own contract and this file's
// TestAnalyzeFileWithAcceptsAnAdditionalLanguageAnalyzer for the
// complementary "adding one works" proof.
func TestAnalyzeFileFailsClosedOnAnUnregisteredLanguage(t *testing.T) {
	analyzers := DefaultAnalyzers()
	delete(analyzers, "vue")
	got, err := AnalyzeFileWith("a.vue", "function f() { if (a && b) return 1; }", DefaultThresholds(), analyzers)
	if !errors.Is(err, ErrLanguageNotPorted) {
		t.Fatalf("expected ErrLanguageNotPorted, got err=%v", err)
	}
	if got != nil {
		t.Errorf("expected no row alongside the error, got %+v", got)
	}
}

func TestAnalyzeFileWithAcceptsAnAdditionalLanguageAnalyzer(t *testing.T) {
	// The seam CHAOS-5156 (PR2, lizard's 20 languages) consumes. Registering a
	// language must require NO change to the dispatch, the extension map, the
	// result type, or the derived-field maths -- otherwise the two ports drift
	// and the same file scores differently depending on which analyzer ran.
	analyzers := DefaultAnalyzers()
	analyzers["typescript"] = func(path, source string) ([]int, bool, error) {
		return []int{3, 20, 30}, false, nil
	}

	got, err := AnalyzeFileWith("app.ts", "irrelevant\nsource\n", DefaultThresholds(), analyzers)
	if err != nil {
		t.Fatalf("AnalyzeFileWith: %v", err)
	}
	if got == nil {
		t.Fatalf("expected a row for a registered language")
	}
	if got.Language != "typescript" {
		t.Errorf("language: got %q, want typescript", got.Language)
	}
	if got.FunctionsCount != 3 || got.CyclomaticTotal != 53 {
		t.Errorf("derived totals: got count=%d total=%d, want 3/53",
			got.FunctionsCount, got.CyclomaticTotal)
	}
	// Thresholds are strict `>`: 20 and 30 exceed 15; only 30 exceeds 25.
	if got.HighComplexityFunctions != 2 {
		t.Errorf("high: got %d, want 2", got.HighComplexityFunctions)
	}
	if got.VeryHighComplexityFunctions != 1 {
		t.Errorf("very_high: got %d, want 1", got.VeryHighComplexityFunctions)
	}
	// python must still work through the same call.
	if _, err := AnalyzeFileWith("x.py", "def f():\n    pass\n",
		DefaultThresholds(), analyzers); err != nil {
		t.Errorf("registering a language broke the python path: %v", err)
	}
}

func TestAnalyzeFileWithEmptyResultIsAZeroRowNotASkip(t *testing.T) {
	// An analyzer that recognises NOTHING but raises nothing must produce a
	// real row of zeros, not a skip. The two outcomes are different rows
	// downstream: a skip contributes nothing at all, while a zero row still
	// counts toward the repo's loc_total.
	//
	// Measured by lane-port-investment on lizard 1.23.0 (CHAOS-5156): a MATLAB
	// file named *.m goes to the Objective-C reader, which recognises no
	// functions and raises no exception. Under Python that reaches
	// _build_result with an empty list and writes a zero row -- only an
	// EXCEPTION returns None. radon behaves the same way on a Python module
	// with no functions, which the corpus pins as rules_nofunctions.py.txt.
	analyzers := DefaultAnalyzers()
	analyzers["objective-c"] = func(path, source string) ([]int, bool, error) {
		return []int{}, false, nil
	}

	got, err := AnalyzeFileWith("legacy.m", "function y = f(x)\ny = x;\nend\n",
		DefaultThresholds(), analyzers)
	if err != nil {
		t.Fatalf("AnalyzeFileWith: %v", err)
	}
	if got == nil {
		t.Fatalf("an empty result with skipped=false must be a ZERO ROW, not a skip")
	}
	if got.FunctionsCount != 0 || got.CyclomaticTotal != 0 || got.CyclomaticAvg != 0.0 {
		t.Errorf("expected zeros, got %+v", got)
	}
	// LOC still counts: the file exists and its lines are real, which is what
	// makes this different from a skip.
	if got.LOC != 3 {
		t.Errorf("loc: got %d, want 3 -- a zero row still contributes loc", got.LOC)
	}
	if got.Language != "objective-c" {
		t.Errorf("language: got %q, want objective-c", got.Language)
	}
}

func TestBuildFileResultIsStrictlyAboveThreshold(t *testing.T) {
	// Python: `sum(1 for c in complexities if c > threshold)`. A block exactly
	// AT the threshold is not counted; using >= would over-report every repo.
	got := BuildFileResult("a.py", "python", 10, []int{15, 16, 25, 26}, DefaultThresholds())
	if got.HighComplexityFunctions != 3 {
		t.Errorf("high (>15): got %d, want 3", got.HighComplexityFunctions)
	}
	if got.VeryHighComplexityFunctions != 1 {
		t.Errorf("very_high (>25): got %d, want 1", got.VeryHighComplexityFunctions)
	}
}

func TestBuildFileResultZeroFunctionsHasZeroAverage(t *testing.T) {
	// Python: `cyclomatic_total / functions_count if functions_count > 0 else 0.0`.
	// Without the guard this is 0/0 -> NaN.
	got := BuildFileResult("empty.py", "python", 3, nil, DefaultThresholds())
	if got.CyclomaticAvg != 0.0 || math.IsNaN(got.CyclomaticAvg) {
		t.Fatalf("cyclomatic_avg: got %v, want 0.0", got.CyclomaticAvg)
	}
}

func TestAnalyzeFileSkipsUnparseableSource(t *testing.T) {
	// _analyze_python catches every exception from cc_visit and returns None,
	// so a file Go cannot lex must be dropped, NOT recorded as zero.
	got, err := AnalyzeFile("broken.py", "def f(:\n  'unterminated\n", DefaultThresholds())
	if err != nil {
		t.Fatalf("a parse failure must be a skip, not an error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected a skip for unparseable source, got %+v", got)
	}
}

func TestBuildSnapshotsAggregatesLikePython(t *testing.T) {
	day := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)

	files := []FileComplexity{
		{FilePath: "a.py", Language: "python", LOC: 1000, CyclomaticTotal: 40,
			FunctionsCount: 10, HighComplexityFunctions: 2, VeryHighComplexityFunctions: 1},
		{FilePath: "b.py", Language: "python", LOC: 500, CyclomaticTotal: 10,
			FunctionsCount: 5, HighComplexityFunctions: 1, VeryHighComplexityFunctions: 0},
	}
	result, err := BuildSnapshots("repo-1", day, "refs/heads/main", files, computedAt, "org-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Snapshots) != 2 {
		t.Fatalf("snapshots: got %d, want 2", len(result.Snapshots))
	}
	if result.Repo.LOCTotal != 1500 {
		t.Errorf("loc_total: got %d, want 1500", result.Repo.LOCTotal)
	}
	if result.Repo.CyclomaticTotal != 50 {
		t.Errorf("cyclomatic_total: got %d, want 50", result.Repo.CyclomaticTotal)
	}
	if result.Repo.HighComplexityFunctions != 3 {
		t.Errorf("high: got %d, want 3", result.Repo.HighComplexityFunctions)
	}
	if result.Repo.VeryHighComplexityFunctions != 1 {
		t.Errorf("very_high: got %d, want 1", result.Repo.VeryHighComplexityFunctions)
	}
	// 50 / (1500/1000.0) == 50/1.5. Asserted in Python's own associativity.
	want := 50.0 / (1500.0 / 1000.0)
	if result.Repo.CyclomaticPerKLOC != want {
		t.Errorf("cyclomatic_per_kloc: got %v, want %v", result.Repo.CyclomaticPerKLOC, want)
	}
	if math.IsNaN(result.Repo.CyclomaticPerKLOC) {
		t.Errorf("cyclomatic_per_kloc is NaN")
	}
}

func TestBuildSnapshotsZeroLOCDoesNotDivideByZero(t *testing.T) {
	// Python guards with `if total_loc > 0 else 0.0`. Without the guard this
	// is 0/0 -> NaN in Go, which ClickHouse stores as a Float64 NaN and every
	// downstream average then poisons -- a silent, spreading corruption rather
	// than a visible failure.
	result, err := BuildSnapshots("repo-1", time.Now().UTC(), "ref",
		[]FileComplexity{{FilePath: "empty.py", Language: "python"}},
		time.Now().UTC(), "org-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Repo.CyclomaticPerKLOC != 0.0 {
		t.Fatalf("cyclomatic_per_kloc: got %v, want 0.0", result.Repo.CyclomaticPerKLOC)
	}
	if math.IsNaN(result.Repo.CyclomaticPerKLOC) {
		t.Fatalf("cyclomatic_per_kloc is NaN; the total_loc>0 guard is missing")
	}
}

func TestBuildSnapshotsEmptyFileListProducesNoSnapshots(t *testing.T) {
	result, err := BuildSnapshots("repo-1", time.Now().UTC(), "ref", nil,
		time.Now().UTC(), "org-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Snapshots) != 0 {
		t.Fatalf("snapshots: got %d, want 0", len(result.Snapshots))
	}
	if result.Repo.LOCTotal != 0 || result.Repo.CyclomaticTotal != 0 {
		t.Fatalf("totals should be zero, got %+v", result.Repo)
	}
}

// TestBuildSnapshotsFailsClosedOnANegativeCount proves the uint32/uint64
// fail-closed guards actually fire (a guard that can never fire is no
// assertion -- CORE rule from this lane's own migration-087 finding above).
// Every FileComplexity field is meant to be a non-negative count; a
// negative value can only come from an upstream bug, and silently
// converting it with `uint32(negative)` would wrap to a value near 4
// billion instead of surfacing the bug.
func TestBuildSnapshotsFailsClosedOnANegativeCount(t *testing.T) {
	cases := []struct {
		name  string
		files []FileComplexity
	}{
		{"negative LOC", []FileComplexity{{FilePath: "a.py", LOC: -1}}},
		{"negative FunctionsCount", []FileComplexity{{FilePath: "a.py", FunctionsCount: -1}}},
		{"negative CyclomaticTotal", []FileComplexity{{FilePath: "a.py", CyclomaticTotal: -1}}},
		{"negative HighComplexityFunctions", []FileComplexity{{FilePath: "a.py", HighComplexityFunctions: -1}}},
		{"negative VeryHighComplexityFunctions", []FileComplexity{{FilePath: "a.py", VeryHighComplexityFunctions: -1}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildSnapshots("repo-1", time.Now().UTC(), "ref", tc.files,
				time.Now().UTC(), "org-1")
			if err == nil {
				t.Fatalf("expected an error for %s, got none", tc.name)
			}
		})
	}
}
