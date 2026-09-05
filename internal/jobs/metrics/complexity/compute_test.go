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

func TestAnalyzeFileFailsClosedOnLizardLanguages(t *testing.T) {
	// PR1 ports .py only. Every other analysed extension must ERROR rather
	// than skip: a skip would emit a plausible, badly-undercounted row for a
	// TypeScript repo, and would let this executor be routed before PR2.
	for _, path := range []string{"a.ts", "b.go", "c.java", "d.rb", "e.vue"} {
		got, err := AnalyzeFile(path, "function f() { if (a && b) return 1; }", DefaultThresholds())
		if !errors.Is(err, ErrLanguageNotPorted) {
			t.Errorf("%s: expected ErrLanguageNotPorted, got err=%v", path, err)
		}
		if got != nil {
			t.Errorf("%s: expected no row alongside the error, got %+v", path, got)
		}
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
	result := BuildSnapshots("repo-1", day, "refs/heads/main", files, computedAt, "org-1")

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
	result := BuildSnapshots("repo-1", time.Now().UTC(), "ref",
		[]FileComplexity{{FilePath: "empty.py", Language: "python"}},
		time.Now().UTC(), "org-1")

	if result.Repo.CyclomaticPerKLOC != 0.0 {
		t.Fatalf("cyclomatic_per_kloc: got %v, want 0.0", result.Repo.CyclomaticPerKLOC)
	}
	if math.IsNaN(result.Repo.CyclomaticPerKLOC) {
		t.Fatalf("cyclomatic_per_kloc is NaN; the total_loc>0 guard is missing")
	}
}

func TestBuildSnapshotsEmptyFileListProducesNoSnapshots(t *testing.T) {
	result := BuildSnapshots("repo-1", time.Now().UTC(), "ref", nil,
		time.Now().UTC(), "org-1")
	if len(result.Snapshots) != 0 {
		t.Fatalf("snapshots: got %d, want 0", len(result.Snapshots))
	}
	if result.Repo.LOCTotal != 0 || result.Repo.CyclomaticTotal != 0 {
		t.Fatalf("totals should be zero, got %+v", result.Repo)
	}
}
