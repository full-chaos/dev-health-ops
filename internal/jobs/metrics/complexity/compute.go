// Package complexity is the pure compute kernel for the `complexity` metrics
// family (CHAOS-4291). It turns file contents into the two rows the family
// stores -- file_complexity_snapshots and repo_complexity_daily -- with no
// ClickHouse connection and no I/O, so its parity with Python can be tested
// directly.
//
// It ports `ComplexityScanner` / `_build_snapshots`
// (src/dev_health_ops/analytics/complexity.py and
// src/dev_health_ops/metrics/job_complexity_db.py:_build_snapshots).
//
// SCOPE, PR1 (CHAOS-4971a): only `.py` is computed natively, through the pycc
// subpackage's port of radon. The other 20 languages Python routes to lizard
// are PR2 (CHAOS-4971b / CHAOS-5156), landing in three stacked PRs: 2a (this
// one) adds C and C++ through the lizardcc subpackage's port of lizard's
// CLikeReader; 2b and 2c add the rest. This package FAILS CLOSED on every
// language not yet registered in DefaultAnalyzers rather than skipping it --
// see AnalyzeFile.
package complexity

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/complexity/lizardcc"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/complexity/pycc"
)

// FileComplexity is one analysed file, mirroring the Python dataclass of the
// same name (analytics/complexity.py:77-85).
type FileComplexity struct {
	FilePath                    string
	Language                    string
	LOC                         int
	FunctionsCount              int
	CyclomaticTotal             int
	CyclomaticAvg               float64
	HighComplexityFunctions     int
	VeryHighComplexityFunctions int
}

// Thresholds are the high/very-high cutoffs. Python reads them from the
// scanner config with defaults 15 and 25 (analytics/complexity.py:91-92) and
// compares with a STRICT `>`, so a block exactly at the threshold is not
// counted.
type Thresholds struct {
	High     int
	VeryHigh int
}

// DefaultThresholds matches ComplexityAnalyzer.__init__'s defaults.
func DefaultThresholds() Thresholds { return Thresholds{High: 15, VeryHigh: 25} }

// ErrLanguageNotPorted reports a file whose language Python analyses with
// lizard and this package cannot yet compute.
//
// This is deliberately an ERROR and not a skip. Skipping would make a repo of
// TypeScript produce a clean, plausible, badly-undercounted row -- the same
// silent-zero shape that made release_impact report success while writing
// nothing (CHAOS-4243). Failing closed is also what keeps PR1 from being
// routable before PR2 lands: the family stays on the Python bridge, and
// nothing here can quietly stand in for it.
var ErrLanguageNotPorted = errors.New("complexity: language not ported natively yet")

// languageByExtension mirrors LANGUAGE_BY_EXTENSION
// (analytics/complexity.py). An extension absent from this map is NOT an
// error: Python's _analyze_content returns None for it, so the file is simply
// not analysed.
var languageByExtension = map[string]string{
	".py":    "python",
	".ts":    "typescript",
	".tsx":   "typescript",
	".js":    "javascript",
	".jsx":   "javascript",
	".mjs":   "javascript",
	".cjs":   "javascript",
	".go":    "go",
	".rs":    "rust",
	".java":  "java",
	".kt":    "kotlin",
	".kts":   "kotlin",
	".rb":    "ruby",
	".php":   "php",
	".c":     "c",
	".h":     "c",
	".cpp":   "cpp",
	".cc":    "cpp",
	".hpp":   "cpp",
	".cs":    "csharp",
	".swift": "swift",
	".scala": "scala",
	".m":     "objective-c",
	".mm":    "objective-c",
	".lua":   "lua",
	".vue":   "vue",
}

// LanguageFor returns the persisted `language` value for a path, and whether
// the extension is one the scanner analyses at all.
func LanguageFor(path string) (string, bool) {
	lang, ok := languageByExtension[strings.ToLower(filepath.Ext(path))]
	return lang, ok
}

// AnalyzerFunc computes the per-function cyclomatic complexities of one file.
//
// It returns the RAW per-function numbers, not a FileComplexity: every derived
// field (the total, the average, the two threshold counters) is computed once,
// language-agnostically, by BuildFileResult -- exactly as Python computes them
// once in _build_result for both the radon and lizard paths. An analyzer that
// derived its own would be free to drift from the other language's.
//
// skipped=true means "analysed nothing, and that is correct" -- Python's
// analyzers return None when the source cannot be parsed, which drops the file
// from the scan. It is NOT an error and must NOT become a zero row.
type AnalyzerFunc func(path, source string) (complexities []int, skipped bool, err error)

// PythonAnalyzer is the radon-equivalent analyzer (CHAOS-4971a).
func PythonAnalyzer(path, source string) ([]int, bool, error) {
	blocks, err := pycc.Visit(source, pycc.Options{})
	if err != nil {
		// Matches _analyze_python's bare `except Exception: return None`.
		// Surfacing this as an error would fail a partition Python completes.
		return nil, true, nil
	}
	complexities := make([]int, 0, len(blocks))
	for _, b := range blocks {
		complexities = append(complexities, b.Complexity)
	}
	return complexities, false, nil
}

// CFamilyAnalyzer is the lizard-equivalent analyzer for C and C++
// (CHAOS-5156 PR2a). Python's LANGUAGE_BY_EXTENSION maps both `.c`/`.h` and
// `.cpp`/`.cc`/`.hpp` to lizard's single CLikeReader (clike.py:24-28), so
// both languages share this one Go analyzer too -- there is no separate
// "C-only" or "C++-only" behaviour to diverge on.
func CFamilyAnalyzer(path, source string) ([]int, bool, error) {
	return lizardcc.Analyze(path, source)
}

// DefaultAnalyzers returns the analyzers available natively, keyed by the
// language name LanguageFor reports.
//
// PR1 (CHAOS-4971a) registered `python` only. PR2a (CHAOS-5156) added `c`
// and `cpp`. PR2253/go-rust added `go` and `rust` (the GoLikeStates clone-
// based infra). A later PR added `csharp`, `kotlin`, `scala`, `swift` and
// `java` (csharp and java both reuse PR2a's CLikeReader-family
// architecture via hooks; kotlin/scala/swift share go-rust's GoLikeStates
// infra). CHAOS-4291 adds `javascript` and `typescript` (TypeScriptReader's
// own state machine, lizardcc/typescript.go -- JavaScriptReader is
// TypeScriptReader unchanged in Python, so both keys share one Go
// analyzer). CHAOS-4291 also adds `ruby` (RubylikeReader's own state
// machine, lizardcc/ruby.go -- NOT a GoLikeStates subclass, see that
// file's package doc). `php`, `objective-c`, `lua` and `vue` remain for a
// follow-up stack in this same PR -- no other function, the dispatch, the
// result type, or the extension map needs to change for any of them.
func DefaultAnalyzers() map[string]AnalyzerFunc {
	return map[string]AnalyzerFunc{
		"python":     PythonAnalyzer,
		"c":          CFamilyAnalyzer,
		"cpp":        CFamilyAnalyzer,
		"go":         lizardcc.AnalyzeGo,
		"rust":       lizardcc.AnalyzeRust,
		"csharp":     lizardcc.AnalyzeCSharp,
		"java":       lizardcc.AnalyzeJava,
		"kotlin":     lizardcc.AnalyzeKotlin,
		"scala":      lizardcc.AnalyzeScala,
		"swift":      lizardcc.AnalyzeSwift,
		"javascript": lizardcc.AnalyzeJavaScript,
		"typescript": lizardcc.AnalyzeTypeScript,
		"ruby":       lizardcc.AnalyzeRuby,
	}
}

// BuildFileResult ports _build_result (analytics/complexity.py:240-259).
//
// Language-agnostic on purpose: radon and lizard produce different per-function
// numbers, but the derivation from those numbers to a stored row is identical,
// and Python does it in one place for both. Both threshold comparisons are
// STRICT `>`, so a function exactly at the threshold is not counted.
func BuildFileResult(
	filePath, language string, loc int, complexities []int, thresholds Thresholds,
) FileComplexity {
	count := len(complexities)
	total := 0
	high := 0
	veryHigh := 0
	for _, c := range complexities {
		total += c
		if c > thresholds.High {
			high++
		}
		if c > thresholds.VeryHigh {
			veryHigh++
		}
	}
	avg := 0.0
	if count > 0 {
		avg = float64(total) / float64(count)
	}
	return FileComplexity{
		FilePath:                    filePath,
		Language:                    language,
		LOC:                         loc,
		FunctionsCount:              count,
		CyclomaticTotal:             total,
		CyclomaticAvg:               avg,
		HighComplexityFunctions:     high,
		VeryHighComplexityFunctions: veryHigh,
	}
}

// AnalyzeFile computes one file's complexity using the natively available
// analyzers. See AnalyzeFileWith for the injectable form.
func AnalyzeFile(path, source string, thresholds Thresholds) (*FileComplexity, error) {
	return AnalyzeFileWith(path, source, thresholds, DefaultAnalyzers())
}

// AnalyzeFileWith is the dispatch seam: extension -> language -> analyzer.
//
// Return contract, matching Python's outcomes exactly:
//
//	(nil, nil)   the extension is not analysed at all -- _analyze_content
//	             returns None, and the file contributes nothing.
//	(nil, nil)   the analyzer skipped it (unparseable source). A parse failure
//	             is a SKIP, never a zero row: zero and absent are different
//	             rows downstream.
//	(result,nil) analysed.
//
// plus one outcome Python does not have:
//
//	(nil, ErrLanguageNotPorted) the extension IS analysed by Python, but no
//	native analyzer is registered for its language yet.
//
// That last case fails closed rather than skipping, deliberately. Skipping
// would emit a clean, plausible, badly-undercounted row for a TypeScript repo
// -- the silent-zero shape of CHAOS-4243 -- and would let this be routed
// before every language Python handles is covered.
func AnalyzeFileWith(
	path, source string, thresholds Thresholds, analyzers map[string]AnalyzerFunc,
) (*FileComplexity, error) {
	language, known := LanguageFor(path)
	if !known {
		return nil, nil
	}
	analyze, ok := analyzers[language]
	if !ok {
		return nil, fmt.Errorf("%w: %s (%s)", ErrLanguageNotPorted, path, language)
	}

	complexities, skipped, err := analyze(path, source)
	if err != nil {
		return nil, err
	}
	if skipped {
		return nil, nil
	}

	// LOC is counted the same way for every language: Python's `loc` is
	// len(code.splitlines()) in _build_result, regardless of analyzer.
	result := BuildFileResult(path, language, pycc.LineCount(source), complexities, thresholds)
	return &result, nil
}

// FileSnapshot is one file_complexity_snapshots row.
type FileSnapshot struct {
	RepoID                      string
	AsOfDay                     time.Time
	Ref                         string
	FilePath                    string
	Language                    string
	LOC                         uint32
	FunctionsCount              uint32
	CyclomaticTotal             uint32
	CyclomaticAvg               float64
	HighComplexityFunctions     uint32
	VeryHighComplexityFunctions uint32
	ComputedAt                  time.Time
	OrgID                       string
}

// RepoDaily is one repo_complexity_daily row.
type RepoDaily struct {
	RepoID                      string
	Day                         time.Time
	LOCTotal                    uint64
	CyclomaticTotal             uint64
	CyclomaticPerKLOC           float64
	HighComplexityFunctions     uint64
	VeryHighComplexityFunctions uint64
	ComputedAt                  time.Time
	OrgID                       string
}

// Result is one repository's output for one day.
type Result struct {
	Snapshots []FileSnapshot
	Repo      RepoDaily
}

// BuildSnapshots ports _build_snapshots (job_complexity_db.py).
//
// ONE DAY ONLY, deliberately. Python writes exactly one row for `date`
// regardless of backfill_days, because complexity has no historical snapshot
// storage: reusing one current-contents scan across a window would fabricate
// flat cyclomatic_per_kloc rows and flatline complexity_delta's 30-day trend
// (CHAOS-2850). A Go executor that "helpfully" filled the window would
// reintroduce exactly that fabrication, so this signature takes a single day
// and cannot express it.
func BuildSnapshots(
	repoID string,
	day time.Time,
	ref string,
	files []FileComplexity,
	computedAt time.Time,
	orgID string,
) (Result, error) {
	snapshots := make([]FileSnapshot, 0, len(files))

	var totalLOC, totalCC, totalHigh, totalVeryHigh int

	for _, f := range files {
		// FAIL CLOSED on the uint32 conversion. Every one of these fields is
		// a count derived from LineCount/len()/a strict-`>` threshold tally
		// in AnalyzeFile/BuildFileResult, so it can legitimately be 0 (an
		// empty file, a file with no functions) but can NEVER be negative.
		// A negative value here means a bug upstream produced impossible
		// data -- and `uint32(negative)` in Go does not error, it silently
		// WRAPS to a value near 4 billion. Writing that wrapped number to
		// file_complexity_snapshots would be a wildly wrong row that reads
		// as a real (if bizarre) measurement, not a visible failure -- the
		// same silent-corruption shape this family's other fail-closed
		// checks exist to prevent (see ErrLanguageNotPorted above).
		loc, err := nonNegativeUint32(f.LOC, "LOC", f.FilePath)
		if err != nil {
			return Result{}, err
		}
		functionsCount, err := nonNegativeUint32(f.FunctionsCount, "FunctionsCount", f.FilePath)
		if err != nil {
			return Result{}, err
		}
		cyclomaticTotal, err := nonNegativeUint32(f.CyclomaticTotal, "CyclomaticTotal", f.FilePath)
		if err != nil {
			return Result{}, err
		}
		high, err := nonNegativeUint32(f.HighComplexityFunctions, "HighComplexityFunctions", f.FilePath)
		if err != nil {
			return Result{}, err
		}
		veryHigh, err := nonNegativeUint32(f.VeryHighComplexityFunctions, "VeryHighComplexityFunctions", f.FilePath)
		if err != nil {
			return Result{}, err
		}

		snapshots = append(snapshots, FileSnapshot{
			RepoID:                      repoID,
			AsOfDay:                     day,
			Ref:                         ref,
			FilePath:                    f.FilePath,
			Language:                    f.Language,
			LOC:                         loc,
			FunctionsCount:              functionsCount,
			CyclomaticTotal:             cyclomaticTotal,
			CyclomaticAvg:               f.CyclomaticAvg,
			HighComplexityFunctions:     high,
			VeryHighComplexityFunctions: veryHigh,
			ComputedAt:                  computedAt,
			OrgID:                       orgID,
		})

		totalLOC += f.LOC
		totalCC += f.CyclomaticTotal
		totalHigh += f.HighComplexityFunctions
		totalVeryHigh += f.VeryHighComplexityFunctions
	}

	// Python: (total_cc / (total_loc / 1000.0)) if total_loc > 0 else 0.0.
	// Written in exactly that associativity -- dividing by (loc/1000) rather
	// than multiplying by 1000/loc -- because the two differ in the last
	// binary place, and this value is compared against Python's own output.
	perKLOC := 0.0
	if totalLOC > 0 {
		perKLOC = float64(totalCC) / (float64(totalLOC) / 1000.0)
	}

	// The repo-level totals go through the same fail-closed conversion,
	// widened to uint64 (a summed field can exceed uint32's range even
	// though no single file's count can).
	locTotal, err := nonNegativeUint64(totalLOC, "LOCTotal", repoID)
	if err != nil {
		return Result{}, err
	}
	cyclomaticTotalAll, err := nonNegativeUint64(totalCC, "CyclomaticTotal", repoID)
	if err != nil {
		return Result{}, err
	}
	highAll, err := nonNegativeUint64(totalHigh, "HighComplexityFunctions", repoID)
	if err != nil {
		return Result{}, err
	}
	veryHighAll, err := nonNegativeUint64(totalVeryHigh, "VeryHighComplexityFunctions", repoID)
	if err != nil {
		return Result{}, err
	}

	return Result{
		Snapshots: snapshots,
		Repo: RepoDaily{
			RepoID:                      repoID,
			Day:                         day,
			LOCTotal:                    locTotal,
			CyclomaticTotal:             cyclomaticTotalAll,
			CyclomaticPerKLOC:           perKLOC,
			HighComplexityFunctions:     highAll,
			VeryHighComplexityFunctions: veryHighAll,
			ComputedAt:                  computedAt,
			OrgID:                       orgID,
		},
	}, nil
}

// nonNegativeUint32 converts a count to uint32, failing closed on a
// negative input rather than letting Go's implicit conversion wrap it into
// a large positive value near the top of uint32's range.
func nonNegativeUint32(v int, field, filePath string) (uint32, error) {
	if v < 0 {
		return 0, fmt.Errorf(
			"complexity: %s=%d is negative for %q; refusing to convert to "+
				"uint32 (would silently wrap to a huge value instead of erroring)",
			field, v, filePath,
		)
	}
	return uint32(v), nil
}

// nonNegativeUint64 is nonNegativeUint32's counterpart for the repo-level
// summed totals, which are widened to uint64 because a sum across many
// files can exceed uint32's range even though no single file's count can.
func nonNegativeUint64(v int, field, repoID string) (uint64, error) {
	if v < 0 {
		return 0, fmt.Errorf(
			"complexity: %s=%d is negative for repo %q; refusing to convert "+
				"to uint64 (would silently wrap to a huge value instead of erroring)",
			field, v, repoID,
		)
	}
	return uint64(v), nil
}
