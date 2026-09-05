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
// are PR2 (CHAOS-4971b). This package FAILS CLOSED on them rather than
// skipping them -- see AnalyzeFile.
package complexity

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

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

// AnalyzeFile computes one file's complexity.
//
// Return contract, matching Python's three outcomes exactly:
//
//	(nil, nil)   the extension is not analysed at all -- _analyze_content
//	             returns None, and the file contributes nothing.
//	(nil, nil)   the source could not be parsed -- _analyze_python catches
//	             every exception from cc_visit and returns None, dropping the
//	             file. A parse failure is a SKIP, never a zero row.
//	(result,nil) analysed.
//
// plus one outcome Python does not have:
//
//	(nil, ErrLanguageNotPorted) a lizard language, deferred to PR2.
func AnalyzeFile(path, source string, thresholds Thresholds) (*FileComplexity, error) {
	language, known := LanguageFor(path)
	if !known {
		return nil, nil
	}
	if language != "python" {
		return nil, fmt.Errorf("%w: %s (%s)", ErrLanguageNotPorted, path, language)
	}

	blocks, err := pycc.Visit(source, pycc.Options{})
	if err != nil {
		// Deliberately swallowed, to match _analyze_python's bare
		// `except Exception: return None`. The file is dropped from the
		// day's scan, exactly as Python drops it -- reporting an error here
		// would make Go fail a partition that Python completes.
		return nil, nil
	}

	count := len(blocks)
	total := pycc.TotalComplexity(blocks)
	avg := 0.0
	if count > 0 {
		avg = float64(total) / float64(count)
	}

	return &FileComplexity{
		FilePath:                    path,
		Language:                    language,
		LOC:                         pycc.LineCount(source),
		FunctionsCount:              count,
		CyclomaticTotal:             total,
		CyclomaticAvg:               avg,
		HighComplexityFunctions:     pycc.CountAbove(blocks, thresholds.High),
		VeryHighComplexityFunctions: pycc.CountAbove(blocks, thresholds.VeryHigh),
	}, nil
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
) Result {
	snapshots := make([]FileSnapshot, 0, len(files))

	var totalLOC, totalCC, totalHigh, totalVeryHigh int

	for _, f := range files {
		snapshots = append(snapshots, FileSnapshot{
			RepoID:                      repoID,
			AsOfDay:                     day,
			Ref:                         ref,
			FilePath:                    f.FilePath,
			Language:                    f.Language,
			LOC:                         uint32(f.LOC),
			FunctionsCount:              uint32(f.FunctionsCount),
			CyclomaticTotal:             uint32(f.CyclomaticTotal),
			CyclomaticAvg:               f.CyclomaticAvg,
			HighComplexityFunctions:     uint32(f.HighComplexityFunctions),
			VeryHighComplexityFunctions: uint32(f.VeryHighComplexityFunctions),
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

	return Result{
		Snapshots: snapshots,
		Repo: RepoDaily{
			RepoID:                      repoID,
			Day:                         day,
			LOCTotal:                    uint64(totalLOC),
			CyclomaticTotal:             uint64(totalCC),
			CyclomaticPerKLOC:           perKLOC,
			HighComplexityFunctions:     uint64(totalHigh),
			VeryHighComplexityFunctions: uint64(totalVeryHigh),
			ComputedAt:                  computedAt,
			OrgID:                       orgID,
		},
	}
}
