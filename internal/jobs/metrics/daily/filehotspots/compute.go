// Package filehotspots is the pure computation kernel for the file_hotspots
// and file_risk_hotspots metrics.daily families (CHAOS-4277) -- the second
// pair of families to leave the Python HTTP compatibility bridge after
// CHAOS-4275/CHAOS-4276's repo_user_commit/team_wellbeing reference
// implementations, and (like repouser) a thin pure-function kernel kept
// separate from any ClickHouse connection so it can be golden-tested without
// a database.
//
// # Fidelity
//
// Both functions port src/dev_health_ops/metrics/hotspots.py byte-for-byte:
//
//   - ComputeFileHotspots ports compute_file_hotspots: per-file churn/
//     contributors/commits aggregated from a window of commit-stat rows,
//     scored by hotspot_score = 0.4*log1p(churn) + 0.3*contributors +
//     0.3*commits_count.
//   - ComputeFileRiskHotspots ports compute_file_risk_hotspots: unions the
//     set of churned files (from the same window) with the set of files
//     carrying a static complexity snapshot, then scores each by
//     risk_score = z(churn) + z(complexity), where z() is the population
//     SAMPLE z-score (ddof=1, matching Python's `variance = sum((x-mean)**2
//     for x in values) / (n - 1)`) over the file set, 0 for every file when
//     fewer than 2 files exist or the sample has zero variance.
//
// Both functions skip AGGREGATE_STATS_MARKER ("__AGGREGATE__") rows -- the
// GitLab/GitHub backfill's aggregate-only commit-stat sentinel, which must
// never be ranked or persisted as a hotspot (CHAOS-2376 round-4: it is not a
// real file and would pollute the risk treemap and hide real files).
//
// Neither function filters window_stats by repo_id as a PRECONDITION: the
// Python originals take a repo_id and do that filtering internally (`if
// row["repo_id"] != repo_id: continue`), because job_daily.py hands them
// EVERY repo's rows for the whole per-window cache. This port keeps the same
// internal filter (never assumes the caller pre-scoped rows), so a caller
// that (like the native executors in the parent daily package) loads
// multiple repos' rows in one query can pass the whole batch through
// unfiltered for each repoID.
//
// # Output ordering
//
// Both functions sort their own output by score descending, exactly like
// Python's `sorted(..., reverse=True)`, which is a STABLE sort tie-broken by
// original (map/dict-insertion) order. This package does not reproduce that
// tie-break: Go map iteration order is randomized, so two files with an
// EXACTLY equal score can come back in either order here. No known caller of
// either function depends on order among tied scores (the daily/filehotspots
// callers sort or key by (repo_id, path) before writing/comparing), so this
// is a deliberate, narrow, callable-visible difference from Python's ordering
// -- not a numeric difference in any single row's fields.
package filehotspots

import (
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// aggregateStatsMarker mirrors dev_health_ops.utils.AGGREGATE_STATS_MARKER.
const aggregateStatsMarker = "__AGGREGATE__"

// hotspotAlpha/Beta/Gamma mirror hotspots.py's compute_file_hotspots weights
// (docs 1.3.2): hotspot_raw = alpha*log(1+churn) + beta*contributors +
// gamma*commit_count.
const (
	hotspotAlpha = 0.4
	hotspotBeta  = 0.3
	hotspotGamma = 0.3
)

// FileMetric is one file_metrics_daily row, mirroring
// dev_health_ops.metrics.schemas.FileMetricsRecord (minus repo_id/day/org_id,
// which the caller already knows and stamps).
type FileMetric struct {
	Path         string
	Churn        int
	Contributors int
	CommitsCount int
	HotspotScore float64
}

// ComplexitySnapshot is the subset of file_complexity_snapshots'
// argMax-per-file read that compute_file_risk_hotspots actually reads
// (`comp.cyclomatic_total`, `comp_obj.cyclomatic_avg` -- hotspots.py never
// reads language/loc/functions_count/high_complexity_functions/
// very_high_complexity_functions from a FileComplexitySnapshot). Mirrors
// job_daily.py's `_load_complexity_map_for_repo` return shape narrowed to
// what this kernel consumes.
type ComplexitySnapshot struct {
	CyclomaticTotal int
	CyclomaticAvg   float64
}

// RiskMetric is one file_hotspot_daily row, mirroring
// dev_health_ops.metrics.schemas.FileHotspotDaily (minus repo_id/day/org_id).
// BlameConcentration is nil exactly when Python's blame_map lookup returns
// None (either blame_map itself is nil/empty, mirroring `if blame_map:`
// short-circuiting on an empty dict same as None, or the specific path is
// absent from it).
type RiskMetric struct {
	FilePath           string
	ChurnLOC30d        int
	ChurnCommits30d    int
	CyclomaticTotal    int
	CyclomaticAvg      float64
	BlameConcentration *float64
	RiskScore          float64
}

// ComputeFileHotspots ports compute_file_hotspots (hotspots.py:39). Rows for
// repositories other than repoID are skipped, matching Python's own
// `row["repo_id"] != repo_id` guard -- windowStats need not be pre-scoped.
func ComputeFileHotspots(repoID uuid.UUID, windowStats []repouser.CommitStatRow) []FileMetric {
	type fileAgg struct {
		churn   int
		authors map[string]struct{}
		commits map[string]struct{}
	}
	files := make(map[string]*fileAgg)

	for _, row := range windowStats {
		if row.RepoID != repoID {
			continue
		}
		path := row.FilePath
		if path == "" || path == aggregateStatsMarker {
			continue
		}
		agg := files[path]
		if agg == nil {
			agg = &fileAgg{authors: make(map[string]struct{}), commits: make(map[string]struct{})}
			files[path] = agg
		}
		agg.churn += nonNegative(row.Additions) + nonNegative(row.Deletions)
		// Mirrors Python's `(author_email or author_name or "unknown").strip()`
		// EXACTLY, including its order of operations: the `or` chain selects
		// on raw truthiness -- a whitespace-only string is truthy in Python,
		// so it is SELECTED before being stripped down to "" -- not stripped
		// first and then treated as empty. Trimming before selecting (as an
		// earlier revision of this function did) would fall through to
		// author_name for a whitespace-only email instead of collapsing it
		// to the empty-string contributor key Python actually produces,
		// silently changing the contributor count (codex round 1, finding 5).
		author := row.AuthorEmail
		if author == "" {
			author = row.AuthorName
		}
		if author == "" {
			author = "unknown"
		}
		author = strings.TrimSpace(author)
		agg.authors[author] = struct{}{}
		agg.commits[row.CommitHash] = struct{}{}
	}

	records := make([]FileMetric, 0, len(files))
	for path, agg := range files {
		churn := agg.churn
		contributors := len(agg.authors)
		commitsCount := len(agg.commits)
		// float64(...) around each product is load-bearing (CHAOS-4818): Go
		// may otherwise fuse a weighted-sum term's multiply into the
		// following add on arm64, rounding once where CPython's
		// hotspot_score = (alpha*log1p(churn)) + (beta*contributors) +
		// (gamma*commits_count) rounds each term and each `+` separately.
		score := float64(hotspotAlpha*math.Log1p(float64(churn))) +
			float64(hotspotBeta*float64(contributors)) +
			float64(hotspotGamma*float64(commitsCount))
		records = append(records, FileMetric{
			Path: path, Churn: churn, Contributors: contributors,
			CommitsCount: commitsCount, HotspotScore: score,
		})
	}
	// Tie-break by path ascending (codex round 1, finding 3): Go builds
	// `files` from a map, whose iteration order is randomized per-process --
	// unlike Python's dict, which preserves first-seen insertion order and
	// so gives ties a REPRODUCIBLE (if window_stats-order-dependent) tie
	// order. A pure score sort here would make two equal-scored files swap
	// places between runs on the IDENTICAL input, which is its own
	// reproducibility defect independent of matching Python. This does not
	// reproduce Python's literal tie order (see package doc comment) but
	// does make Go's own output deterministic for a fixed input.
	sort.Slice(records, func(i, j int) bool {
		if records[i].HotspotScore != records[j].HotspotScore {
			return records[i].HotspotScore > records[j].HotspotScore
		}
		return records[i].Path < records[j].Path
	})
	return records
}

// ComputeFileRiskHotspots ports compute_file_risk_hotspots (hotspots.py:113).
// windowStats scoping and the AGGREGATE_STATS_MARKER skip mirror
// ComputeFileHotspots exactly (same source data, same sentinel).
// complexityMap and blameMap are both keyed by file path; either may be nil
// (an org/repo with no complexity snapshots or no blame data still produces
// churn-only rows, matching Python's `complexity_map.get(f)` /
// `if blame_map: blame_map.get(...)` defaulting).
func ComputeFileRiskHotspots(
	repoID uuid.UUID,
	windowStats []repouser.CommitStatRow,
	complexityMap map[string]ComplexitySnapshot,
	blameMap map[string]float64,
) []RiskMetric {
	type churnAgg struct {
		churn   int
		commits int
	}
	churnByPath := make(map[string]*churnAgg)

	for _, row := range windowStats {
		if row.RepoID != repoID {
			continue
		}
		path := row.FilePath
		if path == "" || path == aggregateStatsMarker {
			continue
		}
		agg := churnByPath[path]
		if agg == nil {
			agg = &churnAgg{}
			churnByPath[path] = agg
		}
		agg.churn += nonNegative(row.Additions) + nonNegative(row.Deletions)
		agg.commits++
	}

	allPaths := make(map[string]struct{}, len(churnByPath)+len(complexityMap))
	for path := range churnByPath {
		allPaths[path] = struct{}{}
	}
	for path := range complexityMap {
		allPaths[path] = struct{}{}
	}
	if len(allPaths) == 0 {
		// Matches Python's `return []` (hotspots.py:174) exactly -- an empty,
		// non-nil slice, not nil. No production caller distinguishes the two
		// today (every consumer only ranges/lens this result, both of which
		// treat nil and empty identically in Go), but returning the literal
		// Python-equivalent value removes any doubt rather than relying on
		// that being true forever (codex round 8).
		return []RiskMetric{}
	}

	// CHAOS-4863: sampleZScores' summation must consume churns/complexities
	// in a DETERMINISTIC order -- Go map iteration (like allPaths above) is
	// randomized per process, and floating-point summation is not
	// associative, so the SAME input could otherwise produce a DIFFERENT
	// risk_score on different runs of the same binary.
	//
	// The order chosen here is NOT an attempt to reproduce Python's own
	// order: compute_file_risk_hotspots (hotspots.py:151) builds its file
	// list from `set(churn_map.keys()) | set(complexity_map.keys())`, a
	// Python set, not a dict -- CPython's set iteration order for string
	// keys depends on hash randomization (PYTHONHASHSEED unset by
	// default) and is NOT fixed across process invocations either
	// (verified directly: the same set produced two different orderings
	// across two separate `python3 -c` runs). There is no "Python's
	// insertion order" here to replicate. What IS true, verified
	// empirically (30 separate python3 invocations, fresh hash seed each
	// time, on a fixed 60-file corpus spanning magnitudes 1 to 10**9):
	// risk_score was bit-identical across every run -- CPython's
	// Neumaier-compensated sum() (CHAOS-4824) is empirically very close to
	// order-invariant for realistic inputs, even though not provably so in
	// the fully general case. That means ANY well-defined, reproducible
	// Go order -- sorted lexicographically here, chosen for being
	// independent of any source ordering rather than for matching
	// Python's -- is expected to agree with Python's (order-invariant in
	// practice) output, PROVIDED Go's own summation (pythonparity.Sum,
	// inside sampleZScores) implements the identical compensated
	// algorithm.
	//
	// Disclosed, not hidden: neither Python's nor Go's compensated sum()
	// has ever been observed to diverge under reordering in this file's
	// testing -- the bit-pattern golden (risk_hotspots_order_golden_test.go)
	// covers cardinality/magnitude/permutation, and a standalone stress
	// probe (not checked in) ran 12,000+ trials across the same corpus
	// shapes plus the original small-n (3-5 file) construction that first
	// prompted this ticket, all with random reordering, zero divergence
	// found. This fix closes the STRUCTURAL risk (Go map iteration is
	// genuinely randomized; the language does not guarantee this can
	// never matter) rather than a bit-exact-proven regression -- unlike
	// most CHAOS-4818/4824 sites, this one has no red-on-baseline case in
	// the corpus tried. If a future corpus finds one, that is real new
	// evidence, not a retry-until-green target.
	sortedPaths := sortedFilePathUnion(allPaths)

	type input struct {
		path       string
		churn      int
		commits    int
		complexity int
		snapshot   *ComplexitySnapshot
	}
	items := make([]input, 0, len(allPaths))
	for _, path := range sortedPaths {
		item := input{path: path}
		if agg, ok := churnByPath[path]; ok {
			item.churn = agg.churn
			item.commits = agg.commits
		}
		if snapshot, ok := complexityMap[path]; ok {
			snapshotCopy := snapshot
			item.snapshot = &snapshotCopy
			item.complexity = snapshot.CyclomaticTotal
		}
		items = append(items, item)
	}

	churns := make([]float64, len(items))
	complexities := make([]float64, len(items))
	for index, item := range items {
		churns[index] = float64(item.churn)
		complexities[index] = float64(item.complexity)
	}
	zChurn := sampleZScores(churns)
	zComplexity := sampleZScores(complexities)

	results := make([]RiskMetric, 0, len(items))
	for index, item := range items {
		risk := zChurn[index] + zComplexity[index]
		var cyclomaticTotal int
		var cyclomaticAvg float64
		if item.snapshot != nil {
			cyclomaticTotal = item.snapshot.CyclomaticTotal
			cyclomaticAvg = item.snapshot.CyclomaticAvg
		}
		var blameConcentration *float64
		if len(blameMap) > 0 {
			if value, ok := blameMap[item.path]; ok {
				valueCopy := value
				blameConcentration = &valueCopy
			}
		}
		results = append(results, RiskMetric{
			FilePath: item.path, ChurnLOC30d: item.churn, ChurnCommits30d: item.commits,
			CyclomaticTotal: cyclomaticTotal, CyclomaticAvg: cyclomaticAvg,
			BlameConcentration: blameConcentration, RiskScore: risk,
		})
	}
	// Tie-break by path ascending -- same determinism rationale as
	// ComputeFileHotspots' sort above (codex round 1, finding 3).
	sort.Slice(results, func(i, j int) bool {
		if results[i].RiskScore != results[j].RiskScore {
			return results[i].RiskScore > results[j].RiskScore
		}
		return results[i].FilePath < results[j].FilePath
	})
	return results
}

// sortedFilePathUnion returns paths' keys in Go's byte-lexicographic string
// order (sort.Strings, i.e. plain []byte comparison -- NOT Unicode
// collation and NOT case-insensitive: "Z" (0x5A) sorts before "z" (0x7A)).
// Extracted as its own function (CHAOS-4863, codex round 1 P2, EXECUTED) so
// the ordering claim is directly testable on its own: TestSortedFilePathUnionIsByteLexicographic
// asserts the exact returned slice for a fixture containing both cases of
// the same letter, rather than only inferring the order indirectly through
// risk_score bit patterns -- which codex correctly pointed out could pass
// under a DIFFERENT (wrong) deterministic sort too, since compensated
// summation is order-invariant for many realistic inputs regardless of
// which well-defined order produced them.
func sortedFilePathUnion(paths map[string]struct{}) []string {
	sorted := make([]string, 0, len(paths))
	for path := range paths {
		sorted = append(sorted, path)
	}
	sort.Strings(sorted)
	return sorted
}

// sampleZScores ports hotspots.py's get_z_scores: population size < 2 or a
// zero sample standard deviation (ddof=1) both return all-zero, matching
// Python's own two early-return branches exactly (never a divide-by-zero
// NaN).
func sampleZScores(values []float64) []float64 {
	n := len(values)
	zeros := make([]float64, n)
	if n < 2 {
		return zeros
	}
	// CHAOS-4824: both reductions mirror a Python `sum()` over floats, which
	// is Neumaier-compensated since CPython 3.12 -- a naive Go `+=` loop is
	// not equivalent (16% disagreement on random 2-8 element inputs, per
	// pythonparity.Sum's doc comment). mean = sum(values) / n.
	mean := pythonparity.Sum(values) / float64(n)
	squaredDiffs := make([]float64, n)
	for i, v := range values {
		diff := v - mean
		// CHAOS-4818 note: this used to be a compound assignment
		// (`sumSquares += diff*diff`), an unguarded FMA-fusion site the
		// lint (fma_lint_test.go) now catches. CHAOS-4824's rewrite
		// (pythonparity.Sum below) eliminated the compound assignment
		// entirely -- a bare per-element multiply-and-store has no
		// adjacent +/- to fuse with, so no float64() guard is needed here
		// anymore. Confirmed: the lint reports this file clean.
		squaredDiffs[i] = diff * diff
	}
	// variance = sum((x - mean) ** 2 for x in values) / (n - 1)
	variance := pythonparity.Sum(squaredDiffs) / float64(n-1)
	stdev := math.Sqrt(variance)
	if stdev == 0 {
		return zeros
	}
	result := make([]float64, n)
	for index, v := range values {
		result[index] = (v - mean) / stdev
	}
	return result
}

func nonNegative(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

// DayBoundaries derives the [dayStart, dayEnd) and windowDays-inclusive
// [windowStart, dayEnd) bounds from a run's TargetDay, mirroring
// job_daily.py's `h_start_date = d - timedelta(days=29)` (30 calendar days
// inclusive of day d itself when windowDays=WindowDays) -- the SAME window
// repouser.RepoUserCommitExecutor already derives for the SAME TargetDay
// (repoUserCommitWindowDays), so both native executors compute identical
// window boundaries.
func DayBoundaries(targetDay time.Time, windowDays int) (dayStart, dayEnd, windowStart time.Time) {
	utc := targetDay.UTC()
	dayStart = time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd = dayStart.AddDate(0, 0, 1)
	windowStart = dayStart.AddDate(0, 0, -(windowDays - 1))
	return dayStart, dayEnd, windowStart
}

// WindowDays mirrors job_daily.py's h_start_date = d - timedelta(days=29): a
// 30-day inclusive window ending on the target day. This is the SAME window
// Python's h_commit_rows spans for both families (job_daily.py:1359,1400).
const WindowDays = 30
