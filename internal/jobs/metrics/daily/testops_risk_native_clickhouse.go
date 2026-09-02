package daily

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// -----------------------------------------------------------------------
// Fidelity notes (CHAOS-4294)
//
// The Python authority is:
//   - src/dev_health_ops/metrics/compute_testops.py
//     (compute_pipeline_metrics_daily, compute_test_metrics_daily,
//     compute_coverage_metrics_daily -- these three families,
//     testops_pipeline/testops_test/testops_coverage, families.json,
//     CHAOS-4284 -- stay "pending"/bridge; ported as the sibling
//     internal/jobs/metrics/testops package, which THIS file loads raw
//     rows for and calls, purely as an in-memory input to testops_risk's
//     own three functions below. See that package's doc comment for why
//     it is a separate, exported package rather than private helpers here
//     -- CHAOS-4284 is meant to import and reuse it verbatim.)
//   - src/dev_health_ops/metrics/compute_testops_risk.py
//     (compute_release_confidence, compute_quality_drag,
//     compute_pipeline_stability -- the family THIS file's
//     compute{ReleaseConfidence,QualityDrag,PipelineStability} port and
//     TestopsRiskExecutor writes.)
//   - src/dev_health_ops/metrics/loaders/clickhouse.py
//     (load_testops_pipeline_data, load_testops_test_data,
//     load_testops_historical_failed_case_names, load_testops_coverage_data)
//
// WHY the pending families are re-derived here instead of read back from
// ClickHouse: job_daily.py computes testops_pipeline_metrics/
// testops_test_metrics/testops_coverage_metrics as LOCAL, IN-PROCESS values
// (job_daily.py:1602-1626) and feeds them directly into
// compute_release_confidence/compute_quality_drag/compute_pipeline_stability
// a few lines later (job_daily.py:1904-1927) -- it never re-reads
// testops_{pipeline,test,coverage}_metrics_daily from ClickHouse. Go's
// PartitionHandler.Work runs every native family BEFORE the one combined
// Python compatibility-bridge call (daily.go: computeNativeFamilies() then
// compatibility.ComputePartition()), so on the day this partition is
// computing, those bridge-written tables do not have TODAY's rows yet --
// only a prior day's, if any. Reading them back from ClickHouse here would
// silently score testops_risk against stale or missing inputs. Recomputing
// the same aggregation Python performs in-process, from the same raw
// tables, is what keeps this native path row-identical.
//
// SCOPE PER CALL: the Python bridge invokes run_daily_metrics_job once PER
// repo_id (worker_metrics.py:1729, CHAOS-4264) with backfill_days=1, so
// `days = [target_day]` and every local variable -- including the
// "pipeline_metrics_buffer" pipeline_stability reads -- is fresh per call.
// In production this buffer therefore holds AT MOST the current day's own
// row for that one repo: pipeline_stability's "7-day rolling window" is a
// real capability of the underlying function, but the live per-repo/per-day
// partition call site never gives it more than one day of history. This is
// a LATENT PYTHON DEFECT (team-lead, 2026-09-01): the buffer was clearly
// meant to accumulate across days, and does not in the live per-repo/
// per-day call shape. This executor reproduces that exact scope --
// one repo, one day, no synthetic history -- rather than "fixing" it,
// since fixing it here would silently diverge from Python's actual output
// until a ticket rules on the intended behavior. See TestopsRiskExecutor's
// doc comment for the loop structure, and CHAOS-4294's ticket comments.
// -----------------------------------------------------------------------

// testopsRiskConn is the narrow ClickHouse capability this file needs.
type testopsRiskConn interface {
	Query(context.Context, string, ...any) (driver.Rows, error)
}

// testopsRiskBatchConn is the narrow write capability writeTestopsRisk needs.
type testopsRiskBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// testopsLoaderMaxRows ports _testops_loader_max_rows (loaders/clickhouse.py:99):
// same env var, same default, same "non-positive or unparseable falls back
// to the default" behavior.
func testopsLoaderMaxRows() int {
	const defaultMaxRows = 200_000
	raw, err := envInt("DEV_HEALTH_TESTOPS_LOADER_MAX_ROWS", defaultMaxRows)
	if err != nil || raw <= 0 {
		return defaultMaxRows
	}
	return raw
}

// errTestopsRowCapExceeded ports TestopsRowCapExceeded (loaders/clickhouse.py:110):
// a bounded, classified refusal rather than an unbounded read. ComputeFamily
// returning this error is not a partition failure -- PartitionHandler's
// fail-open native-family policy (daily.go computeNativeFamilies) simply
// leaves testops_risk off the skip list for this partition, so the Python
// compatibility bridge computes and writes it exactly as it would have
// before this executor existed (the bridge enforces the identical cap on
// its own read).
type errTestopsRowCapExceeded struct {
	table   string
	maxRows int
	fetched int
}

func (err *errTestopsRowCapExceeded) Error() string {
	return fmt.Sprintf(
		"testops_row_cap_exceeded: table=%q max_rows=%d fetched>=%d -- refusing to compute testops metrics on a partial/truncated result",
		err.table, err.maxRows, err.fetched,
	)
}

// -----------------------------------------------------------------------
// Loaders -- ports of ClickHouseDataLoader.load_testops_* (loaders/clickhouse.py),
// building the internal/jobs/metrics/testops package's row types directly.
// Every loader here is scoped to exactly ONE repo (this executor's own
// per-repo loop, mirroring the Python bridge's per-repo_id call) and one
// organization, unlike the Python methods' optional org-wide mode -- a
// native executor is always constructed for one run.OrganizationID, so the
// "repo_id is None" / "self.org_id is empty" branches those methods carry
// for admin/backfill tooling are dropped here as genuinely unreachable from
// this call site.
//
// job_runs (ci_job_runs) is deliberately NOT loaded: compute_pipeline_metrics_daily
// receives job_runs only to `del` it immediately (compute_testops.py:123) --
// it is unused by the family this executor ports.
// -----------------------------------------------------------------------

func loadTestopsPipelineRuns(
	ctx context.Context, conn testopsRiskConn, orgID string, repoID uuid.UUID, start, end time.Time,
) ([]testops.PipelineRunRow, error) {
	rows, err := conn.Query(ctx, `
SELECT status, queued_at, started_at, finished_at, duration_seconds, queue_seconds,
       retry_count, team_id, service_id, org_id
FROM ci_pipeline_runs FINAL
WHERE started_at >= ? AND started_at < ? AND repo_id = ? AND org_id = ?`,
		start.UTC(), end.UTC(), repoID, orgID)
	if err != nil {
		return nil, fmt.Errorf("load testops pipeline runs: %w", err)
	}
	defer rows.Close()

	var result []testops.PipelineRunRow
	for rows.Next() {
		row := testops.PipelineRunRow{RepoID: repoID}
		if err := rows.Scan(
			&row.Status, &row.QueuedAt, &row.StartedAt, &row.FinishedAt,
			&row.DurationSeconds, &row.QueueSeconds, &row.RetryCount,
			&row.TeamID, &row.ServiceID, &row.OrgID,
		); err != nil {
			return nil, fmt.Errorf("scan testops pipeline run: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate testops pipeline runs: %w", err)
	}
	return result, nil
}

// loadTestopsSuiteAndCaseRows ports load_testops_test_data
// (loaders/clickhouse.py:1344), including its two-query semi-join shape:
// suites in [start,end) for this repo, then cases whose run has SOME suite
// in [start,end) for this repo AND whose OWN suite starts before `end`
// (the day-boundary guard load_testops_test_data:1459 documents). Row-cap
// enforced on both, suites first (mirrors the Python ordering rationale).
func loadTestopsSuiteAndCaseRows(
	ctx context.Context, conn testopsRiskConn, orgID string, repoID uuid.UUID, start, end time.Time,
) ([]testops.SuiteRow, []testops.CaseRow, error) {
	maxRows := testopsLoaderMaxRows()
	limit := maxRows + 1

	suiteRows, err := conn.Query(ctx, `
SELECT repo_id, run_id, suite_id, total_count, passed_count, failed_count, skipped_count,
       error_count, quarantined_count, duration_seconds, started_at, finished_at,
       team_id, service_id, org_id
FROM test_suite_results FINAL
WHERE coalesce(started_at, finished_at) >= ? AND coalesce(started_at, finished_at) < ?
  AND repo_id = ? AND org_id = ?
LIMIT ?`,
		start.UTC(), end.UTC(), repoID, orgID, uint64(limit))
	if err != nil {
		return nil, nil, fmt.Errorf("load testops suite results: %w", err)
	}
	var suites []testops.SuiteRow
	for suiteRows.Next() {
		var row testops.SuiteRow
		if err := suiteRows.Scan(
			&row.RepoID, &row.RunID, &row.SuiteID, &row.TotalCount, &row.PassedCount,
			&row.FailedCount, &row.SkippedCount, &row.ErrorCount, &row.QuarantinedCount,
			&row.DurationSeconds, &row.StartedAt, &row.FinishedAt, &row.TeamID, &row.ServiceID, &row.OrgID,
		); err != nil {
			suiteRows.Close()
			return nil, nil, fmt.Errorf("scan testops suite result: %w", err)
		}
		suites = append(suites, row)
	}
	suiteErr := suiteRows.Err()
	suiteRows.Close()
	if suiteErr != nil {
		return nil, nil, fmt.Errorf("iterate testops suite results: %w", suiteErr)
	}
	if len(suites) > maxRows {
		return nil, nil, &errTestopsRowCapExceeded{table: "test_suite_results", maxRows: maxRows, fetched: len(suites)}
	}

	caseRows, err := conn.Query(ctx, `
SELECT c.repo_id, c.run_id, c.suite_id, c.case_name, c.status, c.retry_attempt
FROM test_case_results AS c FINAL
WHERE (c.repo_id, c.run_id) IN (
  SELECT repo_id, run_id FROM test_suite_results FINAL
  WHERE coalesce(started_at, finished_at) >= ? AND coalesce(started_at, finished_at) < ?
    AND repo_id = ? AND org_id = ?
)
AND (c.repo_id, c.run_id, c.suite_id) IN (
  SELECT repo_id, run_id, suite_id FROM test_suite_results FINAL
  WHERE coalesce(started_at, finished_at) < ?
    AND repo_id = ? AND org_id = ?
)
AND c.repo_id = ? AND c.org_id = ?
LIMIT ?`,
		start.UTC(), end.UTC(), repoID, orgID,
		end.UTC(), repoID, orgID,
		repoID, orgID, uint64(limit))
	if err != nil {
		return nil, nil, fmt.Errorf("load testops case results: %w", err)
	}
	var cases []testops.CaseRow
	for caseRows.Next() {
		var row testops.CaseRow
		if err := caseRows.Scan(&row.RepoID, &row.RunID, &row.SuiteID, &row.CaseName, &row.Status, &row.RetryAttempt); err != nil {
			caseRows.Close()
			return nil, nil, fmt.Errorf("scan testops case result: %w", err)
		}
		cases = append(cases, row)
	}
	caseErr := caseRows.Err()
	caseRows.Close()
	if caseErr != nil {
		return nil, nil, fmt.Errorf("iterate testops case results: %w", caseErr)
	}
	if len(cases) > maxRows {
		return nil, nil, &errTestopsRowCapExceeded{table: "test_case_results", maxRows: maxRows, fetched: len(cases)}
	}
	return suites, cases, nil
}

// loadHistoricalFailedCaseNames ports load_testops_historical_failed_case_names
// (loaders/clickhouse.py:1498), narrowed to one repo: distinct case names
// that failed in [start,end) EXCLUDING any run_id that also has a suite in
// [end,currentDayEnd) -- the same day-boundary run_id exclusion that
// prevents a straddling run from being double counted as both "today" and
// "historical".
func loadHistoricalFailedCaseNames(
	ctx context.Context, conn testopsRiskConn, orgID string, repoID uuid.UUID, start, end, currentDayEnd time.Time,
) (map[string]struct{}, error) {
	maxRows := testopsLoaderMaxRows()
	limit := maxRows + 1

	rows, err := conn.Query(ctx, `
SELECT DISTINCT c.case_name AS case_name
FROM test_case_results AS c FINAL
INNER JOIN test_suite_results AS s FINAL
  ON (s.repo_id = c.repo_id) AND (s.run_id = c.run_id) AND (s.suite_id = c.suite_id) AND (s.org_id = c.org_id)
WHERE coalesce(s.started_at, s.finished_at) >= ? AND coalesce(s.started_at, s.finished_at) < ?
  AND lower(trim(c.status)) IN (?, ?, ?, ?, ?, ?)
  AND (s.repo_id, s.run_id) NOT IN (
    SELECT repo_id, run_id FROM test_suite_results FINAL
    WHERE coalesce(started_at, finished_at) >= ? AND coalesce(started_at, finished_at) < ?
      AND repo_id = ? AND org_id = ?
  )
  AND s.repo_id = ? AND s.org_id = ?
LIMIT ?`,
		start.UTC(), end.UTC(),
		"failure", "failed", "error", "errors", "timeout", "timed_out",
		end.UTC(), currentDayEnd.UTC(), repoID, orgID,
		repoID, orgID, uint64(limit))
	if err != nil {
		return nil, fmt.Errorf("load testops historical failed case names: %w", err)
	}
	defer rows.Close()

	result := make(map[string]struct{})
	count := 0
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan testops historical failed case name: %w", err)
		}
		count++
		if name != "" {
			result[name] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate testops historical failed case names: %w", err)
	}
	if count > maxRows {
		return nil, &errTestopsRowCapExceeded{table: "test_case_results:historical_names", maxRows: maxRows, fetched: count}
	}
	return result, nil
}

// loadTestopsCoverageSnapshots ports load_testops_coverage_data
// (loaders/clickhouse.py:1665): coverage snapshots joined to a pipeline run
// starting in [start,end), for one repo.
func loadTestopsCoverageSnapshots(
	ctx context.Context, conn testopsRiskConn, orgID string, repoID uuid.UUID, start, end time.Time,
) ([]testops.CoverageSnapshotRow, error) {
	rows, err := conn.Query(ctx, `
SELECT c.repo_id, c.run_id, c.snapshot_id, c.lines_total, c.lines_covered,
       c.line_coverage_pct, c.branch_coverage_pct, c.team_id, c.service_id, c.org_id
FROM coverage_snapshots AS c FINAL
INNER JOIN ci_pipeline_runs AS p FINAL
  ON (p.repo_id = c.repo_id) AND (p.run_id = c.run_id) AND (p.org_id = c.org_id)
WHERE p.started_at >= ? AND p.started_at < ? AND p.repo_id = ? AND p.org_id = ?`,
		start.UTC(), end.UTC(), repoID, orgID)
	if err != nil {
		return nil, fmt.Errorf("load testops coverage snapshots: %w", err)
	}
	defer rows.Close()

	var result []testops.CoverageSnapshotRow
	for rows.Next() {
		var row testops.CoverageSnapshotRow
		if err := rows.Scan(
			&row.RepoID, &row.RunID, &row.SnapshotID, &row.LinesTotal, &row.LinesCovered,
			&row.LineCoveragePct, &row.BranchCoveragePct, &row.TeamID, &row.ServiceID, &row.OrgID,
		); err != nil {
			return nil, fmt.Errorf("scan testops coverage snapshot: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate testops coverage snapshots: %w", err)
	}
	return result, nil
}

// median mirrors compute.py's module-level _median (compute.py:43) -- used
// by computePipelineStability's median-recovery-time below. The pipeline/
// test/coverage compute itself (including its own median/percentile use)
// lives in internal/jobs/metrics/testops now.
func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

// pyRound ports Python's round(x, ndigits): correctly-rounded decimal at
// ndigits, ties-to-even, based on the double's EXACT binary value -- not
// "multiply, round half away from zero, divide", which disagrees with
// Python at exact .5 boundaries. strconv.FormatFloat('f', ndigits, 64) is
// Go's own correctly-rounded (round-to-even on exact ties) decimal
// conversion of the same IEEE754 double, so re-parsing it reproduces
// CPython's round() for every finite value tested against it in this
// package's tests. Matches Python's OWN round() (not builtins.round on a
// Decimal), which is what every _clamp/round(..., 4)/round(..., 2) call in
// compute_testops_risk.py and compute_testops.py uses.
func pyRound(value float64, ndigits int) float64 {
	if value != value || value > 1e300 || value < -1e300 { // NaN/huge guard, never expected here
		return value
	}
	formatted := strconv.FormatFloat(value, 'f', ndigits, 64)
	rounded, err := strconv.ParseFloat(formatted, 64)
	if err != nil {
		return value
	}
	return rounded
}

// pyMin2 and pyMax2 replicate CPython's two-argument min()/max() comparison
// order exactly: the FIRST argument is the running candidate, and it is
// replaced only on a strict "less than" (min) / "greater than" (max)
// comparison against the second argument -- never on a tie, and never when
// the comparison is unorderable (NaN). This matters because Go's `<`/`>`
// on NaN are both always false, same as Python's, but CPython's algorithm
// starts from a specific argument (not "value"), so a NaN input silently
// resolves to whichever bound CPython happened to start from -- see
// clampUnit below.
func pyMin2(a, b float64) float64 {
	if b < a {
		return b
	}
	return a
}

func pyMax2(a, b float64) float64 {
	if b > a {
		return b
	}
	return a
}

// pyOrZero ports Python's `value or 0.0` idiom (compute_testops_risk.py:55,
// 58, 133, 136 -- `cov.line_coverage_pct or 0.0`, `cov.coverage_delta_pct or
// 0.0`, `pipe.median_duration_seconds or 0.0`, `pipe.avg_queue_seconds or
// 0.0`), a DIFFERENT non-finite-semantics gap than clampUnit's (codex round
// 5, P2, EXECUTED): Python's `bool(float)` is `value != 0.0`, which is
// False (falsy) for BOTH +0.0 and -0.0 -- not just for a missing/None
// value. `X or 0.0` therefore silently collapses a genuine -0.0 to +0.0,
// same as it does for a nil-turned-zero, while NaN and +-Inf are all
// truthy (`bool(nan)` is True; NaN's own `!=` is defined True by IEEE754,
// unlike its `<`/`>`/`==`) and pass through unchanged. A naive Go
// `if ptr != nil { v = *ptr }` only replaces a missing (nil) value -- it
// leaves a genuine -0.0 reading unchanged, diverging from Python's silent
// sign-normalization. Reachable: `ci_pipeline_runs.duration_seconds`/
// `queue_seconds` and `coverage_snapshots.line_coverage_pct` are all
// unconstrained `Nullable(Float64)`, so a stored -0.0 is a real input, not
// hypothetical -- EXECUTED repro: Go's `median_duration_seconds` field kept
// a `-0.0` (sign bit set) where live Python's `pipe.median_duration_seconds
// or 0.0` produces `0.0`.
func pyOrZero(value float64) float64 {
	if value == 0 {
		return 0.0
	}
	return value
}

// clampUnit ports Python's `_clamp(value, lo=0.0, hi=1.0) -> max(lo, min(hi,
// value))` (compute_testops_risk.py:20-21) bit-for-bit, including its NaN
// behavior. A naive `if value < 0 { 0 } else if value > 1 { 1 } else {
// value }` looks equivalent but is NOT: for NaN, both Go comparisons are
// false, so it falls through and returns NaN -- unlike Python, which always
// resolves NaN to 1.0 here (min(1.0, nan) keeps its first arg 1.0 since
// `nan < 1.0` is false; max(0.0, 1.0) then keeps 1.0 since `1.0 > 0.0` is
// true). Verified against a live `python3` interpreter (codex round 4,
// P2 EXECUTED finding): `max(0.0, min(1.0, float('nan'))) == 1.0`.
// coverage_snapshots.line_coverage_pct is an unconstrained Nullable(Float64)
// and reaches this function via coveragePct/100.0, making NaN a real input.
func clampUnit(value float64) float64 {
	return pyMax2(0.0, pyMin2(1.0, value))
}

// factorsJSONField is one key/value pair of a factors_json payload, kept as
// an ordered slice (not a map) because Python's json.dumps on a dict
// preserves INSERTION order while Go's encoding/json on a map sorts keys
// alphabetically -- a real divergence risk against the byte-identical
// row this ticket must prove. isFloat/precision select formatting: Python's
// json.dumps prints a Python int with no decimal point and a Python float
// (even a whole number like 5.0) WITH one, mirrored here by isFloat.
type factorsJSONField struct {
	key      string
	floatVal float64
	intVal   int
	isFloat  bool
}

func ff(key string, value float64) factorsJSONField {
	return factorsJSONField{key: key, floatVal: value, isFloat: true}
}

func fi(key string, value int) factorsJSONField {
	return factorsJSONField{key: key, intVal: value, isFloat: false}
}

// factorsJSON ports json.dumps(factors) as compute_testops_risk.py's two
// factors dicts build it: default separators (", " and ": "), no
// sort_keys, insertion order preserved.
func factorsJSON(fields []factorsJSONField) string {
	var b strings.Builder
	b.WriteByte('{')
	for index, field := range fields {
		if index > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('"')
		b.WriteString(field.key)
		b.WriteString("\": ")
		if field.isFloat {
			b.WriteString(pythonFloatJSON(field.floatVal))
		} else {
			b.WriteString(strconv.Itoa(field.intVal))
		}
	}
	b.WriteByte('}')
	return b.String()
}

// pythonFloatJSON mirrors how Python's json module serializes a float:
// float.__repr__'s SHORTEST round-trip decimal digit string (David Gay's
// algorithm) -- the same well-defined function of a double's bit pattern
// Go's strconv.FormatFloat(-1 precision) computes, so the DIGITS always
// agree -- but Python and Go pick fixed-vs-scientific NOTATION by different
// rules given those same digits. Go's 'g' verb switches to scientific once
// the exponent reaches the shortest digit COUNT (e.g. 1_000_000.0, a single
// significant digit, prints as "1e+06"); CPython's float_repr
// (Objects/floatobject.c via pystrtod.c, mode 0) switches to scientific
// only when the decimal exponent of the leading digit is < -4 or >= 16,
// regardless of how many significant digits there are -- so 1_000_000.0
// stays "1000000.0" and only reaches "1e+16"-style notation at 10**16.
// Codex round 2 (P2, EXECUTED) caught the earlier strconv.FormatFloat('g',
// -1, 64) implementation emitting "1e+06" for a value Python renders
// "1000000.0" -- a real byte-level factors_json divergence for any
// duration/queue-seconds value at or above 1e6. This reimplements Python's
// OWN notation rule on top of Go's shortest-digit scientific form
// (strconv.FormatFloat(value, 'e', -1, 64)) rather than trying to coax the
// 'g'/'f' verbs into matching a different threshold.
func pythonFloatJSON(value float64) string {
	// codex round 3 (P2, ARGUED, confirmed by source read): strconv.FormatFloat
	// with 'e' never contains the byte 'e' for NaN/+-Inf ("NaN", "+Inf",
	// "-Inf"), so the un-guarded IndexByte(...'e') lookup below returned -1
	// and `scientific[:eIndex]` PANICKED (slice bounds out of range [:-1]) --
	// a full process crash, not a returned error, bypassing the native
	// family's fail-open/refused-telemetry path entirely (daily.go's
	// computeNativeFamilies only degrades gracefully on a returned error).
	// coverage_snapshots.line_coverage_pct is an unconstrained
	// Nullable(Float64) with no finite-value guard on the Python writer
	// side, so a NaN (e.g. a 0/0 division upstream) is real, representable
	// input, not a hypothetical one. Python's own json.dumps (default
	// allow_nan=True) emits the literal tokens "NaN"/"Infinity"/"-Infinity"
	// for these -- not valid JSON per the spec, but exactly what the Python
	// authority this port must match byte-for-byte actually writes.
	if math.IsNaN(value) {
		return "NaN"
	}
	if math.IsInf(value, 1) {
		return "Infinity"
	}
	if math.IsInf(value, -1) {
		return "-Infinity"
	}
	if value == 0 {
		if math.Signbit(value) {
			return "-0.0"
		}
		return "0.0"
	}
	negative := value < 0
	magnitude := value
	if negative {
		magnitude = -value
	}
	// Shortest round-trip scientific form, e.g. "1e+06", "1.234e+02",
	// "9.42e+02" -- digits before 'e' are exactly Python's own dtoa digits.
	scientific := strconv.FormatFloat(magnitude, 'e', -1, 64)
	eIndex := strings.IndexByte(scientific, 'e')
	mantissa := scientific[:eIndex]
	exponent, err := strconv.Atoi(scientific[eIndex+1:])
	if err != nil {
		// Unreachable for a value strconv itself just formatted; fail soft
		// to Go's own rendering rather than panic on a malformed parse.
		formatted := strconv.FormatFloat(value, 'g', -1, 64)
		if !strings.ContainsAny(formatted, ".eE") {
			formatted += ".0"
		}
		return formatted
	}
	digits := strings.Replace(mantissa, ".", "", 1)

	var rendered string
	if exponent >= -4 && exponent < 16 {
		rendered = pythonFixedNotation(digits, exponent)
	} else {
		rendered = pythonScientificNotation(digits, exponent)
	}
	if negative {
		return "-" + rendered
	}
	return rendered
}

// pythonFixedNotation renders `digits` (the significant-digit string, no
// sign, no decimal point) with its leading digit at decimal exponent `exp`
// as plain fixed notation, matching CPython's format_float_short fixed-mode
// branch -- always keeping a decimal point (json.dumps(5.0) -> "5.0").
func pythonFixedNotation(digits string, exp int) string {
	if exp >= 0 {
		if len(digits) <= exp+1 {
			return digits + strings.Repeat("0", exp+1-len(digits)) + ".0"
		}
		return digits[:exp+1] + "." + digits[exp+1:]
	}
	return "0." + strings.Repeat("0", -exp-1) + digits
}

// pythonScientificNotation renders `digits` at decimal exponent `exp` as
// Python's json module would: lowercase "e", explicit sign, minimum
// two-digit exponent (e.g. "1e-05", "1e+16"), matching CPython's
// format_float_short scientific-mode branch.
func pythonScientificNotation(digits string, exp int) string {
	mantissa := digits[:1]
	if len(digits) > 1 {
		mantissa += "." + digits[1:]
	}
	sign := "+"
	magnitude := exp
	if magnitude < 0 {
		sign = "-"
		magnitude = -magnitude
	}
	exponentDigits := strconv.Itoa(magnitude)
	if len(exponentDigits) < 2 {
		exponentDigits = "0" + exponentDigits
	}
	return mantissa + "e" + sign + exponentDigits
}

// -----------------------------------------------------------------------
// Output rows -- ports of ReleaseConfidenceRecord/QualityDragRecord/
// PipelineStabilityRecord (testops_schemas.py:421,440,458).
// -----------------------------------------------------------------------

type testopsReleaseConfidenceRow struct {
	RepoID                uuid.UUID
	Day                   time.Time
	ConfidenceScore       float64
	PipelineSuccessFactor float64
	TestPassFactor        float64
	CoverageFactor        float64
	FlakePenalty          float64
	RegressionPenalty     float64
	FactorsJSON           string
	TeamID                *string
	ServiceID             *string
	OrgID                 string
	ComputedAt            time.Time
}

type testopsQualityDragRow struct {
	RepoID                  uuid.UUID
	Day                     time.Time
	DragHours               float64
	FailureReworkHours      float64
	FlakeInvestigationHours float64
	QueueWaitHours          float64
	RetryOverheadHours      float64
	FactorsJSON             string
	TeamID                  *string
	ServiceID               *string
	OrgID                   string
	ComputedAt              time.Time
}

type testopsPipelineStabilityRow struct {
	RepoID                    uuid.UUID
	Day                       time.Time
	StabilityIndex            float64
	SuccessRate7d             float64
	SuccessRateTrend          float64
	FailureClusteringScore    float64
	MedianRecoveryTimeSeconds *float64
	TeamID                    *string
	ServiceID                 *string
	OrgID                     string
	ComputedAt                time.Time
}

// computeReleaseConfidence ports compute_release_confidence
// (compute_testops_risk.py:24), narrowed to one repo: pipe/test/cov are
// this repo's single representative rows (or nil), matching
// pipe_by_repo.get(repo_id) after Python's dict-overwrite collapse -- see
// this file's package doc comment on WHY a multi-team/service pipeline
// list collapses to its LAST entry. Returns nil when none of pipe/test/cov
// is present, matching Python's repo_ids union check.
func computeReleaseConfidence(
	repoID uuid.UUID, day time.Time, pipe *testops.PipelineMetric, test *testops.TestMetric, cov *testops.CoverageMetric, computedAt time.Time,
) *testopsReleaseConfidenceRow {
	if pipe == nil && test == nil && cov == nil {
		return nil
	}
	var successRate, passRate, flakeRate, failureRecurrence float64
	var coveragePct, coverageDelta float64
	if pipe != nil {
		successRate = pipe.SuccessRate
	}
	if test != nil {
		passRate = test.PassRate
		flakeRate = test.FlakeRate
		failureRecurrence = test.FailureRecurrence
	}
	if cov != nil && cov.LineCoveragePct != nil {
		coveragePct = pyOrZero(*cov.LineCoveragePct)
	}
	if cov != nil && cov.CoverageDeltaPct != nil {
		coverageDelta = pyOrZero(*cov.CoverageDeltaPct)
	}

	pipelineFactor := 0.4 * successRate
	testFactor := 0.3 * passRate
	covFactor := 0.2 * clampUnit(coveragePct/100.0)
	flakeFactor := 0.1 * (1.0 - flakeRate)
	baseScore := pipelineFactor + testFactor + covFactor + flakeFactor

	flakePenalty := 0.0
	if flakeRate > 0.05 {
		flakePenalty = 0.1
	}
	regressionPenalty := 0.0
	if coverageDelta < -2.0 {
		regressionPenalty += 0.05
	}
	if failureRecurrence > 0.3 {
		regressionPenalty += 0.1
	}
	score := clampUnit(baseScore - flakePenalty - regressionPenalty)

	factors := factorsJSON([]factorsJSONField{
		ff("pipeline_success_rate", pyRound(successRate, 4)),
		ff("test_pass_rate", pyRound(passRate, 4)),
		ff("coverage_pct", pyRound(coveragePct, 2)),
		ff("flake_rate", pyRound(flakeRate, 4)),
		ff("failure_recurrence", pyRound(failureRecurrence, 4)),
		ff("coverage_delta_pct", pyRound(coverageDelta, 2)),
		ff("base_score", pyRound(baseScore, 4)),
		ff("flake_penalty", pyRound(flakePenalty, 4)),
		ff("regression_penalty", pyRound(regressionPenalty, 4)),
	})

	row := &testopsReleaseConfidenceRow{
		RepoID: repoID, Day: day,
		ConfidenceScore:       pyRound(score, 4),
		PipelineSuccessFactor: pyRound(pipelineFactor, 4),
		TestPassFactor:        pyRound(testFactor, 4),
		CoverageFactor:        pyRound(covFactor, 4),
		FlakePenalty:          pyRound(flakePenalty, 4),
		RegressionPenalty:     pyRound(regressionPenalty, 4),
		FactorsJSON:           factors,
		ComputedAt:            computedAt,
	}
	if pipe != nil {
		row.TeamID, row.ServiceID, row.OrgID = pipe.TeamID, pipe.ServiceID, pipe.OrgID
	} else if test != nil {
		row.TeamID, row.ServiceID, row.OrgID = test.TeamID, test.ServiceID, test.OrgID
	}
	return row
}

// computeQualityDrag ports compute_quality_drag (compute_testops_risk.py:110).
// Unlike release confidence, coverage is never an input and the repo-id
// eligibility set is pipe/test only (compute_testops_risk.py:117-126).
func computeQualityDrag(
	repoID uuid.UUID, day time.Time, pipe *testops.PipelineMetric, test *testops.TestMetric, computedAt time.Time,
) *testopsQualityDragRow {
	if pipe == nil && test == nil {
		return nil
	}
	var medianDur float64
	var failureCount, pipelinesCount int
	var avgQueue, rerunRate float64
	if pipe != nil {
		if pipe.MedianDurationSeconds != nil {
			medianDur = pyOrZero(*pipe.MedianDurationSeconds)
		}
		failureCount = pipe.FailureCount
		pipelinesCount = pipe.PipelinesCount
		if pipe.AvgQueueSeconds != nil {
			avgQueue = pyOrZero(*pipe.AvgQueueSeconds)
		}
		rerunRate = pipe.RerunRate
	}
	var flakeRate float64
	var totalCases int
	if test != nil {
		flakeRate = test.FlakeRate
		totalCases = test.TotalCases
	}

	failureReworkHours := float64(failureCount) * medianDur / 3600.0
	flakeInvestigationHours := flakeRate * float64(totalCases) * 0.25
	queueWaitHours := float64(pipelinesCount) * avgQueue / 3600.0
	retryOverheadHours := rerunRate * float64(pipelinesCount) * medianDur / 3600.0
	dragHours := failureReworkHours + flakeInvestigationHours + queueWaitHours + retryOverheadHours

	factors := factorsJSON([]factorsJSONField{
		fi("failure_count", failureCount),
		ff("median_duration_seconds", pyRound(medianDur, 2)),
		fi("pipelines_count", pipelinesCount),
		ff("avg_queue_seconds", pyRound(avgQueue, 2)),
		ff("rerun_rate", pyRound(rerunRate, 4)),
		ff("flake_rate", pyRound(flakeRate, 4)),
		fi("total_cases", totalCases),
	})

	row := &testopsQualityDragRow{
		RepoID: repoID, Day: day,
		DragHours:               pyRound(dragHours, 4),
		FailureReworkHours:      pyRound(failureReworkHours, 4),
		FlakeInvestigationHours: pyRound(flakeInvestigationHours, 4),
		QueueWaitHours:          pyRound(queueWaitHours, 4),
		RetryOverheadHours:      pyRound(retryOverheadHours, 4),
		FactorsJSON:             factors,
		ComputedAt:              computedAt,
	}
	if pipe != nil {
		row.TeamID, row.ServiceID, row.OrgID = pipe.TeamID, pipe.ServiceID, pipe.OrgID
	} else if test != nil {
		row.TeamID, row.ServiceID, row.OrgID = test.TeamID, test.ServiceID, test.OrgID
	}
	return row
}

// computePipelineStability ports compute_pipeline_stability
// (compute_testops_risk.py:185), scoped to ONE repo: dayEntries is every
// pipeline-metric row this repo produced for the partition's day (there can
// be more than one -- see testops.ComputePipelineMetrics's doc comment on
// (team_id, service_id) grouping), in the SAME order
// testops.ComputePipelineMetrics returned them (mirrors Python's stable
// sort by `.day`, which is a no-op tie for same-day rows and so preserves
// pipeline_metrics_buffer's own insertion order). Returns nil when
// dayEntries is empty, matching `if n == 0: continue`.
func computePipelineStability(repoID uuid.UUID, day time.Time, dayEntries []testops.PipelineMetric, computedAt time.Time) *testopsPipelineStabilityRow {
	n := len(dayEntries)
	if n == 0 {
		return nil
	}
	// CHAOS-4824: pythonparity.Sum replaces every naive `+=` accumulation
	// below -- CPython's builtin sum() has been Neumaier-compensated since
	// 3.12, and a naive Go loop disagrees ~16-26% of the time on small
	// fractional inputs (see pythonparity.Sum's own doc comment). Every
	// term is collected into a slice first so pythonparity.Sum can
	// reproduce CPython's algorithm exactly, rather than accumulating in
	// the loop -- see weightedSuccessRate7d/successRateTrendFromRates
	// (which also carry the CHAOS-4818 FMA guards this same code needed
	// before extraction).
	successRates := make([]float64, n)
	for i, m := range dayEntries {
		successRates[i] = m.SuccessRate
	}
	successRate7d := weightedSuccessRate7d(successRates)

	successRateTrend := 0.0
	if n >= 2 {
		successRateTrend = successRateTrendFromRates(successRates)
	}

	consecutiveFailures := 0
	totalFailures := 0
	for i, m := range dayEntries {
		if m.FailureCount > 0 {
			totalFailures++
			if i > 0 && dayEntries[i-1].FailureCount > 0 {
				consecutiveFailures++
			}
		}
	}
	failureClustering := 0.0
	if totalFailures > 0 {
		denom := totalFailures
		if denom < 1 {
			denom = 1
		}
		failureClustering = float64(consecutiveFailures) / float64(denom)
	}

	var durations []float64
	for _, m := range dayEntries {
		if m.MedianDurationSeconds != nil && m.FailureCount > 0 {
			durations = append(durations, *m.MedianDurationSeconds)
		}
	}
	var medianRecovery *float64
	if len(durations) > 0 {
		v := median(durations)
		medianRecovery = &v
	}

	trendComponent := successRateTrend
	if trendComponent > 0.1 {
		trendComponent = 0.1
	}
	stability := clampUnit(successRate7d * (1.0 - failureClustering) * (1.0 + trendComponent))

	latest := dayEntries[n-1]
	row := &testopsPipelineStabilityRow{
		RepoID: repoID, Day: day,
		StabilityIndex:         pyRound(stability, 4),
		SuccessRate7d:          pyRound(successRate7d, 4),
		SuccessRateTrend:       pyRound(successRateTrend, 4),
		FailureClusteringScore: pyRound(failureClustering, 4),
		TeamID:                 latest.TeamID,
		ServiceID:              latest.ServiceID,
		OrgID:                  latest.OrgID,
		ComputedAt:             computedAt,
	}
	if medianRecovery != nil {
		v := pyRound(*medianRecovery, 2)
		row.MedianRecoveryTimeSeconds = &v
	}
	return row
}

// weightedSuccessRate7d ports the UNROUNDED weighted-average half of
// compute_pipeline_stability (compute_testops_risk.py:200-204):
//
//	weights = [1.0 + i * 0.5 for i in range(n)]
//	total_weight = sum(weights)
//	success_rate_7d = sum(m.success_rate * w for m, w in zip(days_data, weights)) / total_weight
//
// Extracted from computePipelineStability into its own testable function
// (CHAOS-4824) so a live-Python golden can assert the exact bit pattern of
// the value BEFORE computePipelineStability rounds it to 4 decimals for
// storage -- the rounded, stored value only rarely differs between naive
// and compensated summation (measured: 0 divergences in 200,000 random
// 3-8 element fractional trials, since 4-decimal rounding is far coarser
// than the few-ULP difference the two algorithms produce), so a golden
// against the STORED value would almost never go red on the naive
// baseline. The unrounded value is where the defect is actually visible,
// matching every other CHAOS-4818/4824 bit-pattern golden in this repo.
//
// CHAOS-4824: every reduction below mirrors a Python `sum()` over floats,
// which is Neumaier-compensated since CPython 3.12 -- NOT the naive
// left-to-right `total += x` a Go loop would otherwise do (16% disagreement
// on random 2-8 element inputs, per pythonparity.Sum's own doc comment).
// Every term is collected into a slice first so pythonparity.Sum can
// reproduce CPython's algorithm exactly, rather than accumulating in the
// loop.
func weightedSuccessRate7d(successRates []float64) float64 {
	n := len(successRates)
	weights := make([]float64, n)
	for i := range successRates {
		// float64(...) is load-bearing (CHAOS-4818, site 9 on PR #2106,
		// carried forward here since this branch's extraction predates that
		// fix landing on main): Go may otherwise fuse this into one FMA on
		// arm64, rounding once where CPython's
		// `weights = [1.0 + i * 0.5 for i in range(n)]` rounds the multiply
		// and the add separately.
		weights[i] = 1.0 + float64(float64(i)*0.5)
	}
	totalWeight := pythonparity.Sum(weights) // sum(weights)

	weightedRates := make([]float64, n)
	for i, rate := range successRates {
		weightedRates[i] = rate * weights[i]
	}
	// sum(m.success_rate * w for m, w in zip(days_data, weights)) / total_weight
	return pythonparity.Sum(weightedRates) / totalWeight
}

// successRateTrendFromRates ports the UNROUNDED linear-regression-slope
// half of compute_pipeline_stability (compute_testops_risk.py:206-215),
// for n >= 2 (the caller's guard):
//
//	x_mean = (n - 1) / 2.0
//	y_mean = sum(m.success_rate for m in days_data) / n
//	num = sum((i - x_mean) * (m.success_rate - y_mean) for i, m in enumerate(days_data))
//	den = sum((i - x_mean) ** 2 for i in range(n))
//	success_rate_trend = num / den if den > 0 else 0.0
//
// See weightedSuccessRate7d's doc comment for why this returns the
// UNROUNDED value and why every sum below uses pythonparity.Sum.
func successRateTrendFromRates(successRates []float64) float64 {
	n := len(successRates)
	// FMA note (lane-4441 review, CHAOS-4818): the compiler strength-reduces
	// this division into `0.5 * float64(n-1)` and fuses that multiply into
	// the `float64(i) - xMean` subtraction below (FMSUBD, confirmed via
	// `go tool objdump` on arm64) -- NOT into numTerms[i]/denTerms[i]'s own
	// multiplies, which stay unfused. This is harmless today ONLY because
	// multiplying by the power-of-two constant 0.5 is exact for every
	// integer (verified 0..100000 plus the 2**52/2**53 boundaries, 0
	// inexact) -- round(i - 0.5*k) and round(i - round(0.5*k)) are then the
	// same value regardless of fusion. That equivalence breaks if this
	// divisor ever stops being a power of two (e.g. /3.0 is inexact for
	// ~67% of integers in the same range) -- if xMean's formula changes,
	// re-verify this fusion is still exact rather than assuming it.
	xMean := float64(n-1) / 2.0
	// sum(m.success_rate for m in days_data) / n
	yMean := pythonparity.Sum(successRates) / float64(n)
	numTerms := make([]float64, n)
	denTerms := make([]float64, n)
	for i, rate := range successRates {
		numTerms[i] = (float64(i) - xMean) * (rate - yMean)
		denTerms[i] = (float64(i) - xMean) * (float64(i) - xMean)
	}
	// sum((i - x_mean) * (m.success_rate - y_mean) for i, m in enumerate(days_data))
	num := pythonparity.Sum(numTerms)
	// sum((i - x_mean) ** 2 for i in range(n))
	den := pythonparity.Sum(denTerms)
	if den > 0 {
		return num / den
	}
	return 0.0
}

// -----------------------------------------------------------------------
// Write -- ports write_release_confidence/write_quality_drag/
// write_pipeline_stability (sinks/clickhouse/ci.py:190, wellbeing.py:84,106).
// One PrepareBatch/Send per table, exactly the column order those methods
// insert -- these are plain (non-Replacing) MergeTree tables, so this is a
// pure append, matching the append-only + reader-argMax-dedup contract the
// rest of metrics.daily's native families already use.
// -----------------------------------------------------------------------

func writeTestopsRisk(
	ctx context.Context, conn testopsRiskBatchConn, organizationID string,
	releaseConfidence []testopsReleaseConfidenceRow,
	qualityDrag []testopsQualityDragRow,
	pipelineStability []testopsPipelineStabilityRow,
) (int, error) {
	written := 0
	if len(releaseConfidence) > 0 {
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO testops_release_confidence (
			repo_id, day, confidence_score, pipeline_success_factor, test_pass_factor,
			coverage_factor, flake_penalty, regression_penalty, factors_json,
			team_id, service_id, org_id, computed_at)`)
		if err != nil {
			return written, fmt.Errorf("prepare testops_release_confidence batch: %w", err)
		}
		for _, row := range releaseConfidence {
			if err := batch.Append(
				row.RepoID, chDate(row.Day), row.ConfidenceScore, row.PipelineSuccessFactor,
				row.TestPassFactor, row.CoverageFactor, row.FlakePenalty, row.RegressionPenalty,
				row.FactorsJSON, row.TeamID, row.ServiceID, organizationID, row.ComputedAt.UTC(),
			); err != nil {
				return written, fmt.Errorf("append testops_release_confidence row: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return written, fmt.Errorf("send testops_release_confidence batch: %w", err)
		}
		written += len(releaseConfidence)
	}

	if len(qualityDrag) > 0 {
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO testops_quality_drag (
			repo_id, day, drag_hours, failure_rework_hours, flake_investigation_hours,
			queue_wait_hours, retry_overhead_hours, factors_json,
			team_id, service_id, org_id, computed_at)`)
		if err != nil {
			return written, fmt.Errorf("prepare testops_quality_drag batch: %w", err)
		}
		for _, row := range qualityDrag {
			if err := batch.Append(
				row.RepoID, chDate(row.Day), row.DragHours, row.FailureReworkHours,
				row.FlakeInvestigationHours, row.QueueWaitHours, row.RetryOverheadHours,
				row.FactorsJSON, row.TeamID, row.ServiceID, organizationID, row.ComputedAt.UTC(),
			); err != nil {
				return written, fmt.Errorf("append testops_quality_drag row: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return written, fmt.Errorf("send testops_quality_drag batch: %w", err)
		}
		written += len(qualityDrag)
	}

	if len(pipelineStability) > 0 {
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO testops_pipeline_stability (
			repo_id, day, stability_index, success_rate_7d, success_rate_trend,
			failure_clustering_score, median_recovery_time_seconds,
			team_id, service_id, org_id, computed_at)`)
		if err != nil {
			return written, fmt.Errorf("prepare testops_pipeline_stability batch: %w", err)
		}
		for _, row := range pipelineStability {
			if err := batch.Append(
				row.RepoID, chDate(row.Day), row.StabilityIndex, row.SuccessRate7d,
				row.SuccessRateTrend, row.FailureClusteringScore, row.MedianRecoveryTimeSeconds,
				row.TeamID, row.ServiceID, organizationID, row.ComputedAt.UTC(),
			); err != nil {
				return written, fmt.Errorf("append testops_pipeline_stability row: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return written, fmt.Errorf("send testops_pipeline_stability batch: %w", err)
		}
		written += len(pipelineStability)
	}

	return written, nil
}

func chDate(day time.Time) time.Time {
	return time.Date(day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC)
}
