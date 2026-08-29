// Package operatingreview is the Go port of the `operatingReview` GraphQL
// root (DORA weekly review), CHAOS-4352 plan Wave 4 Lane B (CHAOS-4505).
// Two Python source files, both read at and cited against origin/main tip
// 4464c83c1 -- verified BYTE-IDENTICAL to the feature branch this port was
// cut from (`git diff origin/main -- <file>` empty for both) before any
// line was copied, per the epic's Wave 4 standing rule that a port must
// copy the Python source's CURRENT tip, never a stale one:
//
//   - dev_health_ops.metrics.operating_review
//     (ops/src/dev_health_ops/metrics/operating_review.py, last touched on
//     main at 19efcbd4c2) -- the query-building + pure compute layer.
//     build_operating_review_queries (lines 101-368) is the source of the
//     ten ClickHouse queries fetchPeriodRows below runs verbatim (subject
//     to the ONE declared, deliberate exception: fetchAIGovernance, see its
//     own doc comment). compute_operating_review (371-405) and everything
//     it calls (408-902) is the source Resolve/computeReview below port.
//   - dev_health_ops.api.graphql.resolvers.operating_review
//     (ops/src/dev_health_ops/api/graphql/resolvers/operating_review.py,
//     last touched on main at b37ebccdab4) -- the GraphQL-facing
//     orchestration layer: resolve_operating_review (29-63) and
//     _fetch_period_rows (66-96), the source Resolve/fetchPeriodRows below
//     port.
//
// Ported deliberately verbatim, with ONE declared exception (fetchAIGovernance):
//
//  1. Both periods, ten tables each: fetchPeriodRows runs the SAME ten
//     queries twice -- once for [weekStart, weekStart+7d) ("current"), once
//     for [weekStart-7d, weekStart) ("prior") -- exactly mirroring
//     resolve_operating_review's two _fetch_period_rows calls (lines 42-55)
//     and week_bounds/prior_week_start (metrics/operating_review.py:93-98).
//
//  2. Every read collapses the append-only source table with
//     argMax(<col>, computed_at) grouped at the table's natural key BEFORE
//     any outer SUM/AVG/MIN/MAX -- the same "reader dedups, never FINAL"
//     discipline every prior wave's port already documents. Three of the
//     ten tables (work_item_metrics_daily, ai_impact_metrics_daily,
//     ai_governance_coverage_daily) are ReplacingMergeTree(computed_at);
//     the other seven are plain MergeTree. Both groups are read through the
//     identical argMax collapse -- confirmed by reading every CREATE TABLE
//     in src/dev_health_ops/migrations/clickhouse/ against every FROM
//     target in build_operating_review_queries before writing a single
//     query below (CHAOS-4505's DO-THIS-FIRST audit, reported and accepted
//     before this file was started; no CHAOS-4515/4516-class gate exists
//     for this operation).
//
//  3. Per-table error isolation: resolve_operating_review's
//     _fetch_period_rows (lines 66-96) wraps EACH of the ten queries in its
//     own try/except, logs, and defaults that table's rows to empty on
//     failure -- a single missing/broken table degrades that table's
//     section, not the whole response. This is a REAL, deliberate
//     divergence from every other operation ported so far (reviewedges,
//     cognitiveload, complexityTimeseries, hotspots all propagate a
//     ClickHouse error as a real GraphQL error, no swallow). fetchPeriodRows
//     below reproduces the per-table swallow exactly, at the SAME
//     per-query granularity Python has (9 tables can succeed while 1
//     fails -- never all-or-nothing) -- see its own doc comment.
//
//     THIS PORT'S SECOND EXPECTED DIVERGENCE, ruled by the orchestrator: the
//     swallow ships with the shape UNCHANGED (still a partial response, not
//     a hard error -- switching that would be a user-visible product
//     decision, not a porting one) but made LOUD, which Python is not:
//     errSwallow emits an error-level log AND an otel counter
//     (devhealth_query_api_operating_review_fetch_swallowed_total, tagged
//     by table key) on every swallowed failure. This is an OBSERVABILITY
//     divergence, not a VALUE divergence -- unlike fetchAIGovernance's fix
//     (which changes what a metric COMPUTES), this changes only what an
//     operator can SEE; the response a client receives is unaffected. The
//     reasoning matters: this exact swallow-with-no-signal shape is why
//     fetchAIGovernance's defect (CHAOS-4527) survived undetected -- an
//     unexecutable query became a silent 0.0 with nothing to alert on.
//     Porting the swallow without the telemetry would faithfully reproduce
//     the mechanism that hid that defect; porting it WITH telemetry keeps
//     response parity while ending the invisibility. No degradedReason
//     field was added to OperatingReview -- the type has none today, and
//     adding one is an API contract change outside this port's mandate
//     (filed as a separate question, not decided here).
//
//  4. ORDER BY totality (CHAOS-4505 brief item 2, reported to and confirmed
//     by the orchestrator before this file was written): only one of the
//     ten queries (work_items) returns multiple rows with an ORDER BY
//     (`ORDER BY day`, a total order -- day is both the GROUP BY key and
//     unique in the result). Three more (state_durations, investment,
//     ai_impact) return multiple rows with NO ORDER BY at all -- verified
//     SAFE, not a CHAOS-4472/L515-class gap, for two independently
//     sufficient reasons: (a) there is NO LIMIT anywhere in the file (zero
//     hits, confirmed by grep) so an unordered GROUP BY still returns its
//     full, deterministic row SET, never a truncated/varying subset; (b)
//     the response's only ordered lists (`changed`/`improved`/`worsened`
//     per section, `recommendations` on the whole review) are built by
//     iterating a CODE-DEFINED metrics list at each _section call site
//     (_delivery_section..._ai_workflow_section, lines 408-673), never by
//     iterating query rows -- so no client-visible order is SQL-row-order-
//     dependent in the first place. computeReview below reproduces that
//     same code-defined iteration order, not a data-dependent one.
//
// ONE declared exception -- fetchAIGovernance is a Go-side FIX, not a
// verbatim port, per the epic's Wave 4 standing rule ("Python readers are
// not maintained; a Go copy that carries the same defect as Python is not
// parity, it is the SAME bug in both planes"). See fetchAIGovernance's own
// doc comment for the full defect, the fix, and the resulting EXPECTED
// DUAL-RUN DIVERGENCE (Python: ai_governance_coverage/ai_opportunity_signals
// pinned to 0.0 in every real response today; Go: the real ratio). This was
// reported to and confirmed by the orchestrator (option (a): fix in Go,
// declare the divergence) before being written.
//
// Side effects: none to replicate -- every query in this package is
// read-only. Verified by reading resolve_operating_review/
// _fetch_period_rows/compute_operating_review and everything they call, top
// to bottom: ClickHouse reads and pure dataclass construction only, no
// writes, no telemetry/audit hook inside the resolver itself (telemetry
// lives one layer up, in graph/telemetry.go, same convention every prior
// wave already established).
//
// Authorization: resolve_operating_review's Strawberry field
// (api/graphql/schema.py:641-651) accepts an `org_id: str` GraphQL argument
// (matching the wire contract's `operatingReview(orgId: String!, ...)`,
// itself matching web/src/lib/graphql/queries.ts's OPERATING_REVIEW_QUERY,
// which sends $orgId) -- but NEVER passes it to resolve_operating_review,
// which takes only `input`. The org used for every query is
// `require_org_id(context)` alone (resolvers/operating_review.py:37) -- the
// SAME "authorized org always wins, a client-supplied orgId argument is
// parsed but never trusted for scoping" behavior cognitiveload/reviewedges/
// hotspots already document, just with an extra unused wire parameter this
// operation's schema happens to carry (shared by several not-yet-ported
// Query fields in contracts/graphql/v1/schema.graphql, not specific to this
// operation). Resolve below reproduces this by construction: it has no
// orgID-from-caller parameter at all, only the caller's OWN verified org.
package operatingreview

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same single-method shape every sibling operation package
// (hotspots.QueryClient, cognitiveload.QueryClient, etc.) declares
// independently, per those packages' own "self-contained operation
// package" convention.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

const dateLayout = "2006-01-02"

// weekBounds ports week_bounds (metrics/operating_review.py:93-95)
// verbatim: the half-open period [weekStart, weekStart+7days).
func weekBounds(weekStart time.Time) (time.Time, time.Time) {
	return weekStart, weekStart.AddDate(0, 0, 7)
}

// priorWeekStart ports prior_week_start (metrics/operating_review.py:97-98)
// verbatim: weekStart - 7 days.
func priorWeekStart(weekStart time.Time) time.Time {
	return weekStart.AddDate(0, 0, -7)
}

func formatDay(t time.Time) string {
	return t.Format(dateLayout)
}

// ---------------------------------------------------------------------------
// Reducer helpers -- port _value/_present_values/_sum/_avg/_max/_min/
// _weighted_avg (metrics/operating_review.py:769-816) verbatim. Python's
// `_value` does `float(raw)` unconditionally and only treats a raw `None`
// as absent; every numeric column this package scans is already a Go
// float64 (widened at scan time, matching Python's own unconditional
// float() widen) or nil for a genuinely SQL-NULL/Nullable(Float64) column
// -- nil is Python's None, a non-nil pointer is Python's "present" value,
// including a NaN one (avg()/sum() over zero underlying ClickHouse rows in
// a non-Nullable Float64 column returns NaN, not NULL; Python's float(nan)
// does not raise either, so that value flows through unfiltered on both
// planes -- a pre-existing property of the Python system this port
// reproduces rather than "fixes"; see the package doc comment's ORDER BY
// section for the same "verified, not silently patched" treatment of a
// different residual-risk finding).
// ---------------------------------------------------------------------------

func presentValues(vals []*float64) []float64 {
	out := make([]float64, 0, len(vals))
	for _, v := range vals {
		if v != nil {
			out = append(out, *v)
		}
	}
	return out
}

func sumF(vals []*float64) float64 {
	var total float64
	for _, v := range presentValues(vals) {
		total += v
	}
	return total
}

func avgF(vals []*float64) float64 {
	present := presentValues(vals)
	if len(present) == 0 {
		return 0.0
	}
	var total float64
	for _, v := range present {
		total += v
	}
	return total / float64(len(present))
}

func maxF(vals []*float64) float64 {
	present := presentValues(vals)
	if len(present) == 0 {
		return 0.0
	}
	m := present[0]
	for _, v := range present[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func minF(vals []*float64) float64 {
	present := presentValues(vals)
	if len(present) == 0 {
		return 0.0
	}
	m := present[0]
	for _, v := range present[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

// weightedAvgF ports _weighted_avg (metrics/operating_review.py:804-816)
// verbatim: a nil/missing weight defaults to 1.0 (Python's
// `weight = _value(row, weight_key) or 1.0` -- note this also treats a
// PRESENT weight of exactly 0.0 as falsy and substitutes 1.0, a real
// quirk of Python's `or`, reproduced here rather than "corrected", since
// it is not part of this port's declared fix set).
func weightedAvgF(values, weights []*float64) float64 {
	var total, totalWeight float64
	n := len(values)
	if len(weights) < n {
		n = len(weights)
	}
	for i := 0; i < n; i++ {
		if values[i] == nil {
			continue
		}
		w := 1.0
		if weights[i] != nil && *weights[i] != 0 {
			w = *weights[i]
		}
		total += *values[i] * w
		totalWeight += w
	}
	if totalWeight == 0 {
		return 0.0
	}
	return total / totalWeight
}

// pluck extracts one nullable-float column from a typed row slice --
// the Go analogue of Python's generic `row.get(key)` string-keyed access,
// using a field-accessor func instead of a string key (same values, no
// stringly-typed lookup).
func pluck[T any](rows []T, get func(T) *float64) []*float64 {
	out := make([]*float64, len(rows))
	for i, r := range rows {
		out[i] = get(r)
	}
	return out
}

func f(v float64) *float64 { return &v }

// discardOnError drops a fetcher's PARTIAL result on a mid-stream failure --
// codex review round 1 (PR #2008, P2), verified against source before this
// fix: a fetchXxx function's loop can successfully Scan() one or more rows
// via rows.Next() before the native driver's iteration itself fails, at
// which point rows.Next() returns false and rows.Err() (returned as this
// function's err) is non-nil -- but the ALREADY-APPENDED rows are still
// sitting in the slice fetchPeriodRows was about to keep. Without this,
// errSwallow's log-and-continue contract would compute "plausible-looking"
// metrics from a table's INCOMPLETE data instead of treating the failure as
// absent, same as a query-level failure already does (those return nil by
// construction -- the `out` var is never assigned before the query error
// path returns). Python has no equivalent partial-success state to
// reproduce here: resolvers/operating_review.py:66-96's try/except wraps a
// single `await query_dicts(...)` call whose underlying clickhouse-connect
// query() either returns the FULL row list or raises -- there is no
// mid-stream partial-rows-then-error shape on that side to match, so
// discarding the partial slice here is the correct "same effective
// contract" behavior, not a divergence from Python.
func discardOnError[T any](rows []T, err error) []T {
	if err != nil {
		return nil
	}
	return rows
}

// ---------------------------------------------------------------------------
// Row types + fetchers, one pair per table, in build_operating_review_queries'
// own order (metrics/operating_review.py:122-368). Every SELECT below is a
// direct parameter-syntax translation of the cited Python query text
// (`%(name)s` -> `{name:Type}`, the `team_filter`/`team_group` f-string
// splices -> the equivalent Go string conditionals) -- same content, not
// reformatted. ClickHouse aggregate return-type widening is scanned
// verbatim (SUM(UInt32) -> UInt64, MAX/MIN(UInt32) -> UInt32, AVG(*) ->
// (Nullable) Float64), the same "match the driver's real wire type, widen
// after" discipline cognitiveload.fetchUserMetrics's own doc comment
// documents, then converted to float64/*float64 for the reducers above.
// ---------------------------------------------------------------------------

type workItemsRow struct {
	itemsStarted      float64
	itemsCompleted    float64
	wipCountEndOfDay  float64
	cycleTimeP50Hours *float64
	cycleTimeP90Hours *float64
	wipAgeP50Hours    *float64
	wipAgeP90Hours    *float64
}

// fetchWorkItems ports the "work_items" query verbatim
// (metrics/operating_review.py:123-156).
func fetchWorkItems(ctx context.Context, client QueryClient, orgID string, teamID *string, start, end time.Time) ([]workItemsRow, error) {
	teamFilter, teamGroup, teamBinding := teamClauses(teamID)
	query := `
        SELECT
          day,
          sum(items_started) AS items_started,
          sum(items_completed) AS items_completed,
          max(wip_count_end_of_day) AS wip_count_end_of_day,
          avg(cycle_time_p50_hours) AS cycle_time_p50_hours,
          avg(cycle_time_p90_hours) AS cycle_time_p90_hours,
          avg(wip_age_p50_hours) AS wip_age_p50_hours,
          avg(wip_age_p90_hours) AS wip_age_p90_hours
        FROM (
          SELECT
            day,
            provider,
            work_scope_id,
            argMax(items_started, computed_at) AS items_started,
            argMax(items_completed, computed_at) AS items_completed,
            argMax(wip_count_end_of_day, computed_at) AS wip_count_end_of_day,
            argMax(cycle_time_p50_hours, computed_at) AS cycle_time_p50_hours,
            argMax(cycle_time_p90_hours, computed_at) AS cycle_time_p90_hours,
            argMax(wip_age_p50_hours, computed_at) AS wip_age_p50_hours,
            argMax(wip_age_p90_hours, computed_at) AS wip_age_p90_hours
          FROM work_item_metrics_daily
          WHERE org_id = {org_id:String}
            ` + teamFilter + `
            AND day >= {start:Date} AND day < {end:Date}
          GROUP BY day, provider, work_scope_id` + teamGroup + `
        )
        GROUP BY day
        ORDER BY day`

	bindings := periodBindings(orgID, start, end, teamBinding)
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("operatingreview: work_items query: %w", err)
	}
	defer rows.Close()

	var out []workItemsRow
	for rows.Next() {
		var day time.Time
		var itemsStarted, itemsCompleted uint64
		var wipCountEndOfDay uint32
		var cycleP50, cycleP90, wipAgeP50, wipAgeP90 *float64
		if scanErr := rows.Scan(&day, &itemsStarted, &itemsCompleted, &wipCountEndOfDay, &cycleP50, &cycleP90, &wipAgeP50, &wipAgeP90); scanErr != nil {
			return nil, fmt.Errorf("operatingreview: work_items scan: %w", scanErr)
		}
		out = append(out, workItemsRow{
			itemsStarted:      float64(itemsStarted),
			itemsCompleted:    float64(itemsCompleted),
			wipCountEndOfDay:  float64(wipCountEndOfDay),
			cycleTimeP50Hours: cycleP50,
			cycleTimeP90Hours: cycleP90,
			wipAgeP50Hours:    wipAgeP50,
			wipAgeP90Hours:    wipAgeP90,
		})
	}
	return out, rows.Err()
}

type stateDurationRow struct {
	itemsTouched  float64
	durationHours float64
	avgWip        float64
}

// fetchStateDurations ports the "state_durations" query verbatim
// (metrics/operating_review.py:157-182).
func fetchStateDurations(ctx context.Context, client QueryClient, orgID string, teamID *string, start, end time.Time) ([]stateDurationRow, error) {
	teamFilter, teamGroup, teamBinding := teamClauses(teamID)
	query := `
        SELECT
          status,
          sum(items_touched) AS items_touched,
          avg(duration_hours) AS duration_hours,
          avg(avg_wip) AS avg_wip
        FROM (
          SELECT
            day,
            provider,
            work_scope_id,
            status,
            argMax(duration_hours, computed_at) AS duration_hours,
            argMax(items_touched, computed_at) AS items_touched,
            argMax(avg_wip, computed_at) AS avg_wip
          FROM work_item_state_durations_daily
          WHERE org_id = {org_id:String}
            ` + teamFilter + `
            AND day >= {start:Date} AND day < {end:Date}
          GROUP BY day, provider, work_scope_id, status` + teamGroup + `
        )
        GROUP BY status`

	bindings := periodBindings(orgID, start, end, teamBinding)
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("operatingreview: state_durations query: %w", err)
	}
	defer rows.Close()

	var out []stateDurationRow
	for rows.Next() {
		var status string
		var itemsTouched uint64
		var durationHours, avgWip float64
		if scanErr := rows.Scan(&status, &itemsTouched, &durationHours, &avgWip); scanErr != nil {
			return nil, fmt.Errorf("operatingreview: state_durations scan: %w", scanErr)
		}
		out = append(out, stateDurationRow{
			itemsTouched:  float64(itemsTouched),
			durationHours: durationHours,
			avgWip:        avgWip,
		})
	}
	return out, rows.Err()
}

type repoMetricsRow struct {
	prsMerged               float64
	prFirstReviewP50Hours   *float64
	singleOwnerFileRatio30d float64
	codeOwnershipGini       float64
	busFactor               float64
	changeFailureRate       float64
	mttrHours               *float64
}

// fetchRepoMetrics ports the "repo_metrics" query verbatim
// (metrics/operating_review.py:183-211). No team_filter/team_group --
// repo_metrics_daily has no team_id column, matching the Python query
// (which does not splice either f-string placeholder into this one).
func fetchRepoMetrics(ctx context.Context, client QueryClient, orgID string, start, end time.Time) ([]repoMetricsRow, error) {
	query := `
        SELECT
          sum(prs_merged) AS prs_merged,
          avg(pr_first_review_p50_hours) AS pr_first_review_p50_hours,
          avg(single_owner_file_ratio_30d) AS single_owner_file_ratio_30d,
          avg(code_ownership_gini) AS code_ownership_gini,
          min(bus_factor) AS bus_factor,
          avg(change_failure_rate) AS change_failure_rate,
          avg(mttr_hours) AS mttr_hours
        FROM (
          SELECT
            day,
            repo_id,
            argMax(prs_merged, computed_at) AS prs_merged,
            argMax(pr_first_review_p50_hours, computed_at) AS pr_first_review_p50_hours,
            argMax(single_owner_file_ratio_30d, computed_at) AS single_owner_file_ratio_30d,
            argMax(code_ownership_gini, computed_at) AS code_ownership_gini,
            argMax(bus_factor, computed_at) AS bus_factor,
            argMax(change_failure_rate, computed_at) AS change_failure_rate,
            argMax(mttr_hours, computed_at) AS mttr_hours
          FROM repo_metrics_daily
          WHERE org_id = {org_id:String}
            AND day >= {start:Date} AND day < {end:Date}
          GROUP BY day, repo_id
        )`

	bindings := periodBindings(orgID, start, end, nil)
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("operatingreview: repo_metrics query: %w", err)
	}
	defer rows.Close()

	var out []repoMetricsRow
	for rows.Next() {
		var prsMerged uint64
		var prFirstReviewP50 *float64
		var singleOwnerRatio, codeOwnershipGini float64
		var busFactor uint32
		var changeFailureRate float64
		var mttrHours *float64
		if scanErr := rows.Scan(&prsMerged, &prFirstReviewP50, &singleOwnerRatio, &codeOwnershipGini, &busFactor, &changeFailureRate, &mttrHours); scanErr != nil {
			return nil, fmt.Errorf("operatingreview: repo_metrics scan: %w", scanErr)
		}
		out = append(out, repoMetricsRow{
			prsMerged:               float64(prsMerged),
			prFirstReviewP50Hours:   prFirstReviewP50,
			singleOwnerFileRatio30d: singleOwnerRatio,
			codeOwnershipGini:       codeOwnershipGini,
			busFactor:               float64(busFactor),
			changeFailureRate:       changeFailureRate,
			mttrHours:               mttrHours,
		})
	}
	return out, rows.Err()
}

type hotspotsAggRow struct {
	riskScore     float64
	hotspotsCount float64
}

// fetchHotspotsAgg ports the "hotspots" query verbatim
// (metrics/operating_review.py:212-229). Distinct from, and unrelated to,
// this repo's OWN hotspots.go package (the ported `hotspots` GraphQL
// operation, CHAOS-4369) -- this is operatingReview's own risk-summary
// query over the same table, a different SELECT shape entirely.
func fetchHotspotsAgg(ctx context.Context, client QueryClient, orgID string, start, end time.Time) ([]hotspotsAggRow, error) {
	query := `
        SELECT avg(latest_risk_score) AS risk_score, count() AS hotspots_count
        FROM (
          SELECT
            day,
            repo_id,
            file_path,
            argMax(risk_score, computed_at) AS latest_risk_score
          FROM file_hotspot_daily
          WHERE org_id = {org_id:String}
            AND day >= {start:Date} AND day < {end:Date}
          GROUP BY day, repo_id, file_path
          HAVING latest_risk_score > 0
        )`

	bindings := periodBindings(orgID, start, end, nil)
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("operatingreview: hotspots query: %w", err)
	}
	defer rows.Close()

	var out []hotspotsAggRow
	for rows.Next() {
		var riskScore float64
		var hotspotsCount uint64
		if scanErr := rows.Scan(&riskScore, &hotspotsCount); scanErr != nil {
			return nil, fmt.Errorf("operatingreview: hotspots scan: %w", scanErr)
		}
		out = append(out, hotspotsAggRow{riskScore: riskScore, hotspotsCount: float64(hotspotsCount)})
	}
	return out, rows.Err()
}

type complexityAggRow struct {
	cyclomaticPerKloc float64
}

// fetchComplexityAgg ports the "complexity" query verbatim
// (metrics/operating_review.py:230-245).
func fetchComplexityAgg(ctx context.Context, client QueryClient, orgID string, start, end time.Time) ([]complexityAggRow, error) {
	query := `
        SELECT avg(cyclomatic_per_kloc) AS cyclomatic_per_kloc
        FROM (
          SELECT
            day,
            repo_id,
            argMax(cyclomatic_per_kloc, computed_at) AS cyclomatic_per_kloc
          FROM repo_complexity_daily
          WHERE org_id = {org_id:String}
            AND day >= {start:Date} AND day < {end:Date}
          GROUP BY day, repo_id
        )`

	bindings := periodBindings(orgID, start, end, nil)
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("operatingreview: complexity query: %w", err)
	}
	defer rows.Close()

	var out []complexityAggRow
	for rows.Next() {
		var cyclomaticPerKloc float64
		if scanErr := rows.Scan(&cyclomaticPerKloc); scanErr != nil {
			return nil, fmt.Errorf("operatingreview: complexity scan: %w", scanErr)
		}
		out = append(out, complexityAggRow{cyclomaticPerKloc: cyclomaticPerKloc})
	}
	return out, rows.Err()
}

type deploymentsAggRow struct {
	deploymentsCount       float64
	failedDeploymentsCount float64
}

// fetchDeploymentsAgg ports the "deployments" query verbatim
// (metrics/operating_review.py:246-264).
func fetchDeploymentsAgg(ctx context.Context, client QueryClient, orgID string, start, end time.Time) ([]deploymentsAggRow, error) {
	query := `
        SELECT
          sum(deployments_count) AS deployments_count,
          sum(failed_deployments_count) AS failed_deployments_count
        FROM (
          SELECT
            day,
            repo_id,
            argMax(deployments_count, computed_at) AS deployments_count,
            argMax(failed_deployments_count, computed_at) AS failed_deployments_count
          FROM deploy_metrics_daily
          WHERE org_id = {org_id:String}
            AND day >= {start:Date} AND day < {end:Date}
          GROUP BY day, repo_id
        )`

	bindings := periodBindings(orgID, start, end, nil)
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("operatingreview: deployments query: %w", err)
	}
	defer rows.Close()

	var out []deploymentsAggRow
	for rows.Next() {
		var deploymentsCount, failedDeploymentsCount uint64
		if scanErr := rows.Scan(&deploymentsCount, &failedDeploymentsCount); scanErr != nil {
			return nil, fmt.Errorf("operatingreview: deployments scan: %w", scanErr)
		}
		out = append(out, deploymentsAggRow{
			deploymentsCount:       float64(deploymentsCount),
			failedDeploymentsCount: float64(failedDeploymentsCount),
		})
	}
	return out, rows.Err()
}

type incidentsAggRow struct {
	incidentsCount float64
	mttrP50Hours   *float64
}

// fetchIncidentsAgg ports the "incidents" query verbatim
// (metrics/operating_review.py:265-281).
func fetchIncidentsAgg(ctx context.Context, client QueryClient, orgID string, start, end time.Time) ([]incidentsAggRow, error) {
	query := `
        SELECT sum(incidents_count) AS incidents_count, avg(mttr_p50_hours) AS mttr_p50_hours
        FROM (
          SELECT
            day,
            repo_id,
            argMax(incidents_count, computed_at) AS incidents_count,
            argMax(mttr_p50_hours, computed_at) AS mttr_p50_hours
          FROM incident_metrics_daily
          WHERE org_id = {org_id:String}
            AND day >= {start:Date} AND day < {end:Date}
          GROUP BY day, repo_id
        )`

	bindings := periodBindings(orgID, start, end, nil)
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("operatingreview: incidents query: %w", err)
	}
	defer rows.Close()

	var out []incidentsAggRow
	for rows.Next() {
		var incidentsCount uint64
		var mttrP50 *float64
		if scanErr := rows.Scan(&incidentsCount, &mttrP50); scanErr != nil {
			return nil, fmt.Errorf("operatingreview: incidents scan: %w", scanErr)
		}
		out = append(out, incidentsAggRow{incidentsCount: float64(incidentsCount), mttrP50Hours: mttrP50})
	}
	return out, rows.Err()
}

type investmentRow struct {
	investmentArea string
	deliveryUnits  float64
}

// fetchInvestment ports the "investment" query verbatim
// (metrics/operating_review.py:282-301).
func fetchInvestment(ctx context.Context, client QueryClient, orgID string, teamID *string, start, end time.Time) ([]investmentRow, error) {
	teamFilter, teamGroup, teamBinding := teamClauses(teamID)
	query := `
        SELECT investment_area, sum(delivery_units) AS delivery_units
        FROM (
          SELECT
            day,
            repo_id,
            investment_area,
            project_stream,
            argMax(delivery_units, computed_at) AS delivery_units
          FROM investment_metrics_daily
          WHERE org_id = {org_id:String}
            ` + teamFilter + `
            AND day >= {start:Date} AND day < {end:Date}
          GROUP BY day, repo_id, investment_area, project_stream` + teamGroup + `
        )
        GROUP BY investment_area`

	bindings := periodBindings(orgID, start, end, teamBinding)
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("operatingreview: investment query: %w", err)
	}
	defer rows.Close()

	var out []investmentRow
	for rows.Next() {
		var area string
		var deliveryUnits uint64
		if scanErr := rows.Scan(&area, &deliveryUnits); scanErr != nil {
			return nil, fmt.Errorf("operatingreview: investment scan: %w", scanErr)
		}
		out = append(out, investmentRow{investmentArea: area, deliveryUnits: float64(deliveryUnits)})
	}
	return out, rows.Err()
}

type aiImpactRow struct {
	prsTotal              float64
	aiAssistedPrs         float64
	agentCreatedPrs       float64
	humanPrs              float64
	unknownPrs            float64
	aiCycleTimeDeltaHours *float64
	aiReviewAmplification *float64
	reworkDragRate        *float64
	testGapRate           *float64
	incidentDragRate      *float64
}

// fetchAIImpact ports the "ai_impact" query verbatim
// (metrics/operating_review.py:302-342). Verified column-by-column against
// migration 036_ai_metrics.sql's real ai_impact_metrics_daily schema --
// every referenced column exists (unlike ai_governance; see
// fetchAIGovernance's doc comment), so no divergence here.
func fetchAIImpact(ctx context.Context, client QueryClient, orgID string, teamID *string, start, end time.Time) ([]aiImpactRow, error) {
	teamFilter, _, teamBinding := teamClauses(teamID)
	query := `
        SELECT
          attribution_bucket,
          sum(prs_total) AS prs_total,
          sum(ai_assisted_prs) AS ai_assisted_prs,
          sum(agent_created_prs) AS agent_created_prs,
          sum(human_prs) AS human_prs,
          sum(unknown_prs) AS unknown_prs,
          avg(ai_cycle_time_delta_hours) AS ai_cycle_time_delta_hours,
          avg(ai_review_amplification) AS ai_review_amplification,
          avg(rework_drag_rate) AS rework_drag_rate,
          avg(test_gap_rate) AS test_gap_rate,
          avg(incident_drag_rate) AS incident_drag_rate
        FROM (
          SELECT
            day,
            repo_id,
            team_id,
            work_type,
            attribution_bucket,
            argMax(prs_total, computed_at) AS prs_total,
            argMax(ai_assisted_prs, computed_at) AS ai_assisted_prs,
            argMax(agent_created_prs, computed_at) AS agent_created_prs,
            argMax(human_prs, computed_at) AS human_prs,
            argMax(unknown_prs, computed_at) AS unknown_prs,
            argMax(ai_cycle_time_delta_hours, computed_at) AS ai_cycle_time_delta_hours,
            argMax(ai_review_amplification, computed_at) AS ai_review_amplification,
            argMax(rework_drag_rate, computed_at) AS rework_drag_rate,
            argMax(test_gap_rate, computed_at) AS test_gap_rate,
            argMax(incident_drag_rate, computed_at) AS incident_drag_rate
          FROM ai_impact_metrics_daily
          WHERE org_id = {org_id:String}
            ` + teamFilter + `
            AND day >= {start:Date} AND day < {end:Date}
          GROUP BY day, repo_id, team_id, work_type, attribution_bucket
        )
        GROUP BY attribution_bucket`

	bindings := periodBindings(orgID, start, end, teamBinding)
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("operatingreview: ai_impact query: %w", err)
	}
	defer rows.Close()

	var out []aiImpactRow
	for rows.Next() {
		var bucket string
		var prsTotal, aiAssistedPrs, agentCreatedPrs, humanPrs, unknownPrs uint64
		var aiCycleTimeDelta, aiReviewAmp, reworkDrag, testGap, incidentDrag *float64
		if scanErr := rows.Scan(&bucket, &prsTotal, &aiAssistedPrs, &agentCreatedPrs, &humanPrs, &unknownPrs, &aiCycleTimeDelta, &aiReviewAmp, &reworkDrag, &testGap, &incidentDrag); scanErr != nil {
			return nil, fmt.Errorf("operatingreview: ai_impact scan: %w", scanErr)
		}
		out = append(out, aiImpactRow{
			prsTotal:              float64(prsTotal),
			aiAssistedPrs:         float64(aiAssistedPrs),
			agentCreatedPrs:       float64(agentCreatedPrs),
			humanPrs:              float64(humanPrs),
			unknownPrs:            float64(unknownPrs),
			aiCycleTimeDeltaHours: aiCycleTimeDelta,
			aiReviewAmplification: aiReviewAmp,
			reworkDragRate:        reworkDrag,
			testGapRate:           testGap,
			incidentDragRate:      incidentDrag,
		})
	}
	return out, rows.Err()
}

// aiGovernanceRawRow is one (day, team_id, repo_id) group's LATEST raw
// governance-artifact counts -- the REAL columns of
// ai_governance_coverage_daily (migration 038_ai_governance.sql:26-41),
// collapsed with the same argMax(<col>, computed_at) discipline every
// other table in this package uses.
type aiGovernanceRawRow struct {
	aiArtifacts        float64
	declaredArtifacts  float64
	humanReviewedPrs   float64
	securityScannedPrs float64
	inPolicyArtifacts  float64
}

// fetchAIGovernance is the ONE query in this package that is a Go-side FIX,
// not a verbatim port -- CHAOS-4505, ruled by the orchestrator (option (a):
// fix in Go, declare the divergence) after being reported before being
// written.
//
// THE DEFECT (Python, unfixed, main tip 4464c83c1): the "ai_governance"
// query (metrics/operating_review.py:343-367) selects
// `argMax(declaration_coverage, computed_at)`,
// `human_review_coverage`, `security_scan_coverage`, `in_policy_coverage`
// FROM ai_governance_coverage_daily. NONE of those four names are columns
// of that table -- verified against migration 038_ai_governance.sql:26-41,
// whose real columns are `ai_artifacts`, `declared_artifacts`,
// `human_reviewed_prs`, `security_scanned_prs`, `in_policy_artifacts`
// (raw counts). The four names Python's SQL selects are Python
// `@property` RATIOS on a DIFFERENT, ORM-style dataclass,
// `AIGovernanceCoverageDaily` (audit/ai_governance/models.py:125-158,
// main 84875aa4, byte-identical) -- `resolvers/ai.py:1116` reads them
// correctly, as `row.declaration_coverage` on a real loaded row object,
// never as a raw SQL column. operating_review.py's raw SQL copy-pasted
// the derived-property names as if they were table columns; run against a
// real ClickHouse engine this query fails with an unknown-identifier
// error.
//
// THE BLAST RADIUS (Python, TODAY, in production): resolvers/
// operating_review.py:66-96's `_fetch_period_rows` wraps EVERY one of the
// ten queries in its own try/except, logs (`logger.exception`, so this is
// a steady, MEASURABLE stream in prod logs, not a silent failure), and
// defaults that table's rows to `[]` on failure. Tracing
// `_ai_governance_coverage` (metrics/operating_review.py:847-857) with
// `rows=[]`: `_avg([], key)` returns 0.0 for all four inputs, `present =
// [v for v in coverage if v > 0]` is then empty, so the function returns
// 0.0 unconditionally. `ai_governance_coverage` and `ai_opportunity_signals`
// (which folds the same call in, line 871) are therefore pinned to 0.0 in
// EVERY real operatingReview response today. Untested:
// tests/metrics/test_operating_review.py hand-builds
// `OperatingReviewRows(ai_governance=[{"declaration_coverage": 0.9, ...}])`
// directly (lines 80-82, 137-139), never through
// build_operating_review_queries -- the same "mock-only, no live-engine
// proof" gap class root AGENTS.md already names for argMax syntax, here
// hitting column existence instead.
//
// THE FIX: fetch the table's REAL columns (raw counts, argMax-collapsed
// per (day, team_id, repo_id) -- the exact grain the broken query's inner
// subquery already used), then compute each group's four ratios in Go via
// aiGovernanceRatio, an EXACT port of AIGovernanceCoverageDaily's
// declaration_coverage/human_review_coverage/security_scan_coverage/
// in_policy_coverage properties (audit/ai_governance/models.py:140-158,
// including _ratio's edge case: 1.0 -- "fully covered" -- when the
// denominator is <= 0, NOT 0.0). aiGovernanceCoverage below then applies
// EXACTLY Python's own two-level averaging shape on top of those per-group
// ratios: average each ratio ACROSS GROUPS first (mirroring the broken
// query's outer `avg(declaration_coverage)` etc., which -- had the columns
// existed -- would have averaged one already-argMax'd ratio per (day,
// team_id, repo_id) group, sum-then-divide-once across the whole period is
// NOT what that shape computes, since a period could contain differently-
// sized groups), THEN average the four resulting numbers with the
// `> 0` presence filter _ai_governance_coverage itself applies
// (metrics/operating_review.py:848-857) -- same two-level "average of
// per-group values" both levels, never a period-wide sum-then-divide.
//
// EXPECTED DUAL-RUN DIVERGENCE: Python returns 0.0 for
// ai_governance_coverage/ai_opportunity_signals in every real response
// (the query never executes). Go returns the real ratio. A dual-run MATCH
// on this query would mean the fix did not take effect -- see this
// package's proof for the declared expected-divergence list.
func fetchAIGovernance(ctx context.Context, client QueryClient, orgID string, teamID *string, start, end time.Time) ([]aiGovernanceRawRow, error) {
	teamFilter, _, teamBinding := teamClauses(teamID)
	query := `
        SELECT
          day,
          team_id,
          repo_id,
          argMax(ai_artifacts, computed_at) AS ai_artifacts,
          argMax(declared_artifacts, computed_at) AS declared_artifacts,
          argMax(human_reviewed_prs, computed_at) AS human_reviewed_prs,
          argMax(security_scanned_prs, computed_at) AS security_scanned_prs,
          argMax(in_policy_artifacts, computed_at) AS in_policy_artifacts
        FROM ai_governance_coverage_daily
        WHERE org_id = {org_id:String}
          ` + teamFilter + `
          AND day >= {start:Date} AND day < {end:Date}
        GROUP BY day, team_id, repo_id`

	bindings := periodBindings(orgID, start, end, teamBinding)
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("operatingreview: ai_governance query: %w", err)
	}
	defer rows.Close()

	var out []aiGovernanceRawRow
	for rows.Next() {
		var day time.Time
		var groupTeamID *string
		var groupRepoID *string
		var aiArtifacts, declaredArtifacts, humanReviewedPrs, securityScannedPrs, inPolicyArtifacts uint64
		if scanErr := rows.Scan(&day, &groupTeamID, &groupRepoID, &aiArtifacts, &declaredArtifacts, &humanReviewedPrs, &securityScannedPrs, &inPolicyArtifacts); scanErr != nil {
			return nil, fmt.Errorf("operatingreview: ai_governance scan: %w", scanErr)
		}
		out = append(out, aiGovernanceRawRow{
			aiArtifacts:        float64(aiArtifacts),
			declaredArtifacts:  float64(declaredArtifacts),
			humanReviewedPrs:   float64(humanReviewedPrs),
			securityScannedPrs: float64(securityScannedPrs),
			inPolicyArtifacts:  float64(inPolicyArtifacts),
		})
	}
	return out, rows.Err()
}

// aiGovernanceRatio ports AIGovernanceCoverageDaily's _ratio EXACTLY
// (audit/ai_governance/models.py:156-158): 1.0 when the denominator is
// <= 0 ("fully covered" when there is no AI activity to measure coverage
// against -- NOT the same as 0.0, and getting this backwards would invert
// the metric for every org with no AI usage), else numerator/denominator.
func aiGovernanceRatio(numerator, denominator float64) float64 {
	if denominator <= 0 {
		return 1.0
	}
	return numerator / denominator
}

// aiGovernanceGroupRatios computes one of the four coverage ratios for
// every fetched (day, team_id, repo_id) group, via aiGovernanceRatio --
// the per-group values _ai_governance_coverage's `_avg(rows, key)` would
// have averaged had the SQL columns it references actually existed.
func aiGovernanceGroupRatios(rows []aiGovernanceRawRow, numerator func(aiGovernanceRawRow) float64) []*float64 {
	out := make([]*float64, len(rows))
	for i, r := range rows {
		out[i] = f(aiGovernanceRatio(numerator(r), r.aiArtifacts))
	}
	return out
}

// aiGovernanceCoverage ports _ai_governance_coverage
// (metrics/operating_review.py:847-857) on top of the Go-side fix
// (fetchAIGovernance's doc comment): each of the four coverage figures is
// first averaged ACROSS GROUPS (mirroring the broken query's own intended
// outer `avg(...)` shape), then Python's own presence-filtered average of
// those four numbers is applied unchanged.
func aiGovernanceCoverage(rows []aiGovernanceRawRow) float64 {
	coverage := []float64{
		avgF(aiGovernanceGroupRatios(rows, func(r aiGovernanceRawRow) float64 { return r.declaredArtifacts })),
		avgF(aiGovernanceGroupRatios(rows, func(r aiGovernanceRawRow) float64 { return r.humanReviewedPrs })),
		avgF(aiGovernanceGroupRatios(rows, func(r aiGovernanceRawRow) float64 { return r.securityScannedPrs })),
		avgF(aiGovernanceGroupRatios(rows, func(r aiGovernanceRawRow) float64 { return r.inPolicyArtifacts })),
	}
	var present []float64
	for _, v := range coverage {
		if v > 0 {
			present = append(present, v)
		}
	}
	if len(present) == 0 {
		return 0.0
	}
	var total float64
	for _, v := range present {
		total += v
	}
	return total / float64(len(present))
}

// ---------------------------------------------------------------------------
// Period fetch orchestration -- ports resolve_operating_review's
// _fetch_period_rows (resolvers/operating_review.py:66-96) verbatim,
// INCLUDING the per-table try/except-and-default-to-empty behavior. This
// is why fetchPeriodRows itself never returns an error for an individual
// table's failure -- only a true precondition failure (nil client) aborts,
// checked once by the caller (Resolve), same as every sibling package.
// ---------------------------------------------------------------------------

type periodRows struct {
	workItems      []workItemsRow
	stateDurations []stateDurationRow
	repoMetrics    []repoMetricsRow
	hotspots       []hotspotsAggRow
	complexity     []complexityAggRow
	deployments    []deploymentsAggRow
	incidents      []incidentsAggRow
	investment     []investmentRow
	aiImpact       []aiImpactRow
	aiGovernance   []aiGovernanceRawRow
}

// teamClauses returns (teamFilter, teamGroup, teamBinding) mirroring
// build_operating_review_queries' `team_filter`/`team_group` f-string
// splices (metrics/operating_review.py:119-120) exactly: team_filter is
// non-empty (and a binding is returned) only when teamID is set;
// team_group (the extra GROUP BY column that keeps per-team rows from
// collapsing together in cross-team "All Teams" mode, CHAOS-1755) is
// non-empty only when teamID is UNSET -- the two conditions are inverses
// of each other, matching Python exactly.
func teamClauses(teamID *string) (teamFilter, teamGroup string, binding *clickhouse.Binding) {
	if teamID == nil {
		return "", ", team_id", nil
	}
	return "AND team_id = {team_id:String}", "", &clickhouse.Binding{Name: "team_id", Value: *teamID}
}

func periodBindings(orgID string, start, end time.Time, teamBinding *clickhouse.Binding) []clickhouse.Binding {
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start", Value: formatDay(start)},
		{Name: "end", Value: formatDay(end)},
	}
	if teamBinding != nil {
		bindings = append(bindings, *teamBinding)
	}
	return bindings
}

// fetchPeriodRows ports _fetch_period_rows (resolvers/operating_review.py:
// 66-96) verbatim: end = week_bounds(start)[1] (line 74), then all ten
// tables for [start, end), each query's failure logged and defaulted to
// empty independently -- one broken/missing table degrades only that
// table's section, never the whole response. errSwallow below is this
// function's ONLY logging seam; every one of the ten calls goes through it,
// matching Python's identical `except Exception: logger.exception(...)`
// block repeated at every loop iteration (Python loops over one shared
// try/except inside `for query in build_operating_review_queries(...)`;
// this port has ten call sites instead of one loop body, since each query
// has a distinct Go return type -- same swallow-and-log behavior at every
// one).
func fetchPeriodRows(ctx context.Context, client QueryClient, orgID string, teamID *string, start time.Time) periodRows {
	_, end := weekBounds(start)

	workItems, err := fetchWorkItems(ctx, client, orgID, teamID, start, end)
	errSwallow(ctx, "work_items", err)
	workItems = discardOnError(workItems, err)

	stateDurations, err := fetchStateDurations(ctx, client, orgID, teamID, start, end)
	errSwallow(ctx, "state_durations", err)
	stateDurations = discardOnError(stateDurations, err)

	repoMetrics, err := fetchRepoMetrics(ctx, client, orgID, start, end)
	errSwallow(ctx, "repo_metrics", err)
	repoMetrics = discardOnError(repoMetrics, err)

	hotspots, err := fetchHotspotsAgg(ctx, client, orgID, start, end)
	errSwallow(ctx, "hotspots", err)
	hotspots = discardOnError(hotspots, err)

	complexity, err := fetchComplexityAgg(ctx, client, orgID, start, end)
	errSwallow(ctx, "complexity", err)
	complexity = discardOnError(complexity, err)

	deployments, err := fetchDeploymentsAgg(ctx, client, orgID, start, end)
	errSwallow(ctx, "deployments", err)
	deployments = discardOnError(deployments, err)

	incidents, err := fetchIncidentsAgg(ctx, client, orgID, start, end)
	errSwallow(ctx, "incidents", err)
	incidents = discardOnError(incidents, err)

	investment, err := fetchInvestment(ctx, client, orgID, teamID, start, end)
	errSwallow(ctx, "investment", err)
	investment = discardOnError(investment, err)

	aiImpact, err := fetchAIImpact(ctx, client, orgID, teamID, start, end)
	errSwallow(ctx, "ai_impact", err)
	aiImpact = discardOnError(aiImpact, err)

	aiGovernance, err := fetchAIGovernance(ctx, client, orgID, teamID, start, end)
	errSwallow(ctx, "ai_governance", err)
	aiGovernance = discardOnError(aiGovernance, err)

	return periodRows{
		workItems:      workItems,
		stateDurations: stateDurations,
		repoMetrics:    repoMetrics,
		hotspots:       hotspots,
		complexity:     complexity,
		deployments:    deployments,
		incidents:      incidents,
		investment:     investment,
		aiImpact:       aiImpact,
		aiGovernance:   aiGovernance,
	}
}

// fetchSwallowedCounter counts every per-table fetch failure this package
// swallows (returns partial data for instead of a hard error) -- the
// orchestrator's ruling on this port's SECOND expected divergence (see
// the package doc comment's "Per-table error isolation" section): match
// Python's swallow-and-degrade SHAPE exactly, but make the failure LOUD,
// which Python's `logger.exception` alone is not (it only reaches log
// aggregation, never metrics/alerting). Declared locally in this leaf
// package rather than one layer up in graph/telemetry.go (every sibling
// operation's convention) because the per-TABLE granularity this counter
// needs only exists inside fetchPeriodRows -- graph/telemetry.go's spans
// wrap the whole Resolve call and cannot see an individual table's
// failure. mustSwallowCounter mirrors graph/telemetry.go's mustCounter
// exactly: an Int64Counter never returns nil even on error, so a broken
// meter provider must not panic a resolver it instruments.
var fetchSwallowedCounter = mustSwallowCounter()

func mustSwallowCounter() metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api/internal/operatingreview")
	counter, err := meter.Int64Counter(
		"devhealth_query_api_operating_review_fetch_swallowed_total",
		metric.WithDescription("operatingReview per-table ClickHouse fetch failures swallowed into an empty section instead of erroring, by table key"),
	)
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_query_api_operating_review_fetch_swallowed_total")
	}
	return counter
}

// errSwallow logs a per-table fetch failure AND increments
// fetchSwallowedCounter (tagged by table key) -- the Go analogue of
// Python's `logger.exception("Failed to fetch operating review rows for
// %s", query.key)`, PLUS the orchestrator-ruled telemetry addition
// Python does not have (see the package doc comment). Callers already
// default to a nil/empty slice on error (fetchXxx returns the
// accumulated `out` so far, which is nil on the first-row scan/query
// failure), so there is nothing further to do here besides making the
// failure OBSERVABLE on both channels.
func errSwallow(ctx context.Context, table string, err error) {
	if err == nil {
		return
	}
	log.Printf("operatingreview: failed to fetch rows for %s: %v (cause: %s)", table, err, rootCause(err))
	fetchSwallowedCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("table", table)))
}

// rootCause walks err's full errors.Unwrap() chain to its innermost cause
// and returns that cause's OWN Error() text -- required because this
// package's real QueryClient (github.com/full-chaos/dev-health-go/
// clickhouse.Client) wraps every driver error as an unexported
// operationError whose Error() method returns ONLY a fixed
// "ClickHouse <operation> failed" string (clickhouse/client.go: `func (e
// *operationError) Error() string { return "ClickHouse " + e.operation +
// " failed" }`) -- it never includes the real driver message, even
// though its Unwrap() DOES return the real cause. A plain %v/.Error() on
// err (or on this package's own fmt.Errorf(...: %w, err) wrapper around
// it, which embeds operationError's fixed string, not the cause) stops
// at that fixed string. Confirmed live: without this, every one of the
// ten tables' swallowed failures would log the byte-identical
// "ClickHouse query failed" regardless of WHY it failed -- an
// ai_governance UNKNOWN_IDENTIFIER, a genuinely missing table, a
// connection timeout, and a query-syntax mistake would all be
// indistinguishable in the log, which defeats the entire purpose of this
// port's own per-table telemetry (the errSwallow doc comment two
// sections up: "the swallow ships... made LOUD"). This is the exact
// class of defect a sibling lane's codex round found in
// isMissingMembershipTableError's single-level err.Error() substring
// match -- a fake QueryClient whose Error() already returns raw driver
// text cannot catch it; discardOnError's own test-verification discipline
// (revert -> confirm red -> restore -> verify by content digest) is the
// same standard this fix is held to (see this package's test file).
//
// Deliberately walks to the SINGLE innermost cause rather than joining
// every level: each level's Error() (aside from operationError's) already
// EMBEDS the next level's text via %w, so joining all levels would be
// redundant/noisy; only operationError's fixed string actually discards
// information, and only the innermost cause recovers it. The counter
// (fetchSwallowedCounter, immediately below) deliberately does NOT carry
// this text as a label -- unbounded-cardinality error strings belong in
// logs, not metric labels.
func rootCause(err error) string {
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return err.Error()
		}
		err = next
	}
}

// ---------------------------------------------------------------------------
// Compute layer -- ports compute_operating_review and everything it calls
// (metrics/operating_review.py:371-902) verbatim, except aiGovernanceCoverage
// above (the declared fix).
// ---------------------------------------------------------------------------

const (
	lowerIsBetter  = "lower"
	higherIsBetter = "higher"
	neutral        = "neutral"
)

type metricDelta struct {
	value      float64
	priorValue float64
	absolute   float64
	percent    *float64
	status     string
}

type reviewMetric struct {
	key   string
	label string
	value float64
	unit  string
	delta metricDelta
}

type reviewSection struct {
	key      string
	title    string
	metrics  []reviewMetric
	changed  []string
	improved []string
	worsened []string
}

// buildMetric ports _metric (metrics/operating_review.py:700-735) verbatim,
// including the `abs(delta_value) < 0.000001` unchanged epsilon and the
// `percent = None if delta_value else 0.0` zero-prior special case.
func buildMetric(key, label string, value, prior float64, unit, direction string) reviewMetric {
	deltaValue := value - prior
	var percent *float64
	if prior == 0 {
		if deltaValue != 0 {
			percent = nil
		} else {
			percent = f(0.0)
		}
	} else {
		percent = f(deltaValue / absF(prior) * 100.0)
	}

	var status string
	switch {
	case absF(deltaValue) < 0.000001:
		status = "unchanged"
	case direction == higherIsBetter:
		if deltaValue > 0 {
			status = "improved"
		} else {
			status = "worsened"
		}
	case direction == lowerIsBetter:
		if deltaValue < 0 {
			status = "improved"
		} else {
			status = "worsened"
		}
	default:
		status = "changed"
	}

	return reviewMetric{
		key:   key,
		label: label,
		value: value,
		unit:  unit,
		delta: metricDelta{
			value:      value,
			priorValue: prior,
			absolute:   deltaValue,
			percent:    percent,
			status:     status,
		},
	}
}

func absF(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// deltaSummary ports _delta_summary (metrics/operating_review.py:738-745)
// verbatim, including Go's `%+.1f` matching Python's `:+.1f` format spec
// (always-signed, one decimal place).
func deltaSummary(m reviewMetric) string {
	direction := map[string]string{
		"changed":   "changed",
		"improved":  "improved",
		"worsened":  "worsened",
		"unchanged": "did not change",
	}[m.delta.status]
	return fmt.Sprintf("%s %s by %+.1f %s", m.label, direction, m.delta.absolute, m.unit)
}

// buildSection ports _section (metrics/operating_review.py:676-697)
// verbatim.
func buildSection(key, title string, metrics []reviewMetric) reviewSection {
	var changed, improved, worsened []string
	for _, m := range metrics {
		summary := deltaSummary(m)
		switch m.delta.status {
		case "changed":
			changed = append(changed, summary)
		case "improved":
			improved = append(improved, summary)
		case "worsened":
			worsened = append(worsened, summary)
		}
	}
	return reviewSection{
		key:      key,
		title:    title,
		metrics:  metrics,
		changed:  changed,
		improved: improved,
		worsened: worsened,
	}
}

// recommendationsFromSections ports _recommendations_from_sections
// (metrics/operating_review.py:748-766) verbatim: one sentence per
// "worsened" metric, iterating sections then metrics in the CODE-DEFINED
// order computeReview builds them in (never a query-row order -- see the
// package doc comment's ORDER BY section).
func recommendationsFromSections(sections []reviewSection) []string {
	var recommendations []string
	for _, section := range sections {
		for _, m := range section.metrics {
			if m.delta.status == "worsened" {
				recommendations = append(recommendations, fmt.Sprintf(
					"Review %s: worsened by %+.1f %s week-over-week.",
					m.label, m.delta.absolute, m.unit,
				))
			}
		}
	}
	return recommendations
}

// changeFailureRate ports _change_failure_rate
// (metrics/operating_review.py:819-824) verbatim.
func changeFailureRate(current periodRows) float64 {
	deployments := sumF(pluck(current.deployments, func(r deploymentsAggRow) *float64 { return f(r.deploymentsCount) }))
	failed := sumF(pluck(current.deployments, func(r deploymentsAggRow) *float64 { return f(r.failedDeploymentsCount) }))
	if deployments > 0 {
		return failed / deployments
	}
	return avgF(pluck(current.repoMetrics, func(r repoMetricsRow) *float64 { return f(r.changeFailureRate) }))
}

// aiAdoptionRatio ports _ai_adoption_ratio (metrics/operating_review.py:
// 827-832) verbatim.
func aiAdoptionRatio(rows []aiImpactRow) float64 {
	totals := sumF(pluck(rows, func(r aiImpactRow) *float64 { return f(r.prsTotal) }))
	if totals == 0 {
		return 0.0
	}
	aiPrs := sumF(pluck(rows, func(r aiImpactRow) *float64 { return f(r.aiAssistedPrs) })) +
		sumF(pluck(rows, func(r aiImpactRow) *float64 { return f(r.agentCreatedPrs) }))
	return aiPrs / totals
}

// aiRiskDrag ports _ai_risk_drag (metrics/operating_review.py:835-844)
// verbatim.
func aiRiskDrag(rows []aiImpactRow) float64 {
	rates := []float64{
		avgF(pluck(rows, func(r aiImpactRow) *float64 { return r.reworkDragRate })),
		avgF(pluck(rows, func(r aiImpactRow) *float64 { return r.testGapRate })),
		avgF(pluck(rows, func(r aiImpactRow) *float64 { return r.incidentDragRate })),
	}
	var present []float64
	for _, v := range rates {
		if v > 0 {
			present = append(present, v)
		}
	}
	if len(present) == 0 {
		return 0.0
	}
	var total float64
	for _, v := range present {
		total += v
	}
	return total / float64(len(present))
}

// aiOpportunitySignals ports _ai_opportunity_signals
// (metrics/operating_review.py:860-873) verbatim.
func aiOpportunitySignals(impactRows []aiImpactRow, governanceRows []aiGovernanceRawRow) float64 {
	var signals float64
	if avgF(pluck(impactRows, func(r aiImpactRow) *float64 { return r.aiReviewAmplification })) >= 1.5 {
		signals += 1.0
	}
	if avgF(pluck(impactRows, func(r aiImpactRow) *float64 { return r.reworkDragRate })) >= 0.25 {
		signals += 1.0
	}
	if avgF(pluck(impactRows, func(r aiImpactRow) *float64 { return r.testGapRate })) >= 0.50 {
		signals += 1.0
	}
	coverage := aiGovernanceCoverage(governanceRows)
	if coverage > 0.0 && coverage < 0.80 {
		signals += 1.0
	}
	return signals
}

// firstNonZero ports _first_non_zero (metrics/operating_review.py:876-880)
// verbatim.
func firstNonZero(values ...float64) float64 {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0.0
}

// investmentKey ports _investment_key (metrics/operating_review.py:
// 893-902) verbatim.
func investmentKey(area string) (string, bool) {
	normalized := area
	normalized = replaceAll(normalized, "_", " ")
	normalized = replaceAll(normalized, "/", " ")
	switch normalized {
	case "ktlo", "maintenance", "maintenance tech debt":
		return "ktlo", true
	case "new value", "feature delivery", "features":
		return "new_value", true
	case "security", "risk security":
		return "security", true
	case "infra", "infrastructure", "operational support":
		return "infra", true
	default:
		return "", false
	}
}

func replaceAll(s, old, new string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == old[0] && len(old) == 1 {
			out = append(out, new...)
			continue
		}
		out = append(out, s[i])
	}
	return string(out)
}

// investmentUnits ports _investment_units (metrics/operating_review.py:
// 883-890) verbatim, including str(area).strip().lower() (Python's
// `.strip()` trims ASCII whitespace; Go's strings.TrimSpace matches for
// every input this column actually carries -- LowCardinality(String)
// investment_area values from a fixed producer vocabulary, never
// arbitrary Unicode whitespace).
func investmentUnits(rows []investmentRow) map[string]float64 {
	units := map[string]float64{"ktlo": 0.0, "new_value": 0.0, "security": 0.0, "infra": 0.0}
	for _, row := range rows {
		area := lowerTrim(row.investmentArea)
		if key, ok := investmentKey(area); ok {
			units[key] += row.deliveryUnits
		}
	}
	return units
}

func lowerTrim(s string) string {
	start, end := 0, len(s)
	for start < end && isSpace(s[start]) {
		start++
	}
	for end > start && isSpace(s[end-1]) {
		end--
	}
	s = s[start:end]
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		out[i] = c
	}
	return string(out)
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

// computeReview ports compute_operating_review and its six _xxx_section
// builders (metrics/operating_review.py:371-673) verbatim.
func computeReview(orgID string, teamID *string, weekStart time.Time, current, prior periodRows) *model.OperatingReview {
	sections := []reviewSection{
		deliverySection(current, prior),
		bottleneckSection(current, prior),
		riskSection(current, prior),
		reliabilitySection(current, prior),
		investmentSection(current, prior),
		aiWorkflowSection(current, prior),
	}
	recommendations := recommendationsFromSections(sections)

	out := &model.OperatingReview{
		OrgID:                     orgID,
		TeamID:                    teamID,
		WeekStart:                 graphqldate.New(weekStart),
		PriorWeekStart:            graphqldate.New(priorWeekStart(weekStart)),
		Sections:                  make([]model.OperatingReviewSection, 0, len(sections)),
		Recommendations:           recommendations,
		RecommendationsEmptyState: "No signals worsened this week.",
	}
	for _, s := range sections {
		out.Sections = append(out.Sections, toGraphQLSection(s))
	}
	// Non-nil even with zero recommendations: the schema declares
	// recommendations: [String!]! (non-null list), the same "initialize
	// explicitly" convention every sibling port's Resolve documents.
	if out.Recommendations == nil {
		out.Recommendations = []string{}
	}
	return out
}

func toGraphQLSection(s reviewSection) model.OperatingReviewSection {
	metrics := make([]model.OperatingReviewMetric, 0, len(s.metrics))
	for _, m := range s.metrics {
		metrics = append(metrics, model.OperatingReviewMetric{
			Key:   m.key,
			Label: m.label,
			Value: m.value,
			Unit:  m.unit,
			Delta: &model.OperatingReviewDelta{
				Value:      m.delta.value,
				PriorValue: m.delta.priorValue,
				Absolute:   m.delta.absolute,
				Percent:    m.delta.percent,
				Status:     m.delta.status,
			},
		})
	}
	changed, improved, worsened := s.changed, s.improved, s.worsened
	if changed == nil {
		changed = []string{}
	}
	if improved == nil {
		improved = []string{}
	}
	if worsened == nil {
		worsened = []string{}
	}
	return model.OperatingReviewSection{
		Key:      s.key,
		Title:    s.title,
		Metrics:  metrics,
		Changed:  changed,
		Improved: improved,
		Worsened: worsened,
	}
}

// deliverySection ports _delivery_section (metrics/operating_review.py:
// 408-440) verbatim.
func deliverySection(current, prior periodRows) reviewSection {
	return buildSection("delivery_movement", "Delivery movement", []reviewMetric{
		buildMetric("cycle_time_p50_hours", "Cycle time p50",
			avgF(pluck(current.workItems, func(r workItemsRow) *float64 { return r.cycleTimeP50Hours })),
			avgF(pluck(prior.workItems, func(r workItemsRow) *float64 { return r.cycleTimeP50Hours })),
			"hours", lowerIsBetter),
		buildMetric("throughput", "Throughput",
			sumF(pluck(current.workItems, func(r workItemsRow) *float64 { return f(r.itemsCompleted) })),
			sumF(pluck(prior.workItems, func(r workItemsRow) *float64 { return f(r.itemsCompleted) })),
			"items completed", higherIsBetter),
		buildMetric("wip_count", "WIP",
			maxF(pluck(current.workItems, func(r workItemsRow) *float64 { return f(r.wipCountEndOfDay) })),
			maxF(pluck(prior.workItems, func(r workItemsRow) *float64 { return f(r.wipCountEndOfDay) })),
			"items", lowerIsBetter),
	})
}

// bottleneckSection ports _bottleneck_section (metrics/operating_review.py:
// 443-477) verbatim.
func bottleneckSection(current, prior periodRows) reviewSection {
	return buildSection("bottleneck", "Bottleneck", []reviewMetric{
		buildMetric("state_duration_hours", "State duration",
			weightedAvgF(
				pluck(current.stateDurations, func(r stateDurationRow) *float64 { return f(r.durationHours) }),
				pluck(current.stateDurations, func(r stateDurationRow) *float64 { return f(r.itemsTouched) }),
			),
			weightedAvgF(
				pluck(prior.stateDurations, func(r stateDurationRow) *float64 { return f(r.durationHours) }),
				pluck(prior.stateDurations, func(r stateDurationRow) *float64 { return f(r.itemsTouched) }),
			),
			"hours", lowerIsBetter),
		buildMetric("review_latency_hours", "Review latency",
			avgF(pluck(current.repoMetrics, func(r repoMetricsRow) *float64 { return r.prFirstReviewP50Hours })),
			avgF(pluck(prior.repoMetrics, func(r repoMetricsRow) *float64 { return r.prFirstReviewP50Hours })),
			"hours", lowerIsBetter),
		buildMetric("wip_age_p90_hours", "WIP age p90",
			avgF(pluck(current.workItems, func(r workItemsRow) *float64 { return r.wipAgeP90Hours })),
			avgF(pluck(prior.workItems, func(r workItemsRow) *float64 { return r.wipAgeP90Hours })),
			"hours", lowerIsBetter),
	})
}

// riskSection ports _risk_section (metrics/operating_review.py:480-520)
// verbatim.
func riskSection(current, prior periodRows) reviewSection {
	return buildSection("risk", "Risk", []reviewMetric{
		buildMetric("hotspot_risk_score", "Hotspot risk",
			avgF(pluck(current.hotspots, func(r hotspotsAggRow) *float64 { return f(r.riskScore) })),
			avgF(pluck(prior.hotspots, func(r hotspotsAggRow) *float64 { return f(r.riskScore) })),
			"score", lowerIsBetter),
		buildMetric("ownership_concentration", "Ownership concentration",
			avgF(pluck(current.repoMetrics, func(r repoMetricsRow) *float64 { return f(r.singleOwnerFileRatio30d) })),
			avgF(pluck(prior.repoMetrics, func(r repoMetricsRow) *float64 { return f(r.singleOwnerFileRatio30d) })),
			"ratio", lowerIsBetter),
		buildMetric("complexity_per_kloc", "Complexity",
			avgF(pluck(current.complexity, func(r complexityAggRow) *float64 { return f(r.cyclomaticPerKloc) })),
			avgF(pluck(prior.complexity, func(r complexityAggRow) *float64 { return f(r.cyclomaticPerKloc) })),
			"cyclomatic/KLOC", lowerIsBetter),
		buildMetric("bus_factor", "Bus factor",
			minF(pluck(current.repoMetrics, func(r repoMetricsRow) *float64 { return f(r.busFactor) })),
			minF(pluck(prior.repoMetrics, func(r repoMetricsRow) *float64 { return f(r.busFactor) })),
			"people", higherIsBetter),
	})
}

// reliabilitySection ports _reliability_section
// (metrics/operating_review.py:523-569) verbatim.
func reliabilitySection(current, prior periodRows) reviewSection {
	return buildSection("reliability", "Reliability", []reviewMetric{
		buildMetric("deployments_count", "Deployments",
			sumF(pluck(current.deployments, func(r deploymentsAggRow) *float64 { return f(r.deploymentsCount) })),
			sumF(pluck(prior.deployments, func(r deploymentsAggRow) *float64 { return f(r.deploymentsCount) })),
			"deployments", higherIsBetter),
		buildMetric("change_failure_rate", "Change failure rate",
			changeFailureRate(current), changeFailureRate(prior), "ratio", lowerIsBetter),
		buildMetric("incidents_count", "Incidents",
			sumF(pluck(current.incidents, func(r incidentsAggRow) *float64 { return f(r.incidentsCount) })),
			sumF(pluck(prior.incidents, func(r incidentsAggRow) *float64 { return f(r.incidentsCount) })),
			"incidents", lowerIsBetter),
		buildMetric("mttr_hours", "MTTR",
			firstNonZero(
				avgF(pluck(current.incidents, func(r incidentsAggRow) *float64 { return r.mttrP50Hours })),
				avgF(pluck(current.repoMetrics, func(r repoMetricsRow) *float64 { return r.mttrHours })),
			),
			firstNonZero(
				avgF(pluck(prior.incidents, func(r incidentsAggRow) *float64 { return r.mttrP50Hours })),
				avgF(pluck(prior.repoMetrics, func(r repoMetricsRow) *float64 { return r.mttrHours })),
			),
			"hours", lowerIsBetter),
	})
}

// investmentSection ports _investment_section (metrics/operating_review.py:
// 572-614) verbatim.
func investmentSection(current, prior periodRows) reviewSection {
	currentUnits := investmentUnits(current.investment)
	priorUnits := investmentUnits(prior.investment)
	return buildSection("investment", "Investment", []reviewMetric{
		buildMetric("ktlo_units", "KTLO", currentUnits["ktlo"], priorUnits["ktlo"], "delivery units", lowerIsBetter),
		buildMetric("new_value_units", "New value", currentUnits["new_value"], priorUnits["new_value"], "delivery units", higherIsBetter),
		buildMetric("security_units", "Security", currentUnits["security"], priorUnits["security"], "delivery units", neutral),
		buildMetric("infra_units", "Infra", currentUnits["infra"], priorUnits["infra"], "delivery units", neutral),
	})
}

// aiWorkflowSection ports _ai_workflow_section (metrics/operating_review.py:
// 617-673) verbatim -- ai_governance_coverage is the ONE metric fed by the
// Go-side fix (aiGovernanceCoverage); every other metric in this section
// is a verbatim port.
func aiWorkflowSection(current, prior periodRows) reviewSection {
	return buildSection("ai_workflow_intelligence", "AI Workflow Intelligence", []reviewMetric{
		buildMetric("ai_adoption_ratio", "AI adoption mix",
			aiAdoptionRatio(current.aiImpact), aiAdoptionRatio(prior.aiImpact), "ratio", neutral),
		buildMetric("ai_cycle_time_delta_hours", "AI delivery impact",
			avgF(pluck(current.aiImpact, func(r aiImpactRow) *float64 { return r.aiCycleTimeDeltaHours })),
			avgF(pluck(prior.aiImpact, func(r aiImpactRow) *float64 { return r.aiCycleTimeDeltaHours })),
			"hours", lowerIsBetter),
		buildMetric("ai_review_amplification", "AI review pressure",
			avgF(pluck(current.aiImpact, func(r aiImpactRow) *float64 { return r.aiReviewAmplification })),
			avgF(pluck(prior.aiImpact, func(r aiImpactRow) *float64 { return r.aiReviewAmplification })),
			"ratio", lowerIsBetter),
		buildMetric("ai_risk_drag", "AI risk drag",
			aiRiskDrag(current.aiImpact), aiRiskDrag(prior.aiImpact), "ratio", lowerIsBetter),
		buildMetric("ai_governance_coverage", "AI governance coverage",
			aiGovernanceCoverage(current.aiGovernance), aiGovernanceCoverage(prior.aiGovernance),
			"ratio", higherIsBetter),
		buildMetric("ai_opportunity_signals", "AI opportunity signals",
			aiOpportunitySignals(current.aiImpact, current.aiGovernance),
			aiOpportunitySignals(prior.aiImpact, prior.aiGovernance),
			"signals", lowerIsBetter),
	})
}

// ---------------------------------------------------------------------------
// Resolve -- ports resolve_operating_review (resolvers/operating_review.py:
// 29-63) verbatim.
// ---------------------------------------------------------------------------

// Resolve ports resolve_operating_review. orgID must already be the
// AUTHORIZED org (the caller's verified envelope claim) -- see this
// package's doc comment's Authorization section for why the GraphQL
// schema's separate `orgId` argument is intentionally NOT a parameter
// here: Python parses and discards it, using require_org_id(context)
// alone, and this port reproduces that by construction.
func Resolve(ctx context.Context, client QueryClient, orgID string, teamID *string, weekStart graphqldate.Date) (*model.OperatingReview, error) {
	if client == nil {
		return nil, errors.New("operatingreview: clickhouse client is required")
	}

	weekStartT := weekStart.Time()
	current := fetchPeriodRows(ctx, client, orgID, teamID, weekStartT)
	prior := fetchPeriodRows(ctx, client, orgID, teamID, priorWeekStart(weekStartT))

	return computeReview(orgID, teamID, weekStartT, current, prior), nil
}
