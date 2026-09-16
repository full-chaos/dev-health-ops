// GET /api/v1/people/{person_id}/summary -- ports
// build_person_summary_response (services/people.py:453-629) and the
// freshness/coverage queries it calls (api/queries/freshness.py's
// fetch_last_ingested_at/fetch_coverage) plus fetch_identity_coverage
// (queries/people.py:48-62). Identity resolution (resolve.go) and the
// per-metric delta/spark reads (metricconfig.go/metricqueries.go) are
// shared with GET /api/v1/people/{person_id}/metric (metric.go).
package people

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/quadrant"
)

// safeFloat ports safe_float (api/utils/numeric.py:22-36) specialized to
// a value already typed float64 (this package's ClickHouse rows scan
// straight into float64/nil, never an untyped Any the way Python's dict
// rows are) -- a NaN/Inf aggregate (e.g. a ClickHouse avg() over an
// all-NULL group) collapses to the same 0.0 default Python's
// try/except+math.isfinite guard produces.
func safeFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0.0
	}
	return v
}

// safeTransform ports safe_transform (api/utils/numeric.py:55-65).
func safeTransform(transform func(float64) float64, v float64) float64 {
	return safeFloat(transform(v))
}

// deltaPct ports delta_pct (api/utils/numeric.py:68-80).
func deltaPct(current, previous float64) float64 {
	if previous == 0 {
		return 0.0
	}
	return (current - previous) / previous * 100.0
}

// SummaryParams is GET /api/v1/people/{person_id}/summary's
// already-resolved request shape -- the route file owns query-param
// parsing/validation, this package owns the business logic, matching
// SearchParams/drilldown.PRParams's own division of labor.
type SummaryParams struct {
	PersonID    string
	RangeDays   int
	CompareDays int
	// Now stands in for utc_today() (dev_health_ops/utils/datetime.py:
	// 32-34), injected by the route file so this package's own tests can
	// pin it -- matching SearchParams.Now's own doc comment.
	Now time.Time
}

// PersonIdentityWireModel ports PersonSummaryPerson (api/models/schemas.py:
// 368-372).
type PersonIdentityWireModel struct {
	PersonID    string           `json:"person_id"`
	DisplayName string           `json:"display_name"`
	Identities  []PersonIdentity `json:"identities"`
}

// Coverage ports Coverage (api/models/schemas.py:9-12).
type Coverage struct {
	ReposCoveredPct          float64 `json:"repos_covered_pct"`
	PRsLinkedToIssuesPct     float64 `json:"prs_linked_to_issues_pct"`
	IssuesWithCycleStatesPct float64 `json:"issues_with_cycle_states_pct"`
}

// Freshness ports Freshness (api/models/schemas.py:15-19).
// LatestSuccessfulSyncAt is never set by build_person_summary_response
// (services/people.py:611-617 never passes it) -- it stays the model's
// own `= None` default, so this field is always nil and the JSON key is
// always present with a null value (no `omitempty`), matching Pydantic's
// `model_dump(mode="json")` for an explicit None field.
type Freshness struct {
	LastIngestedAt         *time.Time        `json:"last_ingested_at"`
	LatestSuccessfulSyncAt *time.Time        `json:"latest_successful_sync_at"`
	Sources                map[string]string `json:"sources"`
	Coverage               Coverage          `json:"coverage"`
}

// SparkPoint ports SparkPoint (api/models/schemas.py:22-24).
type SparkPoint struct {
	Ts    time.Time `json:"ts"`
	Value float64   `json:"value"`
}

// PersonDelta ports PersonDelta (api/models/schemas.py:378-384).
type PersonDelta struct {
	Metric   string       `json:"metric"`
	Label    string       `json:"label"`
	Value    float64      `json:"value"`
	Unit     string       `json:"unit"`
	DeltaPct float64      `json:"delta_pct"`
	Spark    []SparkPoint `json:"spark"`
}

// SummarySentence ports SummarySentence (api/models/schemas.py:45-48).
type SummarySentence struct {
	ID           string `json:"id"`
	Text         string `json:"text"`
	EvidenceLink string `json:"evidence_link"`
}

// WorkMixItem ports WorkMixItem (api/models/schemas.py:387-390).
type WorkMixItem struct {
	Key   string  `json:"key"`
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

// FlowStageItem ports FlowStageItem (api/models/schemas.py:393-396).
type FlowStageItem struct {
	Stage string  `json:"stage"`
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
}

// CollaborationItem ports CollaborationItem (api/models/schemas.py:
// 399-401).
type CollaborationItem struct {
	Label string  `json:"label"`
	Value float64 `json:"value"`
}

// CollaborationSection ports CollaborationSection (api/models/schemas.py:
// 404-406).
type CollaborationSection struct {
	ReviewLoad    []CollaborationItem `json:"review_load"`
	HandoffPoints []CollaborationItem `json:"handoff_points"`
}

// PersonSummarySections ports PersonSummarySections (api/models/
// schemas.py:409-412).
type PersonSummarySections struct {
	WorkMix       []WorkMixItem        `json:"work_mix"`
	FlowBreakdown []FlowStageItem      `json:"flow_breakdown"`
	Collaboration CollaborationSection `json:"collaboration"`
}

// SummaryResponse ports PersonSummaryResponse (api/models/schemas.py:
// 415-421).
type SummaryResponse struct {
	Person              PersonIdentityWireModel `json:"person"`
	Freshness           Freshness               `json:"freshness"`
	IdentityCoveragePct float64                 `json:"identity_coverage_pct"`
	Deltas              []PersonDelta           `json:"deltas"`
	Narrative           []SummarySentence       `json:"narrative"`
	Sections            PersonSummarySections   `json:"sections"`
}

// fetchLastIngestedAt ports fetch_last_ingested_at (api/queries/
// freshness.py:30-49). repo_metrics_daily is ReplacingMergeTree
// (computed_at) (migration 096) -- read FINAL, this package's own class
// ruling (see metricconfig.go's own doc comment).
func fetchLastIngestedAt(ctx context.Context, client QueryClient, orgID string) (*time.Time, error) {
	query := fmt.Sprintf(`
        SELECT maxOrNull(computed_at) AS last_ingested_at
        FROM repo_metrics_daily FINAL
        WHERE org_id = {org_id:String}
        %s
    `, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{{Name: "org_id", Value: orgID}})
	if err != nil {
		return nil, fmt.Errorf("people: fetch_last_ingested_at query: %w", err)
	}
	defer rows.Close()

	var value *time.Time
	if rows.Next() {
		var t time.Time
		var null bool
		if err := scanNullableTime(rows, &t, &null); err != nil {
			return nil, fmt.Errorf("people: fetch_last_ingested_at scan: %w", err)
		}
		if !null {
			value = &t
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("people: fetch_last_ingested_at: %w", err)
	}
	return value, nil
}

// scanNullableTime scans a single nullable DateTime column via a
// *time.Time destination -- dev-health-go's RowScanner (like ClickHouse's
// own driver) leaves *t at its zero value and reports no error for a SQL
// NULL scanned into a non-pointer destination is not how this binary's
// other packages do it; matching quadrant/drilldown's own convention,
// this package scans into a **time.Time* to distinguish "no row"/"NULL"
// from a real zero time on the wire.
func scanNullableTime(rows dhclickhouse.RowScanner, t *time.Time, isNull *bool) error {
	var ptr *time.Time
	if err := rows.Scan(&ptr); err != nil {
		return err
	}
	if ptr == nil {
		*isNull = true
		return nil
	}
	*t = *ptr
	*isNull = false
	return nil
}

// fetchCoverage ports fetch_coverage (api/queries/freshness.py:52-121):
// three independent percentages over [startDay, endDay). repos/
// repo_metrics_daily are read FINAL for the countDistinct/max(version)
// queries (correctness-neutral for those specific aggregates, but this
// package's class ruling holds uniformly, see metricconfig.go); the
// work_item_cycle_times counts are NOT correctness-neutral -- a
// recomputed item would double-count in count(*)/countIf() without
// FINAL, the exact defect class the ruling exists to close. Every
// count()/countIf()/countDistinct() here is wrapped `toFloat64(...)`:
// ClickHouse's count-family functions return UInt64, and this binary's
// native driver rejects scanning an unsigned column into fetchScalarFloat/
// fetchScalarFloatPair's plain float64 destinations outright -- the same
// class of mismatch cmd/query-api/internal/analytics/investmentquality.go's
// own row-type doc comment documents (that file instead scans into
// uint64 first, a fixed-shape query's own equivalent fix; this file's
// queries feed generic float64-typed helpers shared across several
// different aggregate shapes, so normalizing at the SQL level, the same
// fix cmd/query-api/internal/quadrant/quadrant.go's own config-driven
// `toFloat64(%s) AS value` already applies, is the simpler one here).
func fetchCoverage(ctx context.Context, client QueryClient, startDay, endDay time.Time, orgID string) (Coverage, error) {
	totalRepos, err := fetchScalarFloat(ctx, client, fmt.Sprintf(`
        SELECT toFloat64(countDistinct(id)) AS total
        FROM repos FINAL
        WHERE org_id = {org_id:String}
        %s
    `, settingsMaxExecutionTime()), []dhclickhouse.Binding{{Name: "org_id", Value: orgID}})
	if err != nil {
		return Coverage{}, fmt.Errorf("people: fetch_coverage total_repos: %w", err)
	}

	covered, err := fetchScalarFloat(ctx, client, fmt.Sprintf(`
        SELECT toFloat64(countDistinct(repo_id)) AS covered
        FROM repo_metrics_daily FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND org_id = {org_id:String}
        %s
    `, settingsMaxExecutionTime()), []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	})
	if err != nil {
		return Coverage{}, fmt.Errorf("people: fetch_coverage covered: %w", err)
	}

	linked, total, err := fetchScalarFloatPair(ctx, client, fmt.Sprintf(`
        SELECT
            toFloat64(countIf(work_scope_id != '')) AS linked,
            toFloat64(count(*)) AS total
        FROM work_item_cycle_times FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND org_id = {org_id:String}
        %s
    `, settingsMaxExecutionTime()), []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	})
	if err != nil {
		return Coverage{}, fmt.Errorf("people: fetch_coverage pr_link: %w", err)
	}

	withCycle, totalCycle, err := fetchScalarFloatPair(ctx, client, fmt.Sprintf(`
        SELECT
            toFloat64(countIf(cycle_time_hours IS NOT NULL)) AS with_cycle,
            toFloat64(count(*)) AS total
        FROM work_item_cycle_times FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND org_id = {org_id:String}
        %s
    `, settingsMaxExecutionTime()), []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	})
	if err != nil {
		return Coverage{}, fmt.Errorf("people: fetch_coverage cycle: %w", err)
	}

	reposCoveredPct := 0.0
	if totalRepos != 0 {
		reposCoveredPct = covered / totalRepos * 100.0
	}
	prsLinkedPct := 0.0
	if total != 0 {
		prsLinkedPct = linked / total * 100.0
	}
	issuesCyclePct := 0.0
	if totalCycle != 0 {
		issuesCyclePct = withCycle / totalCycle * 100.0
	}

	return Coverage{
		ReposCoveredPct:          reposCoveredPct,
		PRsLinkedToIssuesPct:     prsLinkedPct,
		IssuesWithCycleStatesPct: issuesCyclePct,
	}, nil
}

// fetchScalarFloat runs a one-row, one-column aggregate query, treating a
// SQL NULL or an absent row as 0 -- matching Python's own
// `float((rows[0].get(...) or 0) if rows else 0)` pattern shared by every
// fetch_coverage sub-query.
func fetchScalarFloat(ctx context.Context, client QueryClient, query string, bindings []dhclickhouse.Binding) (float64, error) {
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var value float64
	if rows.Next() {
		if err := rows.Scan(&value); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return value, nil
}

// fetchScalarFloatPair runs a one-row, two-column aggregate query.
func fetchScalarFloatPair(ctx context.Context, client QueryClient, query string, bindings []dhclickhouse.Binding) (a, b float64, err error) {
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&a, &b); err != nil {
			return 0, 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}
	return a, b, nil
}

// fetchIdentityCoverage ports fetch_identity_coverage (queries/people.py:
// 48-62) and its person_identity_coverage.sql: countDistinct(source) over
// a UNION of one 'code' row (if user_metrics_daily has a matching row)
// and one 'work' row (if work_item_user_metrics_daily FINAL does) -- 0,
// 1 or 2. Both branches read FINAL, matching resolvePersonIdentity's own
// declared fix for the identical user_metrics_daily gap.
func fetchIdentityCoverage(ctx context.Context, client QueryClient, identities []string, orgID string) (int, error) {
	query := fmt.Sprintf(`
        SELECT countDistinct(source) AS sources
        FROM (
            SELECT 'code' AS source
            FROM user_metrics_daily FINAL
            WHERE identity_id IN {identities:Array(String)}
              AND org_id = {org_id:String}
            LIMIT 1

            UNION ALL

            SELECT 'work' AS source
            FROM work_item_user_metrics_daily FINAL
            WHERE user_identity IN {identities:Array(String)}
              AND org_id = {org_id:String}
            LIMIT 1
        )
        %s
    `, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{
		{Name: "identities", Value: identities},
		{Name: "org_id", Value: orgID},
	})
	if err != nil {
		return 0, fmt.Errorf("people: fetch_identity_coverage query: %w", err)
	}
	defer rows.Close()
	// countDistinct() is UInt64 in ClickHouse; scan into uint64 first and
	// narrow only once the value is safely in Go (the same fixed-query
	// fix cmd/query-api/internal/analytics/investmentquality.go's own row
	// type doc comment documents -- this is a single, one-shot query, not
	// a shared generic helper, so there is no need for fetchCoverage's
	// own toFloat64 SQL-side normalization here).
	var sources uint64
	if rows.Next() {
		if err := rows.Scan(&sources); err != nil {
			return 0, fmt.Errorf("people: fetch_identity_coverage scan: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("people: fetch_identity_coverage: %w", err)
	}
	return int(sources), nil
}

// fetchPersonWorkMix ports fetch_person_work_mix (queries/people.py:
// 181-196) and its person_summary_work_mix.sql, with the declared FINAL
// fix (see metricconfig.go's own doc comment: work_item_cycle_times is
// registered in neither of the reference's dedup tables). count() is
// wrapped toFloat64() for the same UInt64-into-float64 scan reason as
// fetchCoverage's own doc comment.
func fetchPersonWorkMix(ctx context.Context, client QueryClient, identities []string, startDay, endDay time.Time, orgID string) ([]WorkMixItem, error) {
	query := fmt.Sprintf(`
        SELECT
            lower(if(type = '' OR type IS NULL, 'unknown', type)) AS key,
            if(type = '' OR type IS NULL, 'Unknown', type) AS name,
            toFloat64(count()) AS value
        FROM work_item_cycle_times FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND assignee IN {identities:Array(String)}
          AND org_id = {org_id:String}
        GROUP BY key, name
        ORDER BY value DESC
        %s
    `, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "identities", Value: identities},
		{Name: "org_id", Value: orgID},
	})
	if err != nil {
		return nil, fmt.Errorf("people: fetch_person_work_mix query: %w", err)
	}
	defer rows.Close()

	out := make([]WorkMixItem, 0)
	for rows.Next() {
		var key, name string
		var value float64
		if err := rows.Scan(&key, &name, &value); err != nil {
			return nil, fmt.Errorf("people: fetch_person_work_mix scan: %w", err)
		}
		out = append(out, WorkMixItem{Key: key, Name: name, Value: safeFloat(value)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("people: fetch_person_work_mix: %w", err)
	}
	return out, nil
}

// fetchPersonFlowBreakdown ports fetch_person_flow_breakdown (queries/
// people.py:199-214) and its person_summary_flow_breakdown.sql, with the
// same declared FINAL fix as fetchPersonWorkMix. Each branch is a
// scalar avg() with NO GROUP BY -- the same "always exactly one row,
// possibly NULL over zero matching rows" shape personMetricValueQuery's
// own doc comment documents (a person idle in this window, or whose
// items never carry lead_time_hours, is unexceptional) -- so both
// branches are wrapped `coalesce(toFloat64(...), 0)`, not just
// toFloat64.
func fetchPersonFlowBreakdown(ctx context.Context, client QueryClient, identities []string, startDay, endDay time.Time, orgID string) ([]FlowStageItem, error) {
	query := fmt.Sprintf(`
        SELECT
            'Active' AS stage,
            coalesce(toFloat64(avg(cycle_time_hours)), 0) AS value,
            'hours' AS unit
        FROM work_item_cycle_times FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND assignee IN {identities:Array(String)}
          AND org_id = {org_id:String}
          AND cycle_time_hours IS NOT NULL

        UNION ALL

        SELECT
            'Waiting' AS stage,
            coalesce(toFloat64(avg(lead_time_hours - cycle_time_hours)), 0) AS value,
            'hours' AS unit
        FROM work_item_cycle_times FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND assignee IN {identities:Array(String)}
          AND org_id = {org_id:String}
          AND lead_time_hours IS NOT NULL
          AND cycle_time_hours IS NOT NULL
        %s
    `, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "identities", Value: identities},
		{Name: "org_id", Value: orgID},
	})
	if err != nil {
		return nil, fmt.Errorf("people: fetch_person_flow_breakdown query: %w", err)
	}
	defer rows.Close()

	out := make([]FlowStageItem, 0)
	for rows.Next() {
		var stage, unit string
		var value float64
		if err := rows.Scan(&stage, &value, &unit); err != nil {
			return nil, fmt.Errorf("people: fetch_person_flow_breakdown scan: %w", err)
		}
		out = append(out, FlowStageItem{Stage: stage, Value: safeFloat(value), Unit: unit})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("people: fetch_person_flow_breakdown: %w", err)
	}
	return out, nil
}

// fetchPersonCollaboration ports fetch_person_collaboration (queries/
// people.py:217-232) and its person_summary_collaboration.sql verbatim:
// the user_metrics_daily branch already dedups correctly (argMax over
// non-Nullable UInt32 columns, GROUP BY the table's own natural key --
// see metricconfig.go's own doc comment on why this is NOT the
// argMax-over-Nullable-column gap), and the work_item_user_metrics_daily branches
// already carry FINAL.
func fetchPersonCollaboration(ctx context.Context, client QueryClient, identities []string, startDay, endDay time.Time, orgID string) ([]struct {
	Section string
	Label   string
	Value   float64
}, error) {
	query := fmt.Sprintf(`
        WITH latest_user_metrics AS (
            SELECT
                day,
                repo_id,
                author_email,
                argMax(reviews_given, computed_at) AS reviews_given,
                argMax(reviews_received, computed_at) AS reviews_received,
                argMax(prs_authored, computed_at) AS prs_authored,
                argMax(prs_merged, computed_at) AS prs_merged
            FROM user_metrics_daily
            WHERE day >= {start_day:Date} AND day < {end_day:Date}
              AND identity_id IN {identities:Array(String)}
              AND org_id = {org_id:String}
            GROUP BY day, repo_id, author_email
        )

        SELECT 'review_load' AS section, 'Reviews given' AS label, toFloat64(sum(reviews_given)) AS value FROM latest_user_metrics

        UNION ALL

        SELECT 'review_load' AS section, 'Reviews received' AS label, toFloat64(sum(reviews_received)) AS value FROM latest_user_metrics

        UNION ALL

        SELECT 'review_load' AS section, 'PRs authored' AS label, toFloat64(sum(prs_authored)) AS value FROM latest_user_metrics

        UNION ALL

        SELECT 'review_load' AS section, 'PRs merged' AS label, toFloat64(sum(prs_merged)) AS value FROM latest_user_metrics

        UNION ALL

        SELECT 'handoff_points' AS section, 'Items started' AS label, toFloat64(sum(items_started)) AS value
        FROM work_item_user_metrics_daily FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND user_identity IN {identities:Array(String)}
          AND org_id = {org_id:String}

        UNION ALL

        SELECT 'handoff_points' AS section, 'Items completed' AS label, toFloat64(sum(items_completed)) AS value
        FROM work_item_user_metrics_daily FINAL
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
          AND user_identity IN {identities:Array(String)}
          AND org_id = {org_id:String}
        %s
    `, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "identities", Value: identities},
		{Name: "org_id", Value: orgID},
	})
	if err != nil {
		return nil, fmt.Errorf("people: fetch_person_collaboration query: %w", err)
	}
	defer rows.Close()

	out := make([]struct {
		Section string
		Label   string
		Value   float64
	}, 0)
	for rows.Next() {
		var section, label string
		var value float64
		if err := rows.Scan(&section, &label, &value); err != nil {
			return nil, fmt.Errorf("people: fetch_person_collaboration scan: %w", err)
		}
		out = append(out, struct {
			Section string
			Label   string
			Value   float64
		}{Section: section, Label: label, Value: value})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("people: fetch_person_collaboration: %w", err)
	}
	return out, nil
}

// timeWindow ports _time_window (services/people.py:249-256).
func timeWindow(now time.Time, rangeDays, compareDays int) (startDay, endDay, compareStart, compareEnd time.Time) {
	endDay = now.Truncate(24*time.Hour).AddDate(0, 0, 1)
	if rangeDays < 1 {
		rangeDays = 1
	}
	if compareDays < 1 {
		compareDays = 1
	}
	startDay = endDay.AddDate(0, 0, -rangeDays)
	compareEnd = startDay
	compareStart = compareEnd.AddDate(0, 0, -compareDays)
	return startDay, endDay, compareStart, compareEnd
}

// metricLink ports _metric_link (services/people.py:316-322).
func metricLink(personID, metric string, rangeDays, compareDays int) string {
	return fmt.Sprintf("/api/v1/people/%s/metric?metric=%s&range_days=%d&compare_days=%d", personID, metric, rangeDays, compareDays)
}

// narrativeForDeltas ports _narrative_for_deltas (services/people.py:
// 331-371): the top-2-by-|delta_pct| deltas, as human sentences, ranked
// with a STABLE sort (Python's `sorted` is stable; ties keep
// personMetrics' own declaration order) -- sort.SliceStable, not
// sort.Slice, for the same reason.
func narrativeForDeltas(deltas []PersonDelta, personID string, rangeDays, compareDays int) []SummarySentence {
	ranked := append([]PersonDelta(nil), deltas...)
	sort.SliceStable(ranked, func(i, j int) bool {
		return math.Abs(ranked[i].DeltaPct) > math.Abs(ranked[j].DeltaPct)
	})

	narrative := make([]SummarySentence, 0, 2)
	for idx, delta := range ranked {
		if idx >= 2 {
			break
		}
		direction := "decreased"
		if delta.DeltaPct > 0 {
			direction = "increased"
		}
		if delta.DeltaPct == 0 {
			direction = "held steady"
		}

		var text string
		switch delta.Metric {
		case "review_latency":
			text = fmt.Sprintf("Review latency %s over the last %d days.", direction, rangeDays)
		case "cycle_time":
			text = fmt.Sprintf("Cycle time %s over the last %d days.", direction, rangeDays)
		case "throughput":
			text = fmt.Sprintf("Throughput %s compared to the prior period.", direction)
		case "churn":
			text = fmt.Sprintf("Code churn %s in this period.", direction)
		case "wip_overlap":
			text = fmt.Sprintf("WIP overlap %s in this period.", direction)
		case "blocked_work":
			text = fmt.Sprintf("Blocked work %s in this period.", direction)
		default:
			text = fmt.Sprintf("%s %s in this period.", delta.Label, direction)
		}

		narrative = append(narrative, SummarySentence{
			ID:           fmt.Sprintf("n%d", idx+1),
			Text:         text,
			EvidenceLink: metricLink(personID, delta.Metric, rangeDays, compareDays),
		})
	}
	return narrative
}

// BuildSummaryResponse is the Go port of build_person_summary_response
// (api/services/people.py:453-629). Auth (current_user.org_id), the
// outer try/except -> 503 fallback, and _reject_comparative_params are
// the CALLER's job (route file) -- this function returns a plain Go
// error for any failure (a *RequestError for the typed 404, a plain
// error for anything else), never an HTTP status, matching
// quadrant.BuildResponse's own contract.
func BuildSummaryResponse(ctx context.Context, reader *Reader, orgID string, params SummaryParams) (SummaryResponse, error) {
	if reader == nil {
		return SummaryResponse{}, ErrUnavailable
	}

	startDay, endDay, compareStart, compareEnd := timeWindow(params.Now, params.RangeDays, params.CompareDays)

	canonical, aliasList, err := resolveIdentityContext(ctx, reader.client, params.PersonID, orgID)
	if err != nil {
		return SummaryResponse{}, err
	}
	if canonical == "" {
		// `if not identity: raise ValueError("person not found")`
		// (services/people.py:473-474) -> main.py's own 404 "Person not
		// found" (main.py:1086-1087).
		return SummaryResponse{}, notFound("Person not found")
	}
	identityInputs := identityVariants(canonical, aliasList)

	person := PersonIdentityWireModel{
		PersonID:    quadrant.PersonIDForIdentity(canonical),
		DisplayName: displayNameForIdentity(canonical),
		Identities:  identitiesForPerson(canonical, aliasList),
	}

	lastIngested, err := fetchLastIngestedAt(ctx, reader.client, orgID)
	if err != nil {
		return SummaryResponse{}, err
	}
	coverage, err := fetchCoverage(ctx, reader.client, startDay, endDay, orgID)
	if err != nil {
		return SummaryResponse{}, err
	}

	status := "down"
	if lastIngested != nil {
		status = "ok"
	}
	sources := map[string]string{
		"github": status,
		"gitlab": status,
		"jira":   status,
		"ci":     status,
	}

	coverageSources, err := fetchIdentityCoverage(ctx, reader.client, identityInputs, orgID)
	if err != nil {
		return SummaryResponse{}, err
	}
	identityCoveragePct := safeFloat(float64(coverageSources) / 2.0 * 100.0)

	deltas := make([]PersonDelta, 0, len(personMetrics))
	for _, metric := range personMetrics {
		currentValue, err := fetchPersonMetricValue(ctx, reader.client, metric.Table, metric.Column, metric.Aggregator, metric.IdentityColumn, identityInputs, startDay, endDay, metric.ExtraWhere, orgID)
		if err != nil {
			return SummaryResponse{}, err
		}
		previousValue, err := fetchPersonMetricValue(ctx, reader.client, metric.Table, metric.Column, metric.Aggregator, metric.IdentityColumn, identityInputs, compareStart, compareEnd, metric.ExtraWhere, orgID)
		if err != nil {
			return SummaryResponse{}, err
		}
		series, err := fetchPersonMetricSeries(ctx, reader.client, metric.Table, metric.Column, metric.Aggregator, metric.IdentityColumn, identityInputs, startDay, endDay, metric.ExtraWhere, orgID)
		if err != nil {
			return SummaryResponse{}, err
		}

		current := safeFloat(currentValue)
		previous := safeFloat(previousValue)
		pctChange := safeFloat(deltaPct(current, previous))

		spark := make([]SparkPoint, 0, len(series))
		for _, row := range series {
			spark = append(spark, SparkPoint{Ts: row.Day, Value: safeTransform(metric.Transform, safeFloat(row.Value))})
		}

		deltas = append(deltas, PersonDelta{
			Metric:   metric.Metric,
			Label:    metric.Label,
			Value:    safeTransform(metric.Transform, current),
			Unit:     metric.Unit,
			DeltaPct: pctChange,
			Spark:    spark,
		})
	}

	workMixRows, err := fetchPersonWorkMix(ctx, reader.client, identityInputs, startDay, endDay, orgID)
	if err != nil {
		return SummaryResponse{}, err
	}

	flowRows, err := fetchPersonFlowBreakdown(ctx, reader.client, identityInputs, startDay, endDay, orgID)
	if err != nil {
		return SummaryResponse{}, err
	}

	collabRows, err := fetchPersonCollaboration(ctx, reader.client, identityInputs, startDay, endDay, orgID)
	if err != nil {
		return SummaryResponse{}, err
	}
	reviewLoad := make([]CollaborationItem, 0)
	handoffPoints := make([]CollaborationItem, 0)
	for _, row := range collabRows {
		item := CollaborationItem{Label: row.Label, Value: safeFloat(row.Value)}
		if row.Section == "handoff_points" {
			handoffPoints = append(handoffPoints, item)
		} else {
			reviewLoad = append(reviewLoad, item)
		}
	}

	narrative := narrativeForDeltas(deltas, person.PersonID, params.RangeDays, params.CompareDays)

	return SummaryResponse{
		Person: person,
		Freshness: Freshness{
			LastIngestedAt:         lastIngested,
			LatestSuccessfulSyncAt: nil,
			Sources:                sources,
			Coverage:               coverage,
		},
		IdentityCoveragePct: identityCoveragePct,
		Deltas:              deltas,
		Narrative:           narrative,
		Sections: PersonSummarySections{
			WorkMix:       workMixRows,
			FlowBreakdown: flowRows,
			Collaboration: CollaborationSection{
				ReviewLoad:    reviewLoad,
				HandoffPoints: handoffPoints,
			},
		},
	}, nil
}
