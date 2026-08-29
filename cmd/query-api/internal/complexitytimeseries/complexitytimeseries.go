// Package complexitytimeseries is the Go port of
// dev_health_ops.api.graphql.resolvers.complexity.resolve_complexity_timeseries
// (ops/src/dev_health_ops/api/graphql/resolvers/complexity.py), CHAOS-4352
// plan §6 Wave 3's first operation, ported after CHAOS-4368 (reviewEdges).
//
// Ported deliberately verbatim: same two-table split by scope (REPO reads
// repo_complexity_daily, FILE reads file_complexity_snapshots), same
// argMax(<col>, computed_expr) "latest compute pass" selection where
// computed_expr is plain computed_at for DAY granularity or the tuple
// (day|as_of_day, computed_at) for WEEK granularity (so the latest row
// within each week's bucket wins, not merely the latest computed_at
// across the whole window), same repo-scope default-top-N-by-complexity
// subquery when repoIds is empty, same repo_ids-array truncation to the
// effective limit before it becomes a bind parameter, same two-stage
// limit clamp (1..MAX_ROWS, then further bounded by
// MAX_TIMESERIES_POINTS/bucketCount so a wide date range cannot return an
// unbounded number of points), and the same repo-label join
// (best-effort: falls back to the repo_id string when the repos catalog
// row is missing).
//
// Side effects: none to replicate. Verified by reading
// resolve_complexity_timeseries/_fetch_repo_timeseries/_fetch_file_timeseries/
// _load_repo_labels top to bottom: three read-only ClickHouse queries at
// most (repo/file fetch + label lookup) and a dataclass construction -- no
// telemetry/audit hook call inside it or anything it calls (same finding
// class as reviewedges's and featureflags's doc comments).
//
// Missing-table behavior: unlike featureFlags, resolve_complexity_timeseries
// has NO try/except around its ClickHouse calls and ComplexityTimeseriesResult
// has no degradedReason field -- a missing repo_complexity_daily /
// file_complexity_snapshots table is a real error on the Python side, not a
// degraded empty result. This port does not invent a degraded path Python
// doesn't have; a ClickHouse error propagates as a Go error (and, via
// schema.resolvers.go, a GraphQL error) -- the same behavior reviewedges
// documents for its own missing-table case.
package complexitytimeseries

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// MaxRows mirrors Python's MAX_ROWS -- the hard cap on the caller-supplied
// limit before the bucket-count adjustment below is applied.
const MaxRows = 1000

// MaxTimeseriesPoints mirrors Python's MAX_TIMESERIES_POINTS -- an
// additional hard cap on total returned points, enforced by shrinking the
// per-scope row limit as the number of date buckets grows.
const MaxTimeseriesPoints = 1000

// DefaultLimit mirrors Python's DEFAULT_TIMESERIES_LIMIT, used when the
// caller does not supply one.
const DefaultLimit = 500

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same shape as featureflags.QueryClient and reviewedges.QueryClient,
// declared locally per those packages' own doc comments (this operation's
// query shape does not fit dev-health-go/readers' id-keyed convention).
// *clickhouse.Client satisfies this interface directly.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// clampToRange bounds limit to [1, MaxRows], mirroring Python's
// `max(1, min(raw_limit, MAX_ROWS))`.
func clampToRange(limit int) int {
	if limit < 1 {
		return 1
	}
	if limit > MaxRows {
		return MaxRows
	}
	return limit
}

// pyWeekday reproduces Python's date.weekday() (Monday=0 .. Sunday=6) from
// Go's time.Weekday (Sunday=0 .. Saturday=6) -- Go and Python disagree on
// which day starts the week, and _bucket_count's WEEK-granularity math
// depends on this exact convention to compute the same week-bucket
// boundaries Python's `day - timedelta(days=day.weekday())` computes.
func pyWeekday(t time.Time) int {
	return (int(t.Weekday()) + 6) % 7
}

// dateOnly truncates t to a UTC calendar date at midnight, mirroring
// Python's `t.astimezone(timezone.utc).date()`.
func dateOnly(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

// bucketCount ports _bucket_count verbatim: the number of date buckets
// (days, or ISO-week-aligned weeks) spanned by [sinceDay, untilDay]
// inclusive. sinceDay and untilDay must already be date-only UTC values
// (see dateOnly).
func bucketCount(sinceDay, untilDay time.Time, granularity model.TimeGranularity) int {
	if untilDay.Before(sinceDay) {
		return 1
	}
	if granularity == model.TimeGranularityWeek {
		sinceBucket := sinceDay.AddDate(0, 0, -pyWeekday(sinceDay))
		untilBucket := untilDay.AddDate(0, 0, -pyWeekday(untilDay))
		days := int(untilBucket.Sub(sinceBucket).Hours() / 24)
		return days/7 + 1
	}
	days := int(untilDay.Sub(sinceDay).Hours() / 24)
	return days + 1
}

// effectiveLimit ports the resolver's two-stage clamp: first to
// [1, MaxRows], then further bounded so bucketCount * limit points never
// exceeds MaxTimeseriesPoints.
func effectiveLimit(rawLimit *int, bucketCount int) int {
	limit := DefaultLimit
	if rawLimit != nil {
		limit = *rawLimit
	}
	limit = clampToRange(limit)
	perBucket := MaxTimeseriesPoints / bucketCount
	if perBucket < 1 {
		perBucket = 1
	}
	if limit > perBucket {
		limit = perBucket
	}
	return limit
}

// boundRepoIDs mirrors Python's `list(repo_ids)[:limit]` -- repoIDs
// truncated to at most limit entries before becoming a bind parameter.
func boundRepoIDs(repoIDs []string, limit int) []string {
	if len(repoIDs) <= limit {
		return repoIDs
	}
	return repoIDs[:limit]
}

// repoIDsFilter is the shared "resolve repoIds through the org-scoped repos
// catalog by slug OR UUID string" WHERE fragment reviewedges's own repo
// filter uses -- kept as a package-local copy per that package's own
// documented convention (each operation package is self-contained; see
// reviewedges.go's doc comment) rather than a shared helper.
const repoIDsFilter = `
          AND repo_id IN (
              SELECT id FROM repos
              WHERE org_id = {org_id:String}
                AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
          )`

// repoTimeseriesRow is one row of _fetch_repo_timeseries's result set, in
// SELECT column order.
type repoTimeseriesRow struct {
	day                         time.Time
	repoID                      string
	locTotal                    uint64
	cyclomaticTotal             uint64
	cyclomaticPerKloc           float64
	highComplexityFunctions     uint64
	veryHighComplexityFunctions uint64
}

// fetchRepoTimeseries ports _fetch_repo_timeseries verbatim, including its
// two branches: repoIDs given -> filter through the repos catalog with NO
// outer LIMIT (the truncated repoIDs array is itself the bound); repoIDs
// empty -> a default top-N-by-latest-complexity subquery bounded by
// {limit:UInt32}. Neither branch appends a LIMIT to the outer query --
// this is a deliberate, verified difference from fetchFileTimeseries
// below (see that function's doc comment).
func fetchRepoTimeseries(ctx context.Context, client QueryClient, orgID, sinceDay, untilDay string, repoIDs []string, limit int, granularity model.TimeGranularity) ([]repoTimeseriesRow, error) {
	dayExpr := "day"
	computedExpr := "computed_at"
	if granularity == model.TimeGranularityWeek {
		dayExpr = "toStartOfWeek(day, 1)"
		computedExpr = "(day, computed_at)"
	}

	query := fmt.Sprintf(`
        SELECT
            %s AS day,
            toString(repo_id) AS repo_id,
            argMax(loc_total,                      %s) AS loc_total,
            argMax(cyclomatic_total,               %s) AS cyclomatic_total,
            argMax(cyclomatic_per_kloc,            %s) AS cyclomatic_per_kloc,
            argMax(high_complexity_functions,      %s) AS high_complexity_functions,
            argMax(very_high_complexity_functions, %s) AS very_high_complexity_functions
        FROM repo_complexity_daily
        WHERE org_id = {org_id:String}
          AND day >= {since_day:Date}
          AND day <= {until_day:Date}`, dayExpr, computedExpr, computedExpr, computedExpr, computedExpr, computedExpr)

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "since_day", Value: sinceDay},
		{Name: "until_day", Value: untilDay},
	}

	if len(repoIDs) > 0 {
		bounded := boundRepoIDs(repoIDs, limit)
		query += repoIDsFilter
		bindings = append(bindings, clickhouse.Binding{Name: "repo_ids", Value: bounded})
	} else {
		query += `
          AND toString(repo_id) IN (
              SELECT repo_id
              FROM (
                  SELECT
                      toString(repo_id) AS repo_id,
                      argMax(cyclomatic_per_kloc, (day, computed_at)) AS latest_complexity
                  FROM repo_complexity_daily
                  WHERE org_id = {org_id:String}
                    AND day >= {since_day:Date}
                    AND day <= {until_day:Date}
                  GROUP BY repo_id
                  ORDER BY latest_complexity DESC NULLS LAST, repo_id
                  LIMIT {limit:UInt32}
              )
          )`
		bindings = append(bindings, clickhouse.Binding{Name: "limit", Value: uint32(limit)})
	}
	query += "\nGROUP BY day, repo_id\nORDER BY day, repo_id"

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("complexitytimeseries: repo query: %w", err)
	}
	defer rows.Close()

	var out []repoTimeseriesRow
	for rows.Next() {
		var r repoTimeseriesRow
		if scanErr := rows.Scan(&r.day, &r.repoID, &r.locTotal, &r.cyclomaticTotal, &r.cyclomaticPerKloc, &r.highComplexityFunctions, &r.veryHighComplexityFunctions); scanErr != nil {
			return nil, fmt.Errorf("complexitytimeseries: repo scan: %w", scanErr)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("complexitytimeseries: repo rows: %w", err)
	}
	return out, nil
}

// fileTimeseriesRow is one row of _fetch_file_timeseries's result set, in
// SELECT column order.
type fileTimeseriesRow struct {
	day                         time.Time
	repoID                      string
	filePath                    string
	cyclomaticTotal             uint32
	cyclomaticAvg               float64
	highComplexityFunctions     uint32
	veryHighComplexityFunctions uint32
}

// fetchFileTimeseries ports _fetch_file_timeseries verbatim. Unlike
// fetchRepoTimeseries, this ALWAYS appends a trailing `LIMIT {limit}` --
// verified against complexity.py line by line, not assumed: the repo
// branch relies on either the repoIds-array bound or the default
// subquery's own LIMIT to cap row count, while the file branch has no
// equivalent default-selection subquery and so needs an explicit outer
// LIMIT regardless of whether repoIds was supplied. The limit is
// interpolated directly into the query text (an f-string substitution in
// Python, not a ClickHouse bind parameter) -- reproduced the same way
// here since limit is always an internally-computed int, never
// user-supplied SQL text.
func fetchFileTimeseries(ctx context.Context, client QueryClient, orgID, sinceDay, untilDay string, repoIDs []string, limit int, granularity model.TimeGranularity) ([]fileTimeseriesRow, error) {
	dayExpr := "as_of_day"
	computedExpr := "computed_at"
	if granularity == model.TimeGranularityWeek {
		dayExpr = "toStartOfWeek(as_of_day, 1)"
		computedExpr = "(as_of_day, computed_at)"
	}

	query := fmt.Sprintf(`
        SELECT
            %s AS day,
            toString(repo_id) AS repo_id,
            file_path,
            argMax(cyclomatic_total,               %s) AS cyclomatic_total,
            argMax(cyclomatic_avg,                 %s) AS cyclomatic_avg,
            argMax(high_complexity_functions,      %s) AS high_complexity_functions,
            argMax(very_high_complexity_functions, %s) AS very_high_complexity_functions
        FROM file_complexity_snapshots
        WHERE org_id = {org_id:String}
          AND as_of_day >= {since_day:Date}
          AND as_of_day <= {until_day:Date}`, dayExpr, computedExpr, computedExpr, computedExpr, computedExpr)

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "since_day", Value: sinceDay},
		{Name: "until_day", Value: untilDay},
	}

	if len(repoIDs) > 0 {
		bounded := boundRepoIDs(repoIDs, limit)
		query += repoIDsFilter
		bindings = append(bindings, clickhouse.Binding{Name: "repo_ids", Value: bounded})
	}
	query += fmt.Sprintf("\nGROUP BY day, repo_id, file_path\nORDER BY day, repo_id\nLIMIT %d", limit)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("complexitytimeseries: file query: %w", err)
	}
	defer rows.Close()

	var out []fileTimeseriesRow
	for rows.Next() {
		var r fileTimeseriesRow
		if scanErr := rows.Scan(&r.day, &r.repoID, &r.filePath, &r.cyclomaticTotal, &r.cyclomaticAvg, &r.highComplexityFunctions, &r.veryHighComplexityFunctions); scanErr != nil {
			return nil, fmt.Errorf("complexitytimeseries: file scan: %w", scanErr)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("complexitytimeseries: file rows: %w", err)
	}
	return out, nil
}

// loadRepoLabels ports _load_repo_labels verbatim: {repo_id: full_name},
// falling back to the repo_id string itself when the catalog row's
// `repo` column is empty (mirrors Python's `row.get("full_name") or
// row["repo_id"]`). Returns an empty map (no query issued) for an empty
// repoIDs input, same as Python's early return.
func loadRepoLabels(ctx context.Context, client QueryClient, orgID string, repoIDs []string) (map[string]string, error) {
	if len(repoIDs) == 0 {
		return map[string]string{}, nil
	}
	query := `
        SELECT toString(id) AS repo_id, repo AS full_name
        FROM repos
        WHERE org_id = {org_id:String}
          AND toString(id) IN {repo_ids:Array(String)}`
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_ids", Value: repoIDs},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("complexitytimeseries: repo labels query: %w", err)
	}
	defer rows.Close()

	labels := map[string]string{}
	for rows.Next() {
		var repoID, fullName string
		if scanErr := rows.Scan(&repoID, &fullName); scanErr != nil {
			return nil, fmt.Errorf("complexitytimeseries: repo labels scan: %w", scanErr)
		}
		if fullName == "" {
			fullName = repoID
		}
		labels[repoID] = fullName
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("complexitytimeseries: repo labels rows: %w", err)
	}
	return labels, nil
}

// distinctRepoIDs returns the distinct repo_id values seen in rows,
// mirroring Python's `list({str(r["repo_id"]) for r in rows})` -- order is
// irrelevant here, the result only ever feeds loadRepoLabels's IN filter.
func distinctRepoIDs(repoIDs []string) []string {
	seen := make(map[string]struct{}, len(repoIDs))
	out := make([]string, 0, len(repoIDs))
	for _, id := range repoIDs {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// Resolve ports resolve_complexity_timeseries. orgID must already be the
// AUTHORIZED org (the caller's verified envelope claim) -- Python's
// resolver silently prefers the authorized org over a mismatched GraphQL
// `input.orgId` argument rather than erroring (only logging), the same
// "authorized org always wins" behavior reviewedges.Resolve documents;
// this port reproduces it by construction, taking only one org parameter
// and never looking at a caller-supplied org id at all. limit is clamped
// internally, same as Python -- callers must not pre-clamp.
func Resolve(ctx context.Context, client QueryClient, orgID string, sinceUtc, untilUtc time.Time, granularity model.TimeGranularity, scope model.ComplexityScope, repoIDs []string, limit *int) (*model.ComplexityTimeseriesResult, error) {
	if client == nil {
		return nil, errors.New("complexitytimeseries: clickhouse client is required")
	}

	sinceDate := dateOnly(sinceUtc)
	untilDate := dateOnly(untilUtc)
	buckets := bucketCount(sinceDate, untilDate, granularity)
	effLimit := effectiveLimit(limit, buckets)
	sinceDay := sinceDate.Format(graphqldate.Layout)
	untilDay := untilDate.Format(graphqldate.Layout)

	points := []model.ComplexityPoint{}

	switch scope {
	case model.ComplexityScopeFile:
		rows, err := fetchFileTimeseries(ctx, client, orgID, sinceDay, untilDay, repoIDs, effLimit, granularity)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			cyclomaticTotal := int(r.cyclomaticTotal)
			cyclomaticAvg := r.cyclomaticAvg
			highComplexity := int(r.highComplexityFunctions)
			veryHighComplexity := int(r.veryHighComplexityFunctions)
			scopeID := fmt.Sprintf("%s/%s", r.repoID, r.filePath)
			scopeName := r.filePath
			if scopeName == "" {
				scopeName = scopeID
			}
			points = append(points, model.ComplexityPoint{
				Date:                        graphqldate.New(r.day),
				ScopeID:                     scopeID,
				ScopeName:                   scopeName,
				CyclomaticTotal:             &cyclomaticTotal,
				CyclomaticAvg:               &cyclomaticAvg,
				HighComplexityFunctions:     &highComplexity,
				VeryHighComplexityFunctions: &veryHighComplexity,
			})
		}
	default: // REPO -- the schema/Python default and the only other valid value.
		rows, err := fetchRepoTimeseries(ctx, client, orgID, sinceDay, untilDay, repoIDs, effLimit, granularity)
		if err != nil {
			return nil, err
		}
		seenRepoIDs := make([]string, 0, len(rows))
		for _, r := range rows {
			seenRepoIDs = append(seenRepoIDs, r.repoID)
		}
		labels, err := loadRepoLabels(ctx, client, orgID, distinctRepoIDs(seenRepoIDs))
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			locTotal := int(r.locTotal)
			cyclomaticPerKloc := r.cyclomaticPerKloc
			cyclomaticTotal := int(r.cyclomaticTotal)
			highComplexity := int(r.highComplexityFunctions)
			veryHighComplexity := int(r.veryHighComplexityFunctions)
			scopeName, ok := labels[r.repoID]
			if !ok {
				scopeName = r.repoID
			}
			points = append(points, model.ComplexityPoint{
				Date:                        graphqldate.New(r.day),
				ScopeID:                     r.repoID,
				ScopeName:                   scopeName,
				LocTotal:                    &locTotal,
				CyclomaticPerKloc:           &cyclomaticPerKloc,
				CyclomaticTotal:             &cyclomaticTotal,
				HighComplexityFunctions:     &highComplexity,
				VeryHighComplexityFunctions: &veryHighComplexity,
			})
		}
	}

	scopeIDs := make(map[string]struct{}, len(points))
	for _, p := range points {
		scopeIDs[p.ScopeID] = struct{}{}
	}

	return &model.ComplexityTimeseriesResult{
		Points:     points,
		TotalScope: len(scopeIDs),
	}, nil
}
