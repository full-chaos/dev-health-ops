package compoundingrisk

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// conn is the narrow ClickHouse capability this package needs, matching
// internal/jobs/metrics/daily/cicd's conn interface shape.
type conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// ClickHouseLoader reads this family's two inputs: the partition's
// repo_metrics_daily slice, and each repo's 30-day complexity delta.
type ClickHouseLoader struct {
	conn conn
}

func NewClickHouseLoader(connection conn) (*ClickHouseLoader, error) {
	if connection == nil {
		return nil, fmt.Errorf("compoundingrisk: clickhouse connection is required")
	}
	return &ClickHouseLoader{conn: connection}, nil
}

// repoMetricsQuery ports _fetch_repo_metrics_for_day
// (src/dev_health_ops/metrics/job_compounding_risk.py:75-86) with two
// deliberate, documented changes.
//
// FIRST: it is scoped to the partition's repos. Python's PRIMARY path does not
// run this query at all -- job_daily.py:1960 passes `result.repo_metrics`, the
// rows this partition just computed for its OWN repo, and only falls back to
// the org-wide fetch when that list is empty (job_daily.py:593-596). Mirroring
// the fallback literally would mean every quiet repo's partition re-emitting a
// full org-wide row set, which is the same cross-partition duplication class
// team-lead ruled out for `benchmarking`. Scoping the read to
// partition.RepoIDs reproduces the primary path exactly and makes the
// degenerate path emit zero rows instead of N copies.
//
// CHAOS-5122. codex r2 on #2230 challenged this as a parity break rather than a
// product choice, which was the right challenge to make -- the family is
// `golden: required`. The evidence settles it as a PYTHON DEFECT that this port
// declines to reproduce, and all four parts were read rather than inferred:
//
//	job_daily.py:592-594      on an empty partition, fall back to
//	                          _fetch_repo_metrics_for_day(sink, org_id, day)
//	job_compounding_risk.py:49  that helper takes NO repo argument; its query is
//	                          WHERE org_id AND day GROUP BY repo_id -- org-wide
//	job_daily.py:598-609      the write that follows is UNCONDITIONAL; nothing
//	                          checks whether another partition already wrote
//	                          this org/day
//	040_compounding_risk_daily.sql:56  plain MergeTree, computed_at in the
//	                          ORDER BY, so each write is a DISTINCT physical row
//
// So an org with N empty partitions performs N org-wide writes of the same row
// set. It is invisible to readers that argMax(computed_at) -- they still get a
// sane latest value -- and shows up only as row-count growth, which is why it
// has survived. It is the same defect shape CHAOS-4365 already fixed one scope
// up, for team rows, in this file's sibling.
//
// Bit-exactness for this family is therefore asserted on the COMPUTE, not on
// physical row counts under a Python defect.
//
// SECOND: it carries an explicit ORDER BY. Python's has none, so its row order
// -- and therefore its output row order -- is whatever ClickHouse's GROUP BY
// happened to return. Nothing in the repo scope is order-sensitive (each row's
// score depends only on its own inputs), so this changes ordering only, never
// a value.
//
// The argMax(..., computed_at) GROUP BY repo_id shape is Python's own and is
// kept: repo_metrics_daily is append-only, so this is the read-side dedup the
// table requires. No FINAL -- it would be wrong under aggregation and is not
// what Python does either.
//
// THIRD, and unlike the two above this one is a CORRECTNESS fix rather than a
// scoping choice: the five columns are packed into ONE argMax(tuple(...))
// rather than five independent argMax calls. Independent argMax aggregates over
// the same GROUP BY each choose their own winning row, and on a computed_at TIE
// they can choose DIFFERENT ones -- producing a repo whose churn ratio comes
// from one write and whose bus factor comes from another, a combination that
// never existed. Python has exactly this exposure
// (job_compounding_risk.py:75-86) and this port does not inherit it. Ties are
// reachable: one job writes all five columns for a repo/day in a single insert,
// so they share a computed_at by construction. Rule found by
// lane-port-ai-families.
const repoMetricsQuery = `
SELECT
    repo_id,
    tupleElement(latest, 1) AS rework_churn_ratio_30d,
    tupleElement(latest, 2) AS single_owner_file_ratio_30d,
    tupleElement(latest, 3) AS code_ownership_gini,
    tupleElement(latest, 4) AS bus_factor,
    tupleElement(latest, 5) AS pr_first_review_p90_hours
FROM (
    SELECT
        repo_id,
        argMax(
            tuple(
                rework_churn_ratio_30d,
                single_owner_file_ratio_30d,
                code_ownership_gini,
                bus_factor,
                pr_first_review_p90_hours
            ),
            computed_at
        ) AS latest
    FROM repo_metrics_daily
    WHERE org_id = {org_id:String}
      AND day = {day:Date}
      AND repo_id IN {repo_ids:Array(UUID)}
    GROUP BY org_id, repo_id
)
ORDER BY repo_id`

// LoadRepoMetrics returns the partition's argMax-deduplicated
// repo_metrics_daily slice for one day, ordered by repo_id.
func (loader *ClickHouseLoader) LoadRepoMetrics(
	ctx context.Context, orgID string, repoIDs []uuid.UUID, day time.Time,
) ([]RepoMetricsRow, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("compoundingrisk: loader unavailable")
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}
	rows, err := loader.conn.Query(ctx, repoMetricsQuery,
		clickhouse.Named("org_id", orgID),
		// ANCHOR: bind a Go `string` for EVERY typed {name:Type} query
		// parameter -- Date, UUID, all of them. This is a property of the
		// driver, not of any one type. clickhouse-go/v2@v2.47.0
		// query_parameters.go:32-40 passes a Go string through VERBATIM and
		// sends everything else through format(), which wraps the value in
		// single quotes. A wire-level typed parameter is then parsed by the
		// SERVER, so those quotes become part of the literal and the type
		// parser rejects it:
		//
		//	time.Time -> "Cannot parse date here: toDateTime('2026-08-24
		//	              00:00:00') cannot be parsed as Date"
		//	uuid.UUID -> "Cannot parse UUID from String: invalid format,
		//	              expected 32 hexadecimal digits"
		//
		// Array(UUID) is the trap: quoted elements ARE valid inside array
		// literal syntax, so a []uuid.UUID bind succeeds right next to a
		// scalar uuid.UUID bind that fails. The passing array bind is not
		// evidence for the scalar one.
		//
		// This comment first said "a Date parameter must be a string"; that
		// narrow framing is precisely what let the UUID instance below reach
		// CI. Same root cause as RecommendationsLoader.windowArguments
		// (recommendations_loader.go:77-97). The string form is also the
		// closer mirror of the wire form Python's clickhouse-connect sends.
		clickhouse.Named("day", day.Format(clickHouseDateLayout)),
		clickhouse.Named("repo_ids", repoIDs),
	)
	if err != nil {
		return nil, fmt.Errorf("load repo metrics: %w", err)
	}
	defer rows.Close()

	var result []RepoMetricsRow
	for rows.Next() {
		// SCAN TYPES FOLLOW THE DDL, NOT THE PYTHON DATACLASS. Four of these
		// five columns are NOT nullable in ClickHouse, and one is not even a
		// float:
		//
		//   rework_churn_ratio_30d      Float64            (004_quality_delivery_metrics.sql:7)
		//   single_owner_file_ratio_30d Float64            (004_quality_delivery_metrics.sql:8)
		//   code_ownership_gini         Float64 DEFAULT 0  (006_knowledge_predictability.sql:3)
		//   bus_factor                  UInt32  DEFAULT 0  (006_knowledge_predictability.sql:2)
		//   pr_first_review_p90_hours   Nullable(Float64)  (001_metrics_v2.sql:189)
		//
		// Scanning a non-Nullable column into *float64 -- or a UInt32 into any
		// float -- is a driver-level type error the compute-side unit tests
		// cannot see, which is exactly CHAOS-4977's point that a fake
		// RowScanner never proves wire compatibility. The integration test is
		// what holds this honest.
		//
		// Python reaches the same values through _nullable_float's float(),
		// so the widening here (uint32 -> float64, exact for every uint32
		// under a 53-bit mantissa) is the same conversion, not a new one.
		var (
			repoID           uuid.UUID
			reworkChurn      float64
			singleOwnerRatio float64
			ownershipGini    float64
			busFactor        uint32
			reviewP90        *float64
		)
		if err := rows.Scan(
			&repoID, &reworkChurn, &singleOwnerRatio, &ownershipGini, &busFactor, &reviewP90,
		); err != nil {
			return nil, fmt.Errorf("scan repo metrics row: %w", err)
		}
		busFactorFloat := float64(busFactor)
		result = append(result, RepoMetricsRow{
			RepoID:                  repoID.String(),
			ReworkChurnRatio30D:     &reworkChurn,
			SingleOwnerFileRatio30D: &singleOwnerRatio,
			CodeOwnershipGini:       &ownershipGini,
			BusFactor:               &busFactorFloat,
			PRFirstReviewP90Hours:   reviewP90,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// repoMetricsOrgWideQuery ports _fetch_repo_metrics_for_day
// (src/dev_health_ops/metrics/job_daily.py:613-660's finalize-step caller,
// via job_compounding_risk.py:49-83's CLI-job twin of the same query shape)
// for CHAOS-5084's TEAM-scope finalize read: every repo's row for the
// org/day, not one partition's repo_ids. This is the query
// _write_compounding_risk_team_rows_for_day actually runs -- unlike
// repoMetricsQuery above (deliberately narrower than its own Python
// caller's primary path), there is no narrower Python path to prefer here:
// team rows need every repo in the org to attribute correctly by team.
//
// Carries the SAME "one argMax(tuple(...))" fix as repoMetricsQuery, for the
// SAME reason (Python's job_compounding_risk.py:75-86 has the same
// tie-on-computed_at exposure repoMetricsQuery's doc comment describes; this
// port does not inherit it here either) -- but NOT the same claim about its
// ORDER BY repo_id being merely cosmetic.
//
// CORRECTION (codex round chaos-5084-2275-r1, P2, an overclaim this comment
// used to make): for the REPO-scope query above, an explicit ORDER BY really
// does "change ordering only, never a value" -- each repo's Compute() result
// depends only on that repo's own inputs, so no ORDER BY choice can affect
// what gets written. TEAM scope is different in kind, not degree:
// BuildTeamRows' means are Neumaier-compensated sums (pythonparity.Sum),
// which are NOT order-invariant at the bit level. Python's own production
// query (job_daily.py:613-660's _fetch_repo_metrics_for_day, and this
// query's CLI twin) has NO ORDER BY on its GROUP BY repo_id -- meaning
// Python's OWN row order for this exact quantity is whatever ClickHouse's
// query planner happens to return, not a single well-defined sequence this
// port could target for guaranteed bit-exact parity. This query's ORDER BY
// repo_id is therefore a DELIBERATE, DETERMINISTIC CANONICALIZATION -- a
// defensible choice (repeatable, auditable, matches this package's own
// golden fixture, which is itself repo_id-ordered by construction) -- but it
// is NOT proven bit-identical to what a specific live Python run would
// compute for a 3+-repo team with order-sensitive floats, because Python's
// own answer for that case is not itself deterministic. See BuildTeamRows'
// doc comment for the same correction on the aggregation side, and
// CHAOS-5204 for the tracking ticket.
const repoMetricsOrgWideQuery = `
SELECT
    repo_id,
    tupleElement(latest, 1) AS rework_churn_ratio_30d,
    tupleElement(latest, 2) AS single_owner_file_ratio_30d,
    tupleElement(latest, 3) AS code_ownership_gini,
    tupleElement(latest, 4) AS bus_factor,
    tupleElement(latest, 5) AS pr_first_review_p90_hours
FROM (
    SELECT
        repo_id,
        argMax(
            tuple(
                rework_churn_ratio_30d,
                single_owner_file_ratio_30d,
                code_ownership_gini,
                bus_factor,
                pr_first_review_p90_hours
            ),
            computed_at
        ) AS latest
    FROM repo_metrics_daily
    WHERE org_id = {org_id:String}
      AND day = {day:Date}
    GROUP BY org_id, repo_id
)
ORDER BY repo_id`

// LoadRepoMetricsForOrgDay returns EVERY repo's argMax-deduplicated
// repo_metrics_daily row for one org/day, ordered by repo_id -- the
// finalize-step, TEAM-scope sibling of LoadRepoMetrics (which is scoped to a
// partition's own repos). See repoMetricsOrgWideQuery's doc comment for why
// this one is org-wide where the other is deliberately not.
func (loader *ClickHouseLoader) LoadRepoMetricsForOrgDay(
	ctx context.Context, orgID string, day time.Time,
) ([]RepoMetricsRow, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("compoundingrisk: loader unavailable")
	}
	if orgID == "" {
		return nil, fmt.Errorf("compoundingrisk: organization id is required")
	}
	rows, err := loader.conn.Query(ctx, repoMetricsOrgWideQuery,
		clickhouse.Named("org_id", orgID),
		// Bound as a string -- see the ANCHOR note in LoadRepoMetrics for why a
		// time.Time cannot be bound directly to a {name:Date} parameter.
		clickhouse.Named("day", day.Format(clickHouseDateLayout)),
	)
	if err != nil {
		return nil, fmt.Errorf("load org-wide repo metrics: %w", err)
	}
	defer rows.Close()

	var result []RepoMetricsRow
	for rows.Next() {
		// Scan types follow the DDL, exactly as LoadRepoMetrics documents above
		// -- same table, same columns, same nullability.
		var (
			repoID           uuid.UUID
			reworkChurn      float64
			singleOwnerRatio float64
			ownershipGini    float64
			busFactor        uint32
			reviewP90        *float64
		)
		if err := rows.Scan(
			&repoID, &reworkChurn, &singleOwnerRatio, &ownershipGini, &busFactor, &reviewP90,
		); err != nil {
			return nil, fmt.Errorf("scan org-wide repo metrics row: %w", err)
		}
		busFactorFloat := float64(busFactor)
		result = append(result, RepoMetricsRow{
			RepoID:                  repoID.String(),
			ReworkChurnRatio30D:     &reworkChurn,
			SingleOwnerFileRatio30D: &singleOwnerRatio,
			CodeOwnershipGini:       &ownershipGini,
			BusFactor:               &busFactorFloat,
			PRFirstReviewP90Hours:   reviewP90,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// ComplexityWindowDays mirrors COMPLEXITY_WINDOW_DAYS (compounding_risk.py:410).
const ComplexityWindowDays = 30

// clickHouseDateLayout is the wire form for a {name:Date} query parameter.
// See LoadRepoMetrics for why a time.Time cannot be bound directly.
const clickHouseDateLayout = "2006-01-02"

// complexityDeltaQuery is load_repo_complexity_delta_30d's query verbatim
// (compounding_risk.py:435-447), including the two ClickHouse-side avg()s. The
// averaging stays in ClickHouse precisely BECAUSE Python leaves it there --
// re-implementing it as a Go mean over the daily rows would be a different
// summation with a different rounding sequence and no oracle covering the
// difference.
const complexityDeltaQuery = `
SELECT
    avg(if(day < {mid:Date}, cpk, NULL)) AS first_half,
    avg(if(day >= {mid:Date}, cpk, NULL)) AS second_half
FROM (
    SELECT day, argMax(cyclomatic_per_kloc, computed_at) AS cpk
    FROM repo_complexity_daily
    WHERE repo_id = {repo_id:UUID}
      AND day >= {start:Date} AND day <= {end:Date}
      AND org_id = {org_id:String}
    GROUP BY org_id, repo_id, day
)`

// LoadComplexityDelta returns the relative change in cyclomatic_per_kloc over
// the window, or nil when either half of the window has no data at all --
// Python's `if first is None or second is None: return None`
// (compounding_risk.py:461-462). A nil result blocks the composite score, which
// is the intended "data unavailable is not zero risk" behaviour.
func (loader *ClickHouseLoader) LoadComplexityDelta(
	ctx context.Context, orgID string, repoID uuid.UUID, day time.Time, windowDays int,
) (*float64, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("compoundingrisk: loader unavailable")
	}
	if windowDays < 2 {
		return nil, fmt.Errorf("compoundingrisk: window_days must be >= 2, got %d", windowDays)
	}
	// window_start = day - (window_days - 1); midpoint = window_start +
	// window_days // 2 -- integer division, matching compounding_risk.py:432-433.
	windowStart := day.AddDate(0, 0, -(windowDays - 1))
	midpoint := windowStart.AddDate(0, 0, windowDays/2)

	rows, err := loader.conn.Query(ctx, complexityDeltaQuery,
		// .String(): a bare uuid.UUID is a fmt.Stringer and gets quoted. See
		// the ANCHOR note in LoadRepoMetrics.
		clickhouse.Named("repo_id", repoID.String()),
		// Bound as strings -- see the ANCHOR note in LoadRepoMetrics.
		clickhouse.Named("start", windowStart.Format(clickHouseDateLayout)),
		clickhouse.Named("mid", midpoint.Format(clickHouseDateLayout)),
		clickhouse.Named("end", day.Format(clickHouseDateLayout)),
		clickhouse.Named("org_id", orgID),
	)
	if err != nil {
		return nil, fmt.Errorf("load complexity delta: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		// Python's `if not rows: return None` (compounding_risk.py:457-458).
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	var firstHalf, secondHalf *float64
	if err := rows.Scan(&firstHalf, &secondHalf); err != nil {
		return nil, fmt.Errorf("scan complexity delta row: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if firstHalf == nil || secondHalf == nil {
		return nil, nil
	}
	delta := ComplexityDeltaRatio(*firstHalf, *secondHalf)
	return &delta, nil
}

// Writer appends Compute's output to compounding_risk_daily.
type Writer struct {
	conn conn
}

func NewWriter(connection conn) (*Writer, error) {
	if connection == nil {
		return nil, fmt.Errorf("compoundingrisk: clickhouse connection is required")
	}
	return &Writer{conn: connection}, nil
}

// WriteRecords writes compounding_risk_daily and returns the number of rows
// written. The column list and its order are write_compounding_risk_daily's
// verbatim (sinks/clickhouse/compounding_risk.py:43-70).
//
// Fails closed on an empty orgID: org_id leads this table's ORDER BY, so an
// unscoped row is not merely mislabelled, it sorts into a different part of the
// primary key and every org-filtered read misses it. Same discipline
// CHAOS-4341 established for repouser's writer.
func (writer *Writer) WriteRecords(ctx context.Context, records []Record, orgID string) (int, error) {
	if writer == nil || writer.conn == nil {
		return 0, fmt.Errorf("compoundingrisk: writer unavailable")
	}
	if orgID == "" {
		return 0, fmt.Errorf("compoundingrisk: organization id is required to write compounding_risk_daily")
	}
	if len(records) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO compounding_risk_daily (
		org_id, day, scope, scope_id,
		compounding_risk, severity,
		churn_norm, complexity_norm, ownership_norm, review_norm,
		rework_churn, complexity_delta, bus_factor, ownership_gini,
		single_owner_ratio, review_latency_p90h,
		w_churn, w_complexity, w_ownership, w_review,
		threshold_elevated, threshold_high, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare compounding_risk_daily batch: %w", err)
	}
	for _, record := range records {
		if err := batch.Append(
			orgID, record.Day, record.Scope, record.ScopeID,
			record.CompoundingRisk, record.Severity,
			record.ChurnNorm, record.ComplexityNorm, record.OwnershipNorm, record.ReviewNorm,
			record.ReworkChurn, record.ComplexityDelta, record.BusFactor, record.OwnershipGini,
			record.SingleOwnerRatio, record.ReviewLatencyP90H,
			record.WChurn, record.WComplexity, record.WOwnership, record.WReview,
			record.ThresholdElevated, record.ThresholdHigh, record.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append compounding_risk_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send compounding_risk_daily batch: %w", err)
	}
	recordRowsWritten(len(records), orgID != "")
	return len(records), nil
}
