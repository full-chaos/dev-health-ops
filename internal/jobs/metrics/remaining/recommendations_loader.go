package remaining

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The recommendations reads, ported from recommendations/loader.py.
//
// # CHAOS-4897: FOUR QUERIES HERE ARE NOT TEAM-SCOPED, DELIBERATELY
//
// q_lat, q_rework, q_cpx and q_hs below carry no team predicate, so they return
// ORG-WIDE values that are identical for every team in the org. That feeds
// review_latency_p75_hours, rework_churn_ratio, hotspot_complexity_delta and
// hotspot_churn_overlap, and therefore three of the five rules
// (review-concentration, thrash, and compounding-risk's legacy proxy).
//
// This is a faithful mirror of a KNOWN DEFECT, not an oversight in the port.
// Do not "fix" it here: the executed-parity harness compares this loader
// against the live Python reference, so adding scoping would make every parity
// test fail. Each of the four carries its own comment saying so.
//
// The reason the reference lacks the predicate is that THERE IS NO COLUMN TO
// FILTER ON -- repo_metrics_daily, repo_complexity_daily and file_hotspot_daily
// carry repo_id and no team_id. Scoping them needs the team's owned-repository
// set derived from team_repo_ownership, which is a resolution subsystem
// (pattern-vs-exact matching, is_primary/specificity/priority precedence,
// valid_from/valid_to windowing, pattern-unresolved repo_id IS NULL rows to
// exclude, and orphaned rows to drop via a repos join). That lands as a
// follow-up PR against a shared internal/teamownership package.

// RecommendationsLoader reads one team's signal window out of ClickHouse.
//
// It mirrors MetricsLoader. The org scope is a field rather than a parameter
// because the reference swaps self._org_id for the duration of a load and
// restores it in a finally block; carrying it per-call makes that swap
// unnecessary and removes the failure mode where an exception leaves the
// loader scoped to the wrong org.
type RecommendationsLoader struct {
	conn  driver.Conn
	orgID string
}

// NewRecommendationsLoader builds a loader bound to one organisation.
func NewRecommendationsLoader(conn driver.Conn, orgID string) (*RecommendationsLoader, error) {
	if conn == nil {
		return nil, fmt.Errorf("recommendations loader: nil ClickHouse connection")
	}
	return &RecommendationsLoader{conn: conn, orgID: orgID}, nil
}

// orgClause mirrors MetricsLoader._oc: the org predicate appears only when an
// org is set, and is absent entirely otherwise. An always-present clause with a
// wildcard would read the same rows in most cases and different rows in the
// empty-org case, so the conditional is reproduced rather than normalised.
func (loader *RecommendationsLoader) orgClause() string {
	if loader.orgID == "" {
		return ""
	}
	return " AND org_id = {org_id:String}"
}

// windowArguments mirrors MetricsLoader._p. team_id is bound even for the
// queries that do not reference it, exactly as the reference does; an unused
// bound parameter is harmless and keeping the binding uniform means the four
// unscoped queries need no special case here, which is where a later reader
// would most easily "helpfully" add one.
func (loader *RecommendationsLoader) windowArguments(
	teamID string, windowStart, windowEnd time.Time,
) map[string]any {
	// The window bounds are bound as YYYY-MM-DD STRINGS, not time.Time.
	//
	// The columns are ClickHouse `Date`, and a time.Time is serialised for a
	// {name:Date} parameter as a full DateTime literal, which the server
	// rejects outright: "Cannot parse date here: toDateTime('2026-08-01
	// 00:00:00') cannot be parsed as Date". Python's clickhouse-connect sends a
	// bare date for a datetime.date, so the string form is also the closer
	// mirror of what the reference puts on the wire.
	arguments := map[string]any{
		"team_id": teamID,
		"start":   windowStart.Format("2006-01-02"),
		"end":     windowEnd.Format("2006-01-02"),
	}
	if loader.orgID != "" {
		arguments["org_id"] = loader.orgID
	}
	return arguments
}

// nullableToZero mirrors Python's `float(row.get(column) or 0.0)`.
//
// The `or` looks like it is doing more than it is. For a float it replaces only
// FALSY values, and the only falsy floats are +0.0 and -0.0, both of which it
// replaces with +0.0 -- so the expression is exactly "NULL becomes 0.0" plus a
// -0.0-to-+0.0 normalisation. NaN is TRUTHY and passes through unchanged, which
// is how NaN reaches the list-derived evidence fields (CHAOS-4897's sibling
// finding: _safe_float guards the scalar loaders, never these lists).
func nullableToZero(value *float64) float64 {
	if value == nil {
		return 0.0
	}
	if *value == 0 {
		// Collapses -0.0 to +0.0 exactly as `or 0.0` does.
		return 0.0
	}
	return *value
}

// safeFloat mirrors loader.py's _safe_float: None and NaN become absent, but
// infinities pass through. See SafeFloat in recommendations_signals.go for why
// that asymmetry is load-bearing rather than an oversight.
func safeFloat(value *float64) (float64, bool) {
	if value == nil {
		return 0, false
	}
	return SafeFloat(*value, true)
}

// loadWIPThroughput ports _load_wip_throughput.
//
// TEAM-SCOPED (work_item_metrics_daily carries team_id). The inner GROUP BY
// includes provider and work_scope_id so each scope's latest computed_at row is
// taken before the per-day sum, then the outer query sums across scopes.
func (loader *RecommendationsLoader) loadWIPThroughput(
	ctx context.Context, teamID string, windowStart, windowEnd time.Time,
) ([]float64, []float64, error) {
	query := `
            SELECT day, sum(wip) AS wip_total, sum(completed) AS tp_total
            FROM (
                SELECT day,
                       argMax(wip_count_end_of_day, computed_at) AS wip,
                       argMax(items_completed, computed_at) AS completed
                FROM work_item_metrics_daily
                WHERE team_id = {team_id:String}
                  AND day >= {start:Date} AND day < {end:Date}` + loader.orgClause() + `
                GROUP BY day, provider, work_scope_id
            )
            GROUP BY day ORDER BY day
        `
	rows, err := loader.conn.Query(ctx, query,
		namedArguments(loader.windowArguments(teamID, windowStart, windowEnd))...)
	if err != nil {
		return nil, nil, fmt.Errorf("load wip/throughput: %w", err)
	}
	defer rows.Close()

	wipRaw := []*float64{}
	throughputRaw := []*float64{}
	for rows.Next() {
		var day time.Time
		// sum() over a UInt32 column returns UInt64, not Float64. Python
		// coerces it client-side with float(...), so the coercion happens here
		// too rather than as a toFloat64() in the SQL -- changing the query
		// text would put a difference between the two implementations exactly
		// where this test is trying to prove there is none.
		var wipSum, throughputSum uint64
		if err := rows.Scan(&day, &wipSum, &throughputSum); err != nil {
			return nil, nil, fmt.Errorf("scan wip/throughput: %w", err)
		}
		wipTotal, throughputTotal := float64(wipSum), float64(throughputSum)
		// Both lists keep every row, NULL included as 0.0. This differs from
		// cycle_time_by_day below, which DROPS null rows and so returns a
		// shorter list. The asymmetry is the reference's and it is observable:
		// list length decides the `len(x) < 2` guards in three rules.
		wipRaw = append(wipRaw, &wipTotal)
		throughputRaw = append(throughputRaw, &throughputTotal)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	wip, throughput := wipThroughputFrom(wipRaw, throughputRaw)
	return wip, throughput, nil
}

// loadReviewSignals ports _load_review_signals.
//
// MIXED SCOPING, and the halves disagree.
//
// q_lat is NOT team-scoped -- CHAOS-4897: repo_metrics_daily has no team_id
// column, so review latency is an org-wide average over every repo. Mirrored
// deliberately; owned-repo derivation pending.
//
// q_gini IS team-scoped (user_metrics_daily carries team_id).
//
// Note also that the review-concentration evidence row records
// metric_table="review_edge_daily" while the gini value comes from
// user_metrics_daily. That mismatch is the sibling finding on CHAOS-4897 and is
// likewise mirrored rather than corrected.
func (loader *RecommendationsLoader) loadReviewSignals(
	ctx context.Context, teamID string, windowStart, windowEnd time.Time,
) (latency float64, latencyKnown bool, gini float64, giniKnown bool, err error) {
	arguments := loader.windowArguments(teamID, windowStart, windowEnd)

	latencyQuery := `
            SELECT avg(p75) AS avg_p75
            FROM (
                SELECT repo_id, argMax(pr_cycle_p75_hours, computed_at) AS p75
                FROM repo_metrics_daily
                WHERE day >= {start:Date} AND day < {end:Date}` + loader.orgClause() + `
                GROUP BY repo_id
            )
        `
	latencyRows, err := loader.conn.Query(ctx, latencyQuery, namedArguments(arguments)...)
	if err != nil {
		return 0, false, 0, false, fmt.Errorf("load review latency: %w", err)
	}
	// The reference reads row zero if any row exists, and treats no rows as
	// None. ClickHouse's avg() over an empty set returns one NULL row rather
	// than zero rows, so both spellings arrive here as "absent".
	if latencyRows.Next() {
		// avg() over a non-Nullable Float64 column is itself non-Nullable and
		// returns NaN for an empty set, which _safe_float turns into absent --
		// so the empty case arrives here as NaN rather than as NULL.
		var average float64
		if scanErr := latencyRows.Scan(&average); scanErr != nil {
			latencyRows.Close()
			return 0, false, 0, false, fmt.Errorf("scan review latency: %w", scanErr)
		}
		latency, latencyKnown = safeFloat(&average)
	}
	if closeErr := latencyRows.Close(); closeErr != nil {
		return 0, false, 0, false, closeErr
	}

	giniQuery := `
            SELECT author_email, sum(rev) AS total_reviews
            FROM (
                SELECT repo_id, author_email, day,
                       argMax(reviews_given, computed_at) AS rev
                FROM user_metrics_daily
                WHERE team_id = {team_id:String}
                  AND day >= {start:Date} AND day < {end:Date}` + loader.orgClause() + `
                GROUP BY repo_id, author_email, day
            )
            GROUP BY author_email
        `
	giniRows, err := loader.conn.Query(ctx, giniQuery, namedArguments(arguments)...)
	if err != nil {
		return 0, false, 0, false, fmt.Errorf("load reviewer gini: %w", err)
	}
	defer giniRows.Close()

	loads := []float64{}
	for giniRows.Next() {
		var authorEmail string
		var totalReviews uint64
		if scanErr := giniRows.Scan(&authorEmail, &totalReviews); scanErr != nil {
			return 0, false, 0, false, fmt.Errorf("scan reviewer gini: %w", scanErr)
		}
		reviews := float64(totalReviews)
		loads = append(loads, nullableToZero(&reviews))
	}
	if rowsErr := giniRows.Err(); rowsErr != nil {
		return 0, false, 0, false, rowsErr
	}
	gini, giniKnown = Gini(loads)
	return latency, latencyKnown, gini, giniKnown, nil
}

// loadReworkRatio ports _load_rework_ratio.
//
// NOT TEAM-SCOPED -- CHAOS-4897: repo_metrics_daily has no team_id column, so
// this is an org-wide average across every repo and every team in the org gets
// the same number. Mirrored deliberately; owned-repo derivation pending.
func (loader *RecommendationsLoader) loadReworkRatio(
	ctx context.Context, teamID string, windowStart, windowEnd time.Time,
) (float64, bool, error) {
	query := `
            SELECT avg(rework) AS avg_rework
            FROM (
                SELECT repo_id, argMax(pr_rework_ratio, computed_at) AS rework
                FROM repo_metrics_daily
                WHERE day >= {start:Date} AND day < {end:Date}` + loader.orgClause() + `
                GROUP BY repo_id
            )
        `
	rows, err := loader.conn.Query(ctx, query,
		namedArguments(loader.windowArguments(teamID, windowStart, windowEnd))...)
	if err != nil {
		return 0, false, fmt.Errorf("load rework ratio: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var average float64
		if scanErr := rows.Scan(&average); scanErr != nil {
			return 0, false, fmt.Errorf("scan rework ratio: %w", scanErr)
		}
		value, known := safeFloat(&average)
		return value, known, rows.Err()
	}
	return 0, false, rows.Err()
}

// loadSustainabilitySignals ports _load_sustainability_signals.
//
// TEAM-SCOPED (both tables carry team_id).
//
// The after-hours query carries the CHAOS-4329 legacy-repo_id discipline and it
// is reproduced exactly, comment and all: team_metrics_daily grew a repo_id
// column in migration 080, so a historical day may hold BOTH a pre-migration
// aggregate row with repo_id=” and post-migration per-repo rows. Summing them
// together double-counts that day. The window function drops the legacy bucket
// for any day that also has real per-repo rows, and the ratio is recomputed
// from the summed counts rather than averaged from per-repo ratios.
func (loader *RecommendationsLoader) loadSustainabilitySignals(
	ctx context.Context, teamID string, windowStart, windowEnd time.Time,
) (afterHours float64, afterHoursKnown bool, cycleTimes []float64, err error) {
	arguments := loader.windowArguments(teamID, windowStart, windowEnd)

	afterHoursQuery := `
            SELECT avg(ratio) AS avg_ratio
            FROM (
                SELECT
                    day,
                    if(sum(commits_count) > 0,
                       sum(after_hours_commits_count) / sum(commits_count), 0.0
                    ) AS ratio
                FROM (
                    SELECT day, repo_id, commits_count, after_hours_commits_count
                    FROM (
                        SELECT
                            day,
                            repo_id,
                            argMax(commits_count, computed_at) AS commits_count,
                            argMax(after_hours_commits_count, computed_at)
                                AS after_hours_commits_count,
                            countIf(repo_id != '') OVER (PARTITION BY day) AS real_repo_count
                        FROM team_metrics_daily
                        WHERE team_id = {team_id:String}
                          AND day >= {start:Date} AND day < {end:Date}` + loader.orgClause() + `
                        GROUP BY day, repo_id
                    )
                    WHERE repo_id != '' OR real_repo_count = 0
                )
                GROUP BY day
            )
        `
	afterHoursRows, err := loader.conn.Query(ctx, afterHoursQuery, namedArguments(arguments)...)
	if err != nil {
		return 0, false, nil, fmt.Errorf("load after-hours ratio: %w", err)
	}
	if afterHoursRows.Next() {
		var average float64
		if scanErr := afterHoursRows.Scan(&average); scanErr != nil {
			afterHoursRows.Close()
			return 0, false, nil, fmt.Errorf("scan after-hours ratio: %w", scanErr)
		}
		afterHours, afterHoursKnown = safeFloat(&average)
	}
	if closeErr := afterHoursRows.Close(); closeErr != nil {
		return 0, false, nil, closeErr
	}

	cycleQuery := `
            SELECT day, avg(ct) AS avg_ct
            FROM (
                SELECT day, provider, work_scope_id,
                       argMax(cycle_time_p50_hours, computed_at) AS ct
                FROM work_item_metrics_daily
                WHERE team_id = {team_id:String}
                  AND day >= {start:Date} AND day < {end:Date}` + loader.orgClause() + `
                GROUP BY day, provider, work_scope_id
            )
            GROUP BY day ORDER BY day
        `
	cycleRows, err := loader.conn.Query(ctx, cycleQuery, namedArguments(arguments)...)
	if err != nil {
		return 0, false, nil, fmt.Errorf("load cycle times: %w", err)
	}
	defer cycleRows.Close()

	cycleRaw := []*float64{}
	for cycleRows.Next() {
		var day time.Time
		var average *float64
		if scanErr := cycleRows.Scan(&day, &average); scanErr != nil {
			return 0, false, nil, fmt.Errorf("scan cycle times: %w", scanErr)
		}
		// DROPS null rows rather than substituting 0.0 -- the opposite of
		// loadWIPThroughput. Reproduced exactly: a dropped row SHORTENS the
		// list, and length decides the `len(cycle_times) < 2` guard in
		// sustainability-risk, so substituting zero here would make the rule
		// fire on windows where the reference declines.
		cycleRaw = append(cycleRaw, average)
	}
	if err := cycleRows.Err(); err != nil {
		return 0, false, nil, err
	}
	return afterHours, afterHoursKnown, cycleTimesFrom(cycleRaw), nil
}

// loadCompoundingSignals ports _load_compounding_signals, the legacy hotspot
// proxy that compounding-risk falls back to.
//
// NEITHER QUERY IS TEAM-SCOPED -- CHAOS-4897: repo_complexity_daily and
// file_hotspot_daily carry repo_id and no team_id, so both the complexity delta
// and the hotspot count are org-wide. Mirrored deliberately; owned-repo
// derivation pending.
func (loader *RecommendationsLoader) loadCompoundingSignals(
	ctx context.Context, teamID string, windowStart, windowEnd time.Time,
) (complexityDelta float64, complexityKnown bool, churnOverlap float64, churnKnown bool, err error) {
	arguments := loader.windowArguments(teamID, windowStart, windowEnd)
	// mid = ws + timedelta(days=max(1, (we - ws).days // 2)). The max(1, ...)
	// keeps the midpoint strictly after the start even for a zero- or one-day
	// window, which would otherwise put every row in the second half.
	windowDays := int(windowEnd.Sub(windowStart).Hours() / 24)
	half := windowDays / 2
	if half < 1 {
		half = 1
	}
	arguments["mid"] = windowStart.AddDate(0, 0, half).Format("2006-01-02")

	complexityQuery := `
            SELECT
                avg(if(day < {mid:Date}, cpk, NULL)) AS first_half,
                avg(if(day >= {mid:Date}, cpk, NULL)) AS second_half
            FROM (
                SELECT day, repo_id,
                       argMax(cyclomatic_per_kloc, computed_at) AS cpk
                FROM repo_complexity_daily
                WHERE day >= {start:Date} AND day < {end:Date}` + loader.orgClause() + `
                GROUP BY day, repo_id
            )
        `
	complexityRows, err := loader.conn.Query(ctx, complexityQuery, namedArguments(arguments)...)
	if err != nil {
		return 0, false, 0, false, fmt.Errorf("load complexity halves: %w", err)
	}
	if complexityRows.Next() {
		var firstHalf, secondHalf *float64
		if scanErr := complexityRows.Scan(&firstHalf, &secondHalf); scanErr != nil {
			complexityRows.Close()
			return 0, false, 0, false, fmt.Errorf("scan complexity halves: %w", scanErr)
		}
		complexityDelta, complexityKnown = complexityDeltaFrom(firstHalf, secondHalf)
	}
	if closeErr := complexityRows.Close(); closeErr != nil {
		return 0, false, 0, false, closeErr
	}

	hotspotQuery := `
            SELECT count(DISTINCT file_path) AS total
            FROM (
                SELECT file_path, argMax(risk_score, computed_at) AS risk_score
                FROM file_hotspot_daily
                WHERE day >= {mid:Date} AND day < {end:Date}` + loader.orgClause() + `
                GROUP BY file_path
            ) WHERE risk_score > 0
        `
	hotspotRows, err := loader.conn.Query(ctx, hotspotQuery, namedArguments(arguments)...)
	if err != nil {
		return 0, false, 0, false, fmt.Errorf("load hotspot count: %w", err)
	}
	var totalHotspots uint64
	if hotspotRows.Next() {
		if scanErr := hotspotRows.Scan(&totalHotspots); scanErr != nil {
			hotspotRows.Close()
			return 0, false, 0, false, fmt.Errorf("scan hotspot count: %w", scanErr)
		}
	}
	if closeErr := hotspotRows.Close(); closeErr != nil {
		return 0, false, 0, false, closeErr
	}

	// churn_overlap stays ABSENT when there are no hotspots, and is 0.0 when
	// there are hotspots but complexity is flat or falling. Absent and 0.0 are
	// not interchangeable: absent makes compounding-risk's legacy path decline
	// outright, while 0.0 fails the threshold comparison instead. Same outcome
	// today, different reasons, and only one survives a threshold change.
	churnOverlap, churnKnown = churnOverlapFrom(complexityDelta, complexityKnown, totalHotspots)
	return complexityDelta, complexityKnown, churnOverlap, churnKnown, nil
}

// loadCompoundingRiskPersisted ports _load_compounding_risk_persisted, the
// CHAOS-1641 composite that shadows the legacy proxy entirely when present.
//
// TEAM-SCOPED, via scope='team' AND scope_id = team_id rather than a team_id
// column.
func (loader *RecommendationsLoader) loadCompoundingRiskPersisted(
	ctx context.Context, teamID string, windowStart, windowEnd time.Time,
) (score float64, scoreKnown bool, severity string, err error) {
	// argMax over a TUPLE, so the score and the severity are taken from the
	// SAME row. Two separate argMax calls could take them from different rows
	// if their computed_at values ever tie, which would pair a score with
	// another row's severity -- and severity is the field that decides whether
	// this path fires at all.
	query := `
            SELECT
                tupleElement(latest_row, 1) AS score,
                tupleElement(latest_row, 2) AS severity
            FROM (
                SELECT argMax(tuple(compounding_risk, severity), computed_at) AS latest_row
                FROM compounding_risk_daily
                WHERE scope = 'team'
                  AND scope_id = {team_id:String}
                  AND day >= {start:Date} AND day < {end:Date}` + loader.orgClause() + `
            )
        `
	rows, err := loader.conn.Query(ctx, query,
		namedArguments(loader.windowArguments(teamID, windowStart, windowEnd))...)
	if err != nil {
		return 0, false, "", fmt.Errorf("load compounding risk: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var rawScore *float64
		var rawSeverity string
		if scanErr := rows.Scan(&rawScore, &rawSeverity); scanErr != nil {
			return 0, false, "", fmt.Errorf("scan compounding risk: %w", scanErr)
		}
		score, scoreKnown = safeFloat(rawScore)
		// `str(severity) if severity else None`: an EMPTY severity becomes
		// absent, not an empty string. The Go mirror already spells absent as
		// "", so both map to "" and no conversion is needed -- the identity is
		// stated rather than written, because a reader checking parity against
		// the Python `if severity else None` will look for it here.
		return score, scoreKnown, rawSeverity, rows.Err()
	}
	return 0, false, "", rows.Err()
}

// LoadTeamMetricsWindow ports load_team_metrics_window: every signal for one
// team over [windowStart, windowEnd).
func (loader *RecommendationsLoader) LoadTeamMetricsWindow(
	ctx context.Context, teamID, orgID string, windowStart, windowEnd time.Time,
) (MetricsSnapshot, error) {
	// The reference swaps self._org_id for the duration of the load and
	// restores it in a finally block. A per-call loader is used instead so the
	// swap cannot leak on an error path.
	scoped := loader
	if orgID != "" && orgID != loader.orgID {
		scoped = &RecommendationsLoader{conn: loader.conn, orgID: orgID}
	}

	wip, throughput, err := scoped.loadWIPThroughput(ctx, teamID, windowStart, windowEnd)
	if err != nil {
		return MetricsSnapshot{}, err
	}
	latency, latencyKnown, gini, giniKnown, err := scoped.loadReviewSignals(ctx, teamID, windowStart, windowEnd)
	if err != nil {
		return MetricsSnapshot{}, err
	}
	rework, reworkKnown, err := scoped.loadReworkRatio(ctx, teamID, windowStart, windowEnd)
	if err != nil {
		return MetricsSnapshot{}, err
	}
	afterHours, afterHoursKnown, cycleTimes, err := scoped.loadSustainabilitySignals(ctx, teamID, windowStart, windowEnd)
	if err != nil {
		return MetricsSnapshot{}, err
	}
	complexityDelta, complexityKnown, churnOverlap, churnKnown, err := scoped.loadCompoundingSignals(ctx, teamID, windowStart, windowEnd)
	if err != nil {
		return MetricsSnapshot{}, err
	}
	score, scoreKnown, severity, err := scoped.loadCompoundingRiskPersisted(ctx, teamID, windowStart, windowEnd)
	if err != nil {
		return MetricsSnapshot{}, err
	}

	return MetricsSnapshot{
		TeamID:                      teamID,
		OrgID:                       orgID,
		WindowStart:                 windowStart,
		WindowEnd:                   windowEnd,
		WIPByDay:                    wip,
		ThroughputByCycle:           throughput,
		ReviewLatencyP75Hours:       latency,
		ReviewLatencyP75HoursKnown:  latencyKnown,
		ReviewerGini:                gini,
		ReviewerGiniKnown:           giniKnown,
		ReworkChurnRatio:            rework,
		ReworkChurnRatioKnown:       reworkKnown,
		AfterHoursRatio:             afterHours,
		AfterHoursRatioKnown:        afterHoursKnown,
		CycleTimeByDay:              cycleTimes,
		HotspotComplexityDelta:      complexityDelta,
		HotspotComplexityDeltaKnown: complexityKnown,
		HotspotChurnOverlap:         churnOverlap,
		HotspotChurnOverlapKnown:    churnKnown,
		CompoundingRiskScore:        score,
		CompoundingRiskScoreKnown:   scoreKnown,
		CompoundingRiskSeverity:     severity,
	}, nil
}

// The pure post-processing, lifted out of the query methods so the parity
// harness exercises THE REAL CODE PATH rather than a second copy of the
// arithmetic written to agree with it.
//
// Everything with parity risk lives here: the null-handling asymmetry, the
// denominator floor, and the absent-versus-zero churn decision. The query
// methods above are then only SQL, scanning and error wrapping.

// complexityDeltaFrom ports the normalised second-half-minus-first-half delta.
//
// max(first, 1.0) is a DENOMINATOR FLOOR, not a clamp on the result: it stops a
// near-zero first half from turning a small absolute rise into an enormous
// normalised delta. It also means a NEGATIVE first half divides by 1.0 rather
// than by itself, so the sign of the delta follows (second - first) alone and
// never flips from dividing by a negative number.
//
// Absent when EITHER half is absent -- and _safe_float treats NaN as absent
// while passing infinities through, so an infinite half yields an infinite or
// NaN delta rather than an absent one.
func complexityDeltaFrom(firstHalf, secondHalf *float64) (float64, bool) {
	first, firstKnown := safeFloat(firstHalf)
	second, secondKnown := safeFloat(secondHalf)
	if !firstKnown || !secondKnown {
		return 0, false
	}
	denominator := first
	if denominator < 1.0 {
		denominator = 1.0
	}
	return (second - first) / denominator, true
}

// churnOverlapFrom ports the legacy hotspot proxy's overlap decision.
//
// Three outcomes, and the first two are NOT interchangeable:
//
//	no hotspots                      -> ABSENT. compounding-risk's legacy path
//	                                    returns None outright without ever
//	                                    reaching a threshold comparison.
//	hotspots, delta absent or <= 0   -> 0.0. Present, and FAILS the threshold.
//	hotspots, delta > 0              -> min(1.0, delta).
//
// Absent and 0.0 reach the same outcome under today's constant (0.4 > 0.0) and
// would diverge the moment that threshold moved to or below zero. Collapsing
// them would be invisible now and wrong later.
//
// NaN is worth tracing: `delta > 0` is false for NaN, so a NaN delta with
// hotspots present yields 0.0, not NaN. That matches Python's `if
// complexity_delta is not None and complexity_delta > 0`.
func churnOverlapFrom(complexityDelta float64, complexityKnown bool, totalHotspots uint64) (float64, bool) {
	if totalHotspots == 0 {
		return 0, false
	}
	if complexityKnown && complexityDelta > 0 {
		if complexityDelta > 1.0 {
			return 1.0, true
		}
		return complexityDelta, true
	}
	return 0.0, true
}

// cycleTimesFrom ports the cycle-time list build, which DROPS null rows.
//
// The opposite of wipThroughputFrom below, in the same loader. A dropped row
// SHORTENS the list, and length decides sustainability-risk's
// `len(cycle_times) < 2` guard, so substituting 0.0 here would make the rule
// fire on windows where the reference declines.
func cycleTimesFrom(values []*float64) []float64 {
	cycleTimes := []float64{}
	for _, value := range values {
		if value != nil {
			cycleTimes = append(cycleTimes, *value)
		}
	}
	return cycleTimes
}

// wipThroughputFrom ports the wip/throughput list build, which KEEPS null rows
// as 0.0 -- see cycleTimesFrom for why the asymmetry is deliberate.
func wipThroughputFrom(wipValues, throughputValues []*float64) ([]float64, []float64) {
	wip := make([]float64, 0, len(wipValues))
	for _, value := range wipValues {
		wip = append(wip, nullableToZero(value))
	}
	throughput := make([]float64, 0, len(throughputValues))
	for _, value := range throughputValues {
		throughput = append(throughput, nullableToZero(value))
	}
	return wip, throughput
}
