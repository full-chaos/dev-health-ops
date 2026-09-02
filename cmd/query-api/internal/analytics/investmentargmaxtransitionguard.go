package analytics

// CHAOS-4759 transition guard.
//
// RULING (chris, 2026-09-01 15:56 PT, "Go is the route"): investment.go's
// latestWorkUnitInvestmentsSource tuple-wraps argMax over four Nullable
// columns (repo_id, provider, work_unit_type, work_unit_name) so it
// reports a work unit's TRUE latest generation, including a NULL, rather
// than Python's null-skipping argMax(col, computed_at)
// (api/queries/investment.py:32), which silently keeps returning a STALE
// non-null value from an OLDER generation once the newest generation
// clears the column. Go's behaviour is the one being kept; Python is not
// being fixed (chris's standing no-Python-graphql-work rule) -- see
// investment.go's own CHAOS-4547 doc comment for the mechanism and
// CHAOS-4759 for the decision record.
//
// The two planes agree exactly as long as no work unit's newest
// generation ever clears one of the four columns relative to an earlier
// generation of the SAME work unit. CHAOS-4759 measured that as a 0-of-203
// multi-generation-unit snapshot on org 70d529e0 at ruling time -- a
// SNAPSHOT, not a property. This file is the guard the ticket's option 3
// requires: detect, per org, the moment that stops being true, so the day
// it happens is OBSERVED rather than re-discovered by another adversarial
// review with no telemetry to point at.
//
// argMaxNullTransitionGuardQuery reproduces CHAOS-4759's own baseline
// measurement (recovered from system.query_log, query_id
// c81b95bf-15fa-4828-8f61-8e00c18d08cb, executed against org
// 70d529e0-3c06-4597-8480-794fd02328b6 where it measured
// "0 0 0 0 203" -- independently reproduced byte-for-byte in this PR's own
// TEST-EVIDENCE), with ONE deliberate correction: the ticket's own SQL
// used `HAVING uniqExact(computed_at) > 1` (candidate = more than one
// DISTINCT timestamp); this guard uses `HAVING count() > 1` (candidate =
// more than one ROW). Codex round 1 (xhigh) constructed and this lane
// independently re-verified against a live engine: two rows for one work
// unit at the IDENTICAL computed_at (one NULL repo_id inserted first, one
// non-NULL inserted second) produce `argMax(repo_id, computed_at)` =
// the non-NULL value while `(argMax(tuple(repo_id), computed_at)).1` =
// NULL -- a genuine, reproducible divergence -- yet `uniqExact(computed_at)`
// is 1, so the ticket's own criterion silently EXCLUDES this unit and the
// guard would report no divergence. `count() > 1` has no corresponding
// false-negative (a byte-identical duplicate row cannot itself produce a
// py-non-null/go-null split) and is therefore strictly more inclusive
// with no new false positives.

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ArgMaxNullTransitionState is one org's snapshot of the CHAOS-4759
// divergence measurement: among work units with more than one generation
// (uniqExact(computed_at) > 1), how many have transitioned -- Python's
// null-skipping argMax still returns a non-NULL (stale) value while Go's
// tuple-argMax correctly returns NULL because the true latest generation
// cleared the column -- broken out per column.
type ArgMaxNullTransitionState struct {
	RepoID               int64
	Provider             int64
	WorkUnitType         int64
	WorkUnitName         int64
	MultiGenerationUnits int64
}

// Diverged reports whether ANY column has transitioned for at least one
// work unit -- the moment the two planes start disagreeing on real data
// rather than merely differing in expression text.
func (s ArgMaxNullTransitionState) Diverged() bool {
	return s.RepoID > 0 || s.Provider > 0 || s.WorkUnitType > 0 || s.WorkUnitName > 0
}

// argMaxColumnCount pairs one transitioned column's OTel attribute value
// with its diverged count.
type argMaxColumnCount struct {
	column string
	count  int64
}

// columnCounts lists the four columns in a fixed order pinned by the
// seeded test and this file's ledger.
func (s ArgMaxNullTransitionState) columnCounts() []argMaxColumnCount {
	return []argMaxColumnCount{
		{"repo_id", s.RepoID},
		{"provider", s.Provider},
		{"work_unit_type", s.WorkUnitType},
		{"work_unit_name", s.WorkUnitName},
	}
}

// argMaxNullTransitionGuardQuery renders the CHAOS-4759 baseline
// measurement query for one org. timeoutSeconds is ALWAYS this package's
// own queryTimeoutSecs constant -- see settingsMaxExecutionTime's doc
// comment (cost.go) for why this value may never carry request-supplied
// input.
//
// Every projected column is wrapped in toInt64(...): count()/countIf()
// return UInt64 in ClickHouse, and the native driver refuses to scan a
// UInt64 column into a Go *int64 destination ("converting UInt64 to
// *int64 is unsupported, try using *uint64") -- caught by the seeded
// real-engine test (investmentargmaxtransitionguard_seeded_integration_test.go),
// NOT by the fake-row-scanner unit tests, which happily accept an int64
// fixture regardless of what the real driver would allow. Same fix
// shape as investmentmembershipscope.go's membershipScopeStateQuery
// wrapping lag_seconds in toInt64(...) for the identical reason.
func argMaxNullTransitionGuardQuery(timeoutSeconds int) string {
	return fmt.Sprintf(`
SELECT
  toInt64(countIf(py_repo IS NOT NULL AND go_repo IS NULL)) AS div_repo_id,
  toInt64(countIf(py_prov IS NOT NULL AND go_prov IS NULL)) AS div_provider,
  toInt64(countIf(py_type IS NOT NULL AND go_type IS NULL)) AS div_work_unit_type,
  toInt64(countIf(py_name IS NOT NULL AND go_name IS NULL)) AS div_work_unit_name,
  toInt64(count()) AS multi_generation_units
FROM (
    SELECT work_unit_id,
        argMax(repo_id, computed_at) AS py_repo,        (argMax(tuple(repo_id), computed_at)).1 AS go_repo,
        argMax(provider, computed_at) AS py_prov,       (argMax(tuple(provider), computed_at)).1 AS go_prov,
        argMax(work_unit_type, computed_at) AS py_type, (argMax(tuple(work_unit_type), computed_at)).1 AS go_type,
        argMax(work_unit_name, computed_at) AS py_name, (argMax(tuple(work_unit_name), computed_at)).1 AS go_name
    FROM work_unit_investments
    WHERE org_id = {org_id:String}
    GROUP BY work_unit_id
    HAVING count() > 1
)
%s
`, settingsMaxExecutionTime(timeoutSeconds))
}

// FetchArgMaxNullTransitionState runs the guard query for one org and
// scans its single result row. Zero rows never actually happens here
// (count()/countIf with no GROUP BY always emits exactly one row, even
// over zero matching work units), but a defensive zero-value return
// matches this package's existing style at
// FetchInvestmentMembershipScopeState.
func FetchArgMaxNullTransitionState(ctx context.Context, client QueryClient, orgID string, timeoutSeconds int) (ArgMaxNullTransitionState, error) {
	rows, err := client.Query(ctx, argMaxNullTransitionGuardQuery(timeoutSeconds), bindingsForOrg(orgID))
	if err != nil {
		return ArgMaxNullTransitionState{}, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return ArgMaxNullTransitionState{}, fmt.Errorf("rows: %w", err)
		}
		return ArgMaxNullTransitionState{}, nil
	}

	var state ArgMaxNullTransitionState
	if scanErr := rows.Scan(&state.RepoID, &state.Provider, &state.WorkUnitType, &state.WorkUnitName, &state.MultiGenerationUnits); scanErr != nil {
		return ArgMaxNullTransitionState{}, fmt.Errorf("scan: %w", scanErr)
	}
	if err := rows.Err(); err != nil {
		return ArgMaxNullTransitionState{}, fmt.Errorf("rows: %w", err)
	}
	return state, nil
}

// argMaxNullTransitionCounter fires once per transitioned COLUMN
// (attribute "column"), each Add carrying that column's own diverged
// count -- an operator can distinguish "one column flipped" from "all
// four flipped at once" without cross-referencing the log line.
var argMaxNullTransitionCounter = mustAnalyticsCounter(
	"devhealth_query_api_investment_argmax_null_transition_total",
	"work units, by column, whose newest generation cleared (NULL) a column an earlier generation had non-NULL -- the CHAOS-4759 Go/Python argMax divergence becoming live rather than latent",
)

// argMaxNullTransitionUnitsGauge mirrors the baseline measurement's own
// denominator (multi_generation_units) as of the last check that found a
// divergence -- a Gauge (last-observed value), not a Counter, matching
// this package's membershipScopeLagGauge precedent.
var argMaxNullTransitionUnitsGauge = mustAnalyticsInt64Gauge(
	"devhealth_query_api_investment_argmax_null_transition_multi_generation_units",
	"multi-generation work units observed in an org at the moment an argMax null-transition was last detected there",
)

// recordArgMaxNullTransition is a package var, not a plain func, for the
// same reason as recordDegradation / recordStaleInvestmentMembershipScope:
// "the metric fired" is the only observable a test can assert on.
var recordArgMaxNullTransition = defaultRecordArgMaxNullTransition

func defaultRecordArgMaxNullTransition(ctx context.Context, orgID string, state ArgMaxNullTransitionState) {
	for _, cc := range state.columnCounts() {
		if cc.count <= 0 {
			continue
		}
		argMaxNullTransitionCounter.Add(ctx, cc.count, metric.WithAttributes(attribute.String("column", cc.column)))
	}
	argMaxNullTransitionUnitsGauge.Record(ctx, state.MultiGenerationUnits)
	// org_id is logged, never a metric label -- unbounded per-tenant
	// cardinality on a metric instrument is exactly what CHAOS-4394 (see
	// go-worker-runtime.md's dev_health_cicd_partial_success_total note)
	// already ruled out for this codebase; the log line is where an
	// operator finds which org.
	slog.WarnContext(ctx, "investment argMax null transition observed: Go and Python planes now disagree (CHAOS-4759)",
		"org_id", orgID,
		"div_repo_id", state.RepoID,
		"div_provider", state.Provider,
		"div_work_unit_type", state.WorkUnitType,
		"div_work_unit_name", state.WorkUnitName,
		"multi_generation_units", state.MultiGenerationUnits,
	)
}

// argMaxNullTransitionCheckCooldown bounds how often the guard's own
// GROUP BY scan over work_unit_investments runs per org. That scan is the
// same aggregate shape and cost as the one latestWorkUnitInvestmentsSource
// already runs on every investment request for this org, so running it
// AGAIN on every request would double this package's ClickHouse load for
// a property that only needs bounded-latency detection -- the ticket asks
// that a transition be OBSERVED, not that it be caught on the exact
// request that produced it.
const argMaxNullTransitionCheckCooldown = 15 * time.Minute

// argMaxNullTransitionGateClock is a package var so a test can control
// time deterministically instead of sleeping 15 minutes.
var argMaxNullTransitionGateClock = time.Now

// argMaxNullTransitionGate holds, per org, the last time the guard query
// ran. Unbounded only in the number of DISTINCT orgs that ever call an
// investment query on this process -- the same bound every other
// per-process in-memory org map in this package already accepts.
var argMaxNullTransitionGate = struct {
	mu          sync.Mutex
	lastChecked map[string]time.Time
}{lastChecked: make(map[string]time.Time)}

// argMaxNullTransitionShouldCheck reports whether orgID's cooldown has
// elapsed and, if so, claims the check immediately -- before the caller
// runs the query -- so concurrent requests for the same org cannot both
// observe an elapsed cooldown and fire the scan twice.
func argMaxNullTransitionShouldCheck(orgID string) bool {
	now := argMaxNullTransitionGateClock()
	argMaxNullTransitionGate.mu.Lock()
	defer argMaxNullTransitionGate.mu.Unlock()
	if last, ok := argMaxNullTransitionGate.lastChecked[orgID]; ok && now.Sub(last) < argMaxNullTransitionCheckCooldown {
		return false
	}
	argMaxNullTransitionGate.lastChecked[orgID] = now
	return true
}

// RecordArgMaxNullTransitionGuard is CHAOS-4759's transition guard: it
// runs FetchArgMaxNullTransitionState for orgID at most once per
// argMaxNullTransitionCheckCooldown and, only when the state has
// diverged on at least one column, reports through
// recordArgMaxNullTransition. A fetch error is swallowed to a debug log
// line -- like RecordStaleInvestmentMembershipScope, this telemetry must
// never be able to break the real query it decorates.
//
// CALLED FROM the same four investment-path entry points that call
// RecordStaleInvestmentMembershipScope (resolve.go's
// resolveOneTimeseries/resolveOneBreakdown, investmentquality.go's
// resolveEvidenceQualityStats, sankeycoverage.go's resolveSankeyCoverage)
// -- every one of them already resolves useInvestment=true before
// calling, which is the sole precondition for
// latestWorkUnitInvestmentsSource to be live for this org's traffic at
// all.
func RecordArgMaxNullTransitionGuard(ctx context.Context, client QueryClient, orgID string, timeoutSeconds int) {
	if orgID == "" || !argMaxNullTransitionShouldCheck(orgID) {
		return
	}
	state, err := FetchArgMaxNullTransitionState(ctx, client, orgID, timeoutSeconds)
	if err != nil {
		slog.DebugContext(ctx, "argMax null transition guard skipped", "error", err)
		return
	}
	if !state.Diverged() {
		return
	}
	recordArgMaxNullTransition(ctx, orgID, state)
}
