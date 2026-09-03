package analytics

// CHAOS-4759 transition guard.
//
// RULING (chris, 2026-09-01 15:56 PT, "Go is the route"): investment.go's
// LatestWorkUnitInvestmentsSource tuple-wraps argMax over four Nullable
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
// GROUP BY scan over work_unit_investments runs per org ON SUCCESS. That
// scan is the same aggregate shape and cost as the one
// LatestWorkUnitInvestmentsSource already runs on every investment
// request for this org, so running it AGAIN on every request would
// double this package's ClickHouse load for a property that only needs
// bounded-latency detection -- the ticket asks that a transition be
// OBSERVED, not that it be caught on the exact request that produced it.
const argMaxNullTransitionCheckCooldown = 15 * time.Minute

// argMaxNullTransitionErrorCooldown is the SHORT cooldown a failed fetch
// gets instead of the full one -- codex round-2 P2 fix. The gate claims
// the cooldown optimistically, before the query runs, so concurrent
// requests for the same org cannot both observe an elapsed cooldown and
// fire the scan twice; but claiming the FULL 15-minute window on a fetch
// that then FAILS would blind this org to a genuinely new, unobserved
// divergence for the whole window on nothing but a transient ClickHouse
// hiccup. Shortening to one minute on error bounds the retry rate for a
// persistently-broken org (e.g. a permissions issue) while letting a
// merely-transient failure self-heal quickly.
const argMaxNullTransitionErrorCooldown = 1 * time.Minute

// argMaxNullTransitionGateClock is a package var so a test can control
// time deterministically instead of sleeping.
var argMaxNullTransitionGateClock = time.Now

// argMaxNullTransitionGate holds, per org, the next time the guard query
// may run. Unbounded only in the number of DISTINCT orgs that ever call
// an investment query on this process -- the same bound every other
// per-process in-memory org map in this package already accepts.
var argMaxNullTransitionGate = struct {
	mu          sync.Mutex
	nextAllowed map[string]time.Time
}{nextAllowed: make(map[string]time.Time)}

// argMaxNullTransitionClaim reports whether orgID's cooldown has elapsed
// and, if so, claims the FULL success cooldown immediately -- before the
// caller runs the query -- so concurrent requests for the same org
// cannot both observe an elapsed cooldown and fire the scan twice. A
// caller whose fetch then fails must call
// argMaxNullTransitionShortenAfterError to release most of that claim
// back early; see that function's doc comment.
func argMaxNullTransitionClaim(orgID string) bool {
	now := argMaxNullTransitionGateClock()
	argMaxNullTransitionGate.mu.Lock()
	defer argMaxNullTransitionGate.mu.Unlock()
	if next, ok := argMaxNullTransitionGate.nextAllowed[orgID]; ok && now.Before(next) {
		return false
	}
	argMaxNullTransitionGate.nextAllowed[orgID] = now.Add(argMaxNullTransitionCheckCooldown)
	return true
}

// argMaxNullTransitionShortenAfterError replaces a just-claimed FULL
// cooldown with the SHORT error cooldown -- see
// argMaxNullTransitionErrorCooldown's doc comment. Unconditional: this is
// only ever called immediately after this same goroutine's own
// argMaxNullTransitionClaim succeeded for orgID, so there is no other
// claimant's window to accidentally shorten.
func argMaxNullTransitionShortenAfterError(orgID string) {
	now := argMaxNullTransitionGateClock()
	argMaxNullTransitionGate.mu.Lock()
	defer argMaxNullTransitionGate.mu.Unlock()
	argMaxNullTransitionGate.nextAllowed[orgID] = now.Add(argMaxNullTransitionErrorCooldown)
}

// argMaxNullTransitionFetchFailedCounter counts guard fetch failures --
// codex round-3 finding: a debug-only log line (this package's existing
// convention for a decorator-telemetry fetch failure, e.g.
// RecordStaleInvestmentMembershipScope's own swallow) is invisible at
// this platform's default INFO level (DEV_HEALTH_LOG_LEVEL), so a
// persistently broken fetch -- a permissions change, a network partition
// -- could silently stop observing this org's divergence state forever,
// with zero operator-visible signal. That silence is worse here than for
// membership-scope's freshness observation: this guard's ENTIRE reason to
// exist, per CHAOS-4759's ruling, is "the day it happens is OBSERVED, not
// silent" -- a guard whose own failure mode is silent defeats that
// purpose. Deliberately diverges from the sibling convention for this
// reason; no attributes (org_id is logged, never a metric label, same
// cardinality discipline as argMaxNullTransitionCounter).
var argMaxNullTransitionFetchFailedCounter = mustAnalyticsCounter(
	"devhealth_query_api_investment_argmax_null_transition_fetch_failed_total",
	"argMax null transition guard fetches that errored before a divergence could even be checked -- a sustained non-zero rate means this org's divergence state is not being observed at all",
)

// recordArgMaxNullTransitionFetchFailure is a package var, not a plain
// func, for the same reason as recordArgMaxNullTransition: "the failure
// was reported" is the only observable a test can assert on.
var recordArgMaxNullTransitionFetchFailure = defaultRecordArgMaxNullTransitionFetchFailure

func defaultRecordArgMaxNullTransitionFetchFailure(ctx context.Context, orgID string, err error) {
	argMaxNullTransitionFetchFailedCounter.Add(ctx, 1)
	slog.WarnContext(ctx, "argMax null transition guard fetch failed; this org's divergence state is NOT being observed",
		"org_id", orgID,
		"error", err,
	)
}

// RecordArgMaxNullTransitionGuard is CHAOS-4759's transition guard: it
// runs FetchArgMaxNullTransitionState for orgID at most once per
// argMaxNullTransitionCheckCooldown (argMaxNullTransitionErrorCooldown on
// a failed attempt) and, only when the state has diverged on at least one
// column, reports through recordArgMaxNullTransition. A fetch error is
// reported through recordArgMaxNullTransitionFetchFailure (counter +
// warn log, see its doc comment) but never propagated -- this telemetry
// must never be able to break the real query it decorates.
//
// CALLED FROM every investment-path entry point that can actually read
// LatestWorkUnitInvestmentsSource: the same four call sites
// RecordStaleInvestmentMembershipScope uses (resolve.go's
// resolveOneTimeseries/resolveOneBreakdown, investmentquality.go's
// resolveEvidenceQualityStats, sankeycoverage.go's resolveSankeyCoverage),
// PLUS resolve.go's resolveSankey (nodes/edges, gated on the AUTO-ROUTED
// useInvestment, not the raw three-state flag resolveSankeyCoverage uses)
// and resolveFlowMatrix (gated on flowMatrixUsesInvestmentSource) --
// codex round-2 P1 fix: unlike RecordStaleInvestmentMembershipScope,
// which mirrors Python's own call sites and is deliberately absent from
// sankey/flowMatrix execution, this guard has no Python site to mirror
// and must fire wherever the Go-only source it watches is actually read.
func RecordArgMaxNullTransitionGuard(ctx context.Context, client QueryClient, orgID string, timeoutSeconds int) {
	if orgID == "" || !argMaxNullTransitionClaim(orgID) {
		return
	}
	state, err := FetchArgMaxNullTransitionState(ctx, client, orgID, timeoutSeconds)
	if err != nil {
		argMaxNullTransitionShortenAfterError(orgID)
		recordArgMaxNullTransitionFetchFailure(ctx, orgID, err)
		return
	}
	if !state.Diverged() {
		return
	}
	recordArgMaxNullTransition(ctx, orgID, state)
}
