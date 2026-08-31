package analytics

// Go port of investment_membership_scope.py:98-141 -- the STATE FETCH
// (fetch_investment_membership_scope_state) and the TELEMETRY HOOK
// (record_stale_investment_membership_scope) itself. CHAOS-4538 scope
// item 4, shipped in this PR alongside the logic it observes (root
// AGENTS.md standing order: no new decision path without its
// telemetry, same PR).
//
// WHAT PYTHON DOES, exactly: every investment query goes through
// _query_investment_dicts (investment.py:175-181), which -- BEFORE
// running the caller's real query -- fires a SEPARATE query
// (fetch_investment_membership_scope_state) and, only if that state is
// "unscoped_fallback" (the membership materializer has fallen stale
// relative to a newer investment computation, so the scope filter is
// disabled and every work unit becomes visible rather than an
// under-scoped subset), records a Prometheus counter increment PLUS a
// gauge set for lag_seconds, and logs a warning. Any other scope_mode
// ("scoped" or "unscoped_no_marker") records nothing. Errors fetching
// the state are swallowed to a debug log line -- the metric must never
// be able to break the real query it decorates.
//
// GO EQUIVALENT: RecordStaleInvestmentMembershipScope below reproduces
// that exact decision (fetch, check scope_mode=="unscoped_fallback",
// record-or-not, swallow fetch errors) using OTel instead of
// Prometheus -- this package's established telemetry substrate
// (telemetry.go's degradedCounter). A counter mirrors
// INVESTMENT_MEMBERSHIP_SCOPE_STALE_TOTAL's .inc(); a gauge mirrors
// INVESTMENT_MEMBERSHIP_SCOPE_LAG_SECONDS's .set(lag_seconds) exactly
// (a gauge, not a histogram/counter, because Python's is a Gauge --
// the most recent staleness lag is what an operator wants to read, not
// an accumulating sum).

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// InvestmentMembershipScopeState is the Go port of
// investment_membership_scope.py's InvestmentMembershipScopeState
// NamedTuple (:20-22).
type InvestmentMembershipScopeState struct {
	ScopeMode  string
	LagSeconds int64
}

var validScopeModes = map[string]bool{
	"scoped":             true,
	"unscoped_no_marker": true,
	"unscoped_fallback":  true,
}

// FetchInvestmentMembershipScopeState ports
// fetch_investment_membership_scope_state (investment_membership_scope.py:98-117)
// -- runs membershipScopeStateQuery() (this file's sibling,
// investmentmembershipscope.go) and normalizes an empty/unrecognized
// scope_mode to "unscoped_no_marker", matching Python's exact fallback
// (`if mode not in {...}: mode = "unscoped_no_marker"`, :112-114 -- the
// SAME normalization extract_scope_state_from_rows applies at :144-155
// for a caller that already has rows in hand).
func FetchInvestmentMembershipScopeState(ctx context.Context, client QueryClient, orgID string, timeoutSeconds int) (InvestmentMembershipScopeState, error) {
	rows, err := client.Query(ctx, membershipScopeStateQuery(), bindingsForOrgAndTimeout(orgID, timeoutSeconds))
	if err != nil {
		return InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		// Python: `if not rows: return InvestmentMembershipScopeState("unscoped_no_marker", 0)`
		// (:109-110) -- zero rows is the SAME fallback as an
		// unrecognized mode, not a distinct error.
		if err := rows.Err(); err != nil {
			return InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}, fmt.Errorf("rows: %w", err)
		}
		return InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}, nil
	}

	var mode string
	var lagSeconds int64
	if scanErr := rows.Scan(&mode, &lagSeconds); scanErr != nil {
		return InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}, fmt.Errorf("scan: %w", scanErr)
	}
	if err := rows.Err(); err != nil {
		return InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}, fmt.Errorf("rows: %w", err)
	}
	if !validScopeModes[mode] {
		mode = "unscoped_no_marker"
	}
	return InvestmentMembershipScopeState{ScopeMode: mode, LagSeconds: lagSeconds}, nil
}

// membershipScopeStaleCounter mirrors
// INVESTMENT_MEMBERSHIP_SCOPE_STALE_TOTAL (metrics/prometheus.py:~1016).
var membershipScopeStaleCounter = mustAnalyticsCounter(
	"devhealth_query_api_investment_membership_scope_stale_total",
	"investment membership scope checks that fell back unscoped because the membership materializer is stale relative to a newer investment computation, by scope_mode",
)

// membershipScopeLagGauge mirrors INVESTMENT_MEMBERSHIP_SCOPE_LAG_SECONDS
// -- a Gauge (last-observed value), not a Counter/Histogram, matching
// Python's .set() semantics rather than an accumulating .observe()/.inc().
var membershipScopeLagGauge = mustAnalyticsInt64Gauge(
	"devhealth_query_api_investment_membership_scope_lag_seconds",
	"seconds by which the latest investment computation trails the latest complete membership run, at the moment scope fell back unscoped",
)

func mustAnalyticsInt64Gauge(name, description string) metric.Int64Gauge {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api/internal/analytics")
	gauge, err := meter.Int64Gauge(name, metric.WithDescription(description))
	if err != nil {
		// Same otel guarantee telemetry.go's mustAnalyticsCounter relies
		// on: the instrument constructor never returns a nil instrument
		// even on error, so a broken meter provider must not panic a
		// resolver over an observability concern.
		gauge, _ = otel.GetMeterProvider().Meter("noop").Int64Gauge(name)
	}
	return gauge
}

// recordStaleInvestmentMembershipScope is a package var (not a plain
// func), same pattern as telemetry.go's recordDegradation and for the
// same reason: "the metric fired" is the only observable that
// distinguishes a correctly-instrumented stale-fallback path from one
// that silently emits nothing, so a test must be able to substitute a
// spy here.
var recordStaleInvestmentMembershipScope = defaultRecordStaleInvestmentMembershipScope

func defaultRecordStaleInvestmentMembershipScope(ctx context.Context, state InvestmentMembershipScopeState) {
	attrs := metric.WithAttributes(attribute.String("scope_mode", state.ScopeMode))
	membershipScopeStaleCounter.Add(ctx, 1, attrs)
	membershipScopeLagGauge.Record(ctx, state.LagSeconds, attrs)
	slog.WarnContext(ctx, "investment membership scope stale; falling back unscoped",
		"lag_seconds", state.LagSeconds, "scope_mode", state.ScopeMode)
}

// RecordStaleInvestmentMembershipScope ports
// record_stale_investment_membership_scope (investment_membership_scope.py:120-141)
// verbatim in decision shape: fetch the state; a fetch error is
// swallowed (Python: `except Exception as exc: logger.debug(...); return`
// -- this metric must never be able to break the real query it
// decorates); a non-"unscoped_fallback" mode records nothing; only
// "unscoped_fallback" fires the counter+gauge+log.
//
// CALLED FROM: every investment-path Compile*/Execute* entry point that
// resolves useInvestment=true, mirroring _query_investment_dicts
// (investment.py:175-181), which every one of investment.py's
// fetch_investment_* functions AND _get_context_params's own
// compiled-query consumers route through in Python. This port's call
// site is investmentContextFor's caller in each of
// timeseries.go/breakdown.go/sankey.go/flowmatrix.go -- see each
// Compile* function's doc comment for the exact call.
func RecordStaleInvestmentMembershipScope(ctx context.Context, client QueryClient, orgID string, timeoutSeconds int) {
	if orgID == "" {
		return
	}
	state, err := FetchInvestmentMembershipScopeState(ctx, client, orgID, timeoutSeconds)
	if err != nil {
		slog.DebugContext(ctx, "investment membership scope metric skipped", "error", err)
		return
	}
	if state.ScopeMode != "unscoped_fallback" {
		return
	}
	recordStaleInvestmentMembershipScope(ctx, state)
}
