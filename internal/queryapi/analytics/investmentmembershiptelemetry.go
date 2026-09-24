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
// (fetch_investment_membership_scope_state) and, only if the membership
// projection trails a newer investment computation, records a Prometheus
// counter increment PLUS a gauge set for lag_seconds, and logs a warning.
// Errors fetching the state are reported through a warn log line (org id +
// error) and never propagated -- the metric must never be able to break the
// real query it decorates.
//
// GO BEHAVIOUR: the lagging state is "scoped_projection_lag" here, and the
// read stays scoped to the latest complete membership run through it
// (investmentMembershipScopeStateSource). The telemetry is recorded by the
// request's one scope resolution (investmentmembershipscopepin.go) -- every
// scope-filtered read resolves through it, and so does
// RecordStaleInvestmentMembershipScope below -- so the state reported is the
// state every query of that request used, recorded once, using OTel instead of
// Prometheus -- this package's established telemetry substrate
// (telemetry.go's degradedCounter). A counter mirrors
// INVESTMENT_MEMBERSHIP_SCOPE_STALE_TOTAL's .inc(); a gauge mirrors
// INVESTMENT_MEMBERSHIP_SCOPE_LAG_SECONDS's .set(lag_seconds) exactly
// (a gauge, not a histogram/counter, because Python's is a Gauge --
// the most recent staleness lag is what an operator wants to read, not
// an accumulating sum).

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// InvestmentMembershipScopeState is the Go port of
// investment_membership_scope.py's InvestmentMembershipScopeState
// NamedTuple (:20-22).
//
// RunID is the latest complete membership run the scope resolves to; it is
// empty exactly when ScopeMode is "unscoped_no_marker".
type InvestmentMembershipScopeState struct {
	ScopeMode  string
	LagSeconds int64
	RunID      string
}

// Scope modes membershipScopeStateQuery reports; see
// investmentMembershipScopeStateSource for their meaning.
const (
	scopeModeScoped         = "scoped"
	scopeModeProjectionLag  = "scoped_projection_lag"
	scopeModeUnscopedNoRuns = "unscoped_no_marker"
)

var validScopeModes = map[string]bool{
	scopeModeScoped:         true,
	scopeModeProjectionLag:  true,
	scopeModeUnscopedNoRuns: true,
}

// FetchInvestmentMembershipScopeState ports
// fetch_investment_membership_scope_state (investment_membership_scope.py:98-117)
// -- runs membershipScopeStateQuery() (this file's sibling,
// investmentmembershipscope.go). Only a state the query positively reports
// is accepted: "unscoped_no_marker" (carrying no run id), or a scoped mode
// naming its run. Anything else -- no row, an empty or unrecognized
// scope_mode, a scoped mode with an empty run id -- is an error, never an
// unscoped read: an unscoped read counts every stored work-unit
// generation, so the gate fails closed on a state it cannot interpret.
// (The Python plane normalizes those cases to "unscoped_no_marker"; Go
// does not.)
func FetchInvestmentMembershipScopeState(ctx context.Context, client QueryClient, orgID string, timeoutSeconds int) (state InvestmentMembershipScopeState, err error) {
	state = InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}

	rows, queryErr := client.Query(ctx, membershipScopeStateQuery(timeoutSeconds), bindingsForOrg(orgID))
	if queryErr != nil {
		return state, fmt.Errorf("query: %w", queryErr)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			// A stream that fails only on Close (Next/Scan/Err all
			// clean) is otherwise invisible -- report it through the
			// same named return every other branch below uses, which
			// RecordStaleInvestmentMembershipScope already swallows to
			// a debug log on any non-nil error (this decorator has no
			// cooldown to shorten, unlike its two siblings).
			state = InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}
			err = fmt.Errorf("close: %w", closeErr)
		}
	}()

	if !rows.Next() {
		// The state query aggregates over single-row derived tables, so it
		// always answers one row; none means the answer cannot be read.
		if rowsErr := rows.Err(); rowsErr != nil {
			err = fmt.Errorf("rows: %w", rowsErr)
			return
		}
		err = errors.New("scope state query returned no row")
		return
	}

	var mode, runID string
	var lagSeconds int64
	if scanErr := rows.Scan(&mode, &lagSeconds, &runID); scanErr != nil {
		err = fmt.Errorf("scan: %w", scanErr)
		return
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		err = fmt.Errorf("rows: %w", rowsErr)
		return
	}
	if !validScopeModes[mode] {
		err = fmt.Errorf("unrecognized scope_mode %q", mode)
		return
	}
	if mode == scopeModeUnscopedNoRuns {
		runID = ""
	} else if runID == "" {
		err = fmt.Errorf("scope_mode %q reported without a membership run id", mode)
		return
	}
	state = InvestmentMembershipScopeState{ScopeMode: mode, LagSeconds: lagSeconds, RunID: runID}
	return
}

// membershipScopeStaleCounter mirrors
// INVESTMENT_MEMBERSHIP_SCOPE_STALE_TOTAL (metrics/prometheus.py:~1016).
var membershipScopeStaleCounter = mustAnalyticsCounter(
	"devhealth_query_api_investment_membership_scope_stale_total",
	"investment membership scope resolutions whose latest complete membership run trails a newer investment computation (reads stay scoped to that run), by scope_mode",
)

// membershipScopeLagGauge mirrors INVESTMENT_MEMBERSHIP_SCOPE_LAG_SECONDS
// -- a Gauge (last-observed value), not a Counter/Histogram, matching
// Python's .set() semantics rather than an accumulating .observe()/.inc().
var membershipScopeLagGauge = mustAnalyticsInt64Gauge(
	"devhealth_query_api_investment_membership_scope_lag_seconds",
	"seconds by which the latest complete membership run trails the latest investment computation, at the last lagging scope resolution",
)

func mustAnalyticsInt64Gauge(name, description string) metric.Int64Gauge {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/internal/queryapi/analytics")
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

func defaultRecordStaleInvestmentMembershipScope(ctx context.Context, orgID string, state InvestmentMembershipScopeState) {
	attrs := metric.WithAttributes(attribute.String("scope_mode", state.ScopeMode))
	membershipScopeStaleCounter.Add(ctx, 1, attrs)
	membershipScopeLagGauge.Record(ctx, state.LagSeconds, attrs)
	slog.WarnContext(ctx, "investment membership projection trails the latest investment computation; reads stay scoped to the latest complete membership run",
		"org_id", orgID, "lag_seconds", state.LagSeconds, "scope_mode", state.ScopeMode, "membership_run_id", state.RunID)
}

// RecordStaleInvestmentMembershipScope ports
// record_stale_investment_membership_scope (investment_membership_scope.py:120-141)
// in decision shape: resolve the state (once per request, shared with the
// request's queries -- resolveInvestmentMembershipScope, which records the
// counter+gauge+log for "scoped_projection_lag" as part of the resolution);
// a resolution error is reported via a warn log (org id + error) and never
// propagated -- this metric must never be able to break the real query it
// decorates.
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
	if _, err := resolveInvestmentMembershipScope(ctx, client, orgID, timeoutSeconds); err != nil {
		// A swallowed fetch error must still be operator-visible: at
		// this platform's default log level a debug line is invisible,
		// which would let a persistently broken fetch stop observing
		// this org's membership scope forever with zero signal.
		slog.WarnContext(ctx, "investment membership scope metric skipped",
			"org_id", orgID, "error", err)
	}
}
