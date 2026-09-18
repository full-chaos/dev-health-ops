package analytics

// One membership-scope resolution per request.
//
// Every investment read composes LatestWorkUnitInvestmentsSource, whose scope
// filter names its membership run through investmentScopeRunIDSQL() -- a
// scalar subquery each ClickHouse query evaluates for itself. A request that
// runs several queries (a flow response runs its link query and its coverage
// query; a GraphQL request can run many) would otherwise let a marker that
// lands between two of them scope the second query to a different run than
// the first, and one response would mix two work-unit sets.
//
// PinInvestmentMembershipScope wraps a route's QueryClient. For a statement
// that carries the scope filter it resolves the organisation's scope ONCE
// per request (the first query of the request pays one small state query),
// replaces the scalar with the {investment_scope_run_id:String} parameter,
// and binds the resolved run id. The request boundary is the context
// WithInvestmentMembershipScopeRequest returns (InvestmentMembershipScopeRequestMiddleware
// installs one per HTTP request). Without it each query resolves for itself.
//
// The resolution is also where the stale-projection telemetry is recorded
// (recordStaleInvestmentMembershipScope, once per resolution), so every read
// that carries the scope filter reports a lagging projection, whichever
// route or query shape runs it.
//
// A resolution failure fails the statement (and, being the request's one
// resolution, every later scope-filtered statement of that request): a
// statement that resolved its own run could land on a different run than
// its siblings, so a request that cannot pin its scope answers with an
// error rather than a mixed response.
//
// The superseded run stays readable while a request is pinned to it: the
// projection keeps the latest two complete runs (membershipRetentionKeep,
// internal/jobs/metrics/remaining), so a pinned run is pruned only after
// two newer markers land.

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// investmentScopeRunIDParam names the query parameter a pinned statement
// reads its membership run id from.
const investmentScopeRunIDParam = "investment_scope_run_id"

func investmentScopeRunIDPlaceholder() string {
	return "{" + investmentScopeRunIDParam + ":String}"
}

type investmentScopeRequestKey struct{}

// investmentScopeRequest holds one request's scope resolutions, one per
// organisation the request reads.
type investmentScopeRequest struct {
	mu    sync.Mutex
	byOrg map[string]*investmentScopeResolution
}

type investmentScopeResolution struct {
	once  sync.Once
	state InvestmentMembershipScopeState
	err   error
}

func (request *investmentScopeRequest) resolution(orgID string) *investmentScopeResolution {
	request.mu.Lock()
	defer request.mu.Unlock()
	entry, ok := request.byOrg[orgID]
	if !ok {
		entry = &investmentScopeResolution{}
		request.byOrg[orgID] = entry
	}
	return entry
}

// WithInvestmentMembershipScopeRequest marks ctx as one request: every
// pinned query and every RecordStaleInvestmentMembershipScope call under the
// returned context shares one scope resolution per organisation.
func WithInvestmentMembershipScopeRequest(ctx context.Context) context.Context {
	return context.WithValue(ctx, investmentScopeRequestKey{}, &investmentScopeRequest{
		byOrg: map[string]*investmentScopeResolution{},
	})
}

// InvestmentMembershipScopeRequestMiddleware gives each HTTP request its own
// scope resolution (WithInvestmentMembershipScopeRequest).
func InvestmentMembershipScopeRequestMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(WithInvestmentMembershipScopeRequest(r.Context())))
	})
}

// fetchInvestmentMembershipScopeState is a package var so a test can count
// resolutions and inject a failure.
var fetchInvestmentMembershipScopeState = FetchInvestmentMembershipScopeState

// resolveInvestmentMembershipScope returns the organisation's scope state:
// the request's shared resolution when ctx carries a request, a fresh one
// otherwise. The first caller's timeout applies to the shared resolution.
func resolveInvestmentMembershipScope(ctx context.Context, client QueryClient, orgID string, timeoutSeconds int) (InvestmentMembershipScopeState, error) {
	request, ok := ctx.Value(investmentScopeRequestKey{}).(*investmentScopeRequest)
	if !ok {
		return resolveAndObserveInvestmentMembershipScope(ctx, client, orgID, timeoutSeconds)
	}
	entry := request.resolution(orgID)
	entry.once.Do(func() {
		entry.state, entry.err = resolveAndObserveInvestmentMembershipScope(ctx, client, orgID, timeoutSeconds)
	})
	return entry.state, entry.err
}

// resolveAndObserveInvestmentMembershipScope fetches the state, logs the
// resolution at debug, and records the stale-projection telemetry when the
// projection trails the newest investment rows.
func resolveAndObserveInvestmentMembershipScope(ctx context.Context, client QueryClient, orgID string, timeoutSeconds int) (InvestmentMembershipScopeState, error) {
	state, err := fetchInvestmentMembershipScopeState(ctx, client, orgID, timeoutSeconds)
	if err != nil {
		return state, err
	}
	slog.DebugContext(ctx, "investment membership scope resolved",
		"org_id", orgID, "scope_mode", state.ScopeMode,
		"membership_run_id", state.RunID, "lag_seconds", state.LagSeconds)
	if state.ScopeMode == scopeModeProjectionLag {
		recordStaleInvestmentMembershipScope(ctx, orgID, state)
	}
	return state, nil
}

// PinInvestmentMembershipScope wraps client so every statement carrying the
// investment membership scope filter reads the request's one resolved run.
// Statements without the filter pass through unchanged.
func PinInvestmentMembershipScope(client QueryClient) QueryClient {
	return pinnedInvestmentScopeClient{next: client}
}

type pinnedInvestmentScopeClient struct {
	next QueryClient
}

func (c pinnedInvestmentScopeClient) Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	runIDSQL := investmentScopeRunIDSQL()
	if !strings.Contains(statement, runIDSQL) {
		return c.next.Query(ctx, statement, bindings)
	}
	orgID, ok := orgIDBinding(bindings)
	if !ok {
		// The filter itself reads {org_id:String}; without that binding the
		// statement fails in ClickHouse whether or not it is pinned.
		return c.next.Query(ctx, statement, bindings)
	}
	state, err := resolveInvestmentMembershipScope(ctx, c.next, orgID, queryTimeoutSecs)
	if err != nil {
		slog.WarnContext(ctx, "investment membership scope unresolved; failing the scope-filtered query",
			"org_id", orgID, "error", err)
		return nil, fmt.Errorf("resolve investment membership scope: %w", err)
	}
	pinnedBindings := make([]clickhouse.Binding, 0, len(bindings)+1)
	pinnedBindings = append(pinnedBindings, bindings...)
	pinnedBindings = append(pinnedBindings, clickhouse.Binding{Name: investmentScopeRunIDParam, Value: state.RunID})
	return c.next.Query(ctx, strings.ReplaceAll(statement, runIDSQL, investmentScopeRunIDPlaceholder()), pinnedBindings)
}

func orgIDBinding(bindings []clickhouse.Binding) (string, bool) {
	for _, binding := range bindings {
		if binding.Name != "org_id" {
			continue
		}
		orgID, ok := binding.Value.(string)
		return orgID, ok && orgID != ""
	}
	return "", false
}
