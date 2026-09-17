// Every route that narrows a repo-keyed read by a team scope must reach the
// ONE shared condition. This file walks them and reads back the statements
// each one actually emitted.
//
// The rule is identical everywhere, so a route that resolves a team's
// repositories some other way answers a different question under the same
// word. The port of /api/v1/explain lost this exact wiring once, silently:
// the request still returned 200, over the whole organization. Nothing in a
// per-package test caught it, because each package only ever checked its own
// copy.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/drilldown"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/explain"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/heatmap"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/home"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investment"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investmentexplain"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investmentflow"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/opportunities"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/sankey"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/teamscope"
)

const (
	teamScopeReachOrgID = "ABC-123-org"
	teamScopeReachTeam  = "ABC-123"
)

// teamScopeReachClient records every statement a builder sends and answers
// with no rows, except for the two system-catalog probes /api/v1/investment
// runs before it reads anything: those decide whether the route continues at
// all, so they echo their own bound values back and let it.
type teamScopeReachClient struct {
	mu         sync.Mutex
	statements []string
	bindings   [][]dhclickhouse.Binding
}

func (c *teamScopeReachClient) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.mu.Lock()
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, bindings)
	c.mu.Unlock()

	switch {
	case strings.Contains(statement, "FROM system.tables"):
		return &teamScopeReachRows{values: append(bindingStringValues(bindings, "table"), bindingStringValues(bindings, "tables")...)}, nil
	case strings.Contains(statement, "FROM system.columns"):
		return &teamScopeReachRows{values: append(bindingStringValues(bindings, "column"), bindingStringValues(bindings, "columns")...)}, nil
	}
	return &teamScopeReachRows{}, nil
}

// sawCondition reports whether any recorded statement carries the shared
// condition together with all three of its bindings.
func (c *teamScopeReachClient) sawCondition() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, statement := range c.statements {
		if !strings.Contains(statement, teamscope.Marker) {
			continue
		}
		if hasBinding(c.bindings[i], teamscope.BindingOrgID) &&
			hasBinding(c.bindings[i], teamscope.BindingTeamIDs) &&
			hasBinding(c.bindings[i], teamscope.BindingAsOf) {
			return true
		}
	}
	return false
}

// sawAnyConditionTrace reports whether the condition's text or ANY of its
// bindings appears anywhere, which is what an org-scoped request must not
// produce.
func (c *teamScopeReachClient) sawAnyConditionTrace() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, statement := range c.statements {
		if strings.Contains(statement, teamscope.Marker) {
			return true
		}
		for _, name := range []string{teamscope.BindingOrgID, teamscope.BindingTeamIDs, teamscope.BindingAsOf} {
			if hasBinding(c.bindings[i], name) {
				return true
			}
		}
	}
	return false
}

func hasBinding(bindings []dhclickhouse.Binding, name string) bool {
	for _, binding := range bindings {
		if binding.Name == name {
			return true
		}
	}
	return false
}

func bindingStringValues(bindings []dhclickhouse.Binding, name string) []string {
	for _, binding := range bindings {
		if binding.Name != name {
			continue
		}
		switch value := binding.Value.(type) {
		case string:
			return []string{value}
		case []string:
			return value
		}
	}
	return nil
}

type teamScopeReachRows struct {
	values []string
	index  int
}

func (r *teamScopeReachRows) Next() bool {
	if r.index >= len(r.values) {
		return false
	}
	r.index++
	return true
}

func (r *teamScopeReachRows) Scan(dest ...any) error {
	if len(dest) > 0 {
		if target, ok := dest[0].(*string); ok {
			*target = r.values[r.index-1]
		}
	}
	return nil
}

func (r *teamScopeReachRows) Err() error   { return nil }
func (r *teamScopeReachRows) Close() error { return nil }

// teamScopeConsumers is every route that narrows a repo-keyed read by a team
// scope. A new one belongs in this table.
func teamScopeConsumers() []struct {
	route string
	drive func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string)
} {
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	return []struct {
		route string
		drive func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string)
	}{
		{"GET /api/v1/work-units", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			reader, err := investmentexplain.NewReader(client)
			if err != nil {
				t.Fatalf("investmentexplain.NewReader: %v", err)
			}
			query := url.Values{"scope_type": {scopeLevel}}
			if len(scopeIDs) > 0 {
				query.Set("scope_id", scopeIDs[0])
			}
			request := httptest.NewRequest(http.MethodGet, "/api/v1/work-units?"+query.Encode(), nil)
			request = request.WithContext(authctx.WithClaims(request.Context(), authctx.Claims{OrgID: teamScopeReachOrgID}))
			newWorkUnitsGetHandler(reader)(httptest.NewRecorder(), request)
		}},
		{"POST /api/v1/work-units", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			reader, err := investmentexplain.NewReader(client)
			if err != nil {
				t.Fatalf("investmentexplain.NewReader: %v", err)
			}
			body, err := json.Marshal(map[string]any{
				"filters": map[string]any{"scope": map[string]any{"level": scopeLevel, "ids": scopeIDs}},
			})
			if err != nil {
				t.Fatalf("marshal body: %v", err)
			}
			request := httptest.NewRequest(http.MethodPost, "/api/v1/work-units", bytes.NewReader(body))
			request = request.WithContext(authctx.WithClaims(request.Context(), authctx.Claims{OrgID: teamScopeReachOrgID}))
			newWorkUnitsPostHandler(reader)(httptest.NewRecorder(), request)
		}},
		{"POST /api/v1/investment/explain", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			reader, err := investmentexplain.NewReader(client)
			if err != nil {
				t.Fatalf("investmentexplain.NewReader: %v", err)
			}
			// The route's own request-to-options translation, then the same
			// reader call the handler makes. The provider is "mock", which
			// IsLLMAvailable answers for without credentials and which reaches
			// no external service, so what runs here is the ClickHouse side:
			// the breakdown read AND the work-unit read behind one response.
			body := investmentExplainRequestBody{Filters: map[string]any{
				"scope": map[string]any{"level": scopeLevel, "ids": anyStrings(scopeIDs)},
			}}
			opts, err := buildExplainOptions(context.Background(), reader, teamScopeReachOrgID, body, "mock", true)
			if err != nil {
				t.Fatalf("buildExplainOptions: %v", err)
			}
			available := func(_ context.Context, requestedProvider, orgID string) bool {
				return investmentexplain.IsLLMAvailable(requestedProvider, orgID)
			}
			_, _ = reader.ExplainInvestmentMix(context.Background(), nil, available,
				investmentexplain.CompleteInvestmentMixExplanation, opts)
		}},
		{"POST /api/v1/work-units/{work_unit_id}/explain", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			reader, err := investmentexplain.NewReader(client)
			if err != nil {
				t.Fatalf("investmentexplain.NewReader: %v", err)
			}
			query := url.Values{"scope_type": {scopeLevel}, "llm_provider": {"mock"}}
			if len(scopeIDs) > 0 {
				query.Set("scope_id", scopeIDs[0])
			}
			const workUnitID = "wu-ABC-123"
			request := httptest.NewRequest(http.MethodPost,
				"/api/v1/work-units/"+workUnitID+"/explain?"+query.Encode(), nil)
			request.SetPathValue("work_unit_id", workUnitID)
			request = request.WithContext(authctx.WithClaims(request.Context(), authctx.Claims{OrgID: teamScopeReachOrgID}))
			newWorkUnitExplainHandler(reader, nil, nil)(httptest.NewRecorder(), request)
		}},
		{"GET/POST /api/v1/opportunities", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			filters := home.DefaultFilters()
			filters.Scope.Level = scopeLevel
			filters.Scope.IDs = scopeIDs
			_, _ = opportunities.BuildResponse(context.Background(), client, teamScopeReachOrgID, filters, end)
		}},
		{"GET/POST /api/v1/home", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			filters := home.DefaultFilters()
			filters.Scope.Level = scopeLevel
			filters.Scope.IDs = scopeIDs
			_, _ = home.BuildResponse(context.Background(), client, nil, teamScopeReachOrgID, filters, end)
		}},
		{"GET/POST /api/v1/explain", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			reader, err := explain.NewReader(client)
			if err != nil {
				t.Fatalf("explain.NewReader: %v", err)
			}
			_, _ = explain.BuildExplainResponse(context.Background(), reader, teamScopeReachOrgID, explain.Params{
				Metric: "review_latency", StartDay: start, EndDay: end,
				CompareStart: start.AddDate(0, -1, 0), CompareEnd: start,
				ScopeLevel: scopeLevel, ScopeIDs: scopeIDs,
			})
		}},
		{"GET /api/v1/heatmap", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			scopeID := ""
			if len(scopeIDs) > 0 {
				scopeID = scopeIDs[0]
			}
			_, _ = heatmap.BuildResponse(context.Background(), client, teamScopeReachOrgID, heatmap.Params{
				Type: "temporal_load", Metric: "review_wait_density", ScopeType: scopeLevel, ScopeID: scopeID, RangeDays: 30,
			})
		}},
		{"GET/POST /api/v1/sankey", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			_, _ = sankey.BuildResponse(context.Background(), client, teamScopeReachOrgID, sankey.Params{
				Mode: "investment", ScopeLevel: scopeLevel, ScopeIDs: scopeIDs, StartDay: start, EndDay: end,
			})
		}},
		{"POST /api/v1/investment/flow", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			_, _ = investmentflow.BuildFlowResponse(context.Background(), client, investmentflow.Params{
				OrgID: teamScopeReachOrgID, StartTS: start, EndTS: end,
				ScopeLevel: scopeLevel, ScopeIDs: scopeIDs, TopNRepos: 12,
			})
		}},
		{"POST /api/v1/investment/flow/repo-team", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			_, _ = investmentflow.BuildRepoTeamFlowResponse(context.Background(), client, investmentflow.RepoTeamParams{
				OrgID: teamScopeReachOrgID, StartTS: start, EndTS: end,
				ScopeLevel: scopeLevel, ScopeIDs: scopeIDs,
			})
		}},
		{"GET/POST /api/v1/drilldown/prs", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			reader, err := drilldown.NewReader(client)
			if err != nil {
				t.Fatalf("drilldown.NewReader: %v", err)
			}
			_, _ = drilldown.BuildPRsResponse(context.Background(), reader, teamScopeReachOrgID, drilldown.PRParams{
				StartDay: start, EndDay: end, ScopeLevel: scopeLevel, ScopeIDs: scopeIDs, Limit: 50,
			})
		}},
		{"GET/POST /api/v1/investment", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			reader, err := investment.NewReader(client)
			if err != nil {
				t.Fatalf("investment.NewReader: %v", err)
			}
			_, _ = investment.BuildResponse(context.Background(), reader, teamScopeReachOrgID, investment.Params{
				OrgID: teamScopeReachOrgID, StartTS: start, EndTS: end,
				ScopeLevel: scopeLevel, ScopeIDs: scopeIDs,
			})
		}},
		{"GET /api/v1/investment/sunburst", func(t *testing.T, client *teamScopeReachClient, scopeLevel string, scopeIDs []string) {
			reader, err := investment.NewReader(client)
			if err != nil {
				t.Fatalf("investment.NewReader: %v", err)
			}
			_, _ = investment.BuildSunburstResponse(context.Background(), reader, teamScopeReachOrgID, investment.SunburstParams{
				OrgID: teamScopeReachOrgID, StartTS: start, EndTS: end,
				ScopeLevel: scopeLevel, ScopeIDs: scopeIDs, Limit: 100,
			})
		}},
	}
}

func anyStrings(values []string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

// TestTeamScopeReachesTheSharedCondition drives every consumer with a team
// scope and requires the shared condition, with all three of its bindings,
// in a statement it actually sent.
func TestTeamScopeReachesTheSharedCondition(t *testing.T) {
	for _, consumer := range teamScopeConsumers() {
		t.Run(consumer.route, func(t *testing.T) {
			client := &teamScopeReachClient{}
			consumer.drive(t, client, "team", []string{teamScopeReachTeam})
			if !client.sawCondition() {
				t.Fatalf("%s emitted no statement carrying the shared team-ownership condition with its %s/%s/%s bindings -- a team-scoped request that skips it answers over the whole organization.\nstatements: %v",
					consumer.route, teamscope.BindingOrgID, teamscope.BindingTeamIDs, teamscope.BindingAsOf, client.statements)
			}
		})
	}
}

// TestOrgScopeCarriesNoTeamCondition is the other half: an org-scoped
// request must carry no trace of the condition, so the assertion above
// cannot be satisfied by splicing it in unconditionally.
func TestOrgScopeCarriesNoTeamCondition(t *testing.T) {
	for _, consumer := range teamScopeConsumers() {
		t.Run(consumer.route, func(t *testing.T) {
			client := &teamScopeReachClient{}
			consumer.drive(t, client, "org", nil)
			if client.sawAnyConditionTrace() {
				t.Fatalf("%s carried the team-ownership condition (or one of its bindings) for an ORG-scoped request:\nstatements: %v",
					consumer.route, client.statements)
			}
		})
	}
}
