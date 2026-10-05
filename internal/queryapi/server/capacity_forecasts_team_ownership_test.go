package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-8727: the capacityForecasts list refuses an unowned team through the real handlers (GraphQL and MCP):
// the ownership read answers no team, so the stored-forecast table must never be read.

type listOwnershipClient struct {
	mu        sync.Mutex
	ownership int
	other     []string
}

func (c *listOwnershipClient) Query(_ context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.Contains(statement, teamscope.OwnedTeamsMarker) {
		c.ownership++
	} else {
		c.other = append(c.other, statement)
	}
	return &cdScanner{}, nil
}

const capacityForecastsListQuery = `query($orgId: String!, $filters: CapacityForecastFilterInput) { capacityForecasts(orgId: $orgId, filters: $filters) { edges { node { forecastId teamId } } totalCount } }`

func requireEmptyUnownedList(t *testing.T, what string, rec *httptest.ResponseRecorder, client *listOwnershipClient) {
	t.Helper()
	var out struct {
		Data struct {
			CapacityForecasts struct {
				Edges      []json.RawMessage `json:"edges"`
				TotalCount int               `json:"totalCount"`
			} `json:"capacityForecasts"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &out) != nil || len(out.Errors) > 0 {
		t.Fatalf("%s: status %d, body %s", what, rec.Code, rec.Body.String())
	}
	if len(out.Data.CapacityForecasts.Edges) != 0 || out.Data.CapacityForecasts.TotalCount != 0 {
		t.Fatalf("%s: an unowned team was listed: %s", what, rec.Body.String())
	}
	if client.ownership != 1 || len(client.other) != 0 {
		t.Fatalf("%s: ownership reads %d, other reads %d; want 1 and 0 (the stored forecasts must not be read)", what, client.ownership, len(client.other))
	}
}

func TestCapacityForecastsRefusesAnUnownedTeamThroughTheGraphQLHandler(t *testing.T) {
	client := &listOwnershipClient{}
	handler := newGraphQLServer(&graph.Resolver{ClickHouse: client})
	body, _ := json.Marshal(map[string]any{"query": capacityForecastsListQuery, "variables": map[string]any{"orgId": "org-7", "filters": map[string]any{"teamId": "team-noown", "limit": 10}}})
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-7"}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	requireEmptyUnownedList(t, "graphql", rec, client)
}

func TestCapacityForecastsRefusesAnUnownedTeamThroughTheMCPHandler(t *testing.T) {
	client := &listOwnershipClient{}
	handler := internalidentity.MCP(newMCPHandlerWithLimits(client, nil, allMCPRootsEnabled(), getenvFunc(func(string) string { return "" }), mcpDefaultLimits()))
	rec := mcpDo(handler, http.MethodPost, validMCPHeaders(), mcpBody(t, capacityForecastsListQuery,
		map[string]any{"orgId": mcpTestOrg, "filters": map[string]any{"teamId": "team-noown", "limit": 10}}))
	requireEmptyUnownedList(t, "mcp", rec, client)
}
