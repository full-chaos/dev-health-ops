package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

type countingGraphQLStore struct{ calls int }

func (s *countingGraphQLStore) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	s.calls++
	return nil, fmt.Errorf("countingGraphQLStore: reached")
}

// A compoundingRisk repoIds list over 500 entries goes through the gqlgen
// executor, the resolver's %w wrap and safeGraphQLErrorPresenter: the client
// gets the BAD_USER_INPUT refusal with its message and extensions, and the
// store is never asked.
func TestGraphQLCompoundingRiskRepoIdsOver500IsServedAsBadUserInput(t *testing.T) {
	store := &countingGraphQLStore{}
	gql := newGraphQLServer(&graph.Resolver{ClickHouse: store})
	ids := make([]string, 501)
	for i := range ids {
		ids[i] = fmt.Sprintf("r-%03d", i)
	}
	body, err := json.Marshal(map[string]any{
		"query":     `query Q($ids: [String!]) { compoundingRisk(orgId: "org-1", filter: {breakout: REPO, repoIds: $ids}) { orgId } }`,
		"variables": map[string]any{"ids": ids},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1", Scope: pepgate.Legacy("org-1")}))
	rec := httptest.NewRecorder()
	gql.ServeHTTP(rec, req)
	var decoded struct {
		Errors []struct {
			Message    string         `json:"message"`
			Extensions map[string]any `json:"extensions"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil || len(decoded.Errors) != 1 {
		t.Fatalf("want one error, got: %s", rec.Body.String())
	}
	got := decoded.Errors[0]
	want := "compoundingRisk: filter.repoIds holds 501 entries; the limit is 500"
	if got.Message != want || got.Extensions["code"] != "BAD_USER_INPUT" || got.Extensions["field"] != "filter.repoIds" || got.Extensions["cap"] != float64(500) {
		t.Errorf("served %q %v; want %q with code BAD_USER_INPUT, field filter.repoIds, cap 500", got.Message, got.Extensions, want)
	}
	if store.calls != 0 {
		t.Errorf("%d store statements; want 0", store.calls)
	}
}
