package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

func TestExperiments_OrgDecisionTable(t *testing.T) {
	cases := []struct {
		name    string
		claims  *authctx.Claims
		orgArg  string
		wantErr bool
	}{
		{"own org", &authctx.Claims{OrgID: "org-1"}, "org-1", false},
		{"other org", &authctx.Claims{OrgID: "org-1"}, "org-2", true},
		{"empty argument", &authctx.Claims{OrgID: "org-1"}, "", true},
		{"empty claim org", &authctx.Claims{OrgID: ""}, "", true},
		{"no envelope", nil, "org-1", true},
		{"superuser of another org", &authctx.Claims{OrgID: "org-1", IsSuperuser: true}, "org-2", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ch := &syncOrgCH{}
			r := &Resolver{ClickHouse: ch}
			ctx := context.Background()
			if tc.claims != nil {
				ctx = authctx.WithClaims(ctx, *tc.claims)
			}
			res, err := r.Query().Experiments(ctx, tc.orgArg, nil)
			if tc.wantErr {
				asAuthorizationError(t, err)
				if ch.seen() != "" {
					t.Fatal("no read may run for a refused request")
				}
				return
			}
			if err != nil || res == nil {
				t.Fatalf("res=%v err=%v", res, err)
			}
			if ch.seen() != "org-1" {
				t.Fatalf("queried org %q", ch.seen())
			}
		})
	}
}

// The generated executor answers the field with the steady-flow card's
// experiment when the home read finds no rows.
func TestExperiments_ThroughTheGeneratedSchema(t *testing.T) {
	ch := &syncOrgCH{}
	server := handler.NewDefaultServer(NewExecutableSchema(Config{Resolvers: &Resolver{ClickHouse: ch}}))
	body, _ := json.Marshal(map[string]any{
		"query":     `query Experiments($orgId: String!, $filters: FilterInput) { experiments(orgId: $orgId, filters: $filters) { items { id opportunityId hypothesis metric owner stopCondition status startDate stopDate outcome } derivedFromOpportunities } }`,
		"variables": map[string]any{"orgId": "org-1", "filters": map[string]any{"scope": map[string]any{"level": "TEAM", "ids": []string{"t-1"}}}},
	})
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	out := rec.Body.String()
	for _, want := range []string{`"opportunityId":"opp-0"`, `"hypothesis":"Share the current playbook with new teams."`, `"metric":""`, `"status":"SUGGESTED"`, `"startDate":null`, `"outcome":null`, `"derivedFromOpportunities":true`} {
		if !strings.Contains(out, want) {
			t.Fatalf("response missing %s: %s", want, out)
		}
	}
}

// syncOrgCH records the org of a read; the home read issues its queries from
// several goroutines, so the record is guarded.
type syncOrgCH struct {
	mu  sync.Mutex
	org string
}

func (c *syncOrgCH) Query(_ context.Context, _ string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, b := range bindings {
		if b.Name == "org_id" {
			c.org, _ = b.Value.(string)
		}
	}
	return emptyRows{}, nil
}

func (c *syncOrgCH) seen() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.org
}
