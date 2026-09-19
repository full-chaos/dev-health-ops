package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
)

func TestDataHealth_OperatorGateAndOrgDecisionTable(t *testing.T) {
	type principal struct {
		name   string
		claims *authctx.Claims
		// wantMsg is the refusal message, empty when the request is served.
		wantMsg string
	}
	withOrg := func(role string, superuser bool, org string) *authctx.Claims {
		return &authctx.Claims{OrgID: org, Role: role, IsSuperuser: superuser}
	}
	const noOperator = "Data health requires operator access"
	const noOrg = "org_id is required for all analytics queries"
	cases := []principal{
		{"envelope absent", nil, "Authentication required"},
		{"role empty", withOrg("", false, "org-1"), noOperator},
		{"role member", withOrg("member", false, "org-1"), noOperator},
		{"role viewer", withOrg("viewer", false, "org-1"), noOperator},
		{"role operator", withOrg("operator", false, "org-1"), ""},
		{"role admin", withOrg("admin", false, "org-1"), ""},
		{"role owner", withOrg("owner", false, "org-1"), ""},
		{"role upper-case", withOrg("OWNER", false, "org-1"), ""},
		{"superuser flag, member role", withOrg("member", true, "org-1"), ""},
		{"superuser flag only", withOrg("", true, "org-1"), ""},
		{"operator, org empty", withOrg("operator", false, ""), noOrg},
		{"superuser, org empty", withOrg("", true, ""), noOrg},
		{"not operator, org empty", withOrg("member", false, ""), noOperator},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ch := &orgCapturingCHClient{}
			r := &Resolver{ClickHouse: ch}
			ctx := context.Background()
			if tc.claims != nil {
				ctx = authctx.WithClaims(ctx, *tc.claims)
			}
			res, err := r.Query().DataHealth(ctx, "team-a")
			if tc.wantMsg != "" {
				gqlErr := asAuthorizationError(t, err)
				if gqlErr.Message != tc.wantMsg {
					t.Fatalf("message = %q, want %q", gqlErr.Message, tc.wantMsg)
				}
				if ch.org != "" {
					t.Fatal("no read may run for a refused request")
				}
				return
			}
			if err != nil || res == nil {
				t.Fatalf("res=%v err=%v", res, err)
			}
			if res.Team != "team-a" || ch.org != tc.claims.OrgID {
				t.Fatalf("team=%q org queried=%q, want team-a / %q", res.Team, ch.org, tc.claims.OrgID)
			}
		})
	}
}

func TestDataHealth_MetricLineageNeedsAnAuthorizedOrg(t *testing.T) {
	ch := &orgCapturingCHClient{}
	r := &Resolver{ClickHouse: ch}
	_, err := r.DataHealth().MetricLineage(context.Background(), nil, "throughput")
	asAuthorizationError(t, err)
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
	if _, err := r.DataHealth().MetricLineage(ctx, nil, "throughput"); err != nil || ch.org != "org-1" {
		t.Fatalf("err=%v org=%q", err, ch.org)
	}
	ch.org = ""
	if got, err := r.DataHealth().MetricLineage(ctx, nil, "not-a-metric"); err != nil || got != nil || ch.org != "" {
		t.Fatalf("an unknown metric answers null without a read: %v %v %q", got, err, ch.org)
	}
}

// TestDataHealth_ThroughTheGeneratedSchema executes the field and its nested
// metricLineage resolver through the generated executor, the path a real
// request takes, so the generated wiring (the field resolver receiving the
// parent's team) is exercised and not just the resolver methods.
func TestDataHealth_ThroughTheGeneratedSchema(t *testing.T) {
	ch := &lineageCHClient{}
	server := handler.NewDefaultServer(NewExecutableSchema(Config{Resolvers: &Resolver{ClickHouse: ch}}))

	post := func(claims *authctx.Claims, query string) string {
		body, _ := json.Marshal(map[string]any{"query": query})
		req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if claims != nil {
			req = req.WithContext(authctx.WithClaims(req.Context(), *claims))
		}
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		return rec.Body.String()
	}
	const query = `{ dataHealth(team: "ALL") { connectors { provider } metricLineage(metricId: "throughput") { metricId sourceTables computeWindow { kind durationDays } rowCount } } }`

	operator := &authctx.Claims{OrgID: "org-1", Role: "operator"}
	out := post(operator, query)
	for _, want := range []string{`"connectors":[]`, `"metricId":"throughput"`, `"sourceTables":["work_item_metrics_daily"]`, `"kind":"daily"`, `"durationDays":null`, `"rowCount":7`} {
		if !strings.Contains(out, want) {
			t.Fatalf("response missing %s: %s", want, out)
		}
	}
	if ch.org != "org-1" {
		t.Fatalf("lineage read org %q", ch.org)
	}

	denied := post(&authctx.Claims{OrgID: "org-1", Role: "member"}, query)
	if !strings.Contains(denied, "Data health requires operator access") || !strings.Contains(denied, "AUTHORIZATION_ERROR") || strings.Contains(denied, `"provider"`) {
		t.Fatalf("a non-operator must be refused: %s", denied)
	}
	anonymous := post(nil, query)
	if !strings.Contains(anonymous, "Authentication required") {
		t.Fatalf("no envelope must be refused: %s", anonymous)
	}
}

type lineageCHClient struct{ org string }

func (c *lineageCHClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	for _, b := range bindings {
		if b.Name == "org_id" {
			c.org, _ = b.Value.(string)
		}
	}
	if strings.Contains(statement, "FROM work_item_metrics_daily") {
		return &lineageRows{}, nil
	}
	return emptyRows{}, nil
}

type lineageRows struct{ done bool }

func (r *lineageRows) Next() bool { defer func() { r.done = true }(); return !r.done }
func (r *lineageRows) Scan(dest ...any) error {
	t := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	*(dest[0].(**time.Time)) = &t
	*(dest[1].(*uint64)) = 7
	return nil
}
func (r *lineageRows) Err() error   { return nil }
func (r *lineageRows) Close() error { return nil }
