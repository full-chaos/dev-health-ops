package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

func telemetryInput(start, end string) model.ProductTelemetryDashboardInput {
	s, _ := time.Parse("2006-01-02", start)
	e, _ := time.Parse("2006-01-02", end)
	return model.ProductTelemetryDashboardInput{StartDate: graphqldate.New(s), EndDate: graphqldate.New(e)}
}

func TestProductTelemetryDashboard_OrgAndRangeDecisionTable(t *testing.T) {
	cases := []struct {
		name       string
		claims     *authctx.Claims
		orgArg     string
		start, end string
		wantAuth   bool
		wantErr    string
	}{
		{"own org", &authctx.Claims{OrgID: "org-1"}, "org-1", "2026-01-01", "2026-01-08", false, ""},
		{"same day range", &authctx.Claims{OrgID: "org-1"}, "org-1", "2026-01-01", "2026-01-01", false, ""},
		{"other org", &authctx.Claims{OrgID: "org-1"}, "org-2", "2026-01-01", "2026-01-08", true, ""},
		{"empty argument", &authctx.Claims{OrgID: "org-1"}, "", "2026-01-01", "2026-01-08", true, ""},
		{"no envelope", nil, "org-1", "2026-01-01", "2026-01-08", true, ""},
		{"superuser of another org", &authctx.Claims{OrgID: "org-1", IsSuperuser: true}, "org-2", "2026-01-01", "2026-01-08", true, ""},
		{"start after end", &authctx.Claims{OrgID: "org-1"}, "org-1", "2026-01-09", "2026-01-08", false, "start_date must be before or equal to end_date"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ch := &orgCapturingCHClient{}
			r := &Resolver{ClickHouse: ch}
			ctx := context.Background()
			if tc.claims != nil {
				ctx = authctx.WithClaims(ctx, *tc.claims)
			}
			res, err := r.Query().ProductTelemetryDashboard(ctx, tc.orgArg, telemetryInput(tc.start, tc.end))
			switch {
			case tc.wantAuth:
				asAuthorizationError(t, err)
				fallthrough
			case tc.wantErr != "":
				if tc.wantErr != "" && (err == nil || err.Error() != tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				if res != nil {
					t.Fatal("a refused request answers nothing")
				}
			default:
				if err != nil || res == nil {
					t.Fatalf("res=%v err=%v", res, err)
				}
			}
		})
	}
}

// countingCH counts reads; with topRows it answers one top-organisation row.
type countingCH struct {
	n       int
	topRows bool
}

func (c *countingCH) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.n++
	if c.topRows && strings.Contains(statement, "GROUP BY org_id_hash") {
		return &oneTopOrgRow{}, nil
	}
	return emptyRows{}, nil
}

type oneTopOrgRow struct{ done bool }

func (r *oneTopOrgRow) Next() bool { defer func() { r.done = true }(); return !r.done }
func (r *oneTopOrgRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = "hash"
	*(dest[1].(*uint64)) = 1
	*(dest[2].(*uint64)) = 1
	*(dest[3].(*uint64)) = 1
	return nil
}
func (r *oneTopOrgRow) Err() error   { return nil }
func (r *oneTopOrgRow) Close() error { return nil }

func TestProductTelemetryPlatformDashboard_GateDecisionTable(t *testing.T) {
	cases := []struct {
		name    string
		claims  *authctx.Claims
		wantMsg string
	}{
		{"no envelope", nil, "Authentication required"},
		{"org member", &authctx.Claims{OrgID: "org-1", Role: "owner"}, "Platform admin access required"},
		{"superuser", &authctx.Claims{OrgID: "org-1", IsSuperuser: true}, ""},
		{"superuser without org", &authctx.Claims{IsSuperuser: true}, ""},
		{"superuser impersonating", &authctx.Claims{OrgID: "org-1", IsSuperuser: true, ImpersonationActive: true}, "Platform admin access required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ch := &countingCH{}
			r := &Resolver{ClickHouse: ch}
			ctx := context.Background()
			if tc.claims != nil {
				ctx = authctx.WithClaims(ctx, *tc.claims)
			}
			res, err := r.Query().ProductTelemetryPlatformDashboard(ctx, telemetryInput("2026-01-01", "2026-01-08"))
			if tc.wantMsg != "" {
				if gqlErr := asAuthorizationError(t, err); gqlErr.Message != tc.wantMsg {
					t.Fatalf("message %q, want %q", gqlErr.Message, tc.wantMsg)
				}
				if ch.n != 0 {
					t.Fatal("no read may run for a refused request")
				}
				return
			}
			if err != nil || res == nil || res.Totals == nil || res.SessionSummary == nil {
				t.Fatalf("res=%v err=%v", res, err)
			}
		})
	}
}

func TestProductTelemetryPlatformDashboard_RangeAndOrgNames(t *testing.T) {
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{IsSuperuser: true})
	r := &Resolver{ClickHouse: &countingCH{}}
	if _, err := r.Query().ProductTelemetryPlatformDashboard(ctx, telemetryInput("2026-01-09", "2026-01-08")); err == nil || !strings.Contains(err.Error(), "start_date must be before or equal to end_date") {
		t.Fatalf("err = %v", err)
	}
	// Top organisations present and no Postgres reader: the read fails rather
	// than answering unnamed rows.
	r = &Resolver{ClickHouse: &countingCH{topRows: true}}
	if _, err := r.Query().ProductTelemetryPlatformDashboard(ctx, telemetryInput("2026-01-01", "2026-01-08")); err == nil {
		t.Fatal("a missing organizations reader must fail the read")
	}
	r = &Resolver{ClickHouse: &countingCH{topRows: true}, Postgres: erroringPG{}}
	if _, err := r.Query().ProductTelemetryPlatformDashboard(ctx, telemetryInput("2026-01-01", "2026-01-08")); err == nil {
		t.Fatal("an organizations read failure must fail the read")
	}
}

type erroringPG struct{}

func (erroringPG) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("boom")
}

func TestProductTelemetry_ThroughTheGeneratedSchema(t *testing.T) {
	server := handler.NewDefaultServer(NewExecutableSchema(Config{Resolvers: &Resolver{ClickHouse: &countingCH{}}}))
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
	const org = `{ productTelemetryDashboard(orgId: "org-1", input: {startDate: "2026-01-01", endDate: "2026-01-08"}) { dailyActiveUsers { day } topRoutes { routePattern } sessionSummary { p50DurationMs avgPagesViewed } } }`
	out := post(&authctx.Claims{OrgID: "org-1"}, org)
	for _, want := range []string{`"dailyActiveUsers":[]`, `"topRoutes":[]`, `"p50DurationMs":null`, `"avgPagesViewed":null`} {
		if !strings.Contains(out, want) {
			t.Fatalf("org response missing %s: %s", want, out)
		}
	}
	const platform = `{ productTelemetryPlatformDashboard(input: {startDate: "2026-01-01", endDate: "2026-01-08"}) { totals { activeOrgs events } topOrgs { orgIdHash } } }`
	out = post(&authctx.Claims{IsSuperuser: true}, platform)
	for _, want := range []string{`"activeOrgs":0`, `"events":0`, `"topOrgs":[]`} {
		if !strings.Contains(out, want) {
			t.Fatalf("platform response missing %s: %s", want, out)
		}
	}
	if out = post(&authctx.Claims{OrgID: "org-1"}, platform); !strings.Contains(out, "Platform admin access required") || !strings.Contains(out, "AUTHORIZATION_ERROR") {
		t.Fatalf("a non-superuser must be refused: %s", out)
	}
}
