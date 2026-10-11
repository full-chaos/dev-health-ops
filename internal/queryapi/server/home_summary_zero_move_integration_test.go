//go:build integration

package server

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

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

// CHAOS-9139 (D5900): the Home summary sentence of a measured move of 0 % names no driver, on both
// surfaces; a real move still names its driver. Rows as the producers write them, answers from the
// production handlers on a real ClickHouse.
func TestHomeSummaryNamesNoDriverForAZeroMove(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	ctx := context.Background()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/home", newHomeGetHandler(client, nil))
	gql := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{
		Resolvers: &graph.Resolver{ClickHouse: client, Postgres: noSyncRunPostgres{}},
	}))
	current, prior := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	seed := func(org string, priorCycle, currentCycle float64) {
		const id = "github:acme/ops"
		for _, row := range []struct {
			day   time.Time
			cycle float64
		}{{prior, priorCycle}, {current, currentCycle}} {
			if err := conn.Exec(ctx, `INSERT INTO work_item_metrics_daily
(org_id, day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day,
 cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
 wip_age_p50_hours, wip_age_p90_hours, bug_completed_ratio, story_points_completed,
 new_bugs_count, new_items_count, defect_intro_rate, wip_congestion_ratio, predictability_score, computed_at)
VALUES (?, ?, 'github', 'scope', ?, '', 1, 1, 0, 0, 1, 0, ?, ?, ?, ?, 1, 1, 0, 0, 0, 0, 0, 0.5, 0.5, ?)`,
				org, row.day, id, row.cycle, row.cycle, row.cycle, row.cycle, computedAt); err != nil {
				t.Fatal(err)
			}
		}
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, org_id, provider, is_active)
VALUES (?, generateUUIDv4(), 'Ops', [], ?, ?, 'github', 1)`, id, computedAt, org); err != nil {
			t.Fatal(err)
		}
	}
	restSentence := func(org string) string {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/home?range_days=7&compare_days=7&end_date=2026-08-25", nil)
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		var out struct {
			Summary []struct {
				Text string `json:"text"`
			} `json:"summary"`
		}
		if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &out) != nil || len(out.Summary) == 0 {
			t.Fatalf("REST: HTTP %d, %d sentences: %.200s", rec.Code, len(out.Summary), rec.Body.String())
		}
		return out.Summary[0].Text
	}
	gqlSentence := func(org string) string {
		payload, _ := json.Marshal(map[string]any{
			"query":     `query Home($orgId: String!, $window: HomeWindowInput) { home(orgId: $orgId, window: $window) { summary { text } } }`,
			"variables": map[string]any{"orgId": org, "window": map[string]any{"rangeDays": 7, "compareDays": 7, "endDate": "2026-08-25"}},
		})
		req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
		rec := httptest.NewRecorder()
		gql.ServeHTTP(rec, req)
		var out struct {
			Data struct {
				Home struct {
					Summary []struct{ Text string } `json:"summary"`
				} `json:"home"`
			} `json:"data"`
			Errors []any `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Errors) != 0 || len(out.Data.Home.Summary) == 0 {
			t.Fatalf("GraphQL: %v %.200s", err, rec.Body.String())
		}
		return out.Data.Home.Summary[0].Text
	}

	seed("zero-move", 24, 24)
	seed("real-move", 24, 48)
	for surface, sentence := range map[string]func(string) string{"REST": restSentence, "GraphQL": gqlSentence} {
		if got := sentence("zero-move"); !strings.Contains(got, "held steady 0%") || strings.Contains(got, "driven by") {
			t.Errorf("%s, a measured move of 0 %%: %q, want \"... held steady 0%%.\" with no driver", surface, got)
		}
		if got := sentence("real-move"); !strings.Contains(got, "rose 100% driven by Ops") {
			t.Errorf("%s, a real move: %q, want its driver named", surface, got)
		}
	}
}
