//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

// The Home prose that names the requested scope names it by display name, never
// by id (CHAOS-9116): health_state.headline ("... across <scope>") and every
// signals[].affected_scope, on both surfaces (REST and GraphQL), for a team and
// for a repository request. A scope with no name, and one whose name is its own
// id, is "the selected team" / "the selected repository"; a recommendation of a
// team with no name says "a team". Rows are what the producers write, the
// answer is the production handler on a real ClickHouse.
var scopeIDShape = regexp.MustCompile(`(?i)\b(github|gitlab|jira|linear|custom):[^\s,.]+|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

type homeProse struct {
	Headline string
	Scopes   []string
}

func TestHomeProseNamesTheRequestedScopeAndNeverItsID(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	ctx := context.Background()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/home", newHomeGetHandler(client, nil))
	gql := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{
		Resolvers: &graph.Resolver{ClickHouse: client, Postgres: noSyncRunPostgres{}},
	}))
	const target = "/api/v1/home?range_days=7&compare_days=7&end_date=2026-08-25"
	current, prior := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	seedTeam := func(org, teamID, name string) {
		for _, row := range []struct {
			day   time.Time
			cycle float64
		}{{prior, 24}, {current, 48}} {
			exec(`INSERT INTO work_item_metrics_daily
(org_id, day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day,
 cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
 wip_age_p50_hours, wip_age_p90_hours, bug_completed_ratio, story_points_completed,
 new_bugs_count, new_items_count, defect_intro_rate, wip_congestion_ratio, predictability_score, computed_at)
VALUES (?, ?, 'github', 'scope', ?, '', 1, 1, 0, 0, 1, 0, ?, ?, ?, ?, 1, 1, 0, 0, 0, 0, 0, 0.5, 0.5, ?)`,
				org, row.day, teamID, row.cycle, row.cycle, row.cycle, row.cycle, computedAt)
		}
		if name != "" {
			exec(`INSERT INTO teams (id, team_uuid, name, members, repo_patterns, updated_at, org_id, provider, is_active)
VALUES (?, generateUUIDv4(), ?, [], [], ?, ?, 'github', 1)`, teamID, name, computedAt, org)
		}
		// a persisted recommendation of the team: its signal has an affected scope too
		exec(`INSERT INTO recommendations_daily
(team_id, org_id, rule_id, window_start, window_end, fired, severity, title, rationale, success_criterion, evidence_json, computed_at)
VALUES (?, ?, 'wip-saturation', '2026-08-18', '2026-08-25', true, 'warning', 'WIP is elevated', 'Rationale', 'Success', '[]', ?)`,
			teamID, org, computedAt)
	}
	seedRepo := func(org string, repo uuid.UUID, name string) {
		for day, churn := range map[time.Time]uint32{prior: 10, current: 30} {
			exec(`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`, repo, day, churn, computedAt, org)
		}
		if name != "" {
			exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES (?, ?, ?, ?, ?, 'github')`, repo, name, computedAt, computedAt, org)
		}
	}

	restProse := func(org, query string) homeProse {
		t.Helper()
		var body struct {
			HealthState struct {
				Headline string `json:"headline"`
			} `json:"health_state"`
			Signals []struct {
				AffectedScope string `json:"affected_scope"`
			} `json:"signals"`
		}
		getJSON(t, mux, org, target+query, &body)
		out := homeProse{Headline: body.HealthState.Headline}
		for _, s := range body.Signals {
			out.Scopes = append(out.Scopes, s.AffectedScope)
		}
		return out
	}
	gqlProse := func(org, level string, ids ...string) homeProse {
		t.Helper()
		query := `query Home($orgId: String!, $filters: FilterInput, $window: HomeWindowInput) { home(orgId: $orgId, filters: $filters, window: $window) { healthState { headline } signals { affectedScope } } }`
		payload, err := json.Marshal(map[string]any{"query": query, "variables": map[string]any{
			"orgId": org, "filters": map[string]any{"scope": map[string]any{"level": level, "ids": ids}},
			"window": map[string]any{"rangeDays": 7, "compareDays": 7, "endDate": "2026-08-25"},
		}})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
		rec := httptest.NewRecorder()
		gql.ServeHTTP(rec, req)
		var out struct {
			Data struct {
				Home struct {
					HealthState struct{ Headline string } `json:"healthState"`
					Signals     []struct {
						AffectedScope string `json:"affectedScope"`
					} `json:"signals"`
				} `json:"home"`
			} `json:"data"`
			Errors []any `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Errors) != 0 {
			t.Fatalf("GraphQL Home: %v %s", err, rec.Body.String())
		}
		prose := homeProse{Headline: out.Data.Home.HealthState.Headline}
		for _, s := range out.Data.Home.Signals {
			prose.Scopes = append(prose.Scopes, s.AffectedScope)
		}
		return prose
	}
	check := func(t *testing.T, surface string, prose homeProse, id, want string) {
		t.Helper()
		if len(prose.Scopes) == 0 {
			t.Fatalf("%s: no signal in the answer: %+v", surface, prose)
		}
		all := append([]string{prose.Headline}, prose.Scopes...)
		for _, text := range all {
			if loc := scopeIDShape.FindString(text); loc != "" || strings.Contains(text, id) {
				t.Errorf("%s: prose prints an id (%q): %q", surface, id, text)
			}
		}
		if !strings.Contains(prose.Headline, want) {
			t.Errorf("%s: headline %q lacks %q", surface, prose.Headline, want)
		}
		sawWant := false
		for _, scope := range prose.Scopes {
			if scope == want || strings.Contains(scope, want) {
				sawWant = true
			}
		}
		if want != "the selected team" && contains(prose.Scopes, "a team") {
			t.Errorf("%s: a recommendation of a team that has a name says a team: %v", surface, prose.Scopes)
		}
		if !sawWant {
			t.Errorf("%s: no signal has the affected scope %q: %v", surface, want, prose.Scopes)
		}
	}

	t.Run("team with a name", func(t *testing.T) {
		const org, id = "scope-labels-team-named", "github:acme/ops"
		seedTeam(org, id, "Ops")
		check(t, "REST", restProse(org, "&scope_type=team&scope_id="+id), id, "Ops")
		check(t, "GraphQL", gqlProse(org, "TEAM", id), id, "Ops")
	})
	t.Run("team with no name: the selected team, and a recommendation says a team", func(t *testing.T) {
		const org, id = "scope-labels-team-unnamed", "linear:ENG"
		seedTeam(org, id, "")
		check(t, "REST", restProse(org, "&scope_type=team&scope_id="+id), id, "the selected team")
		check(t, "GraphQL", gqlProse(org, "TEAM", id), id, "the selected team")
		if prose := restProse(org, "&scope_type=team&scope_id="+id); !contains(prose.Scopes, "a team") {
			t.Errorf("the recommendation signal of a team with no name must say a team: %v", prose.Scopes)
		}
	})
	t.Run("team named by its own id: no name", func(t *testing.T) {
		const org, id = "scope-labels-team-same", "jira:abc"
		seedTeam(org, id, "jira:abc")
		check(t, "REST", restProse(org, "&scope_type=team&scope_id="+id), id, "the selected team")
		check(t, "GraphQL", gqlProse(org, "TEAM", id), id, "the selected team")
	})
	t.Run("two teams, one named: the other is counted, not hidden", func(t *testing.T) {
		const org, named, unnamed = "scope-labels-team-mixed", "github:acme/ops", "linear:ENG"
		seedTeam(org, named, "Ops")
		seedTeam(org, unnamed, "")
		prose := gqlProse(org, "TEAM", named, unnamed)
		if want := "across Ops and 1 other team"; !strings.Contains(prose.Headline, want) {
			t.Errorf("headline %q lacks %q", prose.Headline, want)
		}
		for _, text := range append([]string{prose.Headline}, prose.Scopes...) {
			if strings.Contains(text, named) || strings.Contains(text, unnamed) {
				t.Errorf("prose prints an id: %q", text)
			}
		}
	})
	t.Run("repository with a name", func(t *testing.T) {
		const org = "scope-labels-repo-named"
		repo := uuid.New()
		seedRepo(org, repo, "acme/checkout")
		check(t, "REST", restProse(org, "&scope_type=repo&scope_id="+repo.String()), repo.String(), "acme/checkout")
		check(t, "GraphQL", gqlProse(org, "REPO", repo.String()), repo.String(), "acme/checkout")
	})
	t.Run("repository with no name: the selected repository", func(t *testing.T) {
		const org = "scope-labels-repo-unnamed"
		repo := uuid.New()
		seedRepo(org, repo, "")
		check(t, "REST", restProse(org, "&scope_type=repo&scope_id="+repo.String()), repo.String(), "the selected repository")
		check(t, "GraphQL", gqlProse(org, "REPO", repo.String()), repo.String(), "the selected repository")
	})
}
