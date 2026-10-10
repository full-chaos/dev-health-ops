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

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// The whole served Home JSON is walked for every seeded id (CHAOS-9116): an id
// may appear only in a field that is an id or a link, never in any other string.
// Rows are written the way the producers write them; the answer is the
// production handler (REST GET and POST) on a real ClickHouse. A team named by
// its own id, a team with no name, a repository with no name, a risk row of
// each, and a duplicate id in the scope list are all seeded.
var homeIDFields = map[string]bool{
	"id": true, "link": true, "evidence_link": true, "evidence_ref": true, "href": true, "url": true,
}

func walkHomeStrings(path string, key string, value any, visit func(path, key, text string)) {
	switch v := value.(type) {
	case string:
		visit(path, key, v)
	case map[string]any:
		for k, child := range v {
			walkHomeStrings(path+"."+k, k, child, visit)
		}
	case []any:
		for i, child := range v {
			walkHomeStrings(path+"[]", key+"#"+string(rune('0'+i%10)), child, visit)
		}
	}
}

func TestHomeServesNoSeededIDInAnyProseField(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	ctx := context.Background()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/home", newHomeGetHandler(client, nil))
	mux.HandleFunc("POST /api/v1/home", newHomePostHandler(client, nil))
	current, prior := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	seedTeam := func(org, id, name string) {
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
				org, row.day, id, row.cycle, row.cycle, row.cycle, row.cycle, computedAt)
		}
		if name != "" {
			exec(`INSERT INTO teams (id, team_uuid, name, members, repo_patterns, updated_at, org_id, provider, is_active)
VALUES (?, generateUUIDv4(), ?, [], [], ?, ?, 'github', 1)`, id, name, computedAt, org)
		}
		exec(`INSERT INTO recommendations_daily
(team_id, org_id, rule_id, window_start, window_end, fired, severity, title, rationale, success_criterion, evidence_json, computed_at)
VALUES (?, ?, 'wip-saturation', '2026-08-18', '2026-08-25', true, 'warning', 'WIP is elevated', 'Rationale', 'Success', '[]', ?)`,
			id, org, computedAt)
	}
	seedRisk := func(org, scope, id string) {
		exec(`INSERT INTO compounding_risk_daily
(org_id, day, scope, scope_id, compounding_risk, severity, churn_norm, complexity_norm, ownership_norm, review_norm,
 w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at)
SELECT ?, toDate('2026-08-24'), ?, ?, 0.8, 'high', 0.8, 0.8, NULL, NULL, 0.3, 0.3, 0.2, 0.2, 0.4, 0.65, now()`, org, scope, id)
	}
	seedRepo := func(org string, repo uuid.UUID, name string) {
		for day, churn := range map[time.Time]uint32{prior: 10, current: 30} {
			exec(`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`, repo, day, churn, computedAt, org)
		}
		if name != "" {
			exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES (?, ?, ?, ?, ?, 'github')`, repo, name, computedAt, computedAt, org)
		}
	}

	fetch := func(t *testing.T, org string, post bool, level string, ids []string) any {
		t.Helper()
		var req *http.Request
		const base = "/api/v1/home?range_days=7&compare_days=7&end_date=2026-08-25"
		if post {
			body, _ := json.Marshal(map[string]any{"filters": map[string]any{
				"scope": map[string]any{"level": level, "ids": ids},
				"time":  map[string]any{"range_days": 7, "compare_days": 7, "end_date": "2026-08-25"},
			}})
			req = httptest.NewRequest(http.MethodPost, "/api/v1/home", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
		} else {
			target := base
			if level != "org" {
				target += "&scope_type=" + level + "&scope_id=" + ids[0]
			}
			req = httptest.NewRequest(http.MethodGet, target, nil)
		}
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("HTTP %d: %s", rec.Code, rec.Body.String())
		}
		var out any
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	noID := func(t *testing.T, surface string, doc any, seeded []string) {
		t.Helper()
		walkHomeStrings("$", "", doc, func(path, key, text string) {
			field := key
			if i := strings.Index(field, "#"); i >= 0 {
				field = field[:i]
			}
			if homeIDFields[field] || strings.HasPrefix(text, "/") {
				return
			}
			for _, id := range seeded {
				if strings.Contains(strings.ToLower(text), strings.ToLower(id)) {
					t.Errorf("%s: %s serves the id %q in prose: %q", surface, path, id, text)
				}
			}
		})
	}
	prose := func(doc any) string {
		m, _ := doc.(map[string]any)
		hs, _ := m["health_state"].(map[string]any)
		h, _ := hs["headline"].(string)
		return h
	}

	t.Run("teams: named, unnamed, named by its own id, a duplicate id; REST GET and POST", func(t *testing.T) {
		const org = "walk-teams"
		ids := []string{"github:acme/ops", "linear:ENG", "jira:SAME"}
		seedTeam(org, ids[0], "Ops")
		seedTeam(org, ids[1], "")
		seedTeam(org, ids[2], "jira:same")
		seedRisk(org, "team", ids[0])
		seedRisk(org, "team", ids[2])
		seedRisk(org, "team", ids[1])
		post := fetch(t, org, true, "team", append(append([]string{}, ids...), ids[0]))
		noID(t, "REST POST", post, ids)
		if want := "across Ops and 2 other teams"; !strings.Contains(prose(post), want) {
			t.Errorf("REST POST headline %q lacks %q", prose(post), want)
		}
		noID(t, "REST GET", fetch(t, org, false, "team", []string{ids[1]}), ids)
		if got := prose(fetch(t, org, false, "team", []string{ids[1]})); !strings.Contains(got, "across the selected team") {
			t.Errorf("REST GET headline %q lacks the selected team", got)
		}
		// the org-wide view serves the risk signals of both teams
		orgDoc := fetch(t, org, false, "org", nil)
		noID(t, "REST GET org", orgDoc, ids)
		titles := riskTitles(orgDoc)
		if len(titles) != 3 {
			t.Errorf("the org-wide view serves %d risk signals %v, want the 3 seeded (named, name == id, no name)", len(titles), titles)
		}
		if joined := strings.Join(titles, "|"); !strings.Contains(joined, "high for Ops") || strings.Count(joined, "high for a team") != 2 {
			t.Errorf("risk titles %v: want one for Ops and two for a team", titles)
		}
	})
	t.Run("repositories: mixed, service and developer levels; REST POST", func(t *testing.T) {
		const org = "walk-repos"
		named, unnamed := uuid.New(), uuid.New()
		seedRepo(org, named, "acme/checkout")
		seedRepo(org, unnamed, "")
		seedRisk(org, "repo", unnamed.String())
		ids := []string{named.String(), unnamed.String()}
		post := fetch(t, org, true, "repo", ids)
		noID(t, "REST POST", post, ids)
		if want := "across acme/checkout and 1 other repository"; !strings.Contains(prose(post), want) {
			t.Errorf("REST POST headline %q lacks %q", prose(post), want)
		}
		noID(t, "REST GET org", fetch(t, org, false, "org", nil), ids)
		for level, noun := range map[string]string{"service": "service", "developer": "developer"} {
			doc := fetch(t, org, true, level, []string{"svc-1", "svc-2"})
			noID(t, "REST POST "+level, doc, []string{"svc-1", "svc-2"})
			if got := prose(doc); got != "" && !strings.Contains(got, "the selected "+noun+"s") {
				t.Errorf("%s headline %q lacks the selected %ss", level, got, noun)
			}
		}
	})
}

// riskTitles is the titles of the risk signals of a Home document.
func riskTitles(doc any) []string {
	m, _ := doc.(map[string]any)
	signals, _ := m["signals"].([]any)
	var out []string
	for _, raw := range signals {
		signal, _ := raw.(map[string]any)
		if metric, _ := signal["metric"].(string); metric == "compounding_risk" {
			title, _ := signal["title"].(string)
			out = append(out, title)
		}
	}
	return out
}
