//go:build integration

package operatingreview

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// seedOwnedRepo makes a repository the team owns (the canonical readers narrow a team by the
// repositories it owns, never by a team column).
func seedOwnedRepo(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org, team, repo string, at time.Time) {
	t.Helper()
	if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, repo, "acme/"+repo[:8], org, at, at); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, ?, 'exact', 'native', 1, 100, 0, ?, NULL, ?)`,
		org, team, repo, "acme/"+repo[:8], at.AddDate(-1, 0, 0), at); err != nil {
		t.Fatalf("seed ownership: %v", err)
	}
}

// seedCanonicalUnit writes one categorized unit ending at `to` with the given subcategory shares.
func seedCanonicalUnit(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org, id, repo string, to time.Time, effort float64, distribution map[string]float64) {
	t.Helper()
	themes := map[string]float64{}
	for key, share := range distribution {
		for i := range key {
			if key[i] == '.' {
				themes[key[:i]] += share
				break
			}
		}
	}
	evidence, _ := json.Marshal(map[string]any{"issues": []string{}, "prs": []string{}, "commits": []string{}, "edges": []string{}})
	if err := admin.Exec(ctx, `INSERT INTO work_unit_investments
(work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES (?, ?, ?, ?, 'churn_loc', ?, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`,
		id, to.Add(-24*time.Hour), to, repo, effort, themes, distribution, string(evidence), to.Add(time.Hour), org); err != nil {
		t.Fatalf("seed unit %s: %v", id, err)
	}
}

// CHAOS-9199: the Investment section is the week's work per CANONICAL theme (the Jev
// categorization, work_unit_investments), dated by the unit's end day, narrowed by owned
// repositories for a team, and blind to the legacy rule classifier's table. CHAOS-9622 (D5999): the value is the COUNT of the units (each unit
// counts 1 over the themes it spreads over), never their effort_value (lines of code): wu-a carries 100 lines and
// wu-b 50, and they count 1 each.
func TestRealClickHouse_InvestmentSectionIsTheCanonicalWorkPerTheme(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)
	const org = "org-9199"
	week := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	synced := week.AddDate(0, 0, 8)
	repoA, repoB := "91990001-9199-4199-8199-919991999199", "91990002-9199-4199-8199-919991999199"
	seedOwnedRepo(ctx, t, admin, org, "team-a", repoA, synced)
	seedOwnedRepo(ctx, t, admin, org, "team-b", repoB, synced)
	inWeek := week.Add(60 * time.Hour)
	seedCanonicalUnit(ctx, t, admin, org, "wu-a", repoA, inWeek, 100, map[string]float64{"feature_delivery.roadmap": 0.6, "risk.security": 0.4})
	seedCanonicalUnit(ctx, t, admin, org, "wu-b", repoB, inWeek, 50, map[string]float64{"maintenance.debt": 1})
	seedCanonicalUnit(ctx, t, admin, org, "wu-after", repoA, week.AddDate(0, 0, 9), 1000, map[string]float64{"quality.bugfix": 1}) // ends next week: not this week
	seedCanonicalUnit(ctx, t, admin, org, "wu-prior", repoA, week.AddDate(0, 0, -3), 20, map[string]float64{"feature_delivery.roadmap": 1})

	review := func(teams []string) map[string]float64 {
		t.Helper()
		got, err := ResolveInput(ctx, client, org, nil, teams, graphqldate.New(week))
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]float64{}
		for _, section := range got.Sections {
			if section.Key != "investment" {
				continue
			}
			if len(section.Metrics) != 5 {
				t.Fatalf("investment section has %d metrics, want the 5 canonical themes", len(section.Metrics))
			}
			for _, m := range section.Metrics {
				if !m.HasData {
					t.Errorf("%s: hasData false for a week with units", m.Key)
				}
				out[m.Key] = m.Value
			}
		}
		return out
	}
	want := func(got map[string]float64, expected map[string]float64, label string) {
		t.Helper()
		for key, value := range expected {
			// The shares are Float32 in the shared read (the Investment page's own narrowing).
			if math.Abs(got[key]-value) > 1e-3 {
				t.Errorf("%s: %s = %v, want %v (all: %v)", label, key, got[key], value, got)
			}
		}
	}
	want(review(nil), map[string]float64{"feature_delivery_work": 0.6, "risk_work": 0.4, "maintenance_work": 1, "quality_work": 0, "operational_work": 0}, "all teams")
	want(review([]string{"team-a"}), map[string]float64{"feature_delivery_work": 0.6, "risk_work": 0.4, "maintenance_work": 0}, "team A (owned repo A only)")
	want(review([]string{"team-a", "team-b"}), map[string]float64{"feature_delivery_work": 0.6, "maintenance_work": 1}, "teams A and B")

	// The prior week's value rides the delta: wu-prior (one unit, feature delivery) is the prior week.
	prior, err := ResolveInput(ctx, client, org, nil, nil, graphqldate.New(week))
	if err != nil {
		t.Fatal(err)
	}
	for _, section := range prior.Sections {
		for _, m := range section.Metrics {
			if m.Key == "feature_delivery_work" && (math.Abs(m.Delta.PriorValue-1) > 1e-3 || !m.Delta.HasPriorData) {
				t.Errorf("feature_delivery_work prior = %v (hasPriorData %v), want 1", m.Delta.PriorValue, m.Delta.HasPriorData)
			}
		}
	}
}
