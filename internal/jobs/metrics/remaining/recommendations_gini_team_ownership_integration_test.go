//go:build integration

package remaining

import (
	"context"
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestRecommendationsReviewerGiniTeamIsRepositoryOwnership: the review-concentration Gini of a team reads the reviews
// given in the repositories the team owns at the window end, by anyone; user_metrics_daily.team_id is the person's
// membership label and is not read (ticket 14163, the CHAOS-14101 rule).
func TestRecommendationsReviewerGiniTeamIsRepositoryOwnership(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	conn := openLoaderClickHouse(t, ctx, dsn)

	const org, otherOrg = "org-gini-ownership", "org-gini-other"
	repos := map[string]string{
		"rA":  "14163000-0000-0000-0000-00000000000a",
		"rB":  "14163000-0000-0000-0000-00000000000b",
		"rBC": "14163000-0000-0000-0000-0000000000bc",
		"rN":  "14163000-0000-0000-0000-00000000000f",
		"rE":  "14163000-0000-0000-0000-00000000000e",
		"rT":  "14163000-0000-0000-0000-000000000077",
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, query)
		}
	}
	// team-a owns rA; team-b owns rB; team-b and team-c co-own rBC; rN has no owner; team-d owns nothing; team-e owns
	// rE; rT is team-a's until 2026-08-15 and team-f's from then, so the window end (2026-09-01) sees team-f.
	for _, own := range []struct{ team, repo, from, to string }{
		{"team-a", "rA", "2026-01-01 00:00:00", ""},
		{"team-b", "rB", "2026-01-01 00:00:00", ""},
		{"team-b", "rBC", "2026-01-01 00:00:00", ""},
		{"team-c", "rBC", "2026-01-01 00:00:00", ""},
		{"team-e", "rE", "2026-01-01 00:00:00", ""},
		{"team-a", "rT", "2026-01-01 00:00:00", "2026-08-15 00:00:00"},
		{"team-f", "rT", "2026-08-15 00:00:00", ""},
	} {
		if own.to == "" {
			exec(`INSERT INTO team_repo_ownership
				(org_id, provider, team_id, repo_id, repo_full_name, match_type,
				 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
				VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 0, 1, 0, ?, NULL, ?)`,
				org, own.team, repos[own.repo], "gini/"+own.repo,
				mustTimestamp(t, own.from), mustTimestamp(t, own.from))
			continue
		}
		exec(`INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type,
			 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 0, 1, 0, ?, ?, ?)`,
			org, own.team, repos[own.repo], "gini/"+own.repo,
			mustTimestamp(t, own.from), mustTimestamp(t, own.to), mustTimestamp(t, own.from))
	}
	// Rows as ic_finalize writes them: team_id is the person's membership label. The other org's row sits on rA under
	// team-a's label with an extreme load: it must never reach team-a.
	for _, row := range []struct {
		org, person, label, repo, day string
		reviews                       uint32
	}{
		{org, "p1", "team-a", "rA", "2026-08-05", 2},
		{org, "p1", "team-a", "rB", "2026-08-05", 40},
		{org, "p2", "team-a", "rA", "2026-08-05", 2},
		{org, "p3", "team-b", "rA", "2026-08-05", 6},
		{org, "p3", "team-b", "rB", "2026-08-05", 1},
		{org, "p4", "team-c", "rBC", "2026-08-05", 9},
		{org, "p5", "unassigned", "rBC", "2026-08-05", 3},
		{org, "p5", "unassigned", "rN", "2026-08-05", 50},
		{org, "p6", "team-d", "rA", "2026-08-05", 4},
		{org, "p7", "team-d", "rN", "2026-08-05", 5},
		{org, "p8", "team-e", "rE", "2026-08-05", 10},
		{org, "p9", "team-e", "rE", "2026-08-05", 1},
		{org, "p10", "team-a", "rT", "2026-08-05", 7},
		{org, "p11", "team-f", "rT", "2026-08-20", 3},
		{otherOrg, "leak", "team-a", "rA", "2026-08-05", 5000},
	} {
		exec(`INSERT INTO user_metrics_daily
			(repo_id, day, author_email, commits_count, team_id, reviews_given, computed_at, org_id)
			VALUES (?, ?, ?, 0, ?, ?, ?, ?)`,
			repos[row.repo], mustDate(t, row.day), row.person+"@gini.example", row.label, row.reviews,
			mustTimestamp(t, "2026-08-25 00:00:00"), row.org)
	}

	loader, err := NewRecommendationsLoader(conn, org)
	if err != nil {
		t.Fatal(err)
	}
	start, end := mustDate(t, "2026-08-01"), mustDate(t, "2026-09-01")
	text := func(value float64, known bool) string {
		if !known {
			return "absent"
		}
		return fmt.Sprintf("%.4f", value)
	}
	for _, c := range []struct {
		cell, team, note string
		wantLoads        []float64
	}{
		{"a", "team-a", "a member's reviews in team-b's rB leave; the team-b and team-d members' reviews in rA count", []float64{2, 2, 6, 4}},
		{"a", "team-b", "the owner of rB and co-owner of rBC: every reviewer of both", []float64{40, 1, 9, 3}},
		{"b", "team-c", "the co-owner of rBC: rBC counts whole", []float64{9, 3}},
		{"c", "team-d", "a team that owns no repository has no Gini", nil},
		{"d", "team-e", "control: the members review only in the team's own rE", []float64{10, 1}},
		{"e", "team-f", "the owner at the window end: rT's whole window counts", []float64{7, 3}},
	} {
		snapshot, loadErr := loader.LoadTeamMetricsWindow(ctx, c.team, org, start, end)
		if loadErr != nil {
			t.Fatalf("%s: %v", c.team, loadErr)
		}
		want, wantKnown := Gini(c.wantLoads)
		got := text(snapshot.ReviewerGini, snapshot.ReviewerGiniKnown)
		if got != text(want, wantKnown) {
			t.Errorf("cell %s %s reviewer gini = %s, want %s (loads %v: %s)", c.cell, c.team, got, text(want, wantKnown), c.wantLoads, c.note)
		}
	}
}
