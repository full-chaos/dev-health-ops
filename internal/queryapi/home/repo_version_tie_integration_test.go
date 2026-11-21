//go:build integration

package home

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
)

// Two versions of one repo_metrics_daily key that share computed_at: every metric of one Home or explain response
// reads ONE stored version (review latency and churn hash the table's one column list, not their own columns), in the
// organization form, a team set and one team, in both insert orders.

type repoTieVersion struct {
	review float64
	churn  uint32
}

func TestHomeAndExplainReadOneRepoVersionOnAComputedAtTie(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	reader, err := explain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	prev, cur, _, older, _ := nullableNewestDays()
	stopMerges(ctx, t, admin, "repo_metrics_daily")
	// Two pairings of the same values: each metric sees the same two values in both, so a pick per metric serves a
	// pair that one of the pairings does not store.
	pairings := map[string][2]repoTieVersion{
		"aligned": {{2, 100}, {4, 200}},
		"crossed": {{2, 200}, {4, 100}},
	}
	for name, pair := range pairings {
		for _, order := range [][2]repoTieVersion{{pair[0], pair[1]}, {pair[1], pair[0]}} {
			org := fmt.Sprintf("org-14430-repo-%s-%g-first", name, order[0].review)
			seedHomeTeamViewTeams(ctx, t, admin, org)
			repo := uuid.New()
			if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/tie', 'github', ?, ?, ?)`,
				repo, org, older, older); err != nil {
				t.Fatal(err)
			}
			if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share)
				VALUES (?, 'github', 'P', ?, 'acme/tie', 'exact', 'inferred', 1, 0, 0, ?, NULL, ?, 1.0)`, org, repo, prev.AddDate(0, -1, 0), prev.AddDate(0, -1, 0)); err != nil {
				t.Fatal(err)
			}
			for _, v := range order {
				if err := admin.Exec(ctx, `INSERT INTO repo_metrics_daily (org_id, repo_id, day, pr_first_review_p50_hours, total_loc_touched, computed_at) VALUES (?, ?, ?, ?, ?, ?)`,
					org, repo, cur, v.review, v.churn, older); err != nil {
					t.Fatal(err)
				}
			}
			stored := []string{fmt.Sprintf("%g/%d", pair[0].review, pair[0].churn), fmt.Sprintf("%g/%d", pair[1].review, pair[1].churn)}
			var served []string
			for _, teams := range [][]string{nil, {"P", "C"}, {"P"}} {
				lines, _, _ := nullableServed(ctx, t, client, org, []string{"review_latency", "churn"}, teams...)
				var review, churn float64
				if _, err := fmt.Sscanf(strings.SplitN(lines["review_latency"], " ", 2)[0], "review_latency=%g", &review); err != nil {
					t.Fatalf("%s %v: %q: %v", org, teams, lines["review_latency"], err)
				}
				if _, err := fmt.Sscanf(strings.SplitN(lines["churn"], " ", 2)[0], "churn=%g", &churn); err != nil {
					t.Fatalf("%s %v: %q: %v", org, teams, lines["churn"], err)
				}
				served = append(served, fmt.Sprintf("home %v %g/%g", teams, review, churn))
				params := explain.Params{ScopeLevel: "org", StartDay: cur, EndDay: cur.AddDate(0, 0, 1), CompareStart: prev, CompareEnd: cur}
				if teams != nil {
					params.ScopeLevel, params.ScopeIDs = "team", teams
				}
				var values []float64
				for _, metric := range []string{"review_latency", "churn"} {
					params.Metric = metric
					resp, err := explain.BuildExplainResponse(ctx, reader, org, params)
					if err != nil {
						t.Fatal(err)
					}
					values = append(values, resp.Value)
				}
				served = append(served, fmt.Sprintf("explain %v %g/%g", teams, values[0], values[1]))
			}
			for _, s := range served {
				pairText := s[strings.LastIndex(s, " ")+1:]
				if !slices.Contains(stored, pairText) {
					t.Errorf("%s: %s is not a stored version (stored %v)", org, s, stored)
				}
			}
			t.Logf("%s: stored %v served %v", org, stored, served)
		}
	}
}
