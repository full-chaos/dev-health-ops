//go:build integration

package aianalytics

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// tieSeedOrg writes one org's RC10-shaped rollups (three repositories, seven days, the bucket by row parity, every
// pull request a test gap) one INSERT per row, so each row is its own part, in the given repository order.
func tieSeedOrg(t *testing.T, ctx context.Context, conn chdriver.Conn, org string, repoOrder []int) {
	repos := []string{"c3e3adfc-6dca-569a-9a8d-5ad904daeafd", "6a4ac0aa-aa55-5786-8975-5ee227e88375", "8bb372e1-5732-5b3a-bc42-1bfe910d5388"}
	for _, i := range repoOrder {
		for day := 0; day < 7; day++ {
			n := i*7 + day
			bucket := "human"
			if n%2 == 1 {
				bucket = "ai_assisted"
			}
			exec(t, ctx, conn, `INSERT INTO ai_impact_metrics_daily (org_id, team_id, repo_id, work_type, day, attribution_bucket,
        prs_total, prs_merged, test_gap_prs, computed_at)
        VALUES ('%s', 'parity', '%s', 'pull_request', today() - %d, '%s', %d, %d, %d, now64(3))`,
				org, repos[i], 2+day, bucket, 2+n, 2+n, 2+n)
		}
	}
}

// TestRealClickHouse_AIOpportunitiesTieOrder: equal-score opportunities come in one order and the limit cut keeps
// the same set, whatever the storage layout: two orgs with the same rows written in other orders, read 20 times
// each, then again after the parts are merged (OPTIMIZE FINAL). CHAOS-14256.
func TestRealClickHouse_AIOpportunitiesTieOrder(t *testing.T) {
	ctx, conn, client := startStore(t)
	exec(t, ctx, conn, `SYSTEM STOP MERGES ai_impact_metrics_daily`)
	fwd, rev := "00000000-0000-0000-0000-0000000014a1", "00000000-0000-0000-0000-0000000014a2"
	tieSeedOrg(t, ctx, conn, fwd, []int{0, 1, 2})
	tieSeedOrg(t, ctx, conn, rev, []int{2, 1, 0})

	want := []string{
		`TEST_GENERATION 6a4ac0aa-aa55-5786-8975-5ee227e88375 Human-authored PRs had a 100% test gap rate across 36 PRs over the last 30 days.`,
		`UNCOVERED_TEST_AREA 6a4ac0aa-aa55-5786-8975-5ee227e88375 AI-assisted PRs had a 100% test gap rate over the last 30 days.`,
		`TEST_GENERATION 8bb372e1-5732-5b3a-bc42-1bfe910d5388 Human-authored PRs had a 100% test gap rate across 76 PRs over the last 30 days.`,
		`UNCOVERED_TEST_AREA 8bb372e1-5732-5b3a-bc42-1bfe910d5388 AI-assisted PRs had a 100% test gap rate over the last 30 days.`,
		`TEST_GENERATION c3e3adfc-6dca-569a-9a8d-5ad904daeafd Human-authored PRs had a 100% test gap rate across 20 PRs over the last 30 days.`,
	}
	read := func(label, org string) string {
		got, err := AiOpportunities(ctx, client, org, nil, 5)
		if err != nil {
			t.Fatal(err)
		}
		var rows []string
		for _, r := range got.Recommendations {
			rows = append(rows, string(r.Kind)+" "+*r.RepoID+" "+r.Rationale)
		}
		if !reflect.DeepEqual(rows, want) {
			t.Errorf("%s: served\n %q\nwant\n %q", label, rows, want)
		}
		body, _ := json.Marshal(got.Recommendations)
		return string(body)
	}
	var first string
	check := func(label, org string) {
		for i := 0; i < 20; i++ {
			body := read(label, org)
			if first == "" {
				first = body
			} else if body != first {
				t.Fatalf("%s read %d: the body differs\n got: %s\nwant: %s", label, i, body, first)
			}
		}
	}
	check("separate parts, forward order", fwd)
	check("separate parts, reverse order", rev)
	exec(t, ctx, conn, `SYSTEM START MERGES ai_impact_metrics_daily`)
	exec(t, ctx, conn, `OPTIMIZE TABLE ai_impact_metrics_daily FINAL`)
	check("after OPTIMIZE FINAL, forward order", fwd)
	check("after OPTIMIZE FINAL, reverse order", rev)
}

// TestRealClickHouse_AIOpportunitiesFloatSumOrder: the same rows in 12 orgs answer the same served bytes. The ai
// rows' reviews_per_pr x prs_total products (1.9x5, 2.9x3, 3.3x2) sum to other last bits in other orders, so the
// served score follows the impact read's row order unless that read is ordered. CHAOS-14256.
func TestRealClickHouse_AIOpportunitiesFloatSumOrder(t *testing.T) {
	ctx, conn, client := startStore(t)
	type row struct {
		back    int
		bucket  string
		prs     int
		reviews float64
	}
	rows := []row{{2, "human", 10, 1.0}, {3, "ai_assisted", 5, 1.9}, {4, "ai_assisted", 3, 2.9}, {5, "ai_assisted", 2, 3.3}}
	bodies := map[string][]int{}
	for i := 0; i < 12; i++ {
		org := fmt.Sprintf("00000000-0000-0000-0000-0000000c4%03d", i)
		order := []int{0, 1, 2, 3}
		if i%2 == 1 {
			order = []int{3, 2, 1, 0}
		}
		for _, j := range order {
			r := rows[j]
			exec(t, ctx, conn, `INSERT INTO ai_impact_metrics_daily (org_id, team_id, repo_id, work_type, day, attribution_bucket,
        prs_total, prs_merged, reviews_per_pr, computed_at)
        VALUES ('%s', 'parity', '%s', 'pull_request', today() - %d, '%s', %d, %d, %v, now64(3))`,
				org, tieRepo1, r.back, r.bucket, r.prs, r.prs, r.reviews)
		}
		got, err := AiOpportunities(ctx, client, org, nil, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Recommendations) != 1 || got.Recommendations[0].Kind != model.AIOpportunityKindHighReviewLoad {
			t.Fatalf("org %d: served %+v, want one HIGH_REVIEW_LOAD", i, got.Recommendations)
		}
		body, _ := json.Marshal(got.Recommendations)
		bodies[string(body)] = append(bodies[string(body)], i)
	}
	if len(bodies) != 1 {
		t.Errorf("the same rows in 12 orgs give %d served bodies: %v", len(bodies), bodies)
	}
}
