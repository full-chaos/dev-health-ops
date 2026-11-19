package aianalytics

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

const (
	tieRepo1 = "11111111-1111-1111-1111-111111111111"
	tieRepo2 = "22222222-2222-2222-2222-222222222222"
	tieRepo3 = "33333333-3333-3333-3333-333333333333"
)

func tieImpact(team, repo, bucket string, prs, rework, gap float64) map[string]any {
	return map[string]any{"team_id": team, "repo_id": repo, "attribution_bucket": bucket,
		"prs_total": prs, "rework_prs": rework, "test_gap_prs": gap}
}

func tieCluster(repo, prefix string, count float64, refs ...string) map[string]any {
	list := []any{}
	for _, n := range refs {
		list = append(list, "git_pull_requests:"+repo+":"+n)
	}
	return map[string]any{"repo_id": repo, "title_prefix": prefix, "prs_total": count, "pr_refs": list}
}

// tieOrders are the input orders every case is read in: as given, reversed, rotated by one, and evens then odds.
func tieOrders(n int) [][]int {
	ident, rev, rot, evenOdd := make([]int, n), make([]int, n), make([]int, n), make([]int, 0, n)
	for i := 0; i < n; i++ {
		ident[i], rev[i], rot[i] = i, n-1-i, (i+1)%n
	}
	for i := 0; i < n; i += 2 {
		evenOdd = append(evenOdd, i)
	}
	for i := 1; i < n; i += 2 {
		evenOdd = append(evenOdd, i)
	}
	return [][]int{ident, rev, rot, evenOdd}
}

func tiePermute(rows []map[string]any, order []int) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, i := range order {
		out = append(out, rows[i])
	}
	return out
}

func tieKey(r model.AIOpportunity) string {
	key := string(r.Kind) + "@" + (*r.RepoID)[:4]
	if r.TeamID != nil {
		key += "@" + *r.TeamID
	}
	if r.Kind == model.AIOpportunityKindRepetitiveChange && len(r.EvidenceRefs) > 0 {
		ref := r.EvidenceRefs[0]
		key += "#" + ref[strings.LastIndex(ref, ":")+1:]
	}
	return key
}

// TestAiOpportunitiesTieOrderIsTotal pins the served order of equal-score opportunities (CHAOS-14256): one order
// whatever order the reads return their rows in, and the same limit cut. Each case is decided by one comparator
// clause alone; the clause after it would give the other order.
func TestAiOpportunitiesTieOrderIsTotal(t *testing.T) {
	rc10 := []map[string]any{}
	for i, repo := range []string{"c3e3adfc-6dca-569a-9a8d-5ad904daeafd", "6a4ac0aa-aa55-5786-8975-5ee227e88375", "8bb372e1-5732-5b3a-bc42-1bfe910d5388"} {
		for day := 0; day < 7; day++ {
			n := float64(2 + i*7 + day)
			bucket := "human"
			if (i*7+day)%2 == 1 {
				bucket = "ai_assisted"
			}
			rc10 = append(rc10, tieImpact("parity", repo, bucket, n, 0, n))
		}
	}
	cases := []struct {
		name   string
		impact []map[string]any
		rep    []map[string]any
		want   []string
	}{
		{"rc10 seed: six rows at score 1", rc10, nil, []string{
			"TEST_GENERATION@6a4a@parity", "UNCOVERED_TEST_AREA@6a4a@parity", "TEST_GENERATION@8bb3@parity",
			"UNCOVERED_TEST_AREA@8bb3@parity", "TEST_GENERATION@c3e3@parity", "UNCOVERED_TEST_AREA@c3e3@parity"}},
		{"score decides", []map[string]any{
			tieImpact("", tieRepo1, "human", 10, 0, 6), tieImpact("", tieRepo2, "human", 10, 0, 9)}, nil,
			[]string{"TEST_GENERATION@2222", "TEST_GENERATION@1111"}},
		{"rule group decides over repository", []map[string]any{tieImpact("", tieRepo3, "human", 10, 0, 10)},
			[]map[string]any{tieCluster(tieRepo1, "alpha", 10, "1")},
			[]string{"TEST_GENERATION@3333", "REPETITIVE_CHANGE@1111#1"}},
		{"repository decides over team", []map[string]any{
			tieImpact("team-b", tieRepo1, "human", 10, 0, 10), tieImpact("team-a", tieRepo2, "human", 10, 0, 10)}, nil,
			[]string{"TEST_GENERATION@1111@team-b", "TEST_GENERATION@2222@team-a"}},
		{"team decides over rationale", []map[string]any{
			tieImpact("team-a", tieRepo1, "human", 20, 0, 20), tieImpact("team-b", tieRepo1, "human", 10, 0, 10)}, nil,
			[]string{"TEST_GENERATION@1111@team-a", "TEST_GENERATION@1111@team-b"}},
		{"unclamped score decides", []map[string]any{
			tieImpact("", tieRepo1, "human", 10, 0, 10), tieImpact("", tieRepo2, "human", 10, 0, 0),
			tieImpact("", tieRepo2, "ai_assisted", 10, 5, 0)}, nil,
			[]string{"HIGH_REWORK@2222", "TEST_GENERATION@1111"}},
		{"rule rank decides over title", []map[string]any{
			tieImpact("", tieRepo1, "human", 10, 3, 10), tieImpact("", tieRepo1, "ai_assisted", 10, 5, 0)}, nil,
			[]string{"TEST_GENERATION@1111", "HIGH_REWORK@1111"}},
		{"rationale decides over evidence", nil, []map[string]any{
			tieCluster(tieRepo1, "beta", 10, "1"), tieCluster(tieRepo1, "alpha", 10, "9")},
			[]string{"REPETITIVE_CHANGE@1111#9", "REPETITIVE_CHANGE@1111#1"}},
		{"evidence decides last", nil, []map[string]any{
			tieCluster(tieRepo1, "alpha", 10, "5"), tieCluster(tieRepo1, "alpha", 10, "3")},
			[]string{"REPETITIVE_CHANGE@1111#3", "REPETITIVE_CHANGE@1111#5"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var first []byte
			for i, order := range tieOrders(len(c.impact) + len(c.rep)) {
				var impactOrder, repOrder []int
				for _, j := range order {
					if j < len(c.impact) {
						impactOrder = append(impactOrder, j)
					} else {
						repOrder = append(repOrder, j-len(c.impact))
					}
				}
				oc := oracleDCase{Impact: tiePermute(c.impact, impactOrder), Rep: tiePermute(c.rep, repOrder)}
				for _, limit := range []int{100, 5} {
					got, err := AiOpportunities(context.Background(), &fixtureClientD{c: oc}, "org-1", nil, limit)
					if err != nil {
						t.Fatal(err)
					}
					var keys []string
					for k, r := range got.Recommendations {
						keys = append(keys, tieKey(r))
						if k > 0 && r.Score > got.Recommendations[k-1].Score {
							t.Errorf("order %d %v, limit %d: score %v at %d follows %v: the list is not score-descending",
								i, order, limit, r.Score, k, got.Recommendations[k-1].Score)
						}
					}
					want := c.want
					if len(want) > limit {
						want = want[:limit]
					}
					if !reflect.DeepEqual(keys, want) {
						t.Errorf("order %d %v, limit %d: served %v, want %v", i, order, limit, keys, want)
					}
					if limit != 100 {
						continue
					}
					body, _ := json.Marshal(got)
					if first == nil {
						first = body
					} else if string(body) != string(first) {
						t.Errorf("order %d %v: the body differs from the first order's\n got: %s\nwant: %s", i, order, body, first)
					}
				}
			}
		})
	}
}

// TestOpportunityLessPutsNaNLast: a NaN score (a stored NaN or Inf measure makes one) has no place in the score order;
// it comes after every number, and two NaN scores fall through to the other keys, in any input order.
func TestOpportunityLessPutsNaNLast(t *testing.T) {
	in := []opportunity{
		{kind: model.AIOpportunityKindHighReviewLoad, repoID: tieRepo2, raw: math.NaN()},
		{kind: model.AIOpportunityKindTestGeneration, repoID: tieRepo1, raw: 0.6},
		{kind: model.AIOpportunityKindHighReviewLoad, repoID: tieRepo1, raw: math.NaN()},
		{kind: model.AIOpportunityKindSlowCycle, repoID: tieRepo3, raw: math.Inf(1)},
		{kind: model.AIOpportunityKindHighRework, repoID: tieRepo3, raw: 2.5},
	}
	want := []string{tieRepo3 + " +Inf", tieRepo3 + " 2.5", tieRepo1 + " 0.6", tieRepo1 + " NaN", tieRepo2 + " NaN"}
	for _, order := range tieOrders(len(in)) {
		opps := make([]opportunity, 0, len(in))
		for _, j := range order {
			opps = append(opps, in[j])
		}
		sort.SliceStable(opps, func(i, j int) bool { return opportunityLess(opps[i], opps[j]) })
		var got []string
		for _, o := range opps {
			got = append(got, o.repoID+" "+strconv.FormatFloat(o.raw, 'g', -1, 64))
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("order %v: sorted %v, want %v", order, got, want)
		}
	}
}
