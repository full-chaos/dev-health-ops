package home

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	ch "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/deltarule"
)

// A query double of Home for a repository sum under a team scope (CHAOS-14441): the value of Code Churn for [Ops] sums
// each repository at Ops's share of it (scopeShareForMetric), and its driver reads must weigh the same way. acme/a: Ops
// share 0.1, churn 0 -> 100 (weighted 10); acme/b: Ops share 1.0, churn 0 -> 20 (weighted 20). The double applies the
// share only where the statement carries the share map ({repo_share_team_ids:...}), and records each driver read.
const (
	shareDoubleRepoA = "6f1c3a52-6d1e-4a7e-9d3b-0a1b2c3d4e5a"
	shareDoubleRepoB = "7a2d4b63-7e2f-4b8f-8e4c-1b2c3d4e5f6b"
	shareDoubleTeam  = "github:acme/ops"
)

type shareDoubleRead struct {
	weighted, zero bool
	ids            []string
}

func shareDoubleHome(t *testing.T) (*Response, shareDoubleRead, shareDoubleRead) {
	t.Helper()
	cur, prev := day(2026, 10, 21), day(2026, 10, 20)
	now := cur.AddDate(0, 0, 1).Add(12 * time.Hour)
	share := map[string]float64{shareDoubleRepoA: 0.1, shareDoubleRepoB: 1.0}
	churn := map[string]map[string]float64{
		formatDay(prev): {shareDoubleRepoA: 0, shareDoubleRepoB: 0},
		formatDay(cur):  {shareDoubleRepoA: 100, shareDoubleRepoB: 20},
	}
	reduce := func(begin string, weighted bool) map[string]float64 {
		out := map[string]float64{}
		for repo, v := range churn[begin] {
			if weighted {
				v *= share[repo]
			}
			out[repo] = v
		}
		return out
	}
	const shareMarker = "{" + "repo_share_team_ids" + ":"
	arm := regexp.MustCompile(`SELECT repo_id AS id, toFloat64\(sum\(total_loc_touched`)
	var summary, caption shareDoubleRead
	client := fakeQueryClient{t: t, handler: func(t *testing.T, q string, bindings []ch.Binding) (ch.RowScanner, error) {
		scan := func(rows [][]any) (ch.RowScanner, error) { return &fixtureRowScanner{rows: rows}, nil }
		bound := func(name string) string {
			v, _ := bindingValue(bindings, name)
			s, _ := v.(string)
			return s
		}
		if strings.Contains(q, "AS previous ON") {
			if !strings.Contains(q, "total_loc_touched") {
				return nil, fmt.Errorf("a driver read the double does not know:\n%s", q)
			}
			at := arm.FindAllStringIndex(q, -1)
			if len(at) != 2 {
				return nil, fmt.Errorf("expected two churn driver arms, got %d:\n%s", len(at), q)
			}
			first, second := q[at[0][0]:at[1][0]], q[at[1][0]:]
			weighted := strings.Contains(first, shareMarker) && strings.Contains(second, shareMarker)
			if weighted != (strings.Contains(first, shareMarker) || strings.Contains(second, shareMarker)) {
				return nil, fmt.Errorf("one window of the driver read is weighted, the other not:\n%s", q)
			}
			if weighted {
				if ids, _ := bindingValue(bindings, "repo_share_team_ids"); fmt.Sprint(ids) != fmt.Sprint([]string{shareDoubleTeam}) {
					return nil, fmt.Errorf("the share map of the driver read binds teams %v, want [%s]", ids, shareDoubleTeam)
				}
			}
			read := shareDoubleRead{weighted: weighted, zero: strings.Contains(q, "AS delta_abs")}
			current, prior := reduce(bound("start_day"), weighted), reduce(bound("compare_start"), weighted)
			type ranked struct {
				id            string
				current, rank float64
			}
			var out []ranked
			for repo, c := range current {
				p, ok := prior[repo]
				if read.zero {
					if c > 0 && (!ok || p == 0) {
						out = append(out, ranked{repo, c, c - p})
					}
				} else if delta := deltarule.Of(c, p, true, ok).Pct; delta != nil {
					out = append(out, ranked{repo, c, *delta})
				}
			}
			sort.Slice(out, func(i, j int) bool {
				if out[i].rank != out[j].rank {
					return out[i].rank > out[j].rank
				}
				return out[i].id < out[j].id
			})
			var rows [][]any
			for _, r := range out {
				read.ids = append(read.ids, r.id)
				if strings.Contains(q, "BY metric") {
					rows = append(rows, []any{"churn", r.id})
				} else {
					rows = append(rows, []any{r.id, r.current, r.rank})
				}
			}
			if strings.Contains(q, "BY metric") {
				caption = read
			} else {
				summary = read
			}
			return scan(rows)
		}
		switch {
		case strings.Contains(q, "repo AS display_name"):
			return scan([][]any{{shareDoubleRepoA, "acme/a"}, {shareDoubleRepoB, "acme/b"}})
		case strings.Contains(q, "name AS display_name"):
			return scan([][]any{{shareDoubleTeam, "Ops"}})
		// A value read picks one version of every measure (CHAOS-14430): it is the churn read when it sums this column.
		case strings.Contains(q, "AS picked") && strings.Contains(q, "sum(total_loc_touched") && strings.Contains(q, "AS value"):
			if !strings.Contains(q, shareMarker) {
				return nil, fmt.Errorf("the team-scoped churn value is not weighted:\n%s", q)
			}
			start, _ := time.Parse(time.DateOnly, bound("start_day"))
			total := 0.0
			for _, v := range reduce(bound("start_day"), true) {
				total += v
			}
			if strings.Contains(q, "AS row_count") {
				return scan([][]any{{int64(2), total}})
			}
			return scan([][]any{{start, total}})
		}
		return scan(nil)
	}}
	f := Filters{Time: TimeFilter{RangeDays: 1, CompareDays: 1, EndDate: &now}, Scope: ScopeFilter{Level: "team", IDs: []string{shareDoubleTeam}}}
	resp, err := BuildResponse(context.Background(), client, nil, "org-14441-share-double", f, now)
	if err != nil {
		t.Fatal(err)
	}
	return resp, summary, caption
}

// The driver of a repository sum under a team scope weighs each repository by the team's share, as the value does: a
// move from a measured 0 names acme/b (weighted 20) before acme/a (weighted 10, unweighted 100), in the summary and the
// caption, from weighted reads.
func TestHomeShareWeightedDriverOfATeamScope(t *testing.T) {
	resp, summaryRead, captionRead := shareDoubleHome(t)
	d := driverDoubleDelta(resp, "churn")
	if !d.FromZero() || d.Value != 30 {
		t.Fatalf("SEED: want a move from a measured 0 to the weighted 30, got %+v", d)
	}
	summary, captions := driverDoubleSummary(resp)
	t.Logf("SHARE summary=%q captions=%q summary-read=%+v caption-read=%+v", summary, captions, summaryRead, captionRead)
	if !strings.HasSuffix(summary, " driven by acme/b, acme/a.") {
		t.Errorf("SHARE: summary %q, want the weighted order acme/b, acme/a", summary)
	}
	if len(captions) != 1 || captions[0] != "Code Churn appears to have moved most in acme/b and acme/a." {
		t.Errorf("SHARE: captions %q, want the weighted order acme/b and acme/a", captions)
	}
	for name, read := range map[string]shareDoubleRead{"summary": summaryRead, "caption": captionRead} {
		if !read.weighted || !read.zero {
			t.Errorf("SHARE: the %s driver read is %+v, want the weighted absolute form", name, read)
		}
	}
}
