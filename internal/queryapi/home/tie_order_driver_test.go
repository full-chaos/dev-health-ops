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

// A query double of Home for drivers tied on the rank (CHAOS-14441): four groups move by the same amount, so only the
// tie key orders them. The double orders each driver read by the key its ORDER BY names: toString(id) and the batch's
// string id by the string, a bare native id of the summary read (a UUID repo_id) or toUUID(id) by ClickHouse's UUID
// order (the second half of the UUID first). A driver read with no tie key fails the test.
var tieDoubleRepos = []string{
	"10000000-0000-0000-0000-000000000004",
	"20000000-0000-0000-0000-000000000003",
	"30000000-0000-0000-0000-000000000002",
	"40000000-0000-0000-0000-000000000001",
}

var tieDoubleTeams = []string{"github:tie-a", "github:tie-b", "github:tie-c", "github:tie-d"}

var tieOrderClause = regexp.MustCompile(`ORDER BY (?:metric, )?(?:delta_abs|delta_pct|rank_value) DESC, (toString\(id\)|toUUID\(id\)|id)\s*\n\s*LIMIT`)

func tieDoubleNativeLess(a, b string) bool {
	ha, hb := strings.ReplaceAll(a, "-", ""), strings.ReplaceAll(b, "-", "")
	if ha[16:] != hb[16:] {
		return ha[16:] < hb[16:]
	}
	return ha[:16] < hb[:16]
}

type tieDoubleRead struct {
	key string
	ids []string
}

// tieDoubleHome serves metric (churn: repositories, throughput: teams) for four tied groups: a move from a measured 0
// (0 -> 5 each) or a percent move (10 -> 20 each).
func tieDoubleHome(t *testing.T, metric string, zero bool) (*Response, tieDoubleRead, tieDoubleRead) {
	t.Helper()
	cur, prev := day(2026, 10, 21), day(2026, 10, 20)
	now := cur.AddDate(0, 0, 1).Add(12 * time.Hour)
	column, ids, names := "total_loc_touched", tieDoubleRepos, map[string]string{}
	if metric == "throughput" {
		column, ids = "items_completed", tieDoubleTeams
	}
	for i, id := range ids {
		names[id] = fmt.Sprintf("tie/%c", 'a'+i)
	}
	prior, current := 10.0, 20.0
	if zero {
		prior, current = 0, 5
	}
	valueOf := func(begin string) float64 {
		if begin == formatDay(prev) {
			return prior
		}
		return current
	}
	var summary, caption tieDoubleRead
	client := fakeQueryClient{t: t, handler: func(t *testing.T, q string, bindings []ch.Binding) (ch.RowScanner, error) {
		scan := func(rows [][]any) (ch.RowScanner, error) { return &fixtureRowScanner{rows: rows}, nil }
		bound := func(name string) string {
			v, _ := bindingValue(bindings, name)
			s, _ := v.(string)
			return s
		}
		if strings.Contains(q, "AS previous ON") {
			if !strings.Contains(q, column) {
				return nil, fmt.Errorf("a driver read the double does not know:\n%s", q)
			}
			m := tieOrderClause.FindStringSubmatch(q)
			if m == nil {
				return nil, fmt.Errorf("a driver read with no tie key:\n%s", q)
			}
			batch := strings.Contains(q, "BY metric")
			read := tieDoubleRead{key: m[1]}
			native := m[1] == "toUUID(id)" || (m[1] == "id" && !batch && metric == "churn")
			order := append([]string(nil), ids...)
			sort.Slice(order, func(i, j int) bool {
				if native {
					return tieDoubleNativeLess(order[i], order[j])
				}
				return order[i] < order[j]
			})
			c, p := valueOf(bound("start_day")), valueOf(bound("compare_start"))
			rank := c - p
			if !strings.Contains(q, "AS delta_abs") {
				rank = *deltarule.Of(c, p, true, true).Pct
			}
			var rows [][]any
			for _, id := range order[:3] {
				read.ids = append(read.ids, id)
				if batch {
					rows = append(rows, []any{metric, id})
				} else {
					rows = append(rows, []any{id, c, rank})
				}
			}
			if batch {
				caption = read
			} else {
				summary = read
			}
			return scan(rows)
		}
		switch {
		case strings.Contains(q, "AS display_name"):
			var rows [][]any
			for _, id := range ids {
				rows = append(rows, []any{id, names[id]})
			}
			return scan(rows)
		case strings.HasPrefix(q, "SELECT id FROM teams FINAL"):
			var rows [][]any
			for _, id := range ids {
				rows = append(rows, []any{id})
			}
			return scan(rows)
		// A value read picks one version of every measure (CHAOS-14430): it is the read of this metric when it aggregates
		// this column.
		case strings.Contains(q, "AS picked") && (strings.Contains(q, "avg("+column+")") || strings.Contains(q, "sum("+column)) && strings.Contains(q, "AS value"):
			start, _ := time.Parse(time.DateOnly, bound("start_day"))
			total := valueOf(bound("start_day")) * float64(len(ids))
			if strings.Contains(q, "AS row_count") {
				return scan([][]any{{int64(len(ids)), total}})
			}
			return scan([][]any{{start, total}})
		}
		return scan(nil)
	}}
	f := Filters{Time: TimeFilter{RangeDays: 1, CompareDays: 1, EndDate: &now}, Scope: ScopeFilter{Level: "org"}}
	resp, err := BuildResponse(context.Background(), client, nil, "org-14441-tie-double", f, now)
	if err != nil {
		t.Fatal(err)
	}
	return resp, summary, caption
}

// Four repositories (or teams) tied on the rank: the summary and the caption name the same three drivers in the same
// order, the order of the id as a string, for a move from a measured 0 and for a percent move.
func TestHomeTiedDriversHaveOneOrder(t *testing.T) {
	for _, metric := range []string{"churn", "throughput"} {
		for _, zero := range []bool{true, false} {
			resp, summaryRead, captionRead := tieDoubleHome(t, metric, zero)
			summary, captions := driverDoubleSummary(resp)
			t.Logf("TIE %s zero=%t summary=%q captions=%q summary-key=%s caption-key=%s", metric, zero, summary, captions, summaryRead.key, captionRead.key)
			if summaryRead.key != driverTieKey || captionRead.key != driverTieKey {
				t.Errorf("TIE %s zero=%t: tie keys summary %q caption %q, want %q in both", metric, zero, summaryRead.key, captionRead.key, driverTieKey)
			}
			if !strings.HasSuffix(summary, " driven by tie/a, tie/b, tie/c.") {
				t.Errorf("TIE %s zero=%t: summary %q, want tie/a, tie/b, tie/c (the string order)", metric, zero, summary)
			}
			if len(captions) != 1 || !strings.HasSuffix(captions[0], " appears to have moved most in tie/a, tie/b and tie/c.") {
				t.Errorf("TIE %s zero=%t: captions %q, want tie/a, tie/b and tie/c (the summary's order)", metric, zero, captions)
			}
		}
	}
}
