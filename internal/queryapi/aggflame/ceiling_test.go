package aggflame

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-13088: the aggregated flame of a caller held to a repository ceiling: the cycle breakdown is recomputed from
// the work items linked to the allowed repositories, the throughput reads only those items, the hotspots only the
// allowed repositories.

const (
	flameRepoA = "11111111-1111-1111-1111-111111111111"
	flameRepoB = "22222222-2222-2222-2222-222222222222"
)

func jan(d int) time.Time { return time.Date(2026, 1, d, 0, 0, 0, 0, time.UTC) }

// flameWorld: X (team T1) is linked to A, Y only to B, Z to nothing. Each is todo from the 1st, in_progress from noon
// on the 2nd, and open.
type flameWorld struct {
	t          *testing.T
	fail       string
	statements []string
	bindings   [][]dhclickhouse.Binding
}

func flameBound(bindings []dhclickhouse.Binding, name string) []string {
	for _, b := range bindings {
		if b.Name == name {
			values, _ := b.Value.([]string)
			return values
		}
	}
	return nil
}

func (w *flameWorld) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	w.statements = append(w.statements, query)
	w.bindings = append(w.bindings, bindings)
	if w.fail != "" && strings.Contains(query, w.fail) {
		return nil, errors.New("max_result_rows exceeded")
	}
	links := map[string][]string{"jira:OPS-1": {flameRepoA}, "jira:OPS-2": {flameRepoB}, "jira:OPS-3": nil}
	allowed := map[string]bool{}
	for _, id := range flameBound(bindings, "link_repo_ids") {
		allowed[id] = true
	}
	var linked []string
	for _, id := range []string{"jira:OPS-1", "jira:OPS-2", "jira:OPS-3"} {
		for _, repo := range links[id] {
			if allowed[repo] {
				linked = append(linked, id)
			}
		}
	}
	var rows [][]any
	switch {
	case strings.Contains(query, "work_item_state_durations_daily"):
		w.t.Fatalf("a ceiling read reached the state table:\n%s", query)
	case strings.Contains(query, "FROM work_item_cycle_times") || strings.Contains(query, "FROM file_metrics_daily"):
	case strings.Contains(query, "FROM work_item_transitions"):
		for _, id := range linked {
			rows = append(rows, []any{id, jan(2).Add(12 * time.Hour), "todo", "in_progress"})
		}
	case strings.Contains(query, "FROM work_item_team_attributions"):
		for _, id := range linked {
			rows = append(rows, []any{flameRepoA, "jira", id, "T1", "Team One"})
		}
	case strings.Contains(query, "FROM work_item_dependenc"):
	case strings.Contains(query, "FROM work_items FINAL"):
		for _, id := range linked {
			rows = append(rows, []any{flameRepoA, jan(9), id, "jira", "in_progress", "OPS", "", "", "", jan(1),
				(*time.Time)(nil), "task", []string(nil), (*time.Time)(nil), (*time.Time)(nil), (*float64)(nil)})
		}
	}
	return &flameRows{rows: rows}, nil
}

type flameRows struct {
	rows  [][]any
	index int
}

func (r *flameRows) Next() bool   { r.index++; return r.index <= len(r.rows) }
func (r *flameRows) Err() error   { return nil }
func (r *flameRows) Close() error { return nil }
func (r *flameRows) Scan(dest ...any) error {
	row := r.rows[r.index-1]
	if len(row) != len(dest) {
		return fmt.Errorf("flameRows: %d values for %d destinations", len(row), len(dest))
	}
	for i, value := range row {
		target := reflect.ValueOf(dest[i]).Elem()
		if value == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		target.Set(reflect.ValueOf(value))
	}
	return nil
}

type flameMatcherScan struct {
	ids   []string
	index int
}

func (s *flameMatcherScan) Next() bool   { s.index++; return s.index <= len(s.ids) }
func (s *flameMatcherScan) Close() error { return nil }
func (s *flameMatcherScan) Err() error   { return nil }
func (s *flameMatcherScan) Scan(dest ...any) error {
	*dest[0].(*string) = fmt.Sprintf("acme/r%d", s.index)
	*dest[1].(*[]string) = []string{s.ids[s.index-1]}
	return nil
}

type flameMatcher struct{ ids []string }

func (c flameMatcher) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return &flameMatcherScan{ids: c.ids}, nil
}

func flameCeiling(t *testing.T, ids ...string) teamscope.Ceiling {
	t.Helper()
	keys := make([]string, len(ids))
	for i := range ids {
		keys[i] = fmt.Sprintf("acme/r%d", i+1)
	}
	ceiling, err := teamscope.ResolveCeiling(context.Background(), flameMatcher{ids: ids}, pepgate.Allowlist("0b6a1c9e-55d4-4a43-9e34-6f4c1d2b3a10", keys))
	if err != nil || !ceiling.Restricted() {
		t.Fatalf("ceiling %v restricted %v err %v", ceiling, ceiling.Restricted(), err)
	}
	return ceiling
}

func flameParams(t *testing.T, mode string) Params {
	return Params{Mode: mode, StartDay: jan(1), EndDay: jan(5), Limit: 500, Ceiling: flameCeiling(t, flameRepoA), Now: jan(9)}
}

// C1-C3: the cycle breakdown holds X's hours only: 12 h todo and 12 h in_progress on the 2nd, 48 h in_progress on the
// 3rd and 4th (the 1st has no transition before its end, as the daily job reads it).
func TestCycleBreakdownUnderACeilingIsTheLinkedItems(t *testing.T) {
	resp, err := BuildResponse(context.Background(), &flameWorld{t: t}, "org-1", flameParams(t, "cycle_breakdown"))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	var walk func(Node)
	walk = func(n Node) {
		if len(n.Children) == 0 {
			got[n.Name] += n.Value
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(resp.Root)
	if resp.Root.Value != 72 {
		t.Fatalf("root %v (%v), want 72 h of X only", resp.Root.Value, got)
	}
	if fmt.Sprint(resp.NarrowedCounts) != "[{cycle_breakdown repository ceiling}]" {
		t.Fatalf("narrowed %v", resp.NarrowedCounts)
	}
}

// C13: a bound hit is an error of the request; the state table is never the fallback.
func TestCycleBreakdownUnderACeilingNeverFallsBackToTheStateTable(t *testing.T) {
	if resp, err := BuildResponse(context.Background(), &flameWorld{t: t, fail: "FROM work_item_transitions"}, "org-1", flameParams(t, "cycle_breakdown")); err == nil {
		t.Fatalf("a bound hit answered %+v", resp)
	}
}

// The throughput reads hold wct to the linked items; the hotspot read holds file_metrics_daily to the ceiling.
func TestThroughputAndHotspotsUnderACeilingAreHeld(t *testing.T) {
	for mode, want := range map[string]struct{ marker, binding, text string }{
		"throughput":    {"FROM work_item_cycle_times AS wct FINAL", "link_repo_ids", "wct.work_item_id IN (SELECT work_item_id FROM work_graph_issue_pr FINAL"},
		"code_hotspots": {"FROM file_metrics_daily", teamscope.BindingCeilingIDs, "toString(repo_id) IN {ceiling_ids:Array(String)}"},
	} {
		world := &flameWorld{t: t}
		resp, err := BuildResponse(context.Background(), world, "org-1", flameParams(t, mode))
		if err != nil {
			t.Fatal(err)
		}
		found := 0
		for i, statement := range world.statements {
			if !strings.Contains(statement, want.marker) {
				continue
			}
			found++
			if !strings.Contains(statement, want.text) || !reflect.DeepEqual(flameBound(world.bindings[i], want.binding), []string{flameRepoA}) {
				t.Fatalf("%s: a read is not held to [%s]:\n%s", mode, flameRepoA, statement)
			}
		}
		if found == 0 {
			t.Fatalf("%s: no read ran", mode)
		}
		if mode == "code_hotspots" && resp.NarrowedCounts != nil {
			t.Fatalf("a repository-keyed tree carries a mark it does not need: %v", resp.NarrowedCounts)
		}
	}
}
