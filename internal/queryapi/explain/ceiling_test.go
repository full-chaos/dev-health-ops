package explain

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

// CHAOS-13088: a metric stored per team, for a caller held to a repository ceiling, is recomputed from the work items
// linked to the allowed repositories; its value, drivers, contributors and source never come from the daily tables.

const (
	ceilRepoA = "11111111-1111-1111-1111-111111111111"
	ceilRepoB = "22222222-2222-2222-2222-222222222222"
)

type ceilItem struct {
	id, team string
	done     int // the day of 2026-01 it is done; 0 = open
	links    []string
}

// ceilItems: X (T1) and W (T2) are linked to A and done on the 3rd and 4th; Y is linked only to B, Z to nothing. Every
// item is blocked from the 2nd to the 3rd.
func ceilItems() []ceilItem {
	return []ceilItem{
		{"jira:OPS-1", "T1", 3, []string{ceilRepoA}},
		{"jira:OPS-4", "T2", 4, []string{ceilRepoA}},
		{"jira:OPS-2", "T1", 3, []string{ceilRepoB}},
		{"jira:OPS-3", "T1", 3, nil},
	}
}

func jan(d int) time.Time { return time.Date(2026, 1, d, 0, 0, 0, 0, time.UTC) }

type ceilWorld struct {
	t          *testing.T
	fail       string
	statements []string
	bindings   [][]dhclickhouse.Binding
}

func ceilBound(bindings []dhclickhouse.Binding, name string) []string {
	for _, b := range bindings {
		if b.Name == name {
			values, _ := b.Value.([]string)
			return values
		}
	}
	return nil
}

func (w *ceilWorld) linked(bindings []dhclickhouse.Binding) []ceilItem {
	allowed := map[string]bool{}
	for _, id := range ceilBound(bindings, "link_repo_ids") {
		allowed[id] = true
	}
	var out []ceilItem
	for _, item := range ceilItems() {
		for _, repo := range item.links {
			if allowed[repo] {
				out = append(out, item)
				break
			}
		}
	}
	return out
}

func (w *ceilWorld) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	w.statements = append(w.statements, query)
	w.bindings = append(w.bindings, bindings)
	if w.fail != "" && strings.Contains(query, w.fail) {
		return nil, errors.New("max_result_rows exceeded")
	}
	var rows [][]any
	switch {
	case strings.Contains(query, "work_item_metrics_daily") || strings.Contains(query, "work_item_state_durations_daily"):
		w.t.Fatalf("a ceiling read reached a daily aggregate:\n%s", query)
	case strings.Contains(query, "FROM work_item_transitions"):
		for _, item := range w.linked(bindings) {
			rows = append(rows,
				[]any{item.id, jan(1).Add(time.Hour), "todo", "in_progress"},
				[]any{item.id, jan(2), "in_progress", "blocked"},
				[]any{item.id, jan(3), "blocked", "in_progress"},
				[]any{item.id, jan(item.done).Add(12 * time.Hour), "in_progress", "done"})
		}
	case strings.Contains(query, "FROM work_item_team_attributions"):
		for _, item := range w.linked(bindings) {
			rows = append(rows, []any{ceilRepoA, "jira", item.id, item.team, "Team " + item.team})
		}
	case strings.Contains(query, "FROM work_item_dependenc"):
	case strings.Contains(query, "FROM work_items FINAL"):
		for _, item := range w.linked(bindings) {
			completed := jan(item.done).Add(12 * time.Hour)
			started := jan(1).Add(time.Hour)
			rows = append(rows, []any{ceilRepoA, jan(9), item.id, "jira", "done", "OPS", "", "", "", jan(1),
				&completed, "task", []string{"ann"}, &started, &completed, (*float64)(nil)})
		}
	}
	return &ceilRows{rows: rows}, nil
}

type ceilRows struct {
	rows  [][]any
	index int
}

func (r *ceilRows) Next() bool   { r.index++; return r.index <= len(r.rows) }
func (r *ceilRows) Err() error   { return nil }
func (r *ceilRows) Close() error { return nil }
func (r *ceilRows) Scan(dest ...any) error {
	row := r.rows[r.index-1]
	if len(row) != len(dest) {
		return fmt.Errorf("ceilRows: %d values for %d destinations", len(row), len(dest))
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

type ceilMatcherScan struct {
	ids   []string
	index int
}

func (s *ceilMatcherScan) Next() bool   { s.index++; return s.index <= len(s.ids) }
func (s *ceilMatcherScan) Close() error { return nil }
func (s *ceilMatcherScan) Err() error   { return nil }
func (s *ceilMatcherScan) Scan(dest ...any) error {
	*dest[0].(*string) = fmt.Sprintf("acme/r%d", s.index)
	*dest[1].(*[]string) = []string{s.ids[s.index-1]}
	return nil
}

type ceilMatcher struct{ ids []string }

func (c ceilMatcher) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return &ceilMatcherScan{ids: c.ids}, nil
}

func ceilingFor(t *testing.T, ids ...string) teamscope.Ceiling {
	t.Helper()
	keys := make([]string, len(ids))
	for i := range ids {
		keys[i] = fmt.Sprintf("acme/r%d", i+1)
	}
	ceiling, err := teamscope.ResolveCeiling(context.Background(), ceilMatcher{ids: ids}, pepgate.Allowlist("0b6a1c9e-55d4-4a43-9e34-6f4c1d2b3a10", keys))
	if err != nil || !ceiling.Restricted() {
		t.Fatalf("ceiling %v restricted %v err %v", ceiling, ceiling.Restricted(), err)
	}
	return ceiling
}

func ceilExplain(t *testing.T, world *ceilWorld, metric string) (*Response, error) {
	t.Helper()
	reader, err := NewReader(world)
	if err != nil {
		t.Fatal(err)
	}
	return BuildExplainResponse(context.Background(), reader, "org-1", Params{
		Metric: metric, StartDay: jan(1), EndDay: jan(6), CompareStart: jan(27).AddDate(0, -1, 0), CompareEnd: jan(1),
		ScopeLevel: "org", Ceiling: ceilingFor(t, ceilRepoA), Now: jan(9),
	})
}

// C1-C3: throughput counts X and W (linked to A), never Y (B only) or Z (no link); each team is its own driver and
// contributor; the source is the linked items' provider; the answer says it is held to the ceiling.
func TestExplainThroughputUnderACeilingIsTheLinkedItems(t *testing.T) {
	world := &ceilWorld{t: t}
	resp, err := ceilExplain(t, world, "throughput")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Value != 2 || !resp.HasData || resp.HasPriorData {
		t.Fatalf("value %v has %v prior %v, want 2 completed items in the window and none before", resp.Value, resp.HasData, resp.HasPriorData)
	}
	var ids []string
	for _, c := range resp.Contributors {
		ids = append(ids, fmt.Sprintf("%s=%v", c.ID, c.Value))
	}
	if fmt.Sprint(ids) != "[T1=1 T2=1]" || len(resp.Drivers) != 2 || resp.Drivers[0].DeltaPct != nil {
		t.Fatalf("contributors %v drivers %+v, want T1 and T2 with one item each and no delta", ids, resp.Drivers)
	}
	if resp.Source == nil || *resp.Source != "jira" || fmt.Sprint(resp.NarrowedCounts) != "[{throughput repository ceiling}]" {
		t.Fatalf("source %v narrowed %v", resp.Source, resp.NarrowedCounts)
	}
}

// The state metric reads the state recompute: X is blocked 24 hours on the 2nd, W too; Y's hours are not counted.
func TestExplainBlockedWorkUnderACeilingIsTheLinkedItems(t *testing.T) {
	resp, err := ceilExplain(t, &ceilWorld{t: t}, "blocked_work")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Value != 48 || !resp.HasData {
		t.Fatalf("blocked hours %v has %v, want 48 (X and W, 24 h each)", resp.Value, resp.HasData)
	}
}

// C13: a bound hit of the recompute is an error of the request (the world fails the test if a daily table is read).
func TestExplainUnderACeilingNeverFallsBackToTheDailyTables(t *testing.T) {
	for _, metric := range []string{"throughput", "cycle_time", "wip_saturation", "blocked_work"} {
		if resp, err := ceilExplain(t, &ceilWorld{t: t, fail: "FROM work_items FINAL"}, metric); err == nil {
			t.Fatalf("%s: a bound hit answered %+v", metric, resp)
		}
	}
}

// A metric stored per repository takes the ceiling on its repository column; a scope repository outside the ceiling
// is never named, not even by its URL.
func TestExplainRepositoryMetricUnderACeilingCarriesTheCeiling(t *testing.T) {
	world := &ceilWorld{t: t}
	reader, _ := NewReader(world)
	_, err := BuildExplainResponse(context.Background(), reader, "org-1", Params{
		Metric: "review_latency", StartDay: jan(1), EndDay: jan(6), CompareStart: jan(27).AddDate(0, -1, 0), CompareEnd: jan(1),
		ScopeLevel: "org", Ceiling: ceilingFor(t, ceilRepoA), Now: jan(9),
	})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for i, statement := range world.statements {
		if !strings.Contains(statement, "FROM repo_metrics_daily") {
			continue
		}
		found = true
		if !strings.Contains(statement, "repo_id IN {ceiling_ids:Array(String)}") || !reflect.DeepEqual(ceilBound(world.bindings[i], teamscope.BindingCeilingIDs), []string{ceilRepoA}) {
			t.Fatalf("a repository metric read has no ceiling over [%s]:\n%s", ceilRepoA, statement)
		}
	}
	if !found {
		t.Fatal("no repository metric read ran")
	}
}

// The pure rules: the aggregate skips NULL, the driver percent is the SQL rule.
func TestExplainLinkedRulesAreTheStatementRules(t *testing.T) {
	one, two := 1.0, 2.0
	if v := aggregate("avg", []*float64{&one, nil, &two}); v == nil || *v != 1.5 {
		t.Fatalf("avg = %v, want 1.5 over the stored values", v)
	}
	if v := aggregate("sum", []*float64{nil}); v != nil {
		t.Fatalf("sum over no stored value = %v, want NULL", *v)
	}
	zero := 0.0
	for _, tc := range []struct {
		current, previous *float64
		present           bool
		want              string
	}{
		{&two, &one, true, "100"}, {&zero, &zero, true, "0"}, {&one, &zero, true, "<nil>"}, {nil, &one, true, "<nil>"}, {&one, &one, false, "<nil>"},
	} {
		got := driverPercent(tc.current, tc.previous, tc.present)
		text := "<nil>"
		if got != nil {
			text = fmt.Sprint(*got)
		}
		if text != tc.want {
			t.Errorf("driverPercent(%v, %v, %v) = %s, want %s", tc.current, tc.previous, tc.present, text, tc.want)
		}
	}
}

// repoWorld answers the resolution of a scope repository with B, and records every statement.
type repoWorld struct{ ceilWorld }

func (w *repoWorld) Query(ctx context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(query, "SELECT toString(id) AS id\nFROM repos FINAL") {
		w.statements = append(w.statements, query)
		w.bindings = append(w.bindings, bindings)
		return &ceilRows{rows: [][]any{{ceilRepoB}}}, nil
	}
	return w.ceilWorld.Query(ctx, query, bindings)
}

// A scope repository outside the ceiling is never named: its id reaches no read of URLs or providers, and the answer
// carries no source_url, for a metric stored per team or per repository.
func TestExplainNeverNamesAScopeRepositoryOutsideTheCeiling(t *testing.T) {
	for _, metric := range []string{"throughput", "review_latency"} {
		world := &repoWorld{ceilWorld{t: t}}
		reader, _ := NewReader(world)
		resp, err := BuildExplainResponse(context.Background(), reader, "org-1", Params{
			Metric: metric, StartDay: jan(1), EndDay: jan(6), CompareStart: jan(27).AddDate(0, -1, 0), CompareEnd: jan(1),
			ScopeLevel: "repo", ScopeIDs: []string{ceilRepoB}, Ceiling: ceilingFor(t, ceilRepoA), Now: jan(9),
		})
		if err != nil {
			t.Fatal(err)
		}
		if resp.SourceURL != nil {
			t.Fatalf("%s: source_url %q of a repository outside the ceiling", metric, *resp.SourceURL)
		}
		for i, statement := range world.statements {
			if strings.Contains(statement, "source_url") || strings.Contains(statement, "AS provider") {
				for _, id := range ceilBound(world.bindings[i], "repo_ids") {
					if id == ceilRepoB {
						t.Fatalf("%s: the repository outside the ceiling reaches a read:\n%s", metric, statement)
					}
				}
			}
		}
	}
}
