package sankey

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-13088: the expense and state flows of a caller held to a repository ceiling count the work items linked to an
// allowed repository, recomputed with the daily job's compute; an item linked only elsewhere, or not linked, is not
// counted, and the daily aggregates are never read.

const (
	repoA = "11111111-1111-1111-1111-111111111111"
	repoB = "22222222-2222-2222-2222-222222222222"
)

type worldItem struct {
	id, typ, team string
	links         []string
}

// linkedWorld holds X and W linked to A (teams T1, T2), Y linked only to B, Z linked nowhere. Each is a bug created on
// 2026-01-01 that moves from todo to in_progress at noon on 2026-01-02 and stays open.
func linkedWorld() []worldItem {
	return []worldItem{
		{"jira:OPS-1", "bug", "T1", []string{repoA}},
		{"jira:OPS-4", "bug", "T2", []string{repoA}},
		{"jira:OPS-2", "bug", "T1", []string{repoB}},
		{"jira:OPS-3", "bug", "T1", nil},
	}
}

type worldClient struct {
	t     *testing.T
	items []worldItem
	fail  string

	mu         sync.Mutex
	statements []string
	bindings   [][]dhclickhouse.Binding
}

func boundStrings(bindings []dhclickhouse.Binding, name string) []string {
	for _, b := range bindings {
		if b.Name == name {
			values, _ := b.Value.([]string)
			return values
		}
	}
	return nil
}

func (c *worldClient) linked(bindings []dhclickhouse.Binding) []worldItem {
	allowed := map[string]bool{}
	for _, id := range boundStrings(bindings, "link_repo_ids") {
		allowed[id] = true
	}
	var out []worldItem
	for _, item := range c.items {
		for _, repo := range item.links {
			if allowed[repo] {
				out = append(out, item)
				break
			}
		}
	}
	return out
}

func (c *worldClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.mu.Lock()
	c.statements = append(c.statements, query)
	c.bindings = append(c.bindings, bindings)
	c.mu.Unlock()
	return allTablesColumnsPresent(c.t, query, bindings, func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if c.fail != "" && strings.Contains(query, c.fail) {
			return nil, errors.New("max_result_rows exceeded")
		}
		created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		var rows [][]any
		switch {
		case strings.Contains(query, "work_item_metrics_daily") || strings.Contains(query, "work_item_state_durations_daily"):
			t.Fatalf("a ceiling read reached a daily aggregate:\n%s", query)
		case strings.Contains(query, "FROM work_item_transitions"):
			for _, item := range c.linked(bindings) {
				rows = append(rows, []any{item.id, created.AddDate(0, 0, 1).Add(12 * time.Hour), "todo", "in_progress"})
			}
		case strings.Contains(query, "FROM work_item_team_attributions"):
			for _, item := range c.linked(bindings) {
				rows = append(rows, []any{repoA, "jira", item.id, item.team, item.team})
			}
		case strings.Contains(query, "FROM work_item_dependencies") || strings.Contains(query, "FROM work_item_dependency_first_seen"):
		case strings.Contains(query, "FROM work_items FINAL"):
			for _, item := range c.linked(bindings) {
				rows = append(rows, []any{repoA, created.AddDate(0, 0, 9), item.id, "jira", "in_progress", "OPS", "", "", "", created,
					(*time.Time)(nil), item.typ, []string{"ann"}, (*time.Time)(nil), (*time.Time)(nil), (*float64)(nil)})
			}
		case strings.Contains(query, "FROM work_item_cycle_times"):
			rows = [][]any{{0.0}}
		}
		return &anyRows{rows: rows}, nil
	})
}

type anyRows struct {
	rows  [][]any
	index int
}

func (r *anyRows) Next() bool   { r.index++; return r.index <= len(r.rows) }
func (r *anyRows) Err() error   { return nil }
func (r *anyRows) Close() error { return nil }
func (r *anyRows) Scan(dest ...any) error {
	row := r.rows[r.index-1]
	if len(row) != len(dest) {
		return fmt.Errorf("anyRows: %d values for %d destinations", len(row), len(dest))
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

type matcherScan struct {
	ids   []string
	index int
}

func (s *matcherScan) Next() bool   { s.index++; return s.index <= len(s.ids) }
func (s *matcherScan) Close() error { return nil }
func (s *matcherScan) Err() error   { return nil }
func (s *matcherScan) Scan(dest ...any) error {
	*dest[0].(*string) = fmt.Sprintf("acme/r%d", s.index)
	*dest[1].(*[]string) = []string{s.ids[s.index-1]}
	return nil
}

type matcherClient struct{ ids []string }

func (c matcherClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return &matcherScan{ids: c.ids}, nil
}

func ceilingOf(t *testing.T, ids ...string) teamscope.Ceiling {
	t.Helper()
	keys := make([]string, len(ids))
	for i := range ids {
		keys[i] = fmt.Sprintf("acme/r%d", i+1)
	}
	ceiling, err := teamscope.ResolveCeiling(context.Background(), matcherClient{ids: ids}, pepgate.Allowlist("0b6a1c9e-55d4-4a43-9e34-6f4c1d2b3a10", keys))
	if err != nil || !ceiling.Restricted() {
		t.Fatalf("ceiling %v restricted %v err %v", ceiling, ceiling.Restricted(), err)
	}
	return ceiling
}

func linkValue(links []Link, source, target string) float64 {
	for _, link := range links {
		if link.Source == source && link.Target == target {
			return link.Value
		}
	}
	return 0
}

func ceilingParams(t *testing.T, mode string) Params {
	return Params{Mode: mode, StartDay: day(2026, 1, 1), EndDay: day(2026, 1, 5), Ceiling: ceilingOf(t, repoA)}
}

// C1, C2, C3 (state): X and W are counted (linked to A); Y (linked only to B) and Z (no link) are not. Day 1 has no
// transition before its end, so an item is skipped that day, as the daily job skips it; day 2 touches todo and
// in_progress; days 3 and 4 in_progress.
func TestStateFlowUnderACeilingCountsTheLinkedItemsOnly(t *testing.T) {
	client := &worldClient{t: t, items: linkedWorld()}
	resp, err := BuildResponse(context.Background(), client, "org-1", ceilingParams(t, "state"))
	if err != nil {
		t.Fatal(err)
	}
	if got := linkValue(resp.Links, "Todo", "In Progress"); got != 2 {
		t.Fatalf("todo -> in progress = %v, want 2 (X and W, one todo day each); links %+v", got, resp.Links)
	}
	if names := []string{resp.Nodes[0].Name, resp.Nodes[1].Name}; !reflect.DeepEqual(names, []string{"In Progress", "Todo"}) || len(resp.Nodes) != 2 {
		t.Fatalf("nodes = %+v, want In Progress (6) then Todo (2)", resp.Nodes)
	}
	if want := `[{state repository ceiling}]`; fmt.Sprint(resp.NarrowedCounts) != want {
		t.Fatalf("narrowed counts = %v, want %s", resp.NarrowedCounts, want)
	}
}

// The team scope of the flow is the team column of the recomputed rows: T1 keeps X and drops W.
func TestStateFlowUnderACeilingKeepsTheTeamScope(t *testing.T) {
	client := &worldClient{t: t, items: linkedWorld()}
	params := ceilingParams(t, "state")
	params.ScopeLevel, params.ScopeIDs = "team", []string{"T1"}
	resp, err := BuildResponse(context.Background(), client, "org-1", params)
	if err != nil {
		t.Fatal(err)
	}
	if got := linkValue(resp.Links, "Todo", "In Progress"); got != 1 {
		t.Fatalf("todo -> in progress = %v, want 1 (X only); links %+v", got, resp.Links)
	}
}

// C1, C2, C3 (expense): the new bugs are X and W; the abandoned read is held to the linked items.
func TestExpenseFlowUnderACeilingCountsTheLinkedItemsOnly(t *testing.T) {
	client := &worldClient{t: t, items: linkedWorld()}
	resp, err := BuildResponse(context.Background(), client, "org-1", ceilingParams(t, "expense"))
	if err != nil {
		t.Fatal(err)
	}
	if got := linkValue(resp.Links, "Planned work", "Unplanned work"); got != 2 {
		t.Fatalf("planned -> unplanned = %v, want 2 (the bugs X and W); links %+v", got, resp.Links)
	}
	found := false
	for i, statement := range client.statements {
		if !strings.Contains(statement, "FROM work_item_cycle_times") {
			continue
		}
		found = true
		if !strings.Contains(statement, "work_item_id IN (SELECT work_item_id FROM work_graph_issue_pr FINAL") ||
			!reflect.DeepEqual(boundStrings(client.bindings[i], "link_repo_ids"), []string{repoA}) {
			t.Fatalf("the abandoned read is not held to the items linked to [%s]:\n%s", repoA, statement)
		}
	}
	if !found {
		t.Fatal("the abandoned read did not run")
	}
}

// C13: a bound hit of the recompute is an error of the request; the daily aggregate is never the fallback (the client
// fails the test if it is read).
func TestFlowsUnderACeilingNeverFallBackToTheDailyTables(t *testing.T) {
	for _, mode := range []string{"state", "expense"} {
		client := &worldClient{t: t, items: linkedWorld(), fail: "FROM work_items FINAL"}
		if resp, err := BuildResponse(context.Background(), client, "org-1", ceilingParams(t, mode)); err == nil {
			t.Fatalf("%s: a bound hit answered %+v", mode, resp)
		}
	}
	client := &worldClient{t: t, items: linkedWorld(), fail: "FROM work_item_dependencies"}
	if resp, err := BuildResponse(context.Background(), client, "org-1", ceilingParams(t, "state")); err == nil {
		t.Fatalf("a failed blocker read answered %+v: the rows would miss the blocked hours", resp)
	}
}

// The repository-keyed flows take the ceiling on their repository column, ANDed with the request's own filter.
func TestRepositoryFlowsUnderACeilingCarryTheCeiling(t *testing.T) {
	for mode, column := range map[string]string{"investment": "repo_id IN {ceiling_ids:Array(String)}", "hotspot": "metrics.repo_id IN {ceiling_ids:Array(String)}"} {
		client := &worldClient{t: t, items: linkedWorld()}
		if _, err := BuildResponse(context.Background(), client, "org-1", ceilingParams(t, mode)); err != nil {
			t.Fatal(err)
		}
		found := false
		for i, statement := range client.statements {
			if !strings.Contains(statement, "work_unit_investments") && !strings.Contains(statement, "file_metrics_daily") || strings.Contains(statement, "system.") {
				continue
			}
			found = true
			if !strings.Contains(statement, column) || !reflect.DeepEqual(boundStrings(client.bindings[i], teamscope.BindingCeilingIDs), []string{repoA}) {
				t.Fatalf("%s: a read has no ceiling over [%s]:\n%s", mode, repoA, statement)
			}
		}
		if !found {
			t.Fatalf("%s: no repository read ran", mode)
		}
	}
}

func TestRowFilterIsTheSQLFilterOfTheTwoFlows(t *testing.T) {
	for _, tc := range []struct {
		name        string
		level       string
		ids, repos  []string
		team, scope string
		want        bool
	}{
		{"no filter", "org", nil, nil, "", "s", true},
		{"team kept", "team", []string{"T1"}, nil, "T1", "s", true},
		{"team dropped", "team", []string{"T1"}, nil, "T2", "s", false},
		{"empty team is unassigned", "team", []string{"unassigned"}, nil, "", "s", true},
		{"repo scope is a work scope", "repo", []string{"s"}, nil, "T1", "s", true},
		{"repo scope drops another work scope", "repo", []string{"s"}, nil, "T1", "x", false},
		{"what repos is a work scope", "org", nil, []string{"x"}, "T1", "x", true},
	} {
		if got := newRowFilter(tc.level, tc.ids, tc.repos).keeps(tc.team, tc.scope); got != tc.want {
			t.Errorf("%s: keeps = %v, want %v", tc.name, got, tc.want)
		}
	}
}
