//go:build integration

package home

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// An extended differential oracle (from gwc-vet-9094's, which killed 13 plants the first oracle did not). Raw rows only; the
// daily tables are written by the real executors. For every view (a set of
// repositories, with or without a team) the request-time read of the full world
// must equal the daily-table read of a world that holds only the items linked
// to the view's repositories.

const viewWorldR3 = "00000000-0000-4000-8000-0000000000a3"

type viewWorldStored struct {
	repo, synced string
	status, done string // overrides when not ""
}
type viewWorldAttr struct{ repo, team, computed string }
type viewWorldLink struct {
	repo, provenance string
	pr               int
}
type viewWorldItem struct {
	id, provider, typ, status         string
	projectKey, projectID, nativeTeam string
	created, started, done, closed    string
	assignee                          string
	points                            float64
	stored                            []viewWorldStored
	attrs                             []viewWorldAttr
	links                             []viewWorldLink
}

const viewWorldT = "2026-08-26 00:00:00"

func st(repo string) []viewWorldStored { return []viewWorldStored{{repo: repo, synced: viewWorldT}} }
func at(repo, team string) []viewWorldAttr {
	return []viewWorldAttr{{repo: repo, team: team, computed: viewWorldT}}
}

func viewWorldItems() []viewWorldItem {
	return []viewWorldItem{
		{id: "gh:a/r#1", provider: "github", typ: "bug", status: "done", projectID: "a/r", created: "2026-08-12 09:00:00", started: "2026-08-13 10:00:00", done: "2026-08-19 12:00:00", assignee: "ann", points: 3, stored: st(oracleR1), attrs: at(oracleR1, "t-alpha"), links: []viewWorldLink{{oracleR1, "native", 100}}},
		{id: "gh:a/r#2", provider: "github", typ: "task", status: "done", projectID: "a/r", created: "2026-08-12 09:00:00", started: "2026-08-14 08:00:00", done: "2026-08-20 17:30:00", assignee: "bob", points: 5, stored: st(oracleR1), attrs: at(oracleR1, "t-alpha"), links: []viewWorldLink{{oracleR1, "explicit_text", 101}, {oracleR2, "native", 102}}},
		{id: "gh:a/r#3", provider: "github", typ: "task", status: "in_progress", projectID: "a/r", created: "2026-08-15 09:00:00", started: "2026-08-16 09:00:00", stored: st(oracleR1), attrs: at(oracleR1, "t-alpha"), links: []viewWorldLink{{oracleR1, "heuristic", 103}}},
		{id: "jira:ABC-1", provider: "jira", typ: "bug", status: "done", projectKey: "ABC", created: "2026-08-10 09:00:00", started: "2026-08-11 09:00:00", done: "2026-08-22 10:00:00", assignee: "cy", stored: st(oracleR2), attrs: at(oracleR2, "t-beta"), links: []viewWorldLink{{oracleR2, "native", 104}}},
		{id: "jira:ABC-2", provider: "jira", typ: "task", status: "in_progress", projectKey: "ABC", created: "2026-08-17 09:00:00", started: "2026-08-18 09:00:00", stored: st(oracleR2), attrs: at(oracleR2, "t-beta"), links: []viewWorldLink{{oracleR2, "native", 105}}},
		{id: "gh:a/r#6", provider: "github", typ: "task", status: "todo", projectID: "a/r", created: "2026-08-19 09:00:00", stored: st(oracleR2), links: []viewWorldLink{{oracleR2, "explicit_text", 106}}},
		{id: "linear:OPS-1", provider: "linear", typ: "task", status: "done", projectID: "ops", created: "2026-08-13 09:00:00", started: "2026-08-14 09:00:00", done: "2026-08-24 15:00:00", assignee: "dee", stored: st(oracleR2), attrs: at(oracleR2, "t-beta"), links: []viewWorldLink{{oracleR2, "native", 107}}},
		{id: "gitlab:grp/proj#9", provider: "gitlab", typ: "task", status: "done", projectID: "grp/proj", created: "2026-08-13 09:00:00", started: "2026-08-15 09:00:00", done: "2026-08-21 11:00:00", assignee: "eve", stored: st(oracleR2), attrs: at(oracleR2, "t-beta"), links: []viewWorldLink{{oracleR2, "native", 108}}},
		// two tiers in ONE repository: best tier native, counted once, not multi-repository
		{id: "gh:a/r#20", provider: "github", typ: "task", status: "done", projectID: "a/r", created: "2026-08-13 09:00:00", started: "2026-08-14 09:00:00", done: "2026-08-21 13:00:00", assignee: "ann", points: 2, stored: st(oracleR1), attrs: at(oracleR1, "t-alpha"), links: []viewWorldLink{{oracleR1, "heuristic", 200}, {oracleR1, "native", 201}}},
		// stored under repository 3, linked to repository 1 only (own repository column differs from the link)
		{id: "gitlab:grp/proj#30", provider: "gitlab", typ: "bug", status: "done", projectID: "grp/proj", created: "2026-08-12 09:00:00", started: "2026-08-13 09:00:00", done: "2026-08-20 09:00:00", assignee: "gil", stored: st(viewWorldR3), attrs: at(viewWorldR3, "t-gamma"), links: []viewWorldLink{{oracleR1, "explicit_text", 300}}},
		// stored under repository 1, linked to repository 2 only
		{id: "gh:a/r#40", provider: "github", typ: "task", status: "done", projectID: "a/r", created: "2026-08-12 09:00:00", started: "2026-08-15 09:00:00", done: "2026-08-22 09:00:00", assignee: "ann", stored: st(oracleR1), attrs: at(oracleR1, "t-alpha"), links: []viewWorldLink{{oracleR2, "native", 400}}},
		// linear team-only scope, heuristic links to both repositories, blocked by gh:a/r#2 from 08-10 (so the first day of the comparison window holds blocked hours)
		{id: "linear:T-5", provider: "linear", typ: "task", status: "in_progress", nativeTeam: "TEAMK", created: "2026-08-09 09:00:00", started: "2026-08-10 09:00:00", assignee: "hal", stored: st(oracleR2), attrs: at(oracleR2, "t-beta"), links: []viewWorldLink{{oracleR1, "heuristic", 500}, {oracleR2, "heuristic", 501}}},
		// window-edge items
		{id: "jira:XY-7", provider: "jira", typ: "story", status: "done", projectKey: "XY", projectID: "9001", created: "2026-08-10 09:00:00", started: "2026-08-12 09:00:00", done: "2026-08-18 00:00:00", assignee: "zed", stored: st(oracleR1), attrs: at(oracleR1, "   "), links: []viewWorldLink{{oracleR1, "native", 600}}},
		{id: "jira:XY-8", provider: "jira", typ: "story", status: "done", projectKey: "XY", created: "2026-08-05 09:00:00", started: "2026-08-06 09:00:00", done: "2026-08-11 00:00:00", assignee: "zed", stored: st(oracleR1), attrs: at(oracleR1, "t-alpha"), links: []viewWorldLink{{oracleR1, "native", 601}}},
		{id: "jira:XY-9", provider: "jira", typ: "story", status: "done", projectKey: "XY", created: "2026-08-12 09:00:00", started: "2026-08-13 09:00:00", done: "2026-08-25 00:00:00", assignee: "zed", stored: st(oracleR2), attrs: at(oracleR2, "t-beta"), links: []viewWorldLink{{oracleR2, "native", 602}}},
		{id: "jira:XY-6", provider: "jira", typ: "story", status: "done", projectKey: "XY", created: "2026-08-05 09:00:00", started: "2026-08-06 09:00:00", done: "2026-08-10 23:59:59", assignee: "zed", stored: st(oracleR1), attrs: at(oracleR1, "t-alpha"), links: []viewWorldLink{{oracleR1, "native", 603}}},
		// canceled
		{id: "gh:a/r#50", provider: "github", typ: "task", status: "canceled", projectID: "a/r", created: "2026-08-13 09:00:00", started: "2026-08-14 09:00:00", closed: "2026-08-19 09:00:00", assignee: "ann", stored: st(oracleR1), attrs: at(oracleR1, "t-alpha"), links: []viewWorldLink{{oracleR1, "native", 700}}},
		// stored under two repositories (same state); the newer row is repository 1's (v2: so a read of any row's attribution takes the wrong team); each row has its own attribution
		{id: "gh:a/r#60", provider: "github", typ: "task", status: "done", projectID: "a/r", created: "2026-08-14 09:00:00", started: "2026-08-15 09:00:00", done: "2026-08-23 09:00:00", assignee: "ann", points: 1,
			stored: []viewWorldStored{{repo: oracleR1, synced: viewWorldT}, {repo: oracleR2, synced: "2026-08-25 23:00:00"}},
			attrs:  []viewWorldAttr{{oracleR1, "t-alpha", viewWorldT}, {oracleR2, "t-beta", viewWorldT}}, links: []viewWorldLink{{oracleR2, "heuristic", 800}}},
		// an older primary snapshot (alpha) and the current one (beta)
		{id: "gh:a/r#70", provider: "github", typ: "task", status: "done", projectID: "a/r", created: "2026-08-12 09:00:00", started: "2026-08-13 09:00:00", done: "2026-08-19 09:00:00", assignee: "ann",
			stored: st(oracleR1), attrs: []viewWorldAttr{{oracleR1, "t-zeta", "2026-08-24 00:00:00"}, {oracleR1, "t-beta", viewWorldT}}, links: []viewWorldLink{{oracleR1, "native", 900}}},
		// re-synced: an older version in progress, the newer one done
		{id: "gh:a/r#90", provider: "github", typ: "task", status: "done", projectID: "a/r", created: "2026-08-12 09:00:00", started: "2026-08-13 09:00:00", done: "2026-08-21 09:00:00", assignee: "bob",
			stored: []viewWorldStored{{repo: oracleR1, synced: "2026-08-20 00:00:00", status: "in_progress", done: "NULL"}, {repo: oracleR1, synced: viewWorldT}}, attrs: at(oracleR1, "t-alpha"), links: []viewWorldLink{{oracleR1, "explicit_text", 901}}},
		// done with no start (no cycle time), no assignee, no team
		{id: "gh:a/r#95", provider: "github", typ: "task", status: "done", projectID: "a/r", created: "2026-08-12 09:00:00", done: "2026-08-22 09:00:00", stored: st(oracleR1), links: []viewWorldLink{{oracleR1, "native", 902}}},
		// v3: an open blocker older than both windows, in both repositories' views
		{id: "gh:a/r#99", provider: "github", typ: "task", status: "in_progress", projectID: "a/r", created: "2026-08-05 09:00:00", started: "2026-08-06 09:00:00", assignee: "ann", stored: st(oracleR1), attrs: at(oracleR1, "t-alpha"), links: []viewWorldLink{{oracleR1, "heuristic", 990}, {oracleR2, "heuristic", 991}}},
		// no link at all: in no repository's view
		{id: "gh:a/r#80", provider: "github", typ: "task", status: "done", projectID: "a/r", created: "2026-08-12 09:00:00", started: "2026-08-13 09:00:00", done: "2026-08-20 09:00:00", assignee: "ann", stored: st(oracleR1), attrs: at(oracleR1, "t-alpha")},
	}
}

func viewWorldLinkedTo(item viewWorldItem, repos ...string) bool {
	for _, link := range item.links {
		for _, repo := range repos {
			if link.repo == repo {
				return true
			}
		}
	}
	return false
}

func viewWorldTS(value string) string {
	if value == "" || value == "NULL" {
		return "NULL"
	}
	return "toDateTime64('" + value + "', 3, 'UTC')"
}

func viewWorldSeed(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, items []viewWorldItem, keep func(viewWorldItem) bool) {
	t.Helper()
	exec := func(statement string) {
		t.Helper()
		if err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	for _, repo := range []struct{ id, name string }{{oracleR1, "a/r1"}, {oracleR2, "a/r2"}, {viewWorldR3, "a/r3"}} {
		exec(fmt.Sprintf(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (toUUID('%s'), '%s', 'github', '%s', %s, %s)`, repo.id, repo.name, org, viewWorldTS(viewWorldT), viewWorldTS(viewWorldT)))
	}
	kept := map[string]bool{}
	for _, item := range items {
		if !keep(item) {
			continue
		}
		kept[item.id] = true
		assignees := "[]"
		if item.assignee != "" {
			assignees = "['" + item.assignee + "']"
		}
		for _, row := range item.stored {
			status, done := item.status, item.done
			if row.status != "" {
				status = row.status
			}
			if row.done != "" {
				done = row.done
			}
			exec(fmt.Sprintf(`INSERT INTO work_items (repo_id, work_item_id, provider, status, type, assignees, story_points, project_key, project_id, native_team_key, project_name, created_at, started_at, completed_at, closed_at, org_id, last_synced)
VALUES (toUUID('%s'), '%s', '%s', '%s', '%s', %s, %v, '%s', '%s', '%s', '', %s, %s, %s, %s, '%s', %s)`,
				row.repo, item.id, item.provider, status, item.typ, assignees, item.points, item.projectKey, item.projectID, item.nativeTeam,
				viewWorldTS(item.created), viewWorldTS(item.started), viewWorldTS(done), viewWorldTS(item.closed), org, viewWorldTS(row.synced)))
		}
		repo := item.stored[0].repo
		if item.started != "" {
			exec(fmt.Sprintf(`INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
VALUES (toUUID('%s'), '%s', %s, '%s', 'todo', 'in_progress', 'Todo', 'In Progress', '', '%s', %s)`, repo, item.id, viewWorldTS(item.started), item.provider, org, viewWorldTS(viewWorldT)))
		}
		if item.done != "" {
			exec(fmt.Sprintf(`INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
VALUES (toUUID('%s'), '%s', %s, '%s', 'in_progress', 'done', 'In Progress', 'Done', '', '%s', %s)`, repo, item.id, viewWorldTS(item.done), item.provider, org, viewWorldTS(viewWorldT)))
		}
		for _, a := range item.attrs {
			exec(fmt.Sprintf(`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
VALUES ('%s', toUUID('%s'), '%s', '%s', '%s', '%s', 'project_ownership', 1, 'high', '', %s)`, org, a.repo, item.id, item.provider, a.team, a.team, viewWorldTS(a.computed)))
		}
		for _, link := range item.links {
			exec(fmt.Sprintf(`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
VALUES (toUUID('%s'), '%s', %d, 1.0, '%s', '', %s, '%s')`, link.repo, item.id, link.pr, link.provenance, viewWorldTS(viewWorldT), org))
		}
	}
	// a link whose item is not stored: invisible, in every world
	exec(fmt.Sprintf(`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
VALUES (toUUID('%s'), 'gh:ghost#1', 999, 1.0, 'native', '', %s, '%s')`, oracleR1, viewWorldTS(viewWorldT), org))
	dep := func(source, target, from string) {
		if kept[source] && kept[target] {
			exec(fmt.Sprintf(`INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id, relationship_semantics_version, relation_started_at)
VALUES ('%s', '%s', 'blocks', 'blocks', toDateTime64('%s', 3), '%s', 'canonical-blocks.v2', toDateTime64('%s', 3))`, source, target, viewWorldT, org, from))
		}
	}
	dep("gh:a/r#6", "jira:ABC-2", "2026-08-19 10:00:00")
	dep("gh:a/r#99", "linear:T-5", "2026-08-10 12:00:00")
}

func viewWorldFill(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string) {
	t.Helper()
	metrics, err := daily.NewWorkItemExecutor(admin)
	if err != nil {
		t.Fatal(err)
	}
	state, err := daily.NewWorkItemStateExecutor(admin)
	if err != nil {
		t.Fatal(err)
	}
	from, to := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	for day := from; day.Before(to); day = day.AddDate(0, 0, 1) {
		partition := daily.Partition{
			ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
			RepoIDs: []daily.RepositoryID{daily.RepositoryID(oracleR1), daily.RepositoryID(oracleR2), daily.RepositoryID(viewWorldR3)},
		}
		run := daily.Run{OrganizationID: org, TargetDay: day}
		if _, err := metrics.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("work_item family %s: %v", day.Format("2006-01-02"), err)
		}
		if _, err := state.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("work_item_state family %s: %v", day.Format("2006-01-02"), err)
		}
	}
}

// viewWorldCompare compares EVERY field of MetricDelta by reflection. The excluded
// fields are the ones the two reads must differ in, each with its reason.
func viewWorldCompare(t *testing.T, label string, got, want MetricDelta) (line string, equal bool) {
	t.Helper()
	excluded := map[string]string{
		"RepoFilterApplied":      "nil with no repository named, true with one",
		"RepoLinkState":          "request-time only",
		"RepoLinkBasis":          "request-time only",
		"RepoLinkMultiRepoItems": "request-time only",
		"RepoLinkCoverage":       "request-time only",
	}
	equal = true
	gv, wv := reflect.ValueOf(got), reflect.ValueOf(want)
	compared := 0
	for i := 0; i < gv.NumField(); i++ {
		name := gv.Type().Field(i).Name
		if _, skip := excluded[name]; skip {
			continue
		}
		compared++
		g, w := gv.Field(i).Interface(), wv.Field(i).Interface()
		same := reflect.DeepEqual(g, w)
		if !same {
			switch gt := g.(type) {
			case float64:
				same = math.Abs(gt-w.(float64)) <= 1e-9*math.Max(1, math.Abs(w.(float64)))
			case *float64:
				wt := w.(*float64)
				same = gt != nil && wt != nil && math.Abs(*gt-*wt) <= 1e-9*math.Max(1, math.Abs(*wt))
			case []SparkPoint:
				wt := w.([]SparkPoint)
				same = len(gt) == len(wt)
				for j := 0; same && j < len(gt); j++ {
					same = gt[j].TS == wt[j].TS && math.Abs(gt[j].Value-wt[j].Value) <= 1e-9*math.Max(1, math.Abs(wt[j].Value))
				}
			}
		}
		if !same {
			equal = false
			t.Errorf("%s: field %s: request-time %s, daily tables %s", label, name, viewWorldShow(g), viewWorldShow(w))
		}
	}
	if compared != gv.NumField()-len(excluded) || compared < 9 {
		t.Fatalf("%s: compared %d fields of %d: the comparison measured too little", label, compared, gv.NumField())
	}
	pct := "null"
	if got.DeltaPct != nil {
		pct = fmt.Sprintf("%.4f", *got.DeltaPct)
	}
	return fmt.Sprintf("%-58s value %-12.6g data %-5v prior %-5v pct %-9s points %d state %s", label, got.Value, got.HasData, got.HasPriorData, pct, len(got.Spark), viewWorldStr(got.RepoLinkState)), equal
}

func viewWorldStr(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func viewWorldShow(v any) string {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return "<nil>"
		}
		return fmt.Sprintf("%v", rv.Elem().Interface())
	}
	return fmt.Sprintf("%v", v)
}

func TestRepoLinkedEveryViewEqualsItsWorld(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	items := viewWorldItems()
	start, end := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	cmpStart := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)

	const full = "vet-full"
	viewWorldSeed(ctx, t, admin, full, items, func(viewWorldItem) bool { return true })
	viewWorldFill(ctx, t, admin, full)

	views := []struct {
		name  string
		repos []string
		refs  []string // what the request names
	}{
		{"r1", []string{oracleR1}, []string{oracleR1}},
		{"r2", []string{oracleR2}, []string{oracleR2}},
		{"r1+r2", []string{oracleR1, oracleR2}, []string{oracleR1, oracleR2}},
		{"r1-by-name", []string{oracleR1}, []string{"a/r1"}},
		{"r2-as-repo-scope", []string{oracleR2}, nil},
	}
	teams := []string{"", "t-alpha", "t-beta", "t-gamma", "unassigned"}
	var report []string
	withData := map[string]int{}
	for _, view := range views {
		world := "view-world-" + view.name
		repos := view.repos
		viewWorldSeed(ctx, t, admin, world, items, func(item viewWorldItem) bool { return viewWorldLinkedTo(item, repos...) })
		viewWorldFill(ctx, t, admin, world)
		for _, team := range teams {
			for _, name := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
				spec := metricSpecByName(t, name)
				request := Filters{What: WhatFilter{Repos: view.refs}}
				worldFilter := Filters{}
				if view.refs == nil {
					request = Filters{Scope: ScopeFilter{Level: "repo", IDs: view.repos}}
				}
				if team != "" {
					if view.refs == nil {
						continue // one scope level per request
					}
					request.Scope = ScopeFilter{Level: "team", IDs: []string{team}}
					worldFilter.Scope = ScopeFilter{Level: "team", IDs: []string{team}}
				}
				got, err := computeMetricDelta(ctx, client, spec, start, end, cmpStart, start, request, full, end)
				if err != nil {
					t.Fatalf("%s %s %s: %v", view.name, team, name, err)
				}
				want, err := computeMetricDelta(ctx, client, spec, start, end, cmpStart, start, worldFilter, world, end)
				if err != nil {
					t.Fatalf("%s %s %s (world): %v", view.name, team, name, err)
				}
				label := fmt.Sprintf("%s team=%q %s", view.name, team, name)
				line, _ := viewWorldCompare(t, label, got, want)
				report = append(report, line)
				if want.HasData {
					withData[name]++
				}
				if got.RepoFilterApplied == nil || !*got.RepoFilterApplied {
					t.Errorf("%s: repoFilterApplied = %v, want true", label, got.RepoFilterApplied)
				}
			}
		}
	}
	for _, name := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		if withData[name] < 4 {
			t.Errorf("%s: only %d compared views hold data in the daily tables: the oracle compared too little", name, withData[name])
		}
	}
	// A repository no pull request links to an item of: nothing, although an item is STORED under it.
	for _, name := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		got, err := computeMetricDelta(ctx, client, metricSpecByName(t, name), start, end, cmpStart, start, Filters{What: WhatFilter{Repos: []string{viewWorldR3}}}, full, end)
		if err != nil {
			t.Fatal(err)
		}
		report = append(report, fmt.Sprintf("r3 (stored item, no link) %-20s value %v data %v state %s basis %+v", name, got.Value, got.HasData, viewWorldStr(got.RepoLinkState), *got.RepoLinkBasis))
		if got.HasData || got.HasPriorData || got.Value != 0 || viewWorldStr(got.RepoLinkState) != repoLinkNoLinks {
			t.Errorf("r3 %s: %+v, want no data, no_links (the item's own repository column is not a link)", name, got)
		}
	}
	// basis / multi / coverage per view
	for _, cell := range []struct {
		name  string
		repos []string
		basis RepoLinkBasis
		multi int
	}{
		// r1: #1 n, #2 text(here), #3 heur, #20 native (best of heuristic+native), proj#30 text, T-5 heur, XY-7 n, XY-8 n, #50 n, #70 n, #90 text, #95 n; XY-6 is outside the window
		{"r1", []string{oracleR1}, RepoLinkBasis{Native: 7, ExplicitText: 3, Heuristic: 3}, 3},
		// r2: #2 native, ABC-1 n, ABC-2 n, #6 text, OPS-1 n, proj#9 n, #40 n, T-5 heur, XY-9 n, #60 heur
		{"r2", []string{oracleR2}, RepoLinkBasis{Native: 7, ExplicitText: 1, Heuristic: 3}, 3},
		{"r1+r2", []string{oracleR1, oracleR2}, RepoLinkBasis{Native: 14, ExplicitText: 3, Heuristic: 4}, 3},
	} {
		got, err := computeMetricDelta(ctx, client, metricSpecByName(t, "throughput"), start, end, cmpStart, start, Filters{What: WhatFilter{Repos: cell.repos}}, full, end)
		if err != nil {
			t.Fatal(err)
		}
		report = append(report, fmt.Sprintf("basis %-6s %+v multi %d coverage %+v throughput %v", cell.name, *got.RepoLinkBasis, *got.RepoLinkMultiRepoItems, *got.RepoLinkCoverage, got.Value))
		if *got.RepoLinkBasis != cell.basis || *got.RepoLinkMultiRepoItems != cell.multi {
			t.Errorf("basis %s = %+v multi %d, want %+v multi %d", cell.name, *got.RepoLinkBasis, *got.RepoLinkMultiRepoItems, cell.basis, cell.multi)
		}
		// 21 stored items in the window (22 less XY-6, completed before it), 20 of them linked.
		if *got.RepoLinkCoverage != (RepoLinkCoverage{LinkedItems: 21, ItemsInWindow: 22}) {
			t.Errorf("coverage %s = %+v, want 20 of 21", cell.name, *got.RepoLinkCoverage)
		}
	}
	sort.Strings(report)
}
