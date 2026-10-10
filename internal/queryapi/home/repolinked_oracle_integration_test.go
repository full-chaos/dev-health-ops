//go:build integration

package home

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// The differential oracle of CHAOS-9094: the request-time read of the four
// work-item metrics under a repository filter against the daily tables the real
// executors fill, on the same items.
//
// The world is seeded as raw rows only (work_items, transitions, attributions,
// links, blocking relations); the daily tables are written by the production
// executors (WorkItemExecutor and WorkItemStateExecutor), never by hand.
// A repository view that names EVERY repository of a world whose items are all
// linked must equal the daily-table read of that world, for each metric, value,
// presence flags, percent and series. A view of one repository must equal the
// daily-table read of a second organization that holds only that repository's
// linked items.

type oracleItem struct {
	id, provider, typ, status string
	projectKey, projectID     string
	created, started, done    string // "" = NULL
	assignee                  string
	points                    float64
	repo                      string // stored repository: "r1" / "r2"
	team                      string // "" = no attribution row
	links                     []oracleLink
}

type oracleLink struct {
	repo, provenance string
}

const (
	oracleR1 = "00000000-0000-4000-8000-0000000000a1"
	oracleR2 = "00000000-0000-4000-8000-0000000000a2"
)

func oracleItems() []oracleItem {
	return []oracleItem{
		{id: "gh:a/r#1", provider: "github", typ: "bug", status: "done", projectID: "a/r", created: "2026-08-12 09:00:00", started: "2026-08-13 10:00:00", done: "2026-08-19 12:00:00", assignee: "ann", points: 3, repo: oracleR1, team: "t-alpha", links: []oracleLink{{oracleR1, "native"}}},
		{id: "gh:a/r#2", provider: "github", typ: "task", status: "done", projectID: "a/r", created: "2026-08-12 09:00:00", started: "2026-08-14 08:00:00", done: "2026-08-20 17:30:00", assignee: "bob", points: 5, repo: oracleR1, team: "t-alpha", links: []oracleLink{{oracleR1, "explicit_text"}, {oracleR2, "native"}}},
		{id: "gh:a/r#3", provider: "github", typ: "task", status: "in_progress", projectID: "a/r", created: "2026-08-15 09:00:00", started: "2026-08-16 09:00:00", repo: oracleR1, team: "t-alpha", links: []oracleLink{{oracleR1, "heuristic"}}},
		{id: "jira:ABC-1", provider: "jira", typ: "bug", status: "done", projectKey: "ABC", created: "2026-08-10 09:00:00", started: "2026-08-11 09:00:00", done: "2026-08-22 10:00:00", assignee: "cy", repo: oracleR2, team: "t-beta", links: []oracleLink{{oracleR2, "native"}}},
		{id: "jira:ABC-2", provider: "jira", typ: "task", status: "in_progress", projectKey: "ABC", created: "2026-08-17 09:00:00", started: "2026-08-18 09:00:00", repo: oracleR2, team: "t-beta", links: []oracleLink{{oracleR2, "native"}}},
		{id: "gh:a/r#6", provider: "github", typ: "task", status: "todo", projectID: "a/r", created: "2026-08-19 09:00:00", repo: oracleR2, links: []oracleLink{{oracleR2, "explicit_text"}}},
		{id: "linear:OPS-1", provider: "linear", typ: "task", status: "done", projectID: "ops", created: "2026-08-13 09:00:00", started: "2026-08-14 09:00:00", done: "2026-08-24 15:00:00", assignee: "dee", repo: oracleR2, team: "t-beta", links: []oracleLink{{oracleR2, "native"}}},
		{id: "gitlab:grp/proj#9", provider: "gitlab", typ: "task", status: "done", projectID: "grp/proj", created: "2026-08-13 09:00:00", started: "2026-08-15 09:00:00", done: "2026-08-21 11:00:00", assignee: "eve", repo: oracleR2, team: "t-beta", links: []oracleLink{{oracleR2, "native"}}},
	}
}

func oracleTS(value string) string {
	if value == "" {
		return "NULL"
	}
	return "toDateTime64('" + value + "', 3, 'UTC')"
}

// seedOracleWorld writes the raw rows of one organization. Only the items for
// which keep returns true are written (the subset world).
func seedOracleWorld(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, keep func(oracleItem) bool) {
	t.Helper()
	exec := func(statement string) {
		t.Helper()
		if err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	synced := "toDateTime64('2026-08-26 00:00:00', 3, 'UTC')"
	for _, repo := range []struct{ id, name string }{{oracleR1, "a/r1"}, {oracleR2, "a/r2"}} {
		exec(fmt.Sprintf(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (toUUID('%s'), '%s', 'github', '%s', %s, %s)`, repo.id, repo.name, org, synced, synced))
	}
	for _, item := range oracleItems() {
		if !keep(item) {
			continue
		}
		assignees := "[]"
		if item.assignee != "" {
			assignees = "['" + item.assignee + "']"
		}
		exec(fmt.Sprintf(`INSERT INTO work_items (repo_id, work_item_id, provider, status, type, assignees, story_points, project_key, project_id, native_team_key, project_name, created_at, started_at, completed_at, org_id, last_synced)
VALUES (toUUID('%s'), '%s', '%s', '%s', '%s', %s, %v, '%s', '%s', '', '', %s, %s, %s, '%s', %s)`,
			item.repo, item.id, item.provider, item.status, item.typ, assignees, item.points, item.projectKey, item.projectID,
			oracleTS(item.created), oracleTS(item.started), oracleTS(item.done), org, synced))
		// the item's own status history: todo from creation, in_progress from its start, done at its completion
		if item.started != "" {
			exec(fmt.Sprintf(`INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
VALUES (toUUID('%s'), '%s', %s, '%s', 'todo', 'in_progress', 'Todo', 'In Progress', '', '%s', %s)`, item.repo, item.id, oracleTS(item.started), item.provider, org, synced))
		}
		if item.done != "" {
			exec(fmt.Sprintf(`INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
VALUES (toUUID('%s'), '%s', %s, '%s', 'in_progress', 'done', 'In Progress', 'Done', '', '%s', %s)`, item.repo, item.id, oracleTS(item.done), item.provider, org, synced))
		}
		if item.team != "" {
			exec(fmt.Sprintf(`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
VALUES ('%s', toUUID('%s'), '%s', '%s', '%s', '%s', 'project_ownership', 1, 'high', '', %s)`, org, item.repo, item.id, item.provider, item.team, item.team, synced))
		}
		for n, link := range item.links {
			exec(fmt.Sprintf(`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
VALUES (toUUID('%s'), '%s', %d, 1.0, '%s', '', %s, '%s')`, link.repo, item.id, 100+n, link.provenance, synced, org))
		}
	}
	// jira:ABC-2 is blocked by gh:a/r#6 from 2026-08-19 on (an open blocker).
	if keep(oracleItems()[4]) && keep(oracleItems()[5]) {
		exec(fmt.Sprintf(`INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id, relationship_semantics_version, relation_started_at)
VALUES ('gh:a/r#6', 'jira:ABC-2', 'blocks', 'blocks', toDateTime64('2026-08-26 00:00:00', 3), '%s', 'canonical-blocks.v2', toDateTime64('2026-08-19 10:00:00', 3))`, org))
	}
}

// fillDailyTables runs the production executors for every day of the window.
func fillDailyTables(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, from, to time.Time) {
	t.Helper()
	metrics, err := daily.NewWorkItemExecutor(admin)
	if err != nil {
		t.Fatal(err)
	}
	state, err := daily.NewWorkItemStateExecutor(admin)
	if err != nil {
		t.Fatal(err)
	}
	for day := from; day.Before(to); day = day.AddDate(0, 0, 1) {
		partition := daily.Partition{
			ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
			RepoIDs: []daily.RepositoryID{daily.RepositoryID(oracleR1), daily.RepositoryID(oracleR2)},
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

func sameDelta(t *testing.T, label string, got, want MetricDelta) {
	t.Helper()
	near := func(a, b float64) bool { return math.Abs(a-b) <= 1e-9*math.Max(1, math.Abs(b)) }
	pct := func(p *float64) string {
		if p == nil {
			return "null"
		}
		return fmt.Sprintf("%.9f", *p)
	}
	if !near(got.Value, want.Value) || got.HasData != want.HasData || got.HasPriorData != want.HasPriorData || pct(got.DeltaPct) != pct(want.DeltaPct) {
		t.Errorf("%s: request-time value %v has_data %v prior %v delta %s; daily tables value %v has_data %v prior %v delta %s",
			label, got.Value, got.HasData, got.HasPriorData, pct(got.DeltaPct), want.Value, want.HasData, want.HasPriorData, pct(want.DeltaPct))
	}
	if len(got.Spark) != len(want.Spark) {
		t.Errorf("%s: request-time series has %d points, daily tables %d", label, len(got.Spark), len(want.Spark))
		return
	}
	for i := range got.Spark {
		if got.Spark[i].TS != want.Spark[i].TS || !near(got.Spark[i].Value, want.Spark[i].Value) {
			t.Errorf("%s: series point %d = %v, daily tables %v", label, i, got.Spark[i], want.Spark[i])
		}
	}
}

func TestRepoLinkedWorkItemMetricsEqualTheDailyTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)

	const orgAll, orgR1 = "oracle-all", "oracle-r1only"
	from, to := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	start, end := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	cmpStart, cmpEnd := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC), start

	seedOracleWorld(ctx, t, admin, orgAll, func(oracleItem) bool { return true })
	fillDailyTables(ctx, t, admin, orgAll, from, to)

	// The items linked to repository 1, as a world of their own (the blocking
	// pair is not among them: a blocker outside the view is not in it).
	linkedTo := func(repo string) func(oracleItem) bool {
		return func(item oracleItem) bool {
			for _, link := range item.links {
				if link.repo == repo {
					return true
				}
			}
			return false
		}
	}
	seedOracleWorld(ctx, t, admin, orgR1, linkedTo(oracleR1))
	fillDailyTables(ctx, t, admin, orgR1, from, to)

	asOf := end
	for _, name := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		spec := metricSpecByName(t, name)

		daily, err := computeMetricDelta(ctx, client, spec, start, end, cmpStart, cmpEnd, Filters{}, orgAll, asOf)
		if err != nil {
			t.Fatal(err)
		}
		both, err := computeMetricDelta(ctx, client, spec, start, end, cmpStart, cmpEnd,
			Filters{What: WhatFilter{Repos: []string{oracleR1, oracleR2}}}, orgAll, asOf)
		if err != nil {
			t.Fatal(err)
		}
		if both.RepoLinkState == nil || *both.RepoLinkState != repoLinkLinked {
			t.Fatalf("%s: repo link state = %q, want linked", name, func() string {
				if both.RepoLinkState == nil {
					return "<nil>"
				}
				return *both.RepoLinkState
			}())
		}
		if !daily.HasData {
			t.Fatalf("%s: the daily tables hold no data for the seeded world: the oracle would compare nothing", name)
		}
		sameDelta(t, name+": every repository of the world", both, daily)

		one, err := computeMetricDelta(ctx, client, spec, start, end, cmpStart, cmpEnd,
			Filters{What: WhatFilter{Repos: []string{oracleR1}}}, orgAll, asOf)
		if err != nil {
			t.Fatal(err)
		}
		world, err := computeMetricDelta(ctx, client, spec, start, end, cmpStart, cmpEnd, Filters{}, orgR1, asOf)
		if err != nil {
			t.Fatal(err)
		}
		sameDelta(t, name+": repository 1 against the world of its linked items", one, world)
		if name == "throughput" && math.Abs(one.Value-both.Value) < 1e-9 {
			t.Errorf("throughput of one repository equals the whole world's (%v): the view narrowed nothing", one.Value)
		}
	}
	_ = strings.TrimSpace
}

// The wire says what the number is: items by tier, items in more than one
// repository's view, and the coverage of the repository views.
func TestRepoLinkedViewSaysItsBasisAndCoverage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "oracle-basis"
	seedOracleWorld(ctx, t, admin, org, func(oracleItem) bool { return true })
	// One item no pull request links to: in no repository's view.
	if err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO work_items (repo_id, work_item_id, provider, status, type, project_id, created_at, started_at, org_id, last_synced)
VALUES (toUUID('%s'), 'gh:a/r#8', 'github', 'in_progress', 'task', 'a/r', %s, %s, '%s', %s)`,
		oracleR1, oracleTS("2026-08-16 09:00:00"), oracleTS("2026-08-17 09:00:00"), org, oracleTS("2026-08-26 00:00:00"))); err != nil {
		t.Fatal(err)
	}
	// A second native item of repository 1, so the tiers count differently
	// (native 2, explicit_text 1, heuristic 1) and a swap of two tiers shows.
	for _, statement := range []string{
		fmt.Sprintf(`INSERT INTO work_items (repo_id, work_item_id, provider, status, type, project_id, created_at, started_at, org_id, last_synced)
VALUES (toUUID('%s'), 'gh:a/r#10', 'github', 'in_progress', 'task', 'a/r', %s, %s, '%s', %s)`,
			oracleR1, oracleTS("2026-08-16 09:00:00"), oracleTS("2026-08-17 09:00:00"), org, oracleTS("2026-08-26 00:00:00")),
		fmt.Sprintf(`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
VALUES (toUUID('%s'), 'gh:a/r#10', 300, 1.0, 'native', '', %s, '%s')`, oracleR1, oracleTS("2026-08-26 00:00:00"), org),
	} {
		if err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	start, end := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	spec := metricSpecByName(t, "throughput")
	got, err := computeMetricDelta(ctx, client, spec, start, end, time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC), start,
		Filters{What: WhatFilter{Repos: []string{oracleR1}}}, org, end)
	if err != nil {
		t.Fatal(err)
	}
	if got.RepoLinkState == nil || *got.RepoLinkState != repoLinkLinked {
		t.Fatalf("state = %v, want linked", got.RepoLinkState)
	}
	// Repository 1 holds #1 (native), #2 (explicit_text here, native in repository 2) and #3 (heuristic).
	if got.RepoLinkBasis == nil || *got.RepoLinkBasis != (RepoLinkBasis{Native: 2, ExplicitText: 1, Heuristic: 1}) {
		t.Errorf("basis = %+v, want native 2, explicit_text 1, heuristic 1 (a lower tier is never counted as native)", got.RepoLinkBasis)
	}
	// D5864: the state is a property of the LINK: blocked_work has no blocked hours for
	// repository 1 (no data, as the daily read serves none) while the four metrics of the
	// request all say "linked" and the basis counts are present.
	for _, name := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		d, err := computeMetricDelta(ctx, client, metricSpecByName(t, name), start, end, time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC), start,
			Filters{What: WhatFilter{Repos: []string{oracleR1}}}, org, end)
		if err != nil {
			t.Fatal(err)
		}
		if d.RepoLinkState == nil || *d.RepoLinkState != repoLinkLinked {
			t.Errorf("%s: repoLinkState = %v, want linked (linked items are in the window)", name, d.RepoLinkState)
		}
		if d.RepoLinkBasis == nil || d.RepoLinkBasis.Native != 2 {
			t.Errorf("%s: basis = %+v, want the counts of the view", name, d.RepoLinkBasis)
		}
		if name == "blocked_work" && d.HasData {
			t.Errorf("blocked_work of repository 1 has data (%v): the seed holds no blocked hours there", d.Value)
		}
	}
	if got.RepoLinkMultiRepoItems == nil || *got.RepoLinkMultiRepoItems != 1 {
		t.Errorf("multi-repository items = %v, want 1 (#2 is linked to both repositories)", got.RepoLinkMultiRepoItems)
	}
	// 10 items of the window in the organization (the oracle world's 8, the unlinked one and the second native one); all but the unlinked one have a link.
	if got.RepoLinkCoverage == nil || *got.RepoLinkCoverage != (RepoLinkCoverage{LinkedItems: 9, ItemsInWindow: 10}) {
		t.Errorf("coverage = %+v, want 9 linked of 10 in the window", got.RepoLinkCoverage)
	}
}

// Filters combine by AND: a team and a repository narrow, never widen; named
// repositories that resolve to nothing leave nothing.
func TestRepoLinkedViewCombinesWithTeamByAnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "oracle-and"
	seedOracleWorld(ctx, t, admin, org, func(oracleItem) bool { return true })
	fillDailyTables(ctx, t, admin, org, time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC))
	start, end := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	cmpStart := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	team := func(ids ...string) ScopeFilter { return ScopeFilter{Level: "team", IDs: ids} }
	for _, name := range []string{"throughput", "cycle_time", "wip_saturation"} {
		spec := metricSpecByName(t, name)
		read := func(f Filters) MetricDelta {
			d, err := computeMetricDelta(ctx, client, spec, start, end, cmpStart, start, f, org, end)
			if err != nil {
				t.Fatal(err)
			}
			return d
		}
		// team alpha owns the work of repository 1 only: alpha AND both repositories = alpha alone.
		dailyAlpha := read(Filters{Scope: team("t-alpha")})
		sameDelta(t, name+": team alpha and both repositories", read(Filters{Scope: team("t-alpha"), What: WhatFilter{Repos: []string{oracleR1, oracleR2}}}), dailyAlpha)
		// a repository the team has no linked item in: no data, never the team's value.
		none := read(Filters{Scope: team("t-beta"), What: WhatFilter{Repos: []string{oracleR1}}})
		// D5864: repository 1 HAS linked items in the window (the state is a property of the link: linked);
		// none of them is team beta's, so the metric has no data.
		if none.HasData || none.Value != 0 || none.RepoLinkState == nil || *none.RepoLinkState != repoLinkLinked {
			state := "<nil>"
			if none.RepoLinkState != nil {
				state = *none.RepoLinkState
			}
			t.Errorf("%s: team beta and repository 1 = value %v has_data %v state %s, want no data, linked", name, none.Value, none.HasData, state)
		}
		// named repositories that resolve to nothing.
		unknown := read(Filters{What: WhatFilter{Repos: []string{"00000000-0000-4000-8000-0000000000ff"}}})
		if unknown.HasData || unknown.Value != 0 || unknown.RepoFilterApplied == nil || !*unknown.RepoFilterApplied || unknown.RepoLinkState == nil || *unknown.RepoLinkState != repoLinkNoLinks {
			t.Errorf("%s: an unresolved repository = %+v, want no data, filter applied, no_links", name, unknown)
		}
	}
}

// A link and a blocked-duration row whose work item is not in work_items are
// invisible: the repository has no linked item, so blocked_work says no_links and
// no data, never a value summed from an item the view cannot see.
func TestRepoLinkedBlockedRowOfAnItemWithoutAWorkItemIsInvisible(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "oracle-orphan-blocked"
	synced := oracleTS("2026-08-26 00:00:00")
	if err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (toUUID('%s'), 'a/r1', 'github', '%s', %s, %s)`, oracleR1, org, synced, synced)); err != nil {
		t.Fatal(err)
	}
	if err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
VALUES (toUUID('%s'), 'gh:a/r#orphan', 1, 1.0, 'native', '', %s, '%s')`, oracleR1, synced, org)); err != nil {
		t.Fatal(err)
	}
	if err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO work_item_blocked_durations_daily (day, provider, work_item_id, team_id, duration_hours, org_id, computed_at)
VALUES (toDate('2026-08-20'), 'github', 'gh:a/r#orphan', 't-alpha', 7, '%s', %s)`, org, synced)); err != nil {
		t.Fatalf("seed blocked row: %v", err)
	}
	start, end := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	got, err := computeMetricDelta(ctx, client, metricSpecByName(t, "blocked_work"), start, end, time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC), start,
		Filters{What: WhatFilter{Repos: []string{oracleR1}}}, org, end)
	if err != nil {
		t.Fatal(err)
	}
	if got.HasData || got.Value != 0 {
		t.Errorf("blocked_work = %v (has data %v) from an item with no work_items row, want no data", got.Value, got.HasData)
	}
}
