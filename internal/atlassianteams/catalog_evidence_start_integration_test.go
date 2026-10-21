//go:build integration

package atlassianteams

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestAtlassianTeamsLinksStartAtTheEvidence (CHAOS-10521): the first run sees no
// evidence and writes every link at the run. Once the evidence is synced (an
// item alice reported, an item of project 10001), the next run moves the first
// row of alice's memberships and of the project link to the evidence and retires
// the run-time key as an empty interval; bob and carol keep their start. A third
// run writes nothing. The plant is the first run's state: the same check finds it.
func TestAtlassianTeamsLinksStartAtTheEvidence(t *testing.T) {
	conn := openClickHouse(t)
	ctx := context.Background()
	const org = "org-1"
	t0 := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
	reported := time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)
	projectItem := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)

	g := newGateway(t, everyLinkWritable)
	run := func(now time.Time) {
		t.Helper()
		p := params(everything)
		p.Now = now
		rows, err := Collect(ctx, g.client(), p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Write(ctx, conn, org, rows, everything, soleScope(), everyAbsentFactIsGone{}); err != nil {
			t.Fatal(err)
		}
	}
	// "<team>|<key>|<valid_from>|<valid_to or open>" of every row, after FINAL.
	read := func(table, key string) []string {
		t.Helper()
		return lines(t, conn, `SELECT concat(team_id, '|', `+key+`, '|', toString(valid_from), '|', ifNull(toString(valid_to), 'open')) FROM `+table+
			` FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND source = 'native' ORDER BY 1`)
	}
	count := func(table string) string {
		t.Helper()
		return lines(t, conn, `SELECT toString(count()) FROM `+table+` WHERE org_id = 'org-1' AND provider = 'jira' AND source = 'native'`)[0]
	}
	stamp := func(at time.Time) string { return at.Format("2006-01-02 15:04:05.000") }
	// startsAt is the check: every fact's first row that is not an empty interval starts at want, with one open row.
	startsAt := func(rows []string, want map[string]time.Time) []string {
		first, open := map[string]string{}, map[string]int{}
		for _, row := range rows {
			parts := strings.Split(row, "|")
			fact, from, to := parts[0]+"|"+parts[1], parts[2], parts[3]
			if to == from {
				continue
			}
			if current, ok := first[fact]; !ok || from < current {
				first[fact] = from
			}
			if to == "open" {
				open[fact]++
			}
		}
		var out []string
		for fact, from := range want {
			if first[fact] != stamp(from) || open[fact] != 1 {
				out = append(out, fact+" starts at "+first[fact]+" with "+strconv.Itoa(open[fact])+" open row(s), want "+stamp(from))
			}
		}
		if len(first) != len(want) {
			out = append(out, "facts "+strings.Join(rows, ", "))
		}
		return out
	}

	run(t0)
	firstMembers, firstLinks := read("team_memberships", "member_id"), read("team_project_ownership", "project_id")
	exec(t, conn, `INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, reporter, assignees, created_at, org_id, last_synced)
VALUES (toUUID('00000000-0000-0000-0000-000000000000'), 'jira:PLAT-1', 'jira', 'issue', '10001', 'jira:accountid:Alice-1', [], ?, 'org-1', now64(3))`, reported)
	exec(t, conn, `INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, reporter, assignees, created_at, org_id, last_synced)
VALUES (toUUID('00000000-0000-0000-0000-000000000000'), 'jira:PLAT-2', 'jira', 'issue', '10002', '', [], ?, 'org-1', now64(3))`, projectItem.Add(-24*time.Hour))
	exec(t, conn, `INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, reporter, assignees, created_at, org_id, last_synced)
VALUES (toUUID('00000000-0000-0000-0000-000000000000'), 'jira:PLAT-3', 'jira', 'issue', '10001', '', [], ?, 'org-1', now64(3))`, projectItem)

	wantMembers := map[string]time.Time{
		idA + "|jira:alice-1": reported, idA + "|jira:bob-2": t0, idC + "|jira:carol-3": t0,
	}
	wantLinks := map[string]time.Time{idA + "|10001": reported}
	// The item alice reported is in project 10001 too, and is its earliest item.
	if planted := startsAt(firstMembers, wantMembers); len(planted) == 0 {
		t.Fatalf("the check does not find the sync-time start of the first run: %v", firstMembers)
	}
	if planted := startsAt(firstLinks, wantLinks); len(planted) == 0 {
		t.Fatalf("the check does not find the sync-time start of the first run: %v", firstLinks)
	}

	run(t0.Add(time.Hour))
	members, links := read("team_memberships", "member_id"), read("team_project_ownership", "project_id")
	if violations := startsAt(members, wantMembers); len(violations) > 0 {
		t.Fatalf("memberships after the evidence run: %v (rows %v)", violations, members)
	}
	if violations := startsAt(links, wantLinks); len(violations) > 0 {
		t.Fatalf("project links after the evidence run: %v (rows %v)", violations, links)
	}
	for _, retired := range []struct {
		rows []string
		key  string
	}{{members, idA + "|jira:alice-1|" + stamp(t0) + "|" + stamp(t0)}, {links, idA + "|10001|" + stamp(t0) + "|" + stamp(t0)}} {
		found := false
		for _, row := range retired.rows {
			found = found || row == retired.key
		}
		if !found {
			t.Errorf("the run-time key %s is not retired as an empty interval: %v", retired.key, retired.rows)
		}
	}

	physicalLinks := count("team_project_ownership")
	run(t0.Add(2 * time.Hour))
	if again := read("team_memberships", "member_id"); strings.Join(again, ",") != strings.Join(members, ",") {
		t.Errorf("a repeat run changed the memberships:\n  %v\n  %v", members, again)
	}
	if again := read("team_project_ownership", "project_id"); strings.Join(again, ",") != strings.Join(links, ",") {
		t.Errorf("a repeat run changed the project links:\n  %v\n  %v", links, again)
	}
	if got := count("team_project_ownership"); got != physicalLinks {
		t.Errorf("a repeat run wrote project link rows: %s, then %s", physicalLinks, got)
	}
}
