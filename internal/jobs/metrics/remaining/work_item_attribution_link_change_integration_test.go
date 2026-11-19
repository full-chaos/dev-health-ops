//go:build integration

package remaining

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// linkChangeOrg is one organization of a link-change cell: repositories A, B
// and C owned by team-a, team-b and team-c since three days ago, the rows of
// an org-wide backstop run of yesterday, and a backstop for today.
type linkChangeOrg struct {
	t      *testing.T
	ctx    context.Context
	conn   driver.Conn
	writer *WorkItemAttributionClickHouseWriter
	orgID  string
	now    time.Time
	setup  time.Time
	repos  map[string]uuid.UUID
}

func newLinkChangeOrg(t *testing.T, conn driver.Conn) *linkChangeOrg {
	t.Helper()
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	now := time.Now().UTC()
	org := &linkChangeOrg{
		t: t, ctx: context.Background(), conn: conn, writer: writer,
		orgID: "org-link-change-" + uuid.NewString(), now: now, setup: now.Add(-72 * time.Hour),
		repos: map[string]uuid.UUID{"A": uuid.New(), "B": uuid.New(), "C": uuid.New(), "N": uuid.New()},
	}
	for _, letter := range []string{"A", "B", "C"} {
		org.owner(letter, "team-"+strings.ToLower(letter), org.setup)
	}
	return org
}

func (org *linkChangeOrg) exec(query string, args ...any) {
	org.t.Helper()
	if err := org.conn.Exec(org.ctx, query, args...); err != nil {
		org.t.Fatalf("%s: %v", query, err)
	}
}

// owner gives repository letter to team from validFrom; written two days ago,
// with the team's catalog row (an active team; its change makes yesterday's
// first run org-wide).
func (org *linkChangeOrg) owner(letter, team string, validFrom time.Time) {
	org.t.Helper()
	org.exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key)
		VALUES (?, ?, ?, [], [], [], 1, ?, ?, ?, 'github', '')`, team, uuid.New(), "Team "+team, org.now.Add(-48*time.Hour), org.now.Add(-48*time.Hour), org.orgID)
	org.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source,
		is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, 'github', ?, ?, ?, 'exact', 'native', 1, 100, 0, ?, NULL, ?)`,
		org.orgID, team, org.repos[letter], "acme/"+team, validFrom, org.now.Add(-48*time.Hour))
}

// issue seeds a Jira story of its own project; completed is nil for an open one.
func (org *linkChangeOrg) issue(id string, completed *time.Time) {
	org.t.Helper()
	project := strings.TrimSuffix(strings.TrimPrefix(id, "jira:"), "-1")
	status := "in_progress"
	if completed != nil {
		status = "done"
	}
	org.exec(`INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, project_key, status, created_at, completed_at, org_id, last_synced)
		VALUES (?, ?, 'jira', 'story', ?, ?, ?, ?, ?, ?, ?)`, uuid.Nil, id, project, project, status, org.setup.Add(-48*time.Hour), completed, org.orgID, org.setup)
}

func (org *linkChangeOrg) link(letter, issue string, pr uint32, tier string, at time.Time) {
	org.t.Helper()
	org.exec(`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
		VALUES (?, ?, ?, 1.0, ?, 'cell', ?, ?)`, org.repos[letter], issue, pr, tier, at, org.orgID)
}

func (org *linkChangeOrg) unlink(letter, issue string, pr uint32) {
	org.t.Helper()
	org.exec(`ALTER TABLE work_graph_issue_pr DELETE WHERE org_id = ? AND repo_id = ? AND work_item_id = ? AND pr_number = ?
		SETTINGS mutations_sync = 2`, org.orgID, org.repos[letter], issue, pr)
}

func (org *linkChangeOrg) backstopAt(at time.Time) *WorkItemAttributionExecutor {
	return &WorkItemAttributionExecutor{conn: org.conn, writer: org.writer, nowUTC: func() time.Time { return at }}
}

func (org *linkChangeOrg) run(at time.Time) WorkItemAttributionOutcome {
	org.t.Helper()
	outcome, err := org.backstopAt(at).ComputeOrg(org.ctx, org.orgID)
	if err != nil {
		org.t.Fatalf("ComputeOrg at %s: %v", at, err)
	}
	return outcome
}

// yesterday runs the org's first backstop run, org-wide, a day ago.
func (org *linkChangeOrg) yesterday() {
	org.t.Helper()
	if outcome := org.run(org.now.Add(-24 * time.Hour)); !outcome.OrgWide {
		org.t.Fatalf("yesterday's run %+v, want org-wide", outcome)
	}
}

// newest is each item's newest row set as sorted source|team|is_primary lines.
func (org *linkChangeOrg) newest() map[string][]string {
	org.t.Helper()
	rows, err := org.conn.Query(org.ctx, `
SELECT work_item_id, toString(source), ifNull(team_id, ''), is_primary = 1, is_primary = 2
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND (work_item_id, computed_at) IN (
    SELECT work_item_id, max(computed_at) FROM work_item_team_attributions WHERE org_id = ? GROUP BY work_item_id)`,
		org.orgID, org.orgID)
	if err != nil {
		org.t.Fatalf("query newest rows: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string][]string{}
	for rows.Next() {
		var id, source, team string
		var primary, coOwner bool
		if err := rows.Scan(&id, &source, &team, &primary, &coOwner); err != nil {
			org.t.Fatal(err)
		}
		flag := 0
		if primary {
			flag = 1
		} else if coOwner {
			flag = 2
		}
		out[id] = append(out[id], fmt.Sprintf("%s|%s|%d", source, team, flag))
	}
	if err := rows.Err(); err != nil {
		org.t.Fatal(err)
	}
	for id := range out {
		sort.Strings(out[id])
	}
	return out
}

func (org *linkChangeOrg) primary(id string) string {
	org.t.Helper()
	for _, line := range org.newest()[id] {
		if strings.HasSuffix(line, "|1") {
			return strings.TrimSuffix(line, "|1")
		}
	}
	return ""
}

func (org *linkChangeOrg) wantPrimary(id, want, why string) {
	org.t.Helper()
	if got := org.primary(id); got != want {
		org.t.Errorf("LINK14149 FAIL %s (%s): primary %q, want %q (newest %v)", id, why, got, want, org.newest()[id])
	}
}

func (org *linkChangeOrg) newestComputedAt(id string) time.Time {
	org.t.Helper()
	rows, err := org.conn.Query(org.ctx, `SELECT max(computed_at) FROM work_item_team_attributions WHERE org_id = ? AND work_item_id = ?`, org.orgID, id)
	if err != nil {
		org.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var at time.Time
	if rows.Next() {
		if err := rows.Scan(&at); err != nil {
			org.t.Fatal(err)
		}
	}
	return at
}

// A backstop run with no link change and no ownership change writes nothing:
// not for an issue on an owned link, not for an issue on a link to a
// repository with no owner, and not for a done issue whose link repository
// has an owner today but none on the issue's completion day (today's facts
// give it the owner, its own day's facts do not).
func TestALinkChangeRunWithNoChangeWritesNothing(t *testing.T) {
	org := newLinkChangeOrg(t, workItemAttributionMigratedClickHouse(t, context.Background()))
	completed := org.now.Add(-5 * 24 * time.Hour)
	// Repository F: team-x until six days ago, then no owner (a gap) until
	// team-f two days ago; the done issue's day falls in the gap.
	org.repos["F"] = uuid.New()
	org.owner("F", "team-x", org.now.Add(-20*24*time.Hour))
	org.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source,
		is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, 'github', 'team-x', ?, 'acme/team-x', 'exact', 'native', 1, 100, 0, ?, ?, ?)`,
		org.orgID, org.repos["F"], org.now.Add(-20*24*time.Hour), org.now.Add(-6*24*time.Hour), org.now.Add(-47*time.Hour))
	org.owner("F", "team-f", org.now.Add(-48*time.Hour))
	org.issue("jira:OA-1", nil)
	org.issue("jira:ON-1", nil)
	org.issue("jira:OF-1", &completed)
	org.link("A", "jira:OA-1", 1, "explicit_text", org.setup)
	org.link("N", "jira:ON-1", 2, "native", org.setup)
	org.link("F", "jira:OF-1", 3, "native", org.setup)
	org.yesterday()
	org.wantPrimary("jira:OA-1", "repo_ownership|team-a", "setup")
	org.wantPrimary("jira:ON-1", "unassigned|", "setup")
	org.wantPrimary("jira:OF-1", "unassigned|", "setup: no owner on its day")

	for run := 1; run <= 2; run++ {
		if outcome := org.run(org.now.Add(time.Duration(run) * time.Minute)); !outcome.SkippedNoop || outcome.RowsWritten != 0 {
			t.Errorf("LINK14149 FAIL run %d %+v, want a no-op that writes nothing", run, outcome)
		}
	}
}

// An issue whose row of today predates the link change is deferred (the
// same-day rule, kept): today's run leaves it, the next day's run re-derives it.
func TestALinkChangeOfAnIssueWithARowOfTodayWaitsForTheNextDay(t *testing.T) {
	org := newLinkChangeOrg(t, workItemAttributionMigratedClickHouse(t, context.Background()))
	org.issue("jira:DT-1", nil)
	org.link("A", "jira:DT-1", 1, "explicit_text", org.setup)
	org.yesterday()
	// Today's row, written by the daily family an hour ago, before the link is removed.
	org.exec(`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at, writer, run_id)
		SELECT org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, ?, 'daily', 'cell'
		FROM work_item_team_attributions FINAL WHERE org_id = ? AND work_item_id = 'jira:DT-1'`, org.now.Add(-time.Hour), org.orgID)
	org.wantPrimary("jira:DT-1", "repo_ownership|team-a", "setup: today's row")
	org.unlink("A", "jira:DT-1", 1)

	org.run(org.now)
	org.wantPrimary("jira:DT-1", "repo_ownership|team-a", "deferred today")
	org.run(org.now.Add(24 * time.Hour))
	org.wantPrimary("jira:DT-1", "unassigned|", "re-derived the next day")
}

// A removed link of a co-owner team takes that team's co-owner row out of the
// issue's newest row set.
func TestARemovedCoOwnerLinkDropsTheCoOwnerRow(t *testing.T) {
	org := newLinkChangeOrg(t, workItemAttributionMigratedClickHouse(t, context.Background()))
	org.issue("jira:CO-1", nil)
	org.link("A", "jira:CO-1", 1, "explicit_text", org.setup)
	org.link("A", "jira:CO-1", 2, "explicit_text", org.setup)
	org.link("C", "jira:CO-1", 3, "explicit_text", org.setup)
	org.yesterday()
	if got := fmt.Sprint(org.newest()["jira:CO-1"]); got != "[repo_ownership|team-a|1 repo_ownership|team-c|2]" {
		t.Fatalf("setup: newest %s", got)
	}
	org.unlink("C", "jira:CO-1", 3)
	org.run(org.now)
	if got := fmt.Sprint(org.newest()["jira:CO-1"]); got != "[repo_ownership|team-a|1]" {
		t.Errorf("LINK14149 FAIL jira:CO-1 newest %s, want [repo_ownership|team-a|1]", got)
	}
}

// A run scoped by an ownership change also re-derives a link-changed issue of
// another repository, publishes the scope's marker, and writes no filler.
func TestALinkChangedIssueJoinsAScopedRun(t *testing.T) {
	org := newLinkChangeOrg(t, workItemAttributionMigratedClickHouse(t, context.Background()))
	org.repos["D"] = uuid.New()
	org.issue("jira:SL-1", nil)
	org.link("A", "jira:SL-1", 1, "explicit_text", org.setup)
	seedWorkItemAttributionPullRequest(t, org.ctx, org.conn, org.orgID, "ghpr:acme/d#1", org.repos["D"], org.setup)
	for i := 0; i < 18; i++ {
		seedWorkItemAttributionItem(t, org.ctx, org.conn, org.orgID, fmt.Sprintf("filler-%d", i), uuid.Nil, org.setup)
	}
	org.yesterday()
	org.unlink("A", "jira:SL-1", 1)
	org.exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key)
		VALUES ('team-d', ?, 'Team team-d', [], [], [], 1, ?, ?, ?, 'github', '')`, uuid.New(), org.now.Add(-48*time.Hour), org.now.Add(-48*time.Hour), org.orgID)
	org.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source,
		is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, 'github', 'team-d', ?, 'acme/team-d', 'exact', 'native', 1, 100, 0, ?, NULL, ?)`,
		org.orgID, org.repos["D"], org.setup, org.now.Add(-time.Minute))
	waitForWorkItemAttributionScope(t, org.ctx, org.backstopAt(org.now), org.orgID, org.now,
		func(scope workItemAttributionScopeDecision) bool { return len(scope.repoIDs) == 1 })
	fillersBefore := org.newestComputedAt("filler-0")

	outcome := org.run(org.now)
	if outcome.OrgWide || len(outcome.RepoIDs) != 1 {
		t.Errorf("LINK14149 FAIL outcome %+v, want a run scoped to repository D", outcome)
	}
	org.wantPrimary("ghpr:acme/d#1", "repo_ownership|team-d", "scope item")
	org.wantPrimary("jira:SL-1", "unassigned|", "link removed")
	if got := org.newestComputedAt("filler-0"); !got.Equal(fillersBefore) {
		t.Errorf("LINK14149 FAIL a filler was written: %s -> %s", fillersBefore, got)
	}
	var scoped uint64
	rows, err := org.conn.Query(org.ctx, `SELECT count() FROM work_item_attribution_backstop_scoped_runs WHERE org_id = ? AND scope_kind = 'repo' AND scope_id = ?`,
		org.orgID, org.repos["D"].String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		if err := rows.Scan(&scoped); err != nil {
			t.Fatal(err)
		}
	}
	if scoped == 0 {
		t.Errorf("LINK14149 FAIL no scoped marker for repository D")
	}
}

// A link of a higher tier written beside the old one (no retraction) moves the
// issue to the new link's owner.
func TestAHigherTierLinkMovesTheIssue(t *testing.T) {
	org := newLinkChangeOrg(t, workItemAttributionMigratedClickHouse(t, context.Background()))
	org.issue("jira:TM-1", nil)
	org.link("A", "jira:TM-1", 1, "heuristic", org.setup)
	org.yesterday()
	org.wantPrimary("jira:TM-1", "repo_ownership|team-a", "setup")
	org.link("B", "jira:TM-1", 2, "explicit_text", org.now.Add(-time.Minute))
	org.run(org.now)
	org.wantPrimary("jira:TM-1", "repo_ownership|team-b", "higher tier link to repository B")
}

// An unassigned issue that has a best-tier link to an owned repository takes
// the owner even when the link is stamped before the issue's row (a native
// link carries its dependency row's time), and a second run writes nothing.
func TestAnUnassignedIssueWithAnOwnedLinkTakesTheOwnerOnce(t *testing.T) {
	org := newLinkChangeOrg(t, workItemAttributionMigratedClickHouse(t, context.Background()))
	org.issue("jira:UO-1", nil)
	org.yesterday()
	org.wantPrimary("jira:UO-1", "unassigned|", "setup")
	org.link("A", "jira:UO-1", 5, "native", org.setup)

	first := org.run(org.now)
	org.wantPrimary("jira:UO-1", "repo_ownership|team-a", "owned link stamped before the row")
	if first.RowsWritten == 0 {
		return
	}
	stamped := org.newestComputedAt("jira:UO-1")
	if second := org.run(org.now.Add(time.Minute)); !second.SkippedNoop || second.RowsWritten != 0 {
		t.Errorf("LINK14149 FAIL second run %+v, want a no-op", second)
	}
	if got := org.newestComputedAt("jira:UO-1"); !got.Equal(stamped) {
		t.Errorf("LINK14149 FAIL second run rewrote jira:UO-1: %s -> %s", stamped, got)
	}
}
