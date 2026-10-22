//go:build integration

package remaining

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// D5987 option C: the owners of an item are read at the item's day (section 0.4j), and for a day before the first
// recorded ownership of its repository the repository's FIRST recorded owners apply. work_item_attribution reads
// ownership only through teamattribution's LoadRepos, which renders the one rule of package ownershipwindow; this test
// runs the backstop and reads the rows it writes.
func TestWorkItemAttributionAppliesTheFirstRecordedOwnersBackward(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	const org = "org-first-owners-attribution"
	day := func(month time.Month, d int) time.Time { return time.Date(2026, month, d, 0, 0, 0, 0, time.UTC) }
	synced := day(time.September, 1)
	now := time.Now().UTC()
	repoOf := func(name string) uuid.UUID { return uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+name)) }
	names := []string{"pair", "gap", "inactive-first", "revoked-first", "only-future", "tie"}
	for _, name := range names {
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, 'github', ?)`,
			repoOf(name), "acme/"+repoOf(name).String(), org, synced); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
        VALUES ('team-gone', generateUUIDv4(), 'Gone', [], ?, ?, ?, 'github', 0)`, synced, synced, org); err != nil {
		t.Fatal(err)
	}

	type ownerRow struct {
		team    string
		repo    string
		primary uint8
		share   float64
		from    time.Time
		to      *time.Time
	}
	revokedAt := day(time.February, 20)
	endedAt := day(time.April, 1)
	rows := []ownerRow{
		// pair: team-a main 0.6 and team-b co-owner 0.4 from March 1 (D5988).
		{"team-a", "pair", 1, 0.6, day(time.March, 1), nil},
		{"team-b", "pair", 2, 0.4, day(time.March, 1), nil},
		// gap: team-a from March 1 to April 1, team-c from May 1.
		{"team-a", "gap", 1, 1, day(time.March, 1), &endedAt},
		{"team-c", "gap", 1, 1, day(time.May, 1), nil},
		// inactive-first: an inactive team's earlier row does not set the first record, team-a's March 1 does.
		{"team-gone", "inactive-first", 1, 1, day(time.February, 1), nil},
		{"team-a", "inactive-first", 1, 1, day(time.March, 1), nil},
		// revoked-first: the first owner's row ended before the later owner began; the first record is still its start.
		{"team-x", "revoked-first", 1, 1, day(time.February, 1), &revokedAt},
		{"team-a", "revoked-first", 1, 1, day(time.March, 1), nil},
		// only-future: its one row starts after today.
		{"team-a", "only-future", 1, 1, now.AddDate(0, 0, 30), nil},
		// tie: two main owners at the same first record (D5990).
		{"team-a", "tie", 1, 0.5, day(time.March, 1), nil},
		{"team-b", "tie", 1, 0.5, day(time.March, 1), nil},
	}
	for _, r := range rows {
		if err := conn.Exec(ctx, `
            INSERT INTO team_repo_ownership
                (org_id, provider, team_id, repo_id, repo_full_name, match_type,
                 source, is_primary, specificity, priority, share, valid_from, valid_to, updated_at)
            VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, 100, 0, ?, ?, ?, ?)
        `, org, r.team, repoOf(r.repo), "acme/"+repoOf(r.repo).String(), r.primary, r.share, r.from, r.to, synced); err != nil {
			t.Fatalf("seed team_repo_ownership: %v", err)
		}
	}

	type item struct {
		id        string
		repo      string
		completed *time.Time
	}
	completedOn := func(d time.Time) *time.Time { at := d.Add(13 * time.Hour); return &at }
	var items []item
	add := func(repo, label string, completed *time.Time) {
		items = append(items, item{id: repo + "@" + label, repo: repo, completed: completed})
	}
	for _, name := range []string{"pair", "tie"} {
		add(name, "before-first", completedOn(day(time.February, 15)))
		add(name, "at-first", completedOn(day(time.March, 1)))
		add(name, "after-first", completedOn(day(time.June, 1)))
	}
	add("gap", "before-first", completedOn(day(time.February, 15)))
	add("gap", "in-gap", completedOn(day(time.April, 15)))
	add("gap", "after-gap", completedOn(day(time.June, 1)))
	add("inactive-first", "before-the-inactive-row", completedOn(day(time.January, 15)))
	add("inactive-first", "before-first", completedOn(day(time.February, 15)))
	add("inactive-first", "after-first", completedOn(day(time.June, 1)))
	add("revoked-first", "before-first", completedOn(day(time.January, 15)))
	add("revoked-first", "after-revoke-before-next", completedOn(day(time.February, 25)))
	add("revoked-first", "after-next-start", completedOn(day(time.June, 1)))
	add("only-future", "not-completed", nil)
	add("only-future", "completed-before-first", completedOn(day(time.February, 15)))
	for _, it := range items {
		status := "done"
		if it.completed == nil {
			status = "in_progress"
		}
		batch, err := conn.PrepareBatch(ctx,
			`INSERT INTO work_items (repo_id, work_item_id, provider, project_id, org_id, status, created_at, completed_at, last_synced)`)
		if err != nil {
			t.Fatalf("prepare work_items batch: %v", err)
		}
		if err := batch.Append(repoOf(it.repo), it.id, "github", "", org, status, day(time.January, 1), it.completed, now); err != nil {
			t.Fatalf("append work_items row: %v", err)
		}
		if err := batch.Send(); err != nil {
			t.Fatalf("send work_items batch: %v", err)
		}
	}

	waitForWorkItemAttributionRepoFactVisible(t, ctx, conn, org)
	outcome, err := executor.ComputeOrg(ctx, org)
	if err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}
	if outcome.SkippedNoop || outcome.ItemsSeen != len(items) {
		t.Fatalf("the backstop did not rederive every item: %+v, want %d items", outcome, len(items))
	}

	owners := firstOwnersAttributionRows(t, ctx, conn, org)
	teams := func(id string, primaryOnly bool) []string {
		var out []string
		for _, r := range owners[id] {
			if r.primary == 1 || !primaryOnly {
				out = append(out, r.team)
			}
		}
		sort.Strings(out)
		return out
	}
	want := func(id string, primaryOnly bool, teamIDs ...string) {
		t.Helper()
		got := teams(id, primaryOnly)
		if len(got) != len(teamIDs) || (len(got) > 0 && !reflect.DeepEqual(got, teamIDs)) {
			t.Errorf("item %s: primary teams %v (all rows %+v), want %v", id, got, owners[id], teamIDs)
		}
	}

	// Before, at and after the first record: the item of a day before the first record has the first owners.
	want("pair@before-first", true, "team-a")
	want("pair@at-first", true, "team-a")
	want("pair@after-first", true, "team-a")
	want("tie@before-first", true, "team-a", "team-b")
	want("tie@at-first", true, "team-a", "team-b")
	want("tie@after-first", true, "team-a", "team-b")
	// Shares: the rows before the first record are the rows of the first record, and the co-owner stays a co-owner.
	for _, label := range []string{"before-first", "at-first"} {
		if got, wantRows := owners["pair@"+label], owners["pair@after-first"]; !reflect.DeepEqual(got, wantRows) {
			t.Errorf("pair@%s rows = %+v, want the rows of the first record %+v (D5988)", label, got, wantRows)
		}
		if got, wantRows := owners["tie@"+label], owners["tie@after-first"]; !reflect.DeepEqual(got, wantRows) {
			t.Errorf("tie@%s rows = %+v, want the rows of the first record %+v (D5990)", label, got, wantRows)
		}
	}
	want("pair@before-first", false, "team-a", "team-b")
	// A gap between two records has no owner.
	want("gap@before-first", true, "team-a")
	want("gap@in-gap", true)
	want("gap@after-gap", true, "team-c")
	// An inactive team's earlier row does not set the first record.
	want("inactive-first@before-the-inactive-row", true, "team-a")
	want("inactive-first@before-first", true, "team-a")
	want("inactive-first@after-first", true, "team-a")
	// A row revoked before the next owner began is still the first record.
	want("revoked-first@before-first", true, "team-x")
	want("revoked-first@after-revoke-before-next", true)
	want("revoked-first@after-next-start", true, "team-a")
	// A repository whose only record starts in the future has those owners for today and for the past.
	want("only-future@not-completed", true, "team-a")
	want("only-future@completed-before-first", true, "team-a")
}

type firstOwnersAttributionRow struct {
	team    string
	source  string
	primary uint8
}

func firstOwnersAttributionRows(t *testing.T, ctx context.Context, conn driver.Conn, org string) map[string][]firstOwnersAttributionRow {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT work_item_id, ifNull(team_id, ''), source, is_primary
FROM work_item_team_attributions FINAL
WHERE org_id = ?
ORDER BY work_item_id, source, team_id`, org)
	if err != nil {
		t.Fatalf("query work_item_team_attributions: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string][]firstOwnersAttributionRow{}
	for rows.Next() {
		var id string
		var row firstOwnersAttributionRow
		if err := rows.Scan(&id, &row.team, &row.source, &row.primary); err != nil {
			t.Fatalf("scan work_item_team_attributions: %v", err)
		}
		if row.team != "" {
			out[id] = append(out[id], row)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read work_item_team_attributions: %v", err)
	}
	return out
}
