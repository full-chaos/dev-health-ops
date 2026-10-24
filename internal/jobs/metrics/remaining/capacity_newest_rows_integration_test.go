//go:build integration

package remaining

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"
)

// The capacity reads must give the newest version of each key, with the
// live-row rule applied to that newest version, and must never pick at random
// between versions that carry the same computed_at.
func TestCapacityReadsCollapseVersionsByTheRequiredKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := migratedClickHouse(t, ctx, OperationalOrderingRevision)
	executor := &CapacityExecutor{conn: conn}

	const org = "org-capacity-newest-rows"
	today := time.Now().UTC().Truncate(24 * time.Hour)
	day := today.AddDate(0, 0, -2)
	older := today.Add(-10 * time.Hour)
	newer := today.Add(-9 * time.Hour)

	write := func(team string, computedAt time.Time, teamName string, completed, wip uint32) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(org_id, day, provider, work_scope_id, team_id, team_name, items_completed, wip_count_end_of_day, computed_at)
			VALUES (?, ?, 'github', 'scope', ?, ?, ?, ?, ?)`,
			org, day, team, teamName, completed, wip, computedAt); err != nil {
			t.Fatalf("insert %s: %v", team, err)
		}
	}
	// a: the newest version is a retraction row (every measure 0), the older
	// one is live. The key has no rows in scope.
	write("a", older, "a", 5, 5)
	write("a", newer, "a", 0, 0)
	// b: two versions that differ in a non-key column and in the measures.
	write("b", older, "old", 3, 2)
	write("b", newer, "new", 4, 6)
	// c: two rows with the same computed_at and different measures.
	write("c", newer, "c", 7, 7)
	write("c", newer, "c", 9, 9)

	throughput := func(team string) []int {
		t.Helper()
		history, err := executor.loadThroughput(ctx, org, capacityTarget{TeamID: &team}, 30, today)
		if err != nil {
			t.Fatalf("throughput %s: %v", team, err)
		}
		return history.DailyThroughputs
	}
	backlog := func(team string) int {
		t.Helper()
		value, err := executor.loadBacklog(ctx, org, capacityTarget{TeamID: &team})
		if err != nil {
			t.Fatalf("backlog %s: %v", team, err)
		}
		return value
	}

	if got := throughput("a"); len(got) != 0 {
		t.Errorf("a: a newest retraction row must leave no day, got %v", got)
	}
	if got := backlog("a"); got != 0 {
		t.Errorf("a: backlog = %d, want 0 (newest version holds no work)", got)
	}
	if got := throughput("b"); !reflect.DeepEqual(got, []int{4}) {
		t.Errorf("b: throughput = %v, want [4] (newest version only, not 7)", got)
	}
	if got := backlog("b"); got != 6 {
		t.Errorf("b: backlog = %d, want 6", got)
	}

	firstThroughput, firstBacklog := throughput("c"), backlog("c")
	if len(firstThroughput) != 1 || (firstThroughput[0] != 7 && firstThroughput[0] != 9) {
		t.Errorf("c: throughput = %v, want exactly one of the tied rows (7 or 9), not their sum", firstThroughput)
	}
	if firstBacklog != 7 && firstBacklog != 9 {
		t.Errorf("c: backlog = %d, want 7 or 9, not 16", firstBacklog)
	}
	for attempt := 0; attempt < 5; attempt++ {
		if got := throughput("c"); !reflect.DeepEqual(got, firstThroughput) {
			t.Fatalf("c: tied rows picked differently on read %d: %v then %v", attempt, firstThroughput, got)
		}
		if got := backlog("c"); got != firstBacklog {
			t.Fatalf("c: tied rows picked differently on read %d: %d then %d", attempt, firstBacklog, got)
		}
	}
	if firstBacklog != firstThroughput[0] {
		t.Errorf("c: backlog %d and throughput %d came from different tied rows", firstBacklog, firstThroughput[0])
	}

	targets, err := executor.resolveScopes(ctx, org, capacityScope{AllTeams: true})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	var teams []string
	for _, target := range targets {
		teams = append(teams, *target.TeamID)
	}
	sort.Strings(teams)
	if !reflect.DeepEqual(teams, []string{"b", "c"}) {
		t.Errorf("discovered teams = %v, want [b c] (a's newest version is a retraction row)", teams)
	}
}
