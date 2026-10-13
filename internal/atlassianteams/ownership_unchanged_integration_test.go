//go:build integration

package atlassianteams

import (
	"context"
	"testing"
	"time"
)

// CHAOS-9548: a second run over the same answer writes no ownership row again and still reports the links it
// found; a run whose ownership write fails reports no link as written.
func TestAtlassianOwnershipRepeatIsNotWrittenAgainAndAFailedWriteCountsNothing(t *testing.T) {
	ctx := context.Background()
	first := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
	conn := openClickHouse(t)
	g := newGateway(t, standard)
	collect := func(now time.Time) Rows {
		t.Helper()
		p := params(everything)
		p.Now = now
		rows, err := Collect(ctx, g.client(), p)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	census := integrationCensus{"integration-a": true}
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES team_project_ownership"); err != nil {
		t.Fatal(err)
	}
	stored := func() string {
		return lines(t, conn, `SELECT toString(count()) FROM team_project_ownership WHERE org_id = 'org-1'`)[0]
	}
	firstResult, err := writeForScopeTest(ctx, conn, "org-1", collect(first), census, "integration-a")
	if err != nil {
		t.Fatal(err)
	}
	if firstResult.OwnershipWritten == 0 || firstResult.OwnershipUnchanged != 0 {
		t.Fatalf("setup: first run written=%d unchanged=%d, want links written", firstResult.OwnershipWritten, firstResult.OwnershipUnchanged)
	}
	after := stored()
	repeat, err := writeForScopeTest(ctx, conn, "org-1", collect(first.Add(time.Hour)), census, "integration-a")
	if err != nil {
		t.Fatal(err)
	}
	if repeat.OwnershipWritten != firstResult.OwnershipWritten || repeat.OwnershipUnchanged != firstResult.OwnershipWritten {
		t.Fatalf("repeat run found %d links, %d unchanged; want %d found and all unchanged", repeat.OwnershipWritten, repeat.OwnershipUnchanged, firstResult.OwnershipWritten)
	}
	if got := stored(); got != after {
		t.Fatalf("a repeat run changed the stored rows from %s to %s", after, got)
	}

	// A write that the store rejects counts nothing.
	if err := conn.Exec(ctx, "ALTER TABLE team_project_ownership ADD CONSTRAINT refuse_every_link CHECK project_id = 'no-such-project'"); err != nil {
		t.Fatal(err)
	}
	rows := collect(first.Add(2 * time.Hour))
	failed, err := Write(ctx, conn, "org-2", rows, everything, scopeOf(integrationCensus{"integration-a": true}, "integration-a"), everyAbsentFactIsGone{})
	if err == nil {
		t.Fatal("the ownership write was refused by the store but Write returned no error")
	}
	if failed.OwnershipWritten != 0 {
		t.Fatalf("a refused ownership write reported %d links written", failed.OwnershipWritten)
	}
}
