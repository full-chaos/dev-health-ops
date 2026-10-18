//go:build integration

package atlassianteams

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// CHAOS-9548: a second run over the same answer writes no ownership row again and still reports the links it
// found; a run whose ownership write fails reports no link as written.
func TestAtlassianOwnershipRepeatIsNotWrittenAgainAndAFailedWriteCountsNothing(t *testing.T) {
	ctx := context.Background()
	first := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
	conn := openClickHouse(t)
	g := newGateway(t, standard)
	collectFor := func(orgID string, now time.Time) Rows {
		t.Helper()
		p := params(everything)
		p.OrgID, p.Now = orgID, now
		rows, err := Collect(ctx, g.client(), p)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	collect := func(now time.Time) Rows { t.Helper(); return collectFor("org-1", now) }
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
	// The rows are org-2's own: its teams rows are written before the links,
	// so the links reach the store and are refused.
	rows := collectFor("org-2", first.Add(2*time.Hour))
	failed, err := Write(ctx, conn, "org-2", rows, everything, scopeOf(integrationCensus{"integration-a": true}, "integration-a"), everyAbsentFactIsGone{})
	if err == nil {
		t.Fatal("the ownership write was refused by the store but Write returned no error")
	}
	if failed.OwnershipWritten != 0 {
		t.Fatalf("a refused ownership write reported %d links written", failed.OwnershipWritten)
	}
}

// A write whose rows belong to another organization is refused before any
// read or write, loudly: an error and a Warn, never a silent drop.
func TestAtlassianWriteRefusesRowsOfAnotherOrganization(t *testing.T) {
	ctx := context.Background()
	conn := openClickHouse(t)
	g := newGateway(t, standard)
	rows, err := Collect(ctx, g.client(), params(everything))
	if err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	_, err = Write(ctx, conn, "org-2", rows, everything, scopeOf(integrationCensus{"integration-a": true}, "integration-a"), everyAbsentFactIsGone{})
	slog.SetDefault(previous)
	if !errors.Is(err, providersync.ErrLinkRowsOfAnotherOrganization) || !strings.Contains(err.Error(), "check rows") {
		t.Fatalf("err = %v, want the rows of another organization refused at the check", err)
	}
	if !strings.Contains(logged.String(), `"msg":"atlassian_teams_rows_of_another_organization_refused"`) || !strings.Contains(logged.String(), `"level":"WARN"`) {
		t.Errorf("no Warn for the refused rows:\n%s", logged.String())
	}
	for _, table := range []string{"teams", "team_memberships", "team_project_ownership"} {
		if got := lines(t, conn, `SELECT toString(count()) FROM `+table)[0]; got != "0" {
			t.Errorf("%s holds %s rows after a refused write, want 0", table, got)
		}
	}
}
