//go:build integration

package teamsidentity

import (
	"context"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
)

var rosterRaceAtTeamRead = regexp.MustCompile(`FROM teams FINAL WHERE org_id = \{org_id:String\} AND deleted_at IS NULL AND id = \{team_id:String\}`)

// rosterStep runs other once, at the nth statement writer a sends that
// matches at.
type rosterStep struct {
	at    *regexp.Regexp
	nth   int
	other func()
	seen  int
	fired bool
}

// rosterStepsConn runs other writers' whole writes at chosen points of
// writer a's window.
type rosterStepsConn struct {
	driver.Conn
	steps []*rosterStep
}

func (c *rosterStepsConn) hit(query string) {
	for _, step := range c.steps {
		if !step.fired && step.at.MatchString(query) {
			step.seen++
			if step.seen == step.nth {
				step.fired = true
				step.other()
			}
		}
	}
}

func (c *rosterStepsConn) Exec(ctx context.Context, query string, args ...any) error {
	c.hit(query)
	return c.Conn.Exec(ctx, query, args...)
}

func (c *rosterStepsConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.hit(query)
	return c.Conn.Query(ctx, query, args...)
}

func (c *rosterStepsConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	c.hit(query)
	return c.Conn.QueryRow(ctx, query, args...)
}

func (c *rosterStepsConn) allFired(t *testing.T) {
	t.Helper()
	for i, step := range c.steps {
		if !step.fired {
			t.Fatalf("step %d never reached its point", i)
		}
	}
}

// CHAOS-14081 (r1 class 3): the roster an admin write keeps or edits is read
// inside its insert, not chosen from an earlier read that found no team; and a
// team deleted by an admin has no manual roster for any later writer.
func TestTheRosterAfterAMissingOrDeletedTeamIsReadInTheInsert(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	plain := Store{Conn: conn}
	check := func(t *testing.T, name, teamID string, want []string) {
		t.Helper()
		if got := storedTeamRoster(ctx, t, conn, teamID); slices.Equal(got, want) {
			t.Logf("CELL %s: KEPT roster=%v", name, got)
		} else {
			t.Errorf("CELL %s: LOST roster=%v want=%v", name, got, want)
		}
	}

	// a creates a team it found missing; b creates the same team and sets its
	// roster after a's read and before a's insert.
	t.Run("create_after_missing_read", func(t *testing.T) {
		const teamID = "custom:create-race"
		steps := &rosterStepsConn{Conn: conn, steps: []*rosterStep{{at: rosterRaceAtNewest, nth: 1, other: func() {
			if _, err := plain.CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{Origin: "custom", TeamID: teamID, Name: "B"}); err != nil {
				t.Error(err)
			}
			if _, err := plain.SetMembers(ctx, rosterRaceOrg, teamID, []string{"r"}); err != nil {
				t.Error(err)
			}
		}}}}
		if _, err := (Store{Conn: steps}).CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{Origin: "custom", TeamID: teamID, Name: "A"}); err != nil {
			t.Fatal(err)
		}
		steps.allFired(t)
		check(t, "create_after_missing_read", teamID, []string{"r"})
	})

	// a adds bob to a team it read; the team is deleted before a's second read
	// and created again with its own roster before a's insert.
	t.Run("add_across_delete_and_recreate", func(t *testing.T) {
		const teamID = "gh:recreate-race"
		seedRosterTeam(ctx, t, conn, teamID, "github")
		steps := &rosterStepsConn{Conn: conn, steps: []*rosterStep{
			{at: rosterRaceAtTeamRead, nth: 2, other: func() {
				if _, err := plain.DeleteTeam(ctx, rosterRaceOrg, teamID); err != nil {
					t.Error(err)
				}
			}},
			{at: rosterRaceAtNewest, nth: 1, other: func() {
				if _, err := plain.CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{Origin: "github", TeamID: teamID, Name: "New"}); err != nil {
					t.Error(err)
				}
				if _, err := plain.SetMembers(ctx, rosterRaceOrg, teamID, []string{"fresh"}); err != nil {
					t.Error(err)
				}
			}},
		}}
		if _, err := (Store{Conn: steps}).AddMembers(ctx, rosterRaceOrg, teamID, []string{"bob"}); err != nil {
			t.Fatal(err)
		}
		steps.allFired(t)
		check(t, "add_across_delete_and_recreate", teamID, []string{"bob", "fresh"})
	})

	// A sync that writes a team again after an admin deleted it starts with
	// an empty manual roster.
	t.Run("sync_recreate_after_admin_delete", func(t *testing.T) {
		const teamID = "jira:dddddddd-0000-4000-8000-000000014081"
		seedRosterTeam(ctx, t, conn, teamID, atlassianteams.Provider)
		if deleted, err := plain.DeleteTeam(ctx, rosterRaceOrg, teamID); err != nil || !deleted {
			t.Fatalf("delete: %v %v", deleted, err)
		}
		atlassianTeamWrite(ctx, t, conn, teamID, time.Now().UTC().Add(time.Second))
		check(t, "sync_recreate_after_admin_delete", teamID, []string{})
	})

	// An admin that creates a deleted team again takes nothing of it (control).
	t.Run("admin_recreate_after_delete/control", func(t *testing.T) {
		const teamID = "custom:admin-recreate"
		if _, err := plain.CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{Origin: "custom", TeamID: teamID, Name: "Old"}); err != nil {
			t.Fatal(err)
		}
		if _, err := plain.SetMembers(ctx, rosterRaceOrg, teamID, []string{"old"}); err != nil {
			t.Fatal(err)
		}
		if _, err := plain.DeleteTeam(ctx, rosterRaceOrg, teamID); err != nil {
			t.Fatal(err)
		}
		if _, err := plain.CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{Origin: "custom", TeamID: teamID, Name: "New"}); err != nil {
			t.Fatal(err)
		}
		check(t, "admin_recreate_after_delete/control", teamID, []string{})
	})
}
