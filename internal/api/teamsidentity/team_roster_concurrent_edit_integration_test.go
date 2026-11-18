//go:build integration

package teamsidentity

import (
	"context"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

var (
	// rosterRaceAtInsert runs the other writer just before writer a's
	// INSERT INTO teams: after every read a makes.
	rosterRaceAtInsert = regexp.MustCompile(`(?i)^\s*INSERT\s+INTO\s+teams\b`)
	// rosterRaceAtNewest runs it just before an admin write reads the newest
	// stored version: after its read of the team, before its version bump.
	rosterRaceAtNewest = regexp.MustCompile(`maxOrNull\(updated_at\) FROM teams`)
)

// rosterRaceConn runs another writer's whole write once, when writer a sends
// the first statement that matches at.
type rosterRaceConn struct {
	driver.Conn
	at    *regexp.Regexp
	other func()
	fired bool
}

func (c *rosterRaceConn) hit(query string) {
	if !c.fired && c.at.MatchString(query) {
		c.fired = true
		c.other()
	}
}

func (c *rosterRaceConn) Exec(ctx context.Context, query string, args ...any) error {
	c.hit(query)
	return c.Conn.Exec(ctx, query, args...)
}

func (c *rosterRaceConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.hit(query)
	return c.Conn.PrepareBatch(ctx, query, opts...)
}

func (c *rosterRaceConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.hit(query)
	return c.Conn.Query(ctx, query, args...)
}

func (c *rosterRaceConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	c.hit(query)
	return c.Conn.QueryRow(ctx, query, args...)
}

const rosterRaceOrg = "team-roster-race-org"

// seedRosterTeam writes a team an hour old and gives it the roster [x].
func seedRosterTeam(ctx context.Context, t *testing.T, conn driver.Conn, teamID, provider string) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key)
		VALUES (?, ?, 'Platform', [], [], [], 1, now64(6) - INTERVAL 1 HOUR, ?, ?, ?)`,
		teamID, uuid.New(), rosterRaceOrg, provider, "native-"+teamID); err != nil {
		t.Fatal(err)
	}
	if _, err := (Store{Conn: conn}).SetMembers(ctx, rosterRaceOrg, teamID, []string{"x"}); err != nil {
		t.Fatal(err)
	}
}

func storedTeamRoster(ctx context.Context, t *testing.T, conn driver.Conn, teamID string) []string {
	t.Helper()
	var roster []string
	if err := conn.QueryRow(ctx, `SELECT manual_members FROM teams FINAL WHERE org_id = ? AND id = ?`, rosterRaceOrg, teamID).Scan(&roster); err != nil {
		t.Fatal(err)
	}
	slices.Sort(roster)
	return roster
}

// assertRoster checks teams FINAL before a merge and after OPTIMIZE FINAL.
func assertRoster(ctx context.Context, t *testing.T, conn driver.Conn, name, teamID string, want []string) {
	t.Helper()
	for _, when := range []string{"before merge", "after OPTIMIZE FINAL"} {
		if when != "before merge" {
			if err := conn.Exec(ctx, `OPTIMIZE TABLE teams FINAL`); err != nil {
				t.Fatal(err)
			}
		}
		got := storedTeamRoster(ctx, t, conn, teamID)
		if slices.Equal(got, want) {
			t.Logf("CELL %s %s: KEPT roster=%v", name, when, got)
		} else {
			t.Errorf("CELL %s %s: LOST roster=%v want=%v", name, when, got, want)
		}
	}
}

// atlassianTeamWrite is one Atlassian teams sync of the team (W11), written
// at version.
func atlassianTeamWrite(ctx context.Context, t *testing.T, conn driver.Conn, teamID string, version time.Time) {
	t.Helper()
	rows := atlassianteams.Rows{Teams: []atlassianteams.TeamRow{{
		ID: teamID, TeamUUID: uuid.New(), Name: "Platform", ProjectKeys: []string{}, IsActive: 1, UpdatedAt: version,
		OrgID: rosterRaceOrg, Provider: atlassianteams.Provider, NativeTeamKey: "native-" + teamID,
	}}}
	if _, err := atlassianteams.Write(ctx, conn, rosterRaceOrg, rows, atlassianteams.Selections{Structure: true}, providersync.ScopeProof{}, nil); err != nil {
		t.Fatal(err)
	}
}

// CHAOS-14081: two writers of one team's manual_members interleave (a reads,
// b writes, a inserts). Every admin write path is writer a, at two points of
// its window; writer b is an admin AddMembers. The roster must hold both
// edits (a full set: a's, the last one), before and after a merge.
func TestAConcurrentWriterDoesNotLoseAnAdminRosterEdit(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	plain := Store{Conn: conn}
	addB := func(t *testing.T, teamID string) func() {
		return func() {
			if _, err := plain.AddMembers(ctx, rosterRaceOrg, teamID, []string{"b"}); err != nil {
				t.Errorf("writer b: %v", err)
			}
		}
	}
	importKey := 0
	writers := []struct {
		name     string
		provider string
		write    func(s Store, teamID string) error
		want     []string
	}{
		{"W3_add", "github", func(s Store, id string) error {
			_, err := s.AddMembers(ctx, rosterRaceOrg, id, []string{"a"})
			return err
		}, []string{"a", "b", "x"}},
		{"W3_remove", "github", func(s Store, id string) error {
			_, err := s.RemoveMembers(ctx, rosterRaceOrg, id, map[string]bool{"x": true})
			return err
		}, []string{"b"}},
		{"W2_set_members", "github", func(s Store, id string) error {
			_, err := s.SetMembers(ctx, rosterRaceOrg, id, []string{"s", "x"})
			return err
		}, []string{"s", "x"}},
		{"W1_body_roster", "github", func(s Store, id string) error {
			_, err := s.CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{TeamID: id, Name: "Platform", ManualMembers: &[]string{"s", "x"}})
			return err
		}, []string{"s", "x"}},
		{"W1_W5_no_roster", "github", func(s Store, id string) error {
			_, err := s.CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{TeamID: id, Name: "Platform (renamed)"})
			return err
		}, []string{"b", "x"}},
		{"W4_delete", "github", func(s Store, id string) error {
			_, err := s.DeleteTeam(ctx, rosterRaceOrg, id)
			return err
		}, []string{"b", "x"}},
		{"W6_import", "jira", nil, []string{"b", "x"}},
	}
	for _, point := range []struct {
		name string
		at   *regexp.Regexp
	}{{"at_insert", rosterRaceAtInsert}, {"at_newest_read", rosterRaceAtNewest}} {
		for _, w := range writers {
			name := w.name + "/" + point.name
			t.Run(name, func(t *testing.T) {
				teamID := "gh:race-" + w.name + "-" + point.name
				write := w.write
				if w.write == nil {
					importKey++
					key := "RIMP" + string(rune('A'+importKey))
					teamID = importedTeamID("jira", key)
					write = func(s Store, _ string) error {
						_, err := s.projectTeam(ctx, rosterRaceOrg, discoveredJiraTeam(key, "Imported"), "merge")
						return err
					}
				}
				seedRosterTeam(ctx, t, conn, teamID, w.provider)
				race := &rosterRaceConn{Conn: conn, at: point.at, other: addB(t, teamID)}
				if err := write(Store{Conn: race}, teamID); err != nil {
					t.Fatal(err)
				}
				if !race.fired {
					t.Fatal("writer a never reached the interleave point")
				}
				assertRoster(ctx, t, conn, name, teamID, w.want)
			})
		}
	}

	// W11 (a sync writer) against an admin edit, both orders.
	t.Run("W11_sync_inside_admin_add", func(t *testing.T) {
		const teamID = "jira:aaaaaaaa-0000-4000-8000-000000014081"
		seedRosterTeam(ctx, t, conn, teamID, atlassianteams.Provider)
		race := &rosterRaceConn{Conn: conn, at: rosterRaceAtInsert, other: func() {
			atlassianTeamWrite(ctx, t, conn, teamID, time.Now().UTC())
		}}
		if _, err := (Store{Conn: race}).AddMembers(ctx, rosterRaceOrg, teamID, []string{"a"}); err != nil {
			t.Fatal(err)
		}
		if !race.fired {
			t.Fatal("the admin write never reached its insert")
		}
		assertRoster(ctx, t, conn, "W11_sync_inside_admin_add", teamID, []string{"a", "x"})
	})
	t.Run("W11_sync_clock_ahead", func(t *testing.T) {
		const teamID = "jira:bbbbbbbb-0000-4000-8000-000000014081"
		seedRosterTeam(ctx, t, conn, teamID, atlassianteams.Provider)
		race := &rosterRaceConn{Conn: conn, at: rosterRaceAtInsert, other: addB(t, teamID)}
		atlassianTeamWrite(ctx, t, race, teamID, time.Now().UTC().Add(2*time.Second))
		if !race.fired {
			t.Fatal("the sync never reached its insert")
		}
		assertRoster(ctx, t, conn, "W11_sync_clock_ahead", teamID, []string{"b", "x"})
	})
	t.Run("W11_sync_reads_first/control", func(t *testing.T) {
		const teamID = "jira:cccccccc-0000-4000-8000-000000014081"
		seedRosterTeam(ctx, t, conn, teamID, atlassianteams.Provider)
		version := time.Now().UTC()
		race := &rosterRaceConn{Conn: conn, at: rosterRaceAtInsert, other: addB(t, teamID)}
		atlassianTeamWrite(ctx, t, race, teamID, version)
		if !race.fired {
			t.Fatal("the sync never reached its insert")
		}
		assertRoster(ctx, t, conn, "W11_sync_reads_first/control", teamID, []string{"b", "x"})
	})
}
