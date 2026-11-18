//go:build integration

package providersync

import (
	"context"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var rosterKeptAtInsert = regexp.MustCompile(`(?i)^\s*INSERT\s+INTO\s+teams\b`)

// rosterKeptRaceConn commits an admin roster edit just before the wrapped
// sync writer sends its INSERT INTO teams: after every read the sync makes.
type rosterKeptRaceConn struct {
	driver.Conn
	other func()
	fired bool
}

func (c *rosterKeptRaceConn) hit(query string) {
	if !c.fired && rosterKeptAtInsert.MatchString(query) {
		c.fired = true
		c.other()
	}
}

func (c *rosterKeptRaceConn) Exec(ctx context.Context, query string, args ...any) error {
	c.hit(query)
	return c.Conn.Exec(ctx, query, args...)
}

func (c *rosterKeptRaceConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.hit(query)
	return c.Conn.PrepareBatch(ctx, query, opts...)
}

// CHAOS-14081: a sync team writer (W7-W10) never edits the admin roster. An
// admin edit that commits after the sync read the team and before its insert
// must survive, also when the sync row's version is the newer one (the admin
// stamped its version before the sync did, or the sync host's clock is
// ahead). The roster is read inside the insert statement.
func TestASyncTeamWriteKeepsARosterEditCommittedBeforeItsInsert(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "team-roster-kept-org"
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	claim := nativeTestClaim("jira", "teams")
	claim.OrgID = orgID

	version := func(teamID, provider string, roster []string, at time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider)
			VALUES (?, ?, 'Platform', ?, [], [], 1, ?, ?, ?)`, teamID, uuid.New(), roster, at, orgID, provider); err != nil {
			t.Fatal(err)
		}
	}
	roster := func(teamID string) []string {
		t.Helper()
		var got []string
		if err := conn.QueryRow(ctx, `SELECT manual_members FROM teams FINAL WHERE org_id = ? AND id = ?`, orgID, teamID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		return got
	}

	writers := []struct {
		name, provider, teamID string
		write                  func(conn driver.Conn, teamID string, at time.Time) error
	}{
		{"W7_github", "github", "gh:roster-kept", func(c driver.Conn, id string, at time.Time) error {
			return GitHubTeamCatalogClickHouseEffects{Conn: c}.WriteTeams(ctx, orgID, []githubTeamRow{{
				ID: id, TeamUUID: uuid.NewString(), Name: "Platform", ProjectKeys: []string{}, RepoPatterns: []string{},
				IsActive: 1, UpdatedAt: at, OrgID: orgID, Provider: githubTeamCatalogProvider,
			}})
		}},
		{"W8_gitlab", "gitlab", "gl:roster-kept", func(c driver.Conn, id string, at time.Time) error {
			return GitLabTeamCatalogClickHouseEffects{Conn: c, Lease: lease}.writeTeams(ctx, claim, []gitlabTeamCatalogTeamRow{{
				ID: id, TeamUUID: uuid.NewString(), Name: "Platform", ProjectKeys: []string{}, RepoPatterns: []string{},
				IsActive: 1, UpdatedAt: at, OrgID: orgID, Provider: "gitlab",
			}})
		}},
		{"W9_jira", "jira", "jira:ROSTERKEPT", func(c driver.Conn, id string, at time.Time) error {
			return JiraTeamCatalogClickHouseEffects{Conn: c, Lease: lease}.writeTeams(ctx, claim, []jiraTeamCatalogTeamRow{{
				ID: id, TeamUUID: uuid.NewString(), Name: "Platform", ProjectKeys: []string{}, RepoPatterns: []string{},
				IsActive: 1, UpdatedAt: at, OrgID: orgID, Provider: "jira",
			}})
		}},
		{"W10_linear", "linear", "linear:ROSTERKEPT", func(c driver.Conn, id string, at time.Time) error {
			return LinearReferenceCatalogClickHouseEffects{Conn: c, Lease: lease}.writeTeams(ctx, []linearReferenceTeamRow{{
				ID: id, TeamUUID: uuid.NewString(), Name: "Platform", ProjectKeys: []string{}, RepoPatterns: []string{},
				IsActive: 1, UpdatedAt: at, OrgID: orgID, Provider: "linear",
			}})
		}},
	}
	for _, w := range writers {
		t.Run(w.name, func(t *testing.T) {
			syncAt := time.Now().UTC().Add(2 * time.Second)
			version(w.teamID, w.provider, []string{"x"}, syncAt.Add(-time.Hour))
			race := &rosterKeptRaceConn{Conn: conn, other: func() {
				version(w.teamID, w.provider, []string{"b", "x"}, syncAt.Add(-time.Millisecond))
			}}
			if err := w.write(race, w.teamID, syncAt); err != nil {
				t.Fatal(err)
			}
			if !race.fired {
				t.Fatal("the sync never reached its insert")
			}
			for _, when := range []string{"before merge", "after OPTIMIZE FINAL"} {
				if when != "before merge" {
					if err := conn.Exec(ctx, `OPTIMIZE TABLE teams FINAL`); err != nil {
						t.Fatal(err)
					}
				}
				if got, want := roster(w.teamID), []string{"b", "x"}; slices.Equal(got, want) {
					t.Logf("CELL %s %s: KEPT roster=%v", w.name, when, got)
				} else {
					t.Errorf("CELL %s %s: LOST roster=%v want=%v", w.name, when, got, want)
				}
			}
		})
	}
}
