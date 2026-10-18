package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// Every catalog writer of teams carries created_at from the stored version:
// a stored team keeps its time, a new team takes its own updated_at, and a
// failed read aborts the write before any batch is prepared.
func TestCatalogTeamWritersCarryCreatedAt(t *testing.T) {
	const org, id = "org-1", "team-x"
	teamUUID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+id)).String()
	updated := time.Date(2026, 9, 26, 12, 0, 0, 123456789, time.UTC)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	writers := map[string]func(conn *fakeGitLabWriteTeamsConn) error{
		"github": func(conn *fakeGitLabWriteTeamsConn) error {
			return GitHubTeamCatalogClickHouseEffects{Conn: conn}.WriteTeams(context.Background(), org, []githubTeamRow{{
				ID: id, TeamUUID: teamUUID, Name: id, Members: []string{}, ProjectKeys: []string{}, RepoPatterns: []string{},
				IsActive: 1, UpdatedAt: updated, OrgID: org, Provider: githubTeamCatalogProvider,
			}})
		},
		"jira": func(conn *fakeGitLabWriteTeamsConn) error {
			return JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}.writeTeams(context.Background(),
				Claim{Unit: Unit{OrgID: org}}, []jiraTeamCatalogTeamRow{{
					ID: id, TeamUUID: teamUUID, Name: id, Members: []string{}, ProjectKeys: []string{}, RepoPatterns: []string{},
					IsActive: 1, UpdatedAt: updated, OrgID: org,
				}})
		},
		"linear": func(conn *fakeGitLabWriteTeamsConn) error {
			return LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}.writeTeams(context.Background(),
				[]linearReferenceTeamRow{{
					ID: id, TeamUUID: teamUUID, Name: id, Members: []string{}, ProjectKeys: []string{}, RepoPatterns: []string{},
					IsActive: 1, UpdatedAt: updated, OrgID: org,
				}})
		},
	}
	original := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	createdOf := func(t *testing.T, conn *fakeGitLabWriteTeamsConn) time.Time {
		t.Helper()
		if conn.batch == nil || len(conn.batch.appended) != 1 {
			t.Fatalf("no row reached the batch: %#v", conn.batch)
		}
		row := conn.batch.appended[0]
		created, ok := row[len(row)-1].(time.Time)
		if !ok {
			t.Fatalf("the last value is %T, want created_at", row[len(row)-1])
		}
		return created
	}
	for name, write := range writers {
		t.Run(name+" keeps the stored creation time", func(t *testing.T) {
			conn := &fakeGitLabWriteTeamsConn{created: map[string]time.Time{id: original}}
			if err := write(conn); err != nil {
				t.Fatal(err)
			}
			if got := createdOf(t, conn); !got.Equal(original) {
				t.Fatalf("created_at = %v, want %v", got, original)
			}
		})
		t.Run(name+" stamps a new team with its updated_at", func(t *testing.T) {
			conn := &fakeGitLabWriteTeamsConn{}
			if err := write(conn); err != nil {
				t.Fatal(err)
			}
			if got := createdOf(t, conn); !got.Equal(updated.Truncate(time.Microsecond)) {
				t.Fatalf("created_at = %v, want %v", got, updated.Truncate(time.Microsecond))
			}
		})
		t.Run(name+" aborts when the carry read fails", func(t *testing.T) {
			conn := &fakeGitLabWriteTeamsConn{createdErr: errors.New("clickhouse unavailable")}
			if err := write(conn); err == nil {
				t.Fatal("expected the write to fail")
			}
			if conn.batch != nil {
				t.Fatal("a batch was prepared although created_at could not be carried")
			}
		})
	}
}
