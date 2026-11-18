//go:build integration

package streamhandlers

import (
	"context"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

var externalRosterAtInsert = regexp.MustCompile(`(?i)^\s*INSERT\s+INTO\s+teams\b`)

// externalRosterRaceConn commits an admin roster edit just before the push
// sends its INSERT INTO teams: after every read the push makes.
type externalRosterRaceConn struct {
	driver.Conn
	other func()
	fired bool
}

func (c *externalRosterRaceConn) Exec(ctx context.Context, query string, args ...any) error {
	if !c.fired && externalRosterAtInsert.MatchString(query) {
		c.fired = true
		c.other()
	}
	return c.Conn.Exec(ctx, query, args...)
}

// CHAOS-14081 (W12): a team.v1 push never edits the admin roster. An admin
// edit that commits after the push read the team and before its insert must
// survive, also when the pushed updatedAt is the newer version (a push may
// state a time up to the clamp skew ahead of the server).
func TestAPushedTeamKeepsARosterEditCommittedBeforeItsInsert(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	const orgID = "team-roster-kept-push-org"
	const teamID = "gh:platform"
	pushedAt := time.Now().UTC().Add(2 * time.Second).Truncate(time.Second)
	version := func(roster []string, at time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider)
			VALUES (?, ?, 'Platform', ?, [], [], 1, ?, ?, 'github')`, teamID, uuid.New(), roster, at, orgID); err != nil {
			t.Fatal(err)
		}
	}
	version([]string{"x"}, pushedAt.Add(-time.Hour))
	race := &externalRosterRaceConn{Conn: conn, other: func() { version([]string{"b", "x"}, pushedAt.Add(-time.Millisecond)) }}
	sink, err := NewClickHouseExternalBatchSink(race)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Write(context.WithoutCancel(ctx), externalSinkBatch{
		Pointer: externalPointer{
			IngestionID: uuid.New(), OrgID: orgID, SourceSystem: "github",
			SourceInstance: "acme/api", SchemaVersion: externalSchemaVersion,
		},
		SourceID: uuid.New(),
		Records: []externalSinkRecord{{Index: 0, Kind: "team.v1", ExternalID: "team-" + uuid.NewString(), Payload: map[string]any{
			"id": "platform", "name": "Platform", "updatedAt": pushedAt.Format(time.RFC3339),
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	if !race.fired {
		t.Fatal("the push never reached its insert")
	}
	for _, when := range []string{"before merge", "after OPTIMIZE FINAL"} {
		if when != "before merge" {
			if err := conn.Exec(ctx, `OPTIMIZE TABLE teams FINAL`); err != nil {
				t.Fatal(err)
			}
		}
		var got []string
		if err := conn.QueryRow(ctx, `SELECT manual_members FROM teams FINAL WHERE org_id = ? AND id = ?`, orgID, teamID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		if want := []string{"b", "x"}; slices.Equal(got, want) {
			t.Logf("CELL W12_push %s: KEPT roster=%v", when, got)
		} else {
			t.Errorf("CELL W12_push %s: LOST roster=%v want=%v", when, got, want)
		}
	}
}
