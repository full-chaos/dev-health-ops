package providersync

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fakeLinearOrphanRows is a driver.Rows double over the (id, name) shape
// RetireOrphanedLinearPseudoProjects reads -- mirrors the established
// fakeGitLabGuardMembershipRows convention (gitlab_team_catalog_guards_fakeconn_test.go)
// for pre-write reconciliation reads in this package.
type fakeLinearOrphanRows struct {
	driver.Rows
	rows  [][2]string
	index int
}

func (r *fakeLinearOrphanRows) Next() bool {
	r.index++
	return r.index < len(r.rows)
}

func (r *fakeLinearOrphanRows) Scan(dest ...any) error {
	id, ok1 := dest[0].(*string)
	name, ok2 := dest[1].(*string)
	if !ok1 || !ok2 {
		return fmt.Errorf("fakeLinearOrphanRows: unexpected Scan dest shape")
	}
	*id = r.rows[r.index][0]
	*name = r.rows[r.index][1]
	return nil
}

func (r *fakeLinearOrphanRows) Close() error { return nil }
func (r *fakeLinearOrphanRows) Err() error   { return nil }

// fakeLinearOrphanConn is a driver.Conn double covering exactly the one
// query RetireOrphanedLinearPseudoProjects issues.
type fakeLinearOrphanConn struct {
	driver.Conn
	rows     [][2]string
	queryErr error
}

func (f *fakeLinearOrphanConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return &fakeLinearOrphanRows{rows: f.rows, index: -1}, nil
}

var linearOrphanTestAt = time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)

// TestRetireOrphanedLinearPseudoProjectsRetiresOnlyTheDroppedTeam is the
// red-first proof for CHAOS-4530 codex review round 2: a team-key-shaped
// pseudo-project row still active for a team key THIS sync did NOT observe
// (deleted or re-keyed since the last sync) must be retired; a row for a
// team key THIS sync DID observe must be left alone -- the per-response
// tombstone loop already handles that one.
func TestRetireOrphanedLinearPseudoProjectsRetiresOnlyTheDroppedTeam(t *testing.T) {
	conn := &fakeLinearOrphanConn{rows: [][2]string{
		{"org-1:linear:ENG", "Engineering"},
		{"org-1:linear:OPS", "Operations"},
	}}
	tombstones, err := RetireOrphanedLinearPseudoProjects(context.Background(), conn, "org-1", []string{"ENG"}, linearOrphanTestAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(tombstones) != 1 {
		t.Fatalf("tombstones=%+v, want exactly 1 (OPS, dropped from the current response)", tombstones)
	}
	row := tombstones[0]
	if row.ID != "org-1:linear:OPS" || row.OrgID != "org-1" || row.Provider != "linear" ||
		row.ProjectKey != nil || row.IsActive != 0 || row.Name != "Operations" ||
		!row.UpdatedAt.Equal(linearOrphanTestAt) || !row.LastSynced.Equal(linearOrphanTestAt) {
		t.Fatalf("tombstone row=%+v", row)
	}
}

// TestRetireOrphanedLinearPseudoProjectsRetiresNothingWhenEveryTeamStillSeen
// pins the non-regression case: when every previously-active pseudo-project
// row's team key is still present in the current response, nothing is
// retired -- the per-response tombstone loop (route.go) is the one that
// handles those, this function must not duplicate or race it.
func TestRetireOrphanedLinearPseudoProjectsRetiresNothingWhenEveryTeamStillSeen(t *testing.T) {
	conn := &fakeLinearOrphanConn{rows: [][2]string{
		{"org-1:linear:ENG", "Engineering"},
	}}
	tombstones, err := RetireOrphanedLinearPseudoProjects(context.Background(), conn, "org-1", []string{"ENG", "OPS"}, linearOrphanTestAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(tombstones) != 0 {
		t.Fatalf("tombstones=%+v, want none -- every existing pseudo-project's team key is still observed", tombstones)
	}
}

// TestRetireOrphanedLinearPseudoProjectsPropagatesQueryFailure pins the
// fail-closed contract shared with PreserveExistingTeamManualMembers /
// PreserveExistingTeamMembersRoster: a query error must abort the caller's
// write, never be silently swallowed into "nothing to retire".
func TestRetireOrphanedLinearPseudoProjectsPropagatesQueryFailure(t *testing.T) {
	boom := errors.New("boom")
	conn := &fakeLinearOrphanConn{queryErr: boom}
	_, err := RetireOrphanedLinearPseudoProjects(context.Background(), conn, "org-1", []string{"ENG"}, linearOrphanTestAt)
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v, want the query failure propagated", err)
	}
}

func TestRetireOrphanedLinearPseudoProjectsRejectsInvalidInput(t *testing.T) {
	conn := &fakeLinearOrphanConn{}
	cases := []struct {
		name  string
		conn  driver.Conn
		orgID string
		at    time.Time
	}{
		{"nil conn", nil, "org-1", linearOrphanTestAt},
		{"blank org", conn, "  ", linearOrphanTestAt},
		{"zero time", conn, "org-1", time.Time{}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := RetireOrphanedLinearPseudoProjects(context.Background(), testCase.conn, testCase.orgID, nil, testCase.at); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("err=%v, want ErrInvalidConfiguration", err)
			}
		})
	}
}
