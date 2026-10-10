//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

type rosterMoveFixture struct {
	t    *testing.T
	ctx  context.Context
	conn driver.Conn
	at   time.Time
}

func (f rosterMoveFixture) team(org, provider, id string, active uint8, roster []string) {
	f.t.Helper()
	if err := f.conn.Exec(f.ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider) VALUES (?, ?, ?, ?, [], [], [], ?, ?, ?, ?)`,
		id, uuid.New(), "team "+id, roster, active, f.at.Add(-time.Hour), org, provider); err != nil {
		f.t.Fatalf("insert team %s: %v", id, err)
	}
}

func (f rosterMoveFixture) membership(org, provider, team, member string, email *string, facets []string, validTo *time.Time) {
	f.t.Helper()
	if err := f.conn.Exec(f.ctx, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, ?, 'native', 1, 100, 10, ?, ?, ?)`,
		org, provider, team, member, email, facets, f.at.Add(-2*time.Hour), validTo, f.at.Add(-2*time.Hour)); err != nil {
		f.t.Fatalf("insert membership %s/%s: %v", team, member, err)
	}
}

type movedRow struct {
	Provider, TeamID, MemberID, Source string
	RawEmail                           *string
	Facets                             []string
	Primary                            uint8
	Specificity                        uint16
	Priority                           int32
	ValidFrom                          time.Time
	ValidTo                            *time.Time
}

func (f rosterMoveFixture) moved(org string) []movedRow {
	f.t.Helper()
	rows, err := f.conn.Query(f.ctx, `SELECT provider, team_id, member_id, toString(source), raw_email, identity_facets, is_primary, specificity, priority, valid_from, valid_to FROM team_memberships FINAL WHERE org_id = ? AND source = 'manual' ORDER BY team_id, member_id`, org)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var out []movedRow
	for rows.Next() {
		var row movedRow
		if err := rows.Scan(&row.Provider, &row.TeamID, &row.MemberID, &row.Source, &row.RawEmail, &row.Facets, &row.Primary, &row.Specificity, &row.Priority, &row.ValidFrom, &row.ValidTo); err != nil {
			f.t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return out
}

// An admin-made team keeps every member it holds only in the roster column:
// the entry no open membership row of the team covers becomes a manual
// membership; a covered entry (any case), an entry of a provider team, of an
// inactive team and of another organization is not moved; a closed membership
// covers nothing. A dry run writes nothing; a second run moves nothing.
func TestMoveAdminTeamRosterToMembershipsMovesOnlyTheUncoveredAdminEntries(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	f := rosterMoveFixture{t: t, ctx: ctx, conn: conn, at: at}
	const org, other = "roster-move-org", "roster-move-other"
	email := "b@x.example"
	closed := at.Add(-time.Minute)

	// admin team: covered by member_id (other case), covered by a raw e-mail,
	// covered by an identity facet, covered only by a CLOSED row, uncovered x2.
	f.team(org, "", "custom:ops", 1, []string{"a@x.example", "b@x.example", " Carol ", "dave", "erin@x.example", "Frank"})
	f.membership(org, "", "custom:ops", "A@X.EXAMPLE", nil, nil, nil)
	f.membership(org, "", "custom:ops", "m-b", &email, nil, nil)
	f.membership(org, "", "custom:ops", "m-c", nil, []string{"carol"}, nil)
	f.membership(org, "", "custom:ops", "dave", nil, nil, &closed)
	// a membership of ANOTHER team covers nothing here.
	f.membership(org, "", "custom:other", "frank", nil, nil, nil)
	f.team(org, "", "custom:other", 1, nil)
	// provider team: one covered entry, one entry the provider no longer lists.
	f.team(org, "github", "gh:platform", 1, []string{"github:octocat", "github:gone"})
	f.membership(org, "github", "gh:platform", "github:octocat", nil, nil, nil)
	// inactive admin team and another organization's admin team.
	f.team(org, "", "custom:retired", 0, []string{"z@x.example"})
	f.team(other, "", "custom:ops", 1, []string{"o@x.example"})

	// Uncovered: dave (his row is closed), erin@x.example (no row), Frank (only
	// another team has him).
	dry, err := MoveAdminTeamRosterToMemberships(ctx, conn, org, at, true)
	if err != nil {
		t.Fatal(err)
	}
	want := TeamRosterMoveOutcome{
		DryRun: true, ColumnPresent: true, RosterFacets: 8, Covered: 4, AdminToMove: 3, TeamsToMove: 1,
		ProviderRosterOnly: 1, OpenBefore: 5, OpenAfter: 5,
	}
	if dry != want {
		t.Fatalf("dry run = %+v, want %+v", dry, want)
	}
	if rows := f.moved(org); len(rows) != 0 {
		t.Fatalf("a dry run wrote %d membership rows", len(rows))
	}

	real, err := MoveAdminTeamRosterToMemberships(ctx, conn, org, at, false)
	if err != nil {
		t.Fatal(err)
	}
	if real.Moved != 3 || real.OpenBefore != 5 || real.OpenAfter != 8 || real.AdminToMove != 3 {
		t.Fatalf("real run = %+v, want 3 moved, open memberships 5 -> 8", real)
	}
	rows := f.moved(org)
	got := map[string]movedRow{}
	for _, row := range rows {
		got[row.MemberID] = row
	}
	if len(rows) != 3 || got["dave"].TeamID != "custom:ops" || got["erin@x.example"].TeamID != "custom:ops" || got["Frank"].TeamID != "custom:ops" {
		t.Fatalf("moved rows = %+v, want dave, erin@x.example, Frank of custom:ops", rows)
	}
	erin := got["erin@x.example"]
	if erin.TeamID != "custom:ops" || erin.Provider != "" || erin.Source != "manual" || erin.Primary != 1 || erin.Specificity != 100 ||
		erin.Priority != 0 || !erin.ValidFrom.Equal(at) || erin.ValidTo != nil || len(erin.Facets) != 1 || erin.Facets[0] != "erin@x.example" ||
		erin.RawEmail == nil || *erin.RawEmail != "erin@x.example" {
		t.Fatalf("moved row = %+v", erin)
	}
	if got["dave"].RawEmail != nil {
		t.Fatalf("a facet with no @ must not become a raw e-mail: %+v", got["dave"])
	}
	if others := f.moved(other); len(others) != 0 {
		t.Fatalf("another organization got %d membership rows", len(others))
	}

	again, err := MoveAdminTeamRosterToMemberships(ctx, conn, org, at.Add(time.Minute), false)
	if err != nil {
		t.Fatal(err)
	}
	if again.Moved != 0 || again.AdminToMove != 0 || again.OpenBefore != 8 || again.OpenAfter != 8 {
		t.Fatalf("second run = %+v, want nothing to move", again)
	}
}

// A call without an organization, a time or a connection is refused.
func TestMoveAdminTeamRosterToMembershipsRefusesAnInvalidCall(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	for name, call := range map[string]func() error{
		"no org": func() error { _, err := MoveAdminTeamRosterToMemberships(ctx, conn, " ", at, false); return err },
		"no time": func() error {
			_, err := MoveAdminTeamRosterToMemberships(ctx, conn, "org", time.Time{}, false)
			return err
		},
		"no conn": func() error { _, err := MoveAdminTeamRosterToMemberships(ctx, nil, "org", at, false); return err },
	} {
		if err := call(); !errors.Is(err, ErrInvalidConfiguration) {
			t.Errorf("%s: err = %v, want ErrInvalidConfiguration", name, err)
		}
	}
}
