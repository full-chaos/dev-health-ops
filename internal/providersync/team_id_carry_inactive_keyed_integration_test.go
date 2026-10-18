//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// carryOutcomeHas reports whether the outcome, as the carry verb prints it,
// holds key = value.
func carryOutcomeHas(t *testing.T, outcome TeamIDCarryOutcome, key string, value int) bool {
	t.Helper()
	raw, err := json.Marshal(outcome)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	got, ok := fields[key].(float64)
	return ok && int(got) == value
}

const carryTeamsState = `SELECT arrayStringConcat(groupArray(concat(id, '|', provider, '|', arrayStringConcat(manual_members, ','), '|', toString(is_active), '|', if(deleted_at IS NULL, 'live', 'deleted'))), ';') FROM (SELECT * FROM teams FINAL WHERE org_id = ? ORDER BY id)`

// CHAOS-9003, ruling D6023 (custom = admin = push API): an active admin team
// that moves onto a keyed row whose current version is inactive carries as
// the ACTIVE team: the kept row is written again active, with the admin's
// manual members it does not hold, and the bare admin row goes inactive.
// One team stays active. This holds for an admin's own team (custom:<id>,
// the kept row pushed) and for an admin team that one provider's observation
// names (the kept row is that provider's), for every provider, and for a kept
// row older or newer than the admin row.
func TestCarryTeamIDsCarriesAnAdminTeamAsTheActiveTeamOverAnInactiveKeyedRow(t *testing.T) {
	cases := []struct {
		name, provider, id, native string
		observed                   bool
	}{
		{name: "custom", provider: "", id: "eng", native: "eng"},
		{name: "linear", provider: "linear", id: "ENG", native: "ENG", observed: true},
		{name: "github", provider: "github", id: "eng", native: "eng", observed: true},
		{name: "gitlab", provider: "gitlab", id: "eng", native: "eng", observed: true},
		{name: "jira", provider: "jira", id: carryAtlassianID, native: carryAtlassianARI, observed: true},
	}
	ctx, conn := newWorkItemEffectsConn(t)
	for _, c := range cases {
		for _, keptNewer := range []bool{false, true} {
			name := c.name + "/kept older"
			if keptNewer {
				name = c.name + "/kept newer"
			}
			t.Run(name, func(t *testing.T) {
				f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
				adminAt, keptAt := carryOld.Add(time.Minute), carryOld
				if keptNewer {
					adminAt, keptAt = keptAt, adminAt
				}
				keyed := teamid.Of(c.provider, c.id)
				if c.provider == "" {
					keyed = teamid.Of(teamid.Custom, c.id)
				}
				f.team("", c.id, nil, nil, 1, adminAt, []string{"admin@example.com"}, nil)
				if c.observed {
					f.observation(c.provider, c.native, c.id)
				}
				f.team(c.provider, keyed, carryPtr(c.native), nil, 0, keptAt, []string{"kept@example.com"}, nil)

				outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
				if err != nil {
					t.Fatal(err)
				}
				if outcome.TeamsAlreadyKeyed != 1 || outcome.ManualMembersFolded != 1 || !carryOutcomeHas(t, outcome, "keyed_teams_activated", 1) {
					t.Errorf("carry = %+v; want the kept row activated and the admin member folded", outcome)
				}
				rows := map[string]string{
					keyed: keyed + "|" + c.provider + "|kept@example.com,admin@example.com|1|live",
					c.id:  c.id + "||admin@example.com|0|live",
				}
				want := []string{rows[c.id], rows[keyed]}
				if keyed < c.id {
					want = []string{rows[keyed], rows[c.id]}
				}
				if got := f.str(carryTeamsState); got != strings.Join(want, ";") {
					t.Errorf("teams = %q, want %q", got, strings.Join(want, ";"))
				}
				if got := f.str(`SELECT toString(count()) FROM teams FINAL WHERE org_id = ? AND is_active = 1`); got != "1" {
					t.Errorf("active teams = %s, want 1", got)
				}
				again, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt.Add(time.Hour), false)
				if err != nil || again.Found() || again.RowsWritten != 0 {
					t.Errorf("second carry = %+v, %v; want nothing", again, err)
				}
			})
		}
	}
}

// The kept row is activated even when it holds every manual member already.
func TestCarryTeamIDsActivatesAnInactiveKeyedRowThatHoldsTheMembers(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("", "eng", nil, nil, 1, carryOld.Add(time.Minute), []string{"admin@example.com"}, nil)
	f.team("", "custom:eng", carryPtr("eng"), nil, 0, carryOld, []string{"admin@example.com"}, nil)

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.ManualMembersFolded != 0 || !carryOutcomeHas(t, outcome, "keyed_teams_activated", 1) {
		t.Errorf("carry = %+v; want the kept row activated, nothing folded", outcome)
	}
	if got := f.str(carryTeamsState); got != "custom:eng||admin@example.com|1|live;eng||admin@example.com|0|live" {
		t.Errorf("teams = %q", got)
	}
}

// A keyed row an admin deleted is no team (a write under the id of a deleted
// team is a new team, teamsidentity Store.CreateOrUpdateTeam): the active
// admin team is written under the id as a new team, past the delete, with
// nothing of the deleted row. The deleted row is never folded into: that
// would write it again without its deleted_at.
func TestCarryTeamIDsWritesAnAdminTeamAsANewTeamOverADeletedKeyedRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("", "eng", nil, nil, 1, carryOld, []string{"admin@example.com"}, nil)
	deletedAt := carryOld.Add(time.Hour)
	f.exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key, deleted_at) VALUES ('custom:eng', ?, 'deleted eng', ['deleted@example.com'], ['DEL'], [], 0, ?, ?, ?, '', 'eng', ?)`,
		uuid.New(), deletedAt, deletedAt, f.orgID, deletedAt)

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.TeamsAlreadyKeyed != 0 || outcome.ManualMembersFolded != 0 || !carryOutcomeHas(t, outcome, "deleted_keyed_teams", 1) {
		t.Errorf("carry = %+v; want the admin team written over the deleted row", outcome)
	}
	if got := f.str(carryTeamsState); got != "custom:eng||admin@example.com|1|live;eng||admin@example.com|0|live" {
		t.Errorf("teams = %q", got)
	}
	if got := f.str(`SELECT concat(name, '|', arrayStringConcat(project_keys, ',')) FROM teams FINAL WHERE org_id = ? AND id = 'custom:eng'`); got != "team eng|" {
		t.Errorf("custom:eng name|project keys = %q, want the admin team's own (team eng, none)", got)
	}
}

// A provider's own bare team that moves onto an inactive keyed row of that
// provider is not an admin team: the keyed row (the provider's newer word)
// stays inactive, as before.
func TestCarryTeamIDsKeepsAnInactiveKeyedRowUnderAProviderTeam(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	f.team("linear", "linear:ENG", carryPtr("ENG"), nil, 0, carryOld.Add(time.Minute), nil, nil)

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.TeamsAlreadyKeyed != 1 || carryOutcomeHas(t, outcome, "keyed_teams_activated", 1) {
		t.Errorf("carry = %+v; want the keyed row kept as it is", outcome)
	}
	if got := f.str(carryTeamsState); got != "ENG|linear||0|live;linear:ENG|linear||0|live" {
		t.Errorf("teams = %q", got)
	}
}

// afterKeyedRead runs another writer's commit before the first statement the
// carry sends after its read of the kept rows: the window between that read
// and anything the carry does next.
type afterKeyedRead struct {
	driver.Conn
	other       func()
	read, fired bool
}

func (c *afterKeyedRead) before() {
	if c.read && !c.fired {
		c.fired = true
		c.other()
	}
}

func (c *afterKeyedRead) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.before()
	if strings.Contains(query, "deleted_at IS NOT NULL AS deleted") {
		c.read = true
	}
	return c.Conn.Query(ctx, query, args...)
}

func (c *afterKeyedRead) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.before()
	return c.Conn.PrepareBatch(ctx, query, opts...)
}

func (c *afterKeyedRead) Exec(ctx context.Context, query string, args ...any) error {
	c.before()
	return c.Conn.Exec(ctx, query, args...)
}

// An admin delete of the kept row that commits after the carry read it as
// inactive is the newer word: the team stays deleted, and the deleted row's
// content is never written again as a live team.
func TestCarryTeamIDsLeavesAKeptRowDeletedWhileItRunsDeleted(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("", "eng", nil, nil, 1, carryOld.Add(time.Minute), []string{"admin@example.com"}, nil)
	f.team("", "custom:eng", carryPtr("eng"), nil, 0, carryOld, []string{"kept@example.com"}, nil)
	deletedAt := carryOld.Add(time.Hour)
	wrapped := &afterKeyedRead{Conn: conn, other: func() {
		f.exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key, deleted_at) VALUES ('custom:eng', ?, 'deleted eng', ['deleted@example.com'], [], [], 0, ?, ?, ?, '', 'eng', ?)`,
			uuid.New(), deletedAt, deletedAt, f.orgID, deletedAt)
	}}

	if _, err := CarryTeamIDs(ctx, wrapped, f.orgID, carryAt, false); err != nil {
		t.Fatal(err)
	}
	if !wrapped.fired {
		t.Fatal("the carry sent nothing after its read of the kept rows: the delete was not interleaved")
	}
	if got := f.str(carryTeamsState); got != "custom:eng||deleted@example.com|0|deleted;eng||admin@example.com|0|live" {
		t.Errorf("teams = %q, want custom:eng still deleted", got)
	}
}

// The activation is logged once the carry wrote it, never at planning: a carry
// that fails after it read the kept rows logs no activation, and the kept row
// stays as it was; a carry that writes logs one Info line with the count.
func TestCarryTeamIDsLogsAnActivationOnlyAfterItIsWritten(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	seed := func(f carryFixture) {
		f.team("", "eng", nil, nil, 1, carryOld.Add(time.Minute), []string{"admin@example.com"}, nil)
		f.team("", "custom:eng", carryPtr("eng"), nil, 0, carryOld, []string{"kept@example.com"}, nil)
		f.membership("", "eng", "m1", carryOld, nil)
	}

	failed := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	seed(failed)
	logs := captureMemberLogs(t)
	failing := &failingCarryConn{Conn: conn, failOn: "FROM team_memberships FINAL"}
	if _, err := CarryTeamIDs(ctx, failing, failed.orgID, carryAt, false); !errors.Is(err, errCarryReadFailed) {
		t.Fatalf("carry err = %v, want the planted read failure (after the kept rows were read)", err)
	}
	if lines := logs.lines("team_id_carry_inactive_keyed_team_activated"); len(lines) != 0 {
		t.Errorf("a failed carry logged an activation: %v", lines)
	}
	if got := failed.str(carryTeamsState); got != "custom:eng||kept@example.com|0|live;eng||admin@example.com|1|live" {
		t.Errorf("teams after the failed carry = %q, want them unchanged", got)
	}

	written := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	seed(written)
	if _, err := CarryTeamIDs(ctx, conn, written.orgID, carryAt, false); err != nil {
		t.Fatal(err)
	}
	lines := logs.lines("team_id_carry_inactive_keyed_team_activated")
	if len(lines) != 1 || lines[0]["keyed_teams_activated"] != float64(1) || lines[0]["dry_run"] != false {
		t.Errorf("activation log lines = %v, want one with keyed_teams_activated 1 and dry_run false", lines)
	}
}

// A provider's own bare team over a kept row of its id that an admin deleted
// is not a custom team: the kept row stays deleted, nothing is folded into it,
// and the bare row goes inactive.
func TestCarryTeamIDsLeavesADeletedKeyedRowUnderAProviderTeamDeleted(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, []string{"admin@example.com"}, nil)
	deletedAt := carryOld.Add(time.Hour)
	f.exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key, deleted_at) VALUES ('linear:ENG', ?, 'deleted eng', [], [], [], 0, ?, ?, ?, 'linear', 'ENG', ?)`,
		uuid.New(), deletedAt, deletedAt, f.orgID, deletedAt)

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.TeamsAlreadyKeyed != 1 || outcome.ManualMembersFolded != 0 || carryOutcomeHas(t, outcome, "deleted_keyed_teams", 1) || carryOutcomeHas(t, outcome, "keyed_teams_activated", 1) {
		t.Errorf("carry = %+v; want the deleted keyed row kept as it is", outcome)
	}
	if got := f.str(carryTeamsState); got != "ENG|linear|admin@example.com|0|live;linear:ENG|linear||0|deleted" {
		t.Errorf("teams = %q", got)
	}
}
