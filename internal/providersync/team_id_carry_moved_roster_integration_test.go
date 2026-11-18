//go:build integration

package providersync

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

var movedRosterAtTeamsInsert = regexp.MustCompile(`(?i)^\s*INSERT\s+INTO\s+teams\b`)

// movedRosterRaceConn commits another writer's version of the source team
// just before the carry sends its INSERT INTO teams: after every read the
// carry makes.
type movedRosterRaceConn struct {
	driver.Conn
	other func()
	fired bool
}

func (c *movedRosterRaceConn) Exec(ctx context.Context, query string, args ...any) error {
	if !c.fired && movedRosterAtTeamsInsert.MatchString(query) {
		c.fired = true
		c.other()
	}
	return c.Conn.Exec(ctx, query, args...)
}

// movedRosterRow is one stored version of a bare team. Its team_uuid is fixed
// by its content, so two runs of a fixture store the same bytes.
type movedRosterRow struct {
	provider, id string
	native       *string
	active       uint8
	at           time.Time
	roster       []string
}

func (r movedRosterRow) write(f carryFixture) {
	f.t.Helper()
	teamUUID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(r.provider+"|"+r.id+"|"+r.at.Format(time.RFC3339Nano)))
	f.exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, ?, [], [], ?, ?, ?, ?, ?, ?)`,
		r.id, teamUUID, "team "+r.id, r.roster, r.active, r.at, r.at, f.orgID, r.provider, r.native)
}

// movedRosterShape is a bare team the carry moves to a new id: its source
// rows and the observations that name it.
type movedRosterShape struct {
	name         string
	source       []movedRosterRow
	observations [][3]string
}

// The nine moved-row shapes: a provider team moved to its prefixed id, an
// admin team adopted by the provider whose observation names it, and an
// admin team no observation names moved to custom:.
func movedRosterShapes(roster []string) []movedRosterShape {
	native := func(provider, id, key string) movedRosterShape {
		return movedRosterShape{name: provider + "_native", source: []movedRosterRow{{provider: provider, id: id, native: carryPtr(key), active: 1, at: carryOld, roster: roster}}}
	}
	adopted := func(provider, id, key string) movedRosterShape {
		return movedRosterShape{name: provider + "_adopts_admin", source: []movedRosterRow{{provider: "", id: id, active: 1, at: carryOld, roster: roster}},
			observations: [][3]string{{provider, key, id}}}
	}
	return []movedRosterShape{
		native("github", "web", "web"), native("gitlab", "ENG", "ENG"), native("jira", carryAtlassianID, carryAtlassianARI), native("linear", "OPS", "OPS"),
		adopted("github", "web", "web"), adopted("gitlab", "ENG", "ENG"), adopted("jira", carryAtlassianID, carryAtlassianARI), adopted("linear", "OPS", "OPS"),
		{name: "admin_to_custom", source: []movedRosterRow{{provider: "", id: "chosen", active: 1, at: carryOld, roster: roster}}},
	}
}

func (s movedRosterShape) seed(f carryFixture) {
	f.t.Helper()
	for _, row := range s.source {
		row.write(f)
	}
	for _, o := range s.observations {
		f.observation(o[0], o[1], o[2])
	}
}

// movedRosterActive is "id|roster" of every active team of the organization.
func movedRosterActive(f carryFixture) []string {
	f.t.Helper()
	rows, err := f.conn.Query(f.ctx, `SELECT concat(id, '|', arrayStringConcat(manual_members, ',')) AS r FROM teams FINAL WHERE org_id = ? AND is_active = 1 ORDER BY r`, f.orgID)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			f.t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return out
}

// CHAOS-14081: the carry writes a moved team's new row from its read of the
// source team. An admin roster edit of the source team that commits after
// that read and before the carry's insert is the roster the new row takes:
// the source's newest roster is read inside the insert statement.
func TestCarryTeamIDsMovedRowTakesASourceRosterEditCommittedBeforeItsInsert(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, shape := range movedRosterShapes([]string{"before-edit"}) {
		t.Run(shape.name, func(t *testing.T) {
			f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: "moved-roster-race-" + shape.name}
			shape.seed(f)
			race := &movedRosterRaceConn{Conn: conn, other: func() {
				edit := shape.source[0]
				edit.at, edit.roster = carryOld.Add(time.Hour), []string{"after-edit"}
				edit.write(f)
			}}
			if _, err := CarryTeamIDs(ctx, race, f.orgID, carryAt, false); err != nil {
				t.Fatal(err)
			}
			if !race.fired {
				t.Fatal("the carry sent no teams insert: the edit was not interleaved")
			}
			for _, when := range []string{"before merge", "after OPTIMIZE FINAL"} {
				if when != "before merge" {
					f.exec(`OPTIMIZE TABLE teams FINAL`)
				}
				active := movedRosterActive(f)
				if len(active) == 1 && strings.HasSuffix(active[0], "|after-edit") {
					t.Logf("CELL %s %s: KEPT %v", shape.name, when, active)
				} else {
					t.Errorf("CELL %s %s: LOST active=%v want one moved team with roster [after-edit]", shape.name, when, active)
				}
			}
		})
	}
}

// With no write between the carry's read and its insert, the moved rows are
// the ones the carry wrote from its read before the source roster was read in
// the insert: every stored version is logged for a byte compare, and each
// moved team holds the roster of the version the carry moves.
func TestCarryTeamIDsMovedRowRosterWithoutARace(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	// No background merge folds two stored versions of one id before the
	// carry reads them: every run stores the same rows.
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES teams`); err != nil {
		t.Fatal(err)
	}
	shapes := movedRosterShapes([]string{"m1", "m2"})
	shapes = append(shapes,
		movedRosterShape{name: "provider_newer_than_admin_edit", source: []movedRosterRow{
			{provider: "linear", id: "QA", native: carryPtr("QA"), active: 1, at: carryOld.Add(time.Hour), roster: []string{"provider"}},
			{provider: "", id: "QA", active: 1, at: carryOld, roster: []string{"admin"}},
		}},
		movedRosterShape{name: "admin_edit_newer_than_provider", source: []movedRosterRow{
			{provider: "linear", id: "QA", native: carryPtr("QA"), active: 1, at: carryOld, roster: []string{"provider"}},
			{provider: "", id: "QA", active: 1, at: carryOld.Add(time.Hour), roster: []string{"admin"}},
		}},
		movedRosterShape{name: "two_providers_one_id", source: []movedRosterRow{
			{provider: "github", id: "TWO", native: carryPtr("TWO"), active: 1, at: carryOld, roster: []string{"g"}},
			{provider: "linear", id: "TWO", native: carryPtr("TWO"), active: 1, at: carryOld.Add(time.Minute), roster: []string{"l"}},
		}},
		movedRosterShape{name: "newer_inactive_row_of_another_provider", source: []movedRosterRow{
			{provider: "linear", id: "SOLO", native: carryPtr("SOLO"), active: 1, at: carryOld, roster: []string{"l"}},
			{provider: "gitlab", id: "SOLO", native: carryPtr("SOLO"), active: 0, at: carryOld.Add(2 * time.Hour), roster: []string{"z"}},
		}},
	)
	want := map[string][]string{
		"provider_newer_than_admin_edit":         {"linear:QA|provider"},
		"admin_edit_newer_than_provider":         {"linear:QA|admin"},
		"two_providers_one_id":                   {"gh:TWO|g", "linear:TWO|l"},
		"newer_inactive_row_of_another_provider": {"linear:SOLO|l"},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: "moved-roster-control-" + shape.name}
			shape.seed(f)
			if _, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false); err != nil {
				t.Fatal(err)
			}
			rows, err := conn.Query(ctx, `SELECT toString((id, team_uuid, name, description, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key, parent_team_id, source_id, created_at, deleted_at, provider_team_id, provider_name, gone_at)) AS r FROM teams WHERE org_id = ? ORDER BY r`, f.orgID)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var row string
				if err := rows.Scan(&row); err != nil {
					t.Fatal(err)
				}
				t.Logf("CONTROL ROW %s", row)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			active := movedRosterActive(f)
			expected, named := want[shape.name]
			ok := (named && len(active) == len(expected)) || (!named && len(active) == 1)
			for i := range active {
				if named {
					ok = ok && active[i] == expected[i]
				} else {
					ok = ok && strings.HasSuffix(active[i], "|m1,m2")
				}
			}
			if !ok {
				t.Errorf("CONTROL %s: active=%v want %v (or one moved team with roster m1,m2)", shape.name, active, expected)
			} else {
				t.Logf("CONTROL %s: OK active=%v", shape.name, active)
			}
		})
	}
}
