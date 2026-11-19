//go:build integration

package providersync

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const movedDeleteStamp = "2006-01-02 15:04:05.000000"

// movedDeleteRow is "id|active|deleted_at" of every team of the organization
// (teams FINAL).
func movedDeleteRows(f carryFixture) map[string][2]string {
	f.t.Helper()
	rows, err := f.conn.Query(f.ctx, `SELECT id, toString(is_active), ifNull(toString(deleted_at), '') FROM teams FINAL WHERE org_id = ?`, f.orgID)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string][2]string{}
	for rows.Next() {
		var id, active, deleted string
		if err := rows.Scan(&id, &active, &deleted); err != nil {
			f.t.Fatal(err)
		}
		out[id] = [2]string{active, deleted}
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return out
}

// writeAdminDelete stores the admin delete of row as teamsidentity's
// DeleteTeam does: a new version of the same team, inactive, with the roster
// kept and deleted_at = updated_at = at.
func (r movedRosterRow) writeAdminDelete(f carryFixture, at time.Time) {
	f.t.Helper()
	teamUUID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(r.provider+"|"+r.id+"|delete|"+at.Format(time.RFC3339Nano)))
	// The times go as microsecond text: the copy is compared to the microsecond.
	stamp := at.UTC().Format(movedDeleteStamp)
	f.exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key, deleted_at) `+
		`VALUES (?, ?, ?, ?, [], [], 0, toDateTime64(?, 6, 'UTC'), toDateTime64(?, 6, 'UTC'), ?, ?, ?, toDateTime64(?, 6, 'UTC'))`,
		r.id, teamUUID, "team "+r.id, r.roster, stamp, stamp, f.orgID, r.provider, r.native, stamp)
}

// CHAOS-14135: an admin delete of the team the carry moves, committed after
// the carry read it and before the carry's INSERT INTO teams, is honoured: no
// team of the organization is active, the moved row is deleted with the
// delete's time, and the old id stays deleted. The delete is stamped after
// the carry's time (one clock) and before it (another host's clock); the
// moved row's deleted_at is the delete's, to the microsecond.
func TestCarryTeamIDsHonoursAnAdminDeleteOfTheSourceCommittedBeforeItsInsert(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	stamps := []struct {
		name string
		at   time.Time
	}{{"delete_after_carry_at", carryAt.Add(time.Minute + 123456*time.Microsecond)}, {"delete_before_carry_at", carryOld.Add(time.Hour + 654321*time.Microsecond)}}
	for _, stamp := range stamps {
		for _, shape := range movedRosterShapes([]string{"r1"}) {
			t.Run(stamp.name+"/"+shape.name, func(t *testing.T) {
				f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: "moved-delete-" + stamp.name + "-" + shape.name}
				shape.seed(f)
				source := shape.source[0]
				race := &movedRosterRaceConn{Conn: conn, other: func() { source.writeAdminDelete(f, stamp.at) }}
				if _, err := CarryTeamIDs(ctx, race, f.orgID, carryAt, false); err != nil {
					t.Fatal(err)
				}
				if !race.fired {
					t.Fatal("the carry sent no teams insert: the delete was not interleaved")
				}
				wantDeleted := stamp.at.UTC().Format(movedDeleteStamp)
				for _, when := range []string{"before merge", "after OPTIMIZE FINAL"} {
					if when != "before merge" {
						f.exec(`OPTIMIZE TABLE teams FINAL`)
					}
					rows := movedDeleteRows(f)
					var problems []string
					if active := movedRosterActive(f); len(active) != 0 {
						problems = append(problems, fmt.Sprintf("active teams %v", active))
					}
					if old, ok := rows[source.id]; !ok || old[1] != wantDeleted {
						problems = append(problems, fmt.Sprintf("old id %s not deleted at %q (%v)", source.id, wantDeleted, old))
					}
					moved := 0
					for id, row := range rows {
						if id == source.id {
							continue
						}
						moved++
						if row[0] != "0" || row[1] != wantDeleted {
							problems = append(problems, fmt.Sprintf("moved id %s active=%s deleted_at=%q want 0 %q", id, row[0], row[1], wantDeleted))
						}
					}
					if moved != 1 {
						problems = append(problems, fmt.Sprintf("%d moved ids, want 1", moved))
					}
					ids := make([]string, 0, len(rows))
					for id, row := range rows {
						ids = append(ids, id+"|"+row[0]+"|"+row[1])
					}
					sort.Strings(ids)
					if len(problems) == 0 {
						t.Logf("CELL %s %s %s: HONOURED %v", stamp.name, shape.name, when, ids)
					} else {
						t.Errorf("CELL %s %s %s: BROKEN %s rows=%v", stamp.name, shape.name, when, strings.Join(problems, "; "), ids)
					}
				}
			})
		}
	}
}

// A delete committed before the carry reads the team: the carry moves
// nothing, and the deleted team stays the only (deleted) row.
func TestCarryTeamIDsLeavesATeamDeletedBeforeItsRead(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, shape := range movedRosterShapes([]string{"r1"}) {
		t.Run(shape.name, func(t *testing.T) {
			f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: "moved-delete-first-" + shape.name}
			shape.seed(f)
			shape.source[0].writeAdminDelete(f, carryOld.Add(time.Hour))
			if _, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false); err != nil {
				t.Fatal(err)
			}
			rows := movedDeleteRows(f)
			if active := movedRosterActive(f); len(active) != 0 || len(rows) != 1 || rows[shape.source[0].id][1] == "" {
				t.Errorf("CONTROL %s: active=%v rows=%v want only the deleted old id", shape.name, active, rows)
			} else {
				t.Logf("CONTROL %s: OK rows=%v", shape.name, rows)
			}
		})
	}
}
