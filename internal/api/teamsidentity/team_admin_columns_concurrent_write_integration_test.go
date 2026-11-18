//go:build integration

package teamsidentity

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// adminRacePoints are the two points of an admin write's window where the
// other writer runs: before its insert (after every read it makes), and
// before its read of the newest stored version (after its read of the team).
var adminRacePoints = []struct {
	name string
	at   *regexp.Regexp
}{{"at_insert", rosterRaceAtInsert}, {"at_newest_read", rosterRaceAtNewest}}

func storedTeamColumns(ctx context.Context, t *testing.T, conn driver.Conn, teamID string) (name string, description *string) {
	t.Helper()
	if err := conn.QueryRow(ctx, `SELECT name, description FROM teams FINAL WHERE org_id = ? AND id = ?`, rosterRaceOrg, teamID).Scan(&name, &description); err != nil {
		t.Fatal(err)
	}
	return name, description
}

// callTeamRoute calls a team route of h with the path value team_id.
func callTeamRoute(t *testing.T, h handlers, handler http.HandlerFunc, method, teamID string, payload map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	return callWithBody(t, h, func(w http.ResponseWriter, r *http.Request) {
		r.SetPathValue("team_id", teamID)
		handler(w, r)
	}, method, "/api/v1/admin/teams/"+teamID, rosterRaceOrg, payload)
}

func reasonCode(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	detail, _ := decodeBody(t, recorder)["detail"].(map[string]any)
	code, _ := detail["reason_code"].(string)
	return code
}

func optionalText(text *string) string {
	if text == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%q", *text)
}

func checkFired(t *testing.T, race *rosterRaceConn) {
	t.Helper()
	if !race.fired {
		t.Fatal("the other writer never ran")
	}
}

// CHAOS-14094: an admin write whose window holds another admin write of the
// same team keeps its own columns: a rename keeps its name, a delete keeps
// the team deleted. The other write (an admin AddMembers) keeps its member.
func TestAnAdminWriteRacedByAnotherAdminWriteLosesItsOtherColumns(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	plain := Store{Conn: conn}
	for _, point := range adminRacePoints {
		t.Run("rename_name_lost/"+point.name, func(t *testing.T) {
			teamID := "gh:side-rename-" + point.name
			seedRosterTeam(ctx, t, conn, teamID, "github")
			race := &rosterRaceConn{Conn: conn, at: point.at, other: func() {
				if _, err := plain.AddMembers(ctx, rosterRaceOrg, teamID, []string{"b"}); err != nil {
					t.Error(err)
				}
			}}
			if _, err := (Store{Conn: race}).CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{TeamID: teamID, Name: "Platform (renamed)"}); err != nil {
				t.Fatal(err)
			}
			checkFired(t, race)
			name, _ := storedTeamColumns(ctx, t, conn, teamID)
			roster := storedTeamRoster(ctx, t, conn, teamID)
			if name != "Platform (renamed)" || !slices.Equal(roster, []string{"b", "x"}) {
				t.Errorf("SIDE rename_name_lost/%s: LOST name=%q roster=%v want %q [b x]", point.name, name, roster, "Platform (renamed)")
			} else {
				t.Logf("SIDE rename_name_lost/%s: KEPT name=%q roster=%v", point.name, name, roster)
			}
		})
		t.Run("delete_lost/"+point.name, func(t *testing.T) {
			teamID := "gh:side-delete-" + point.name
			seedRosterTeam(ctx, t, conn, teamID, "github")
			race := &rosterRaceConn{Conn: conn, at: point.at, other: func() {
				if _, err := plain.AddMembers(ctx, rosterRaceOrg, teamID, []string{"b"}); err != nil {
					t.Error(err)
				}
			}}
			deleted, err := (Store{Conn: race}).DeleteTeam(ctx, rosterRaceOrg, teamID)
			if err != nil {
				t.Fatal(err)
			}
			checkFired(t, race)
			team, err := plain.GetTeam(ctx, rosterRaceOrg, teamID)
			if err != nil {
				t.Fatal(err)
			}
			if !deleted || team != nil {
				t.Errorf("SIDE delete_lost/%s: LOST deleted=%v team=%v", point.name, deleted, team)
			} else {
				t.Logf("SIDE delete_lost/%s: KEPT (team deleted)", point.name)
			}
		})
	}
}

// CHAOS-14094: a delete states only that the team is deleted. A rename
// committed in its window is the name its deleted row keeps (a reader of a
// stored id finds that name).
func TestADeleteKeepsARenameCommittedInItsWindow(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	plainRoutes := newTestHandlers(Store{Conn: conn})
	for _, point := range adminRacePoints {
		t.Run(point.name, func(t *testing.T) {
			teamID := "gh:delete-rename-" + point.name
			seedRosterTeam(ctx, t, conn, teamID, "github")
			race := &rosterRaceConn{Conn: conn, at: point.at, other: func() {
				if recorder := callTeamRoute(t, plainRoutes, plainRoutes.updateTeam, http.MethodPatch, teamID, map[string]any{"name": "Platform (renamed)"}); recorder.Code != http.StatusOK {
					t.Errorf("the other PATCH: %d %s", recorder.Code, recorder.Body.String())
				}
			}}
			deleted, err := (Store{Conn: race}).DeleteTeam(ctx, rosterRaceOrg, teamID)
			if err != nil {
				t.Fatal(err)
			}
			checkFired(t, race)
			name, _ := storedTeamColumns(ctx, t, conn, teamID)
			team, err := (Store{Conn: conn}).GetTeam(ctx, rosterRaceOrg, teamID)
			if err != nil {
				t.Fatal(err)
			}
			if !deleted || team != nil || name != "Platform (renamed)" {
				t.Errorf("CELL delete_vs_rename/%s: LOST deleted=%v team=%v name=%q", point.name, deleted, team, name)
			} else {
				t.Logf("CELL delete_vs_rename/%s: KEPT (deleted, name=%q)", point.name, name)
			}
		})
	}
}

// CHAOS-14094: a roster edit states only the roster. A rename committed in
// its window keeps its name, and a delete committed in its window keeps the
// team deleted: the edit is refused (delete wins), never written over it.
func TestARosterEditDoesNotUndoARenameOrADelete(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	plain := Store{Conn: conn}
	for _, point := range adminRacePoints {
		t.Run("rename/"+point.name, func(t *testing.T) {
			teamID := "gh:edit-rename-" + point.name
			seedRosterTeam(ctx, t, conn, teamID, "github")
			race := &rosterRaceConn{Conn: conn, at: point.at, other: func() {
				if _, err := plain.CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{TeamID: teamID, Name: "Platform (renamed)"}); err != nil {
					t.Error(err)
				}
			}}
			if _, err := (Store{Conn: race}).AddMembers(ctx, rosterRaceOrg, teamID, []string{"b"}); err != nil {
				t.Fatal(err)
			}
			checkFired(t, race)
			name, _ := storedTeamColumns(ctx, t, conn, teamID)
			roster := storedTeamRoster(ctx, t, conn, teamID)
			if name != "Platform (renamed)" || !slices.Equal(roster, []string{"b", "x"}) {
				t.Errorf("CELL edit_vs_rename/%s: LOST name=%q roster=%v", point.name, name, roster)
			} else {
				t.Logf("CELL edit_vs_rename/%s: KEPT name=%q roster=%v", point.name, name, roster)
			}
		})
		t.Run("delete/"+point.name, func(t *testing.T) {
			teamID := "gh:edit-delete-" + point.name
			seedRosterTeam(ctx, t, conn, teamID, "github")
			race := &rosterRaceConn{Conn: conn, at: point.at, other: func() {
				if _, err := plain.DeleteTeam(ctx, rosterRaceOrg, teamID); err != nil {
					t.Error(err)
				}
			}}
			_, editErr := (Store{Conn: race}).AddMembers(ctx, rosterRaceOrg, teamID, []string{"b"})
			checkFired(t, race)
			team, err := plain.GetTeam(ctx, rosterRaceOrg, teamID)
			if err != nil {
				t.Fatal(err)
			}
			if team != nil || !errors.Is(editErr, ErrTeamChangedConcurrently) {
				t.Errorf("CELL edit_vs_delete/%s: LOST team=%v err=%v, want the team deleted and ErrTeamChangedConcurrently", point.name, team, editErr)
			} else {
				t.Logf("CELL edit_vs_delete/%s: KEPT (team deleted, edit refused)", point.name)
			}
		})
	}
}

// CHAOS-14094: PATCH writes only the fields of its body. A rename committed
// in its window keeps its name, and the PATCH keeps its description.
func TestAPatchWritesOnlyTheFieldsItSends(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	plainRoutes := newTestHandlers(Store{Conn: conn})
	for _, point := range adminRacePoints {
		t.Run("description_vs_rename/"+point.name, func(t *testing.T) {
			teamID := "gh:patch-" + point.name
			seedRosterTeam(ctx, t, conn, teamID, "github")
			race := &rosterRaceConn{Conn: conn, at: point.at, other: func() {
				if recorder := callTeamRoute(t, plainRoutes, plainRoutes.updateTeam, http.MethodPatch, teamID, map[string]any{"name": "Platform (renamed)"}); recorder.Code != http.StatusOK {
					t.Errorf("the other PATCH: %d %s", recorder.Code, recorder.Body.String())
				}
			}}
			raced := newTestHandlers(Store{Conn: race})
			recorder := callTeamRoute(t, raced, raced.updateTeam, http.MethodPatch, teamID, map[string]any{"description": "Runs the platform"})
			checkFired(t, race)
			name, description := storedTeamColumns(ctx, t, conn, teamID)
			if recorder.Code != http.StatusOK || name != "Platform (renamed)" || description == nil || *description != "Runs the platform" {
				t.Errorf("CELL patch_vs_rename/%s: LOST status=%d name=%q description=%s", point.name, recorder.Code, name, optionalText(description))
			} else {
				t.Logf("CELL patch_vs_rename/%s: KEPT name=%q description=%q", point.name, name, *description)
			}
		})
	}
}

// CHAOS-14094: an approved drift change writes only its field. A rename
// committed in its window keeps its name, and the change keeps its value.
func TestADriftApplyWritesOnlyItsField(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	plainRoutes := newTestHandlers(Store{Conn: conn})
	for _, point := range adminRacePoints {
		t.Run("description_vs_rename/"+point.name, func(t *testing.T) {
			teamID := "gh:drift-" + point.name
			seedRosterTeam(ctx, t, conn, teamID, "github")
			if err := conn.Exec(ctx, `INSERT INTO team_provider_observations (org_id, provider, native_team_key, team_id, name, description, members_json, project_keys_json, repo_patterns_json, is_active, discovered_at, updated_at)
				VALUES (?, 'github', ?, ?, 'Platform', 'From the provider', '[]', '[]', '[]', 1, now64(6), now64(6))`, rosterRaceOrg, "native-"+teamID, teamID); err != nil {
				t.Fatal(err)
			}
			field, native := "description", "native-"+teamID
			race := &rosterRaceConn{Conn: conn, at: point.at, other: func() {
				if recorder := callTeamRoute(t, plainRoutes, plainRoutes.updateTeam, http.MethodPatch, teamID, map[string]any{"name": "Platform (renamed)"}); recorder.Code != http.StatusOK {
					t.Errorf("the other PATCH: %d %s", recorder.Code, recorder.Body.String())
				}
			}}
			if err := (Store{Conn: race}).applyChange(ctx, rosterRaceOrg, reviewChangeRow{
				ChangeID: "change-" + point.name, EntityType: "team", TeamID: teamID, TeamName: "Platform", Provider: "github",
				NativeTeamKey: &native, ChangeType: "field_changed", Field: &field,
			}); err != nil {
				t.Fatal(err)
			}
			checkFired(t, race)
			name, description := storedTeamColumns(ctx, t, conn, teamID)
			if name != "Platform (renamed)" || description == nil || *description != "From the provider" {
				t.Errorf("CELL drift_vs_rename/%s: LOST name=%q description=%s", point.name, name, optionalText(description))
			} else {
				t.Logf("CELL drift_vs_rename/%s: KEPT name=%q description=%q", point.name, name, *description)
			}
		})
	}
}

// CHAOS-14094 (acp Q1 b, delete wins): a PATCH or an update of a team that an
// admin deletes in its window is refused (409 team_changed_concurrently), and
// the team stays deleted.
func TestAWriteOfATeamDeletedInItsWindowIsRefused(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	plain := Store{Conn: conn}
	deleteOther := func(t *testing.T, teamID string, at *regexp.Regexp) *rosterRaceConn {
		return &rosterRaceConn{Conn: conn, at: at, other: func() {
			if deleted, err := plain.DeleteTeam(ctx, rosterRaceOrg, teamID); err != nil || !deleted {
				t.Errorf("the other delete: %v %v", deleted, err)
			}
		}}
	}
	stillDeleted := func(t *testing.T, teamID string) bool {
		t.Helper()
		team, err := plain.GetTeam(ctx, rosterRaceOrg, teamID)
		if err != nil {
			t.Fatal(err)
		}
		return team == nil
	}
	for _, point := range adminRacePoints {
		t.Run("patch/"+point.name, func(t *testing.T) {
			teamID := "gh:wins-patch-" + point.name
			seedRosterTeam(ctx, t, conn, teamID, "github")
			race := deleteOther(t, teamID, point.at)
			raced := newTestHandlers(Store{Conn: race})
			recorder := callTeamRoute(t, raced, raced.updateTeam, http.MethodPatch, teamID, map[string]any{"name": "Back"})
			checkFired(t, race)
			if recorder.Code != http.StatusConflict || reasonCode(t, recorder) != teamChangedConcurrentlyReason || !stillDeleted(t, teamID) {
				t.Errorf("CELL delete_wins/patch/%s: LOST %d %s deleted=%v, want 409 and the team deleted", point.name, recorder.Code, recorder.Body.String(), stillDeleted(t, teamID))
			} else {
				t.Logf("CELL delete_wins/patch/%s: 409 %s", point.name, recorder.Body.String())
			}
		})
		t.Run("update/"+point.name, func(t *testing.T) {
			teamID := "gh:wins-update-" + point.name
			seedRosterTeam(ctx, t, conn, teamID, "github")
			race := deleteOther(t, teamID, point.at)
			_, err := (Store{Conn: race}).CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{Origin: "github", TeamID: teamID, Name: "Back"})
			checkFired(t, race)
			if !errors.Is(err, ErrTeamChangedConcurrently) || !stillDeleted(t, teamID) {
				t.Errorf("CELL delete_wins/update/%s: LOST err=%v deleted=%v, want ErrTeamChangedConcurrently and the team deleted", point.name, err, stillDeleted(t, teamID))
			} else {
				t.Logf("CELL delete_wins/update/%s: REFUSED %v", point.name, err)
			}
		})
	}
}

// CHAOS-14094 (acp Q1 c): two deletes of one team both answer deleted.
func TestADeleteRacedByADeleteAnswersDeleted(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	plain := Store{Conn: conn}
	for _, point := range adminRacePoints {
		t.Run(point.name, func(t *testing.T) {
			teamID := "gh:delete-delete-" + point.name
			seedRosterTeam(ctx, t, conn, teamID, "github")
			var otherDeleted bool
			race := &rosterRaceConn{Conn: conn, at: point.at, other: func() {
				deleted, err := plain.DeleteTeam(ctx, rosterRaceOrg, teamID)
				if err != nil {
					t.Error(err)
				}
				otherDeleted = deleted
			}}
			deleted, err := (Store{Conn: race}).DeleteTeam(ctx, rosterRaceOrg, teamID)
			checkFired(t, race)
			team, getErr := plain.GetTeam(ctx, rosterRaceOrg, teamID)
			if err != nil || getErr != nil || !deleted || !otherDeleted || team != nil {
				t.Errorf("CELL delete_vs_delete/%s: deleted=%v other=%v err=%v team=%v", point.name, deleted, otherDeleted, err, team)
			} else {
				t.Logf("CELL delete_vs_delete/%s: both deleted", point.name)
			}
		})
	}
}

// otherNameConn writes, before every read-back of teamID, a newer version of
// it named "Other" (a writer that read the team earlier and wins on version),
// and counts the teams inserts of the writer it wraps.
type otherNameConn struct {
	driver.Conn
	t       *testing.T
	teamID  string
	inserts int
}

func (c *otherNameConn) Exec(ctx context.Context, query string, args ...any) error {
	if rosterRaceAtInsert.MatchString(query) {
		c.inserts++
	}
	return c.Conn.Exec(ctx, query, args...)
}

func (c *otherNameConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if rosterReadBack.MatchString(query) && namedTeamID(args) == c.teamID {
		if err := c.Conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider)
			SELECT id, team_uuid, 'Other', manual_members, project_keys, repo_patterns, 1, updated_at + INTERVAL 1 SECOND, org_id, provider
			FROM teams FINAL WHERE org_id = ? AND id = ?`, rosterRaceOrg, c.teamID); err != nil {
			c.t.Fatal(err)
		}
	}
	return c.Conn.QueryRow(ctx, query, args...)
}

// CHAOS-14094 (acp Q1 a): a rename, PATCH or delete that another writer wins
// over on every round is written rosterRounds times, then answered 409
// team_changed_concurrently.
func TestAnAdminTeamWriteThatNeverHoldsIsRefusedAfterThreeWrites(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	refused := func(t *testing.T, name string, recorder *httptest.ResponseRecorder, inserts int) {
		t.Helper()
		if recorder.Code != http.StatusConflict || reasonCode(t, recorder) != teamChangedConcurrentlyReason || inserts != rosterRounds {
			t.Errorf("CELL refusal/%s: %d %s after %d writes, want 409 team_changed_concurrently after %d", name, recorder.Code, recorder.Body.String(), inserts, rosterRounds)
		} else {
			t.Logf("CELL refusal/%s: 409 %s after %d writes", name, recorder.Body.String(), inserts)
		}
	}
	t.Run("post_update", func(t *testing.T) {
		const teamID = "custom:refused-post"
		if _, err := (Store{Conn: conn}).CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{Origin: "custom", TeamID: teamID, Name: "Refused"}); err != nil {
			t.Fatal(err)
		}
		other := &otherNameConn{Conn: conn, t: t, teamID: teamID}
		h := newTestHandlers(Store{Conn: other})
		recorder := callWithBody(t, h, h.createOrUpdateTeam, http.MethodPost, "/api/v1/admin/teams", rosterRaceOrg, map[string]any{"team_id": teamID, "name": "Mine"})
		refused(t, "post_update", recorder, other.inserts)
	})
	t.Run("patch", func(t *testing.T) {
		const teamID = "gh:refused-patch"
		seedRosterTeam(ctx, t, conn, teamID, "github")
		other := &otherNameConn{Conn: conn, t: t, teamID: teamID}
		h := newTestHandlers(Store{Conn: other})
		refused(t, "patch", callTeamRoute(t, h, h.updateTeam, http.MethodPatch, teamID, map[string]any{"name": "Mine"}), other.inserts)
	})
	t.Run("delete", func(t *testing.T) {
		const teamID = "gh:refused-delete"
		seedRosterTeam(ctx, t, conn, teamID, "github")
		// The stale version carries no deleted_at: the team is there again.
		stale := &staleCarryConn{Conn: conn, t: t, teamID: teamID}
		h := newTestHandlers(Store{Conn: stale})
		refused(t, "delete", callTeamRoute(t, h, h.deleteTeam, http.MethodDelete, teamID, nil), stale.inserts)
	})
}
