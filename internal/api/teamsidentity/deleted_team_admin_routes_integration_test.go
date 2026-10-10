//go:build integration

package teamsidentity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/filteroptions"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The admin delete keeps the team row (inactive, with the time of the delete).
// To everything that SERVES teams the team must be gone, as it was when the
// row was removed: the admin list (with and without inactive teams), the read
// of one team, an update, a second delete, the filter options and their team
// names. A team that is inactive for another reason is still served where it
// was (the list with inactive teams, the read of one team), so the two are
// told apart.
//
// Then what brings the id back: an admin create under the same id is a NEW
// team (active, served, with nothing of the deleted team's members, project
// keys or repository patterns), and a later write of the row by a writer that
// does not know the delete (a provider sync, a push) is the team again.
//
// One case for a team of each provider and for an admin's custom team,
// through the real handlers and the real filter options read.
func TestADeletedTeamIsNotServedAndComesBackOnlyByANewWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	store := Store{Conn: conn}
	h := newTestHandlers(store)

	for index, origin := range []string{"github", "gitlab", "jira", "linear", teamid.Custom} {
		t.Run(origin, func(t *testing.T) {
			org := fmt.Sprintf("00000000-0000-4000-8000-00000091191%d", index)
			gone, stays, retired := teamid.Of(origin, "platform"), teamid.Of(origin, "apps"), teamid.Of(origin, "legacy")
			members, keys, patterns := []string{"dev@example.com"}, []string{"PLAT"}, []string{"acme/platform-*"}
			for id, name := range map[string]string{gone: "Platform", stays: "Apps", retired: "Legacy"} {
				if _, err := store.CreateOrUpdateTeam(ctx, org, TeamWrite{Origin: origin, TeamID: id, Name: name,
					Members: &members, ManualMembers: &members, ProjectKeys: &keys, RepoPatterns: &patterns}); err != nil {
					t.Fatalf("create team %s: %v", id, err)
				}
			}
			// A team that is inactive and NOT deleted: a new version of its row
			// with is_active = 0, as a retire at the provider writes it.
			if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, updated_at, org_id, provider, is_active)
SELECT id, team_uuid, name, members, now64(6) + INTERVAL 1 SECOND, org_id, provider, 0 FROM teams FINAL WHERE org_id = ? AND id = ?`, org, retired); err != nil {
				t.Fatal(err)
			}

			call := func(handler http.HandlerFunc, method, path, teamID string, payload map[string]any) *httptest.ResponseRecorder {
				t.Helper()
				return callWithBody(t, h, func(w http.ResponseWriter, r *http.Request) {
					if teamID != "" {
						r.SetPathValue("team_id", teamID)
					}
					handler(w, r)
				}, method, path, org, payload)
			}
			listed := func(query string) []string {
				t.Helper()
				recorder := call(h.listTeams, http.MethodGet, "/api/v1/admin/teams"+query, "", nil)
				if recorder.Code != http.StatusOK {
					t.Fatalf("list teams%s: status %d body %s", query, recorder.Code, recorder.Body.String())
				}
				var rows []map[string]any
				if err := json.Unmarshal(recorder.Body.Bytes(), &rows); err != nil {
					t.Fatal(err)
				}
				ids := []string{}
				for _, row := range rows {
					ids = append(ids, row["team_id"].(string))
				}
				sort.Strings(ids)
				return ids
			}
			options := func() ([]string, map[string]string) {
				t.Helper()
				response, err := filteroptions.BuildResponse(ctx, client, org)
				if err != nil {
					t.Fatal(err)
				}
				return response.Teams, response.TeamNames
			}
			sorted := func(ids ...string) []string { sort.Strings(ids); return ids }
			status := func(recorder *httptest.ResponseRecorder) (int, string) {
				detail, _ := decodeBody(t, recorder)["detail"].(string)
				return recorder.Code, detail
			}

			// While the team is there.
			if got, want := listed(""), sorted(gone, stays); !reflect.DeepEqual(got, want) {
				t.Fatalf("the list before the delete = %v, want %v", got, want)
			}
			if got, want := listed("?active_only=false"), sorted(gone, stays, retired); !reflect.DeepEqual(got, want) {
				t.Fatalf("the list with inactive teams before the delete = %v, want %v", got, want)
			}

			recorder := call(h.deleteTeam, http.MethodDelete, "/api/v1/admin/teams/"+gone, gone, nil)
			if recorder.Code != http.StatusOK || decodeBody(t, recorder)["deleted"] != true {
				t.Fatalf("delete: status %d body %s", recorder.Code, recorder.Body.String())
			}
			newest := func() time.Time {
				t.Helper()
				var at time.Time
				if err := conn.QueryRow(ctx, `SELECT max(updated_at) FROM teams WHERE org_id = ? AND id = ?`, org, gone).Scan(&at); err != nil {
					t.Fatal(err)
				}
				return at
			}
			deletedAt := newest()

			if got, want := listed(""), sorted(stays); !reflect.DeepEqual(got, want) {
				t.Errorf("the list after the delete = %v, want %v", got, want)
			}
			if got, want := listed("?active_only=false"), sorted(stays, retired); !reflect.DeepEqual(got, want) {
				t.Errorf("the list with inactive teams after the delete = %v, want %v: an inactive team is served there, a deleted team is not", got, want)
			}
			if code, detail := status(call(h.getTeam, http.MethodGet, "/api/v1/admin/teams/"+gone, gone, nil)); code != http.StatusNotFound || detail != "Team not found" {
				t.Errorf("read of the deleted team: %d %q, want 404 Team not found", code, detail)
			}
			if code, _ := status(call(h.getTeam, http.MethodGet, "/api/v1/admin/teams/"+retired, retired, nil)); code != http.StatusOK {
				t.Errorf("read of the inactive team: %d, want 200", code)
			}
			if code, detail := status(call(h.updateTeam, http.MethodPatch, "/api/v1/admin/teams/"+gone, gone, map[string]any{"name": "Back"})); code != http.StatusNotFound || detail != "Team not found" {
				t.Errorf("update of the deleted team: %d %q, want 404 Team not found", code, detail)
			}
			if code, detail := status(call(h.deleteTeam, http.MethodDelete, "/api/v1/admin/teams/"+gone, gone, nil)); code != http.StatusNotFound || detail != "Team not found" {
				t.Errorf("second delete: %d %q, want 404 Team not found", code, detail)
			}
			teams, names := options()
			if !reflect.DeepEqual(teams, []string{stays}) || !reflect.DeepEqual(names, map[string]string{stays: "Apps"}) {
				t.Errorf("filter options after the delete: teams %v names %v, want only %s", teams, names, stays)
			}
			// The row of the deleted team keeps every field it had: a reader of
			// the table finds for it what it finds for any inactive team.
			var kept struct {
				Name                        string
				Members, Manual, Keys, Repo []string
			}
			if err := conn.QueryRow(ctx, `SELECT name, members, manual_members, project_keys, repo_patterns FROM teams FINAL WHERE org_id = ? AND id = ?`,
				org, gone).Scan(&kept.Name, &kept.Members, &kept.Manual, &kept.Keys, &kept.Repo); err != nil {
				t.Fatal(err)
			}
			if kept.Name != "Platform" || !reflect.DeepEqual(kept.Members, members) || !reflect.DeepEqual(kept.Manual, members) ||
				!reflect.DeepEqual(kept.Keys, keys) || !reflect.DeepEqual(kept.Repo, patterns) {
				t.Errorf("the row of the deleted team = %+v, want the name, members, project keys and repository patterns it had", kept)
			}
			// Neither the refused update nor the second delete wrote a version.
			var rows, activeRows, marked uint64
			if err := conn.QueryRow(ctx, `SELECT count(), countIf(is_active = 1), countIf(deleted_at IS NOT NULL)
FROM teams FINAL WHERE org_id = ? AND id = ?`, org, gone).Scan(&rows, &activeRows, &marked); err != nil {
				t.Fatal(err)
			}
			if rows != 1 || activeRows != 0 || marked != 1 || !newest().Equal(deletedAt) {
				t.Errorf("the deleted team: %d rows, %d active, %d marked, newest version %s (the delete: %s); want one inactive marked row and no version after the delete",
					rows, activeRows, marked, newest(), deletedAt)
			}

			// A team whose stored row carries a time AHEAD of this clock (a
			// writer on another host, a clock that was set back): the delete
			// must still be the newest version, or the team stays active.
			ahead := teamid.Of(origin, "ahead")
			if _, err := store.CreateOrUpdateTeam(ctx, org, TeamWrite{Origin: origin, TeamID: ahead, Name: "Ahead"}); err != nil {
				t.Fatal(err)
			}
			if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, updated_at, org_id, provider, is_active)
SELECT id, team_uuid, name, members, now64(6) + INTERVAL 1 HOUR, org_id, provider, 1 FROM teams FINAL WHERE org_id = ? AND id = ?`, org, ahead); err != nil {
				t.Fatal(err)
			}
			if code, _ := status(call(h.deleteTeam, http.MethodDelete, "/api/v1/admin/teams/"+ahead, ahead, nil)); code != http.StatusOK {
				t.Fatalf("delete of the team with a row ahead of the clock: %d", code)
			}
			var aheadActive, aheadMarked uint64
			if err := conn.QueryRow(ctx, `SELECT countIf(is_active = 1), countIf(deleted_at IS NOT NULL) FROM teams FINAL WHERE org_id = ? AND id = ?`,
				org, ahead).Scan(&aheadActive, &aheadMarked); err != nil {
				t.Fatal(err)
			}
			if aheadActive != 0 || aheadMarked != 1 {
				t.Errorf("a team with a row ahead of the clock, after its delete: %d active, %d marked; want the delete to be the newest version", aheadActive, aheadMarked)
			}
			if got, want := listed("?active_only=false"), sorted(stays, retired); !reflect.DeepEqual(got, want) {
				t.Errorf("the list with inactive teams after that delete = %v, want %v", got, want)
			}

			// An admin create under the same id: a new team.
			created, err := store.CreateOrUpdateTeam(ctx, org, TeamWrite{Origin: origin, TeamID: gone, Name: "Platform again"})
			if err != nil {
				t.Fatal(err)
			}
			if len(created.Members)+len(created.ManualMembers)+len(created.ProjectKeys)+len(created.RepoPatterns) != 0 || !created.IsActive {
				t.Errorf("the team created under the id of a deleted team took something of it: %+v", created)
			}
			again, err := store.GetTeam(ctx, org, gone)
			if err != nil || again == nil || again.Name != "Platform again" || !again.IsActive || len(again.Members) != 0 {
				t.Errorf("the stored team after the create = %+v (err %v), want the new active team with no member", again, err)
			}
			if got, want := listed(""), sorted(gone, stays); !reflect.DeepEqual(got, want) {
				t.Errorf("the list after the create = %v, want %v", got, want)
			}

			// A delete again, then a write of the row by a writer that does not
			// know the delete: the columns a provider sync names, no deleted_at.
			if deleted, err := store.DeleteTeam(ctx, org, gone); err != nil || !deleted {
				t.Fatalf("second delete of the new team: %v %v", deleted, err)
			}
			if got, want := listed(""), sorted(stays); !reflect.DeepEqual(got, want) {
				t.Fatalf("the list after the delete of the new team = %v, want %v", got, want)
			}
			if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id, created_at)
SELECT id, team_uuid, 'Platform synced', description, ['sync@example.com'], manual_members, project_keys, repo_patterns, 1, now64(6) + INTERVAL 2 SECOND, org_id, provider, native_team_key, parent_team_id, created_at
FROM teams FINAL WHERE org_id = ? AND id = ?`, org, gone); err != nil {
				t.Fatal(err)
			}
			synced, err := store.GetTeam(ctx, org, gone)
			if err != nil || synced == nil || synced.Name != "Platform synced" || !synced.IsActive {
				t.Errorf("after a later write of the row the team = %+v (err %v), want the team there again", synced, err)
			}
			teams, _ = options()
			if !reflect.DeepEqual(teams, sorted(gone, stays)) {
				t.Errorf("filter options after the later write: teams %v, want %v", teams, sorted(gone, stays))
			}
		})
	}
}
