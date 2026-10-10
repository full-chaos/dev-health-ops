//go:build integration

package teamsidentity

import (
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// An approval of a staged change WRITES under the team id: a membership of
// the team, or a new version of the team row. The change was staged while the
// team was there. When an admin deleted the team since, the approval answers
// "Team not found", as the other team routes do, and writes nothing: no
// membership under the deleted team, and no new version of the team row that
// brings the deleted team back with one field. The change stays pending and
// can still be dismissed. For a team that is there the approval applies the
// change.
//
// One case for a team of each provider.
func TestAnApprovalOfAChangeOfADeletedTeamIsRefused(t *testing.T) {
	staged := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			s, ctx := writeSeamStore(t)
			h := newTestHandlers(s)
			gone, stays := teamid.Of(provider, "platform"), teamid.Of(provider, "apps")
			for id, name := range map[string]string{gone: "Platform", stays: "Apps"} {
				if _, err := s.CreateOrUpdateTeam(ctx, "org-1", TeamWrite{Origin: provider, TeamID: id, Name: name}); err != nil {
					t.Fatalf("create team %s: %v", id, err)
				}
			}
			exec := func(what, query string, args ...any) {
				t.Helper()
				if err := s.Conn.Exec(ctx, query, args...); err != nil {
					t.Fatalf("%s: %v", what, err)
				}
			}
			for _, team := range []struct{ id, native, observed string }{{gone, "platform", "Platform renamed"}, {stays, "apps", "Apps renamed"}} {
				exec("observation of "+team.id, `INSERT INTO team_provider_observations
    (org_id, provider, native_team_key, team_id, name, description, members_json, project_keys_json, repo_patterns_json, is_active, parent_team_id, discovered_at, updated_at)
    VALUES ('org-1', ?, ?, ?, ?, NULL, '[]', '[]', '[]', 1, NULL, ?, ?)`, provider, team.native, team.id, team.observed, staged, staged)
				// A team field change, and a change of a member of the team.
				exec("name change of "+team.id, `INSERT INTO team_drift_changes
    (org_id, change_id, entity_type, entity_id, provider, native_team_key, change_type, field, old_value_json, new_value_json, status, first_seen_at, last_seen_at, updated_at)
    VALUES ('org-1', ?, 'team', ?, ?, ?, 'updated', 'name', '"old"', ?, 'pending', ?, ?, ?)`,
					"name-"+team.id, team.id, provider, team.native, `"`+team.observed+`"`, staged, staged, staged)
			}
			exec("member change of "+gone, `INSERT INTO team_drift_changes
    (org_id, change_id, entity_type, entity_id, provider, native_team_key, change_type, field, old_value_json, new_value_json, status, first_seen_at, last_seen_at, updated_at)
    VALUES ('org-1', ?, 'identity', ?, ?, NULL, 'membership_conflict', 'team_memberships', '', ?, 'pending', ?, ?, ?)`,
				"member-"+gone, gone, provider,
				`{"provider":"`+provider+`","team_id":"`+gone+`","member_id":"dev","source":"native","is_primary":1,"specificity":100,"priority":10,"valid_from":"2026-09-01T00:00:00Z","identity_facets":["dev@example.com"]}`,
				staged, staged, staged)

			count := func(query string, args ...any) uint64 {
				t.Helper()
				var n uint64
				if err := s.Conn.QueryRow(ctx, query, args...).Scan(&n); err != nil {
					t.Fatalf("%s: %v", query, err)
				}
				return n
			}
			approve := func(teamID string) (int, string) {
				t.Helper()
				rec := writeSeamCall(t, h, h.approveChanges, http.MethodPost, "/api/v1/admin/teams/"+teamID+"/approve-changes", teamID,
					map[string]any{"approve_all": true})
				detail, _ := decodeBody(t, rec)["detail"].(string)
				return rec.Code, detail
			}

			// The team that is there: the approval applies the change.
			if code, _ := approve(stays); code != http.StatusOK {
				t.Fatalf("approval for a team that is there: status %d, want 200", code)
			}
			team, err := s.GetTeam(ctx, "org-1", stays)
			if err != nil || team == nil || team.Name != "Apps renamed" {
				t.Fatalf("after the approval the team is %+v (err %v), want the observed name", team, err)
			}

			deleted, err := s.DeleteTeam(ctx, "org-1", gone)
			if err != nil || !deleted {
				t.Fatalf("delete team %s: deleted = %v, err = %v", gone, deleted, err)
			}
			var deletedAt time.Time
			if err := s.Conn.QueryRow(ctx, `SELECT max(updated_at) FROM teams WHERE org_id = 'org-1' AND id = ?`, gone).Scan(&deletedAt); err != nil {
				t.Fatal(err)
			}
			code, detail := approve(gone)
			if code != http.StatusNotFound || detail != "Team not found" {
				t.Errorf("approval for the deleted team: status %d detail %q, want 404 Team not found", code, detail)
			}
			// The delete keeps the row, inactive and marked. The approval wrote
			// no version after it, and the newest is still the deleted one with
			// the stored name.
			var newest time.Time
			if err := s.Conn.QueryRow(ctx, `SELECT max(updated_at) FROM teams WHERE org_id = 'org-1' AND id = ?`, gone).Scan(&newest); err != nil {
				t.Fatal(err)
			}
			if !newest.Equal(deletedAt) {
				t.Errorf("the refused approval wrote a version of the team row after the delete (newest %s, the delete %s)", newest, deletedAt)
			}
			if back := count(`SELECT count() FROM teams FINAL WHERE org_id = 'org-1' AND id = ? AND (is_active = 1 OR deleted_at IS NULL OR name != 'Platform')`, gone); back != 0 {
				t.Errorf("the approval brought the deleted team back or changed it")
			}
			if rows := count(`SELECT count() FROM team_memberships WHERE org_id = 'org-1' AND team_id = ?`, gone); rows != 0 {
				t.Errorf("the approval stored %d membership rows under the deleted team", rows)
			}
			if decided := count(`SELECT count() FROM team_drift_changes FINAL WHERE org_id = 'org-1' AND entity_id = ? AND status != 'pending'`, gone); decided != 0 {
				t.Errorf("%d changes of the deleted team were decided by the refused approval", decided)
			}

			// The changes stay pending and can be dismissed.
			rec := writeSeamCall(t, h, h.dismissChanges, http.MethodPost, "/api/v1/admin/teams/"+gone+"/dismiss-changes", gone,
				map[string]any{"dismiss_all": true})
			if rec.Code != http.StatusOK || decodeBody(t, rec)["dismissed"] != float64(2) {
				t.Errorf("dismissal for the deleted team: status %d body %s, want 200 with 2 dismissed", rec.Code, rec.Body.String())
			}
		})
	}
}
