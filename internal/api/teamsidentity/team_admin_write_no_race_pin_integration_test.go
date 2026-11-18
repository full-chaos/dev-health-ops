//go:build integration

package teamsidentity

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// pinnedTeamRow is the newest stored version of a team as one line: every
// column, with the time columns as rules (the version is after the one before
// it, deleted_at is the version, created_at is the first version's time).
func pinnedTeamRow(ctx context.Context, t *testing.T, conn driver.Conn, orgID, teamID string, before time.Time) (string, time.Time, time.Time) {
	t.Helper()
	var (
		line                 string
		updatedAt, createdAt time.Time
	)
	if err := conn.QueryRow(ctx, `SELECT concat(id, '|', toString(team_uuid), '|', name, '|', ifNull(description, '<null>'), '|',
			arrayStringConcat(manual_members, ','), '|', arrayStringConcat(project_keys, ','), '|', arrayStringConcat(repo_patterns, ','), '|',
			toString(is_active), '|', provider, '|', ifNull(native_team_key, '<null>'), '|', ifNull(parent_team_id, '<null>'), '|',
			ifNull(toString(source_id), '<null>'), '|', multiIf(deleted_at IS NULL, 'live', deleted_at = updated_at, 'deleted@version', 'deleted@other'), '|',
			provider_team_id, '|', provider_name, '|', if(gone_at IS NULL, 'not-gone', 'gone'), '|',
			if(created_at = (SELECT min(updated_at) FROM teams WHERE org_id = {org:String} AND id = {id:String}), 'created@first', 'created@other')),
			updated_at, coalesce(created_at, updated_at)
		FROM teams WHERE org_id = {org:String} AND id = {id:String} ORDER BY updated_at DESC, last_synced DESC LIMIT 1`,
		namedArg("org", orgID), namedArg("id", teamID)).Scan(&line, &updatedAt, &createdAt); err != nil {
		t.Fatal(err)
	}
	if !updatedAt.After(before) {
		t.Errorf("the version %s is not after the one before it %s", updatedAt, before)
	}
	return line, updatedAt, createdAt
}

func namedArg(name string, value any) driver.NamedValue {
	return driver.NamedValue{Name: name, Value: value}
}

func teamVersionCount(ctx context.Context, t *testing.T, conn driver.Conn, orgID, teamID string) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM teams WHERE org_id = ? AND id = ?`, orgID, teamID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// pinnedBody is a response body with its two stamps checked against the
// stored row and replaced by <T>.
func pinnedBody(t *testing.T, body string, updatedAt, createdAt time.Time) string {
	t.Helper()
	for key, stamp := range map[string]time.Time{"updated_at": updatedAt, "created_at": createdAt} {
		field := `"` + key + `":` + mustJSON(t, writtenDatetime(stamp))
		if !strings.Contains(body, field) {
			t.Errorf("the response %s does not carry the stored %s %s", body, key, writtenDatetime(stamp))
		}
		body = strings.Replace(body, field, `"`+key+`":"<T>"`, 1)
	}
	return strings.TrimSpace(body)
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// CHAOS-14094 (acp C3): with no other writer, every admin write of a team
// stores the same row and answers the same body as before the fix: one new
// version per write, each column pinned, the stamps the stored ones.
func TestAnAdminWriteWithNoRaceStoresAndAnswersAsBefore(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	// No background merge folds the versions this test counts and compares.
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES teams`); err != nil {
		t.Fatal(err)
	}
	const orgID, teamID = "team-admin-pin-org", "custom:pin"
	store := Store{Conn: conn}
	h := newTestHandlers(store)
	route := func(handler http.HandlerFunc, method string, payload map[string]any) (int, string) {
		recorder := callWithBody(t, h, func(w http.ResponseWriter, r *http.Request) {
			if method != http.MethodPost {
				r.SetPathValue("team_id", teamID)
			}
			handler(w, r)
		}, method, "/api/v1/admin/teams", orgID, payload)
		return recorder.Code, recorder.Body.String()
	}
	edited := func(team *Team, err error) (int, string) {
		if err != nil || team == nil {
			t.Fatalf("roster edit: %v %v", team, err)
		}
		return http.StatusOK, mustJSON(t, teamWrittenJSON(*team))
	}
	steps := []struct {
		name string
		do   func() (int, string)
		row  string
		body string
	}{
		{"post_create", func() (int, string) {
			return route(h.createOrUpdateTeam, http.MethodPost, map[string]any{"team_id": teamID, "name": "Pin", "description": "First",
				"repo_patterns": []string{"acme/pin-*"}, "project_keys": []string{"PIN"}})
		}, `custom:pin|3490dec6-21c8-58ac-ac5d-91cb85150989|Pin|First||PIN|acme/pin-*|1||<null>|<null>|<null>|live|||not-gone|created@first`, `{"id":"3490dec6-21c8-58ac-ac5d-91cb85150989","team_id":"custom:pin","name":"Pin","description":"First","repo_patterns":["acme/pin-*"],"project_keys":["PIN"],"extra_data":{},"managed_fields":[],"sync_policy":2,"flagged_changes":null,"last_drift_sync_at":null,"is_active":true,"created_at":"<T>","updated_at":"<T>"}`},
		{"set_members", func() (int, string) {
			return edited(store.SetMembers(ctx, orgID, teamID, []string{"m2", "m1"}))
		}, `custom:pin|3490dec6-21c8-58ac-ac5d-91cb85150989|Pin|First|m1,m2|PIN|acme/pin-*|1||<null>|<null>|<null>|live|||not-gone|created@first`, `{"id":"3490dec6-21c8-58ac-ac5d-91cb85150989","team_id":"custom:pin","name":"Pin","description":"First","repo_patterns":["acme/pin-*"],"project_keys":["PIN"],"extra_data":{},"managed_fields":[],"sync_policy":2,"flagged_changes":null,"last_drift_sync_at":null,"is_active":true,"created_at":"<T>","updated_at":"<T>"}`},
		{"post_update", func() (int, string) {
			return route(h.createOrUpdateTeam, http.MethodPost, map[string]any{"team_id": teamID, "name": "Pin 2", "project_keys": []string{"PIN", "PIN2"}})
		}, `custom:pin|3490dec6-21c8-58ac-ac5d-91cb85150989|Pin 2|<null>|m1,m2|PIN,PIN2||1||<null>|<null>|<null>|live|||not-gone|created@first`, `{"id":"3490dec6-21c8-58ac-ac5d-91cb85150989","team_id":"custom:pin","name":"Pin 2","description":null,"repo_patterns":[],"project_keys":["PIN","PIN2"],"extra_data":{},"managed_fields":[],"sync_policy":2,"flagged_changes":null,"last_drift_sync_at":null,"is_active":true,"created_at":"<T>","updated_at":"<T>"}`},
		{"patch_name", func() (int, string) {
			return route(h.updateTeam, http.MethodPatch, map[string]any{"name": "Pin 3"})
		}, `custom:pin|3490dec6-21c8-58ac-ac5d-91cb85150989|Pin 3|<null>|m1,m2|PIN,PIN2||1||<null>|<null>|<null>|live|||not-gone|created@first`, `{"id":"3490dec6-21c8-58ac-ac5d-91cb85150989","team_id":"custom:pin","name":"Pin 3","description":null,"repo_patterns":[],"project_keys":["PIN","PIN2"],"extra_data":{},"managed_fields":[],"sync_policy":2,"flagged_changes":null,"last_drift_sync_at":null,"is_active":true,"created_at":"<T>","updated_at":"<T>"}`},
		{"patch_description", func() (int, string) {
			return route(h.updateTeam, http.MethodPatch, map[string]any{"description": "Third"})
		}, `custom:pin|3490dec6-21c8-58ac-ac5d-91cb85150989|Pin 3|Third|m1,m2|PIN,PIN2||1||<null>|<null>|<null>|live|||not-gone|created@first`, `{"id":"3490dec6-21c8-58ac-ac5d-91cb85150989","team_id":"custom:pin","name":"Pin 3","description":"Third","repo_patterns":[],"project_keys":["PIN","PIN2"],"extra_data":{},"managed_fields":[],"sync_policy":2,"flagged_changes":null,"last_drift_sync_at":null,"is_active":true,"created_at":"<T>","updated_at":"<T>"}`},
		{"patch_lists", func() (int, string) {
			return route(h.updateTeam, http.MethodPatch, map[string]any{"repo_patterns": []string{"acme/x"}, "project_keys": []string{}})
		}, `custom:pin|3490dec6-21c8-58ac-ac5d-91cb85150989|Pin 3|Third|m1,m2||acme/x|1||<null>|<null>|<null>|live|||not-gone|created@first`, `{"id":"3490dec6-21c8-58ac-ac5d-91cb85150989","team_id":"custom:pin","name":"Pin 3","description":"Third","repo_patterns":["acme/x"],"project_keys":[],"extra_data":{},"managed_fields":[],"sync_policy":2,"flagged_changes":null,"last_drift_sync_at":null,"is_active":true,"created_at":"<T>","updated_at":"<T>"}`},
		{"add_members", func() (int, string) {
			return edited(store.AddMembers(ctx, orgID, teamID, []string{"m3"}))
		}, `custom:pin|3490dec6-21c8-58ac-ac5d-91cb85150989|Pin 3|Third|m1,m2,m3||acme/x|1||<null>|<null>|<null>|live|||not-gone|created@first`, `{"id":"3490dec6-21c8-58ac-ac5d-91cb85150989","team_id":"custom:pin","name":"Pin 3","description":"Third","repo_patterns":["acme/x"],"project_keys":[],"extra_data":{},"managed_fields":[],"sync_policy":2,"flagged_changes":null,"last_drift_sync_at":null,"is_active":true,"created_at":"<T>","updated_at":"<T>"}`},
		{"remove_members", func() (int, string) {
			return edited(store.RemoveMembers(ctx, orgID, teamID, map[string]bool{"m1": true}))
		}, `custom:pin|3490dec6-21c8-58ac-ac5d-91cb85150989|Pin 3|Third|m2,m3||acme/x|1||<null>|<null>|<null>|live|||not-gone|created@first`, `{"id":"3490dec6-21c8-58ac-ac5d-91cb85150989","team_id":"custom:pin","name":"Pin 3","description":"Third","repo_patterns":["acme/x"],"project_keys":[],"extra_data":{},"managed_fields":[],"sync_policy":2,"flagged_changes":null,"last_drift_sync_at":null,"is_active":true,"created_at":"<T>","updated_at":"<T>"}`},
		{"delete", func() (int, string) {
			return route(h.deleteTeam, http.MethodDelete, nil)
		}, `custom:pin|3490dec6-21c8-58ac-ac5d-91cb85150989|Pin 3|Third|m2,m3||acme/x|0||<null>|<null>|<null>|deleted@version|||not-gone|created@first`, `{"deleted":true}`},
	}
	var before time.Time
	for _, step := range steps {
		versions := teamVersionCount(ctx, t, conn, orgID, teamID)
		code, body := step.do()
		if code != http.StatusOK {
			t.Fatalf("%s: %d %s", step.name, code, body)
		}
		if added := teamVersionCount(ctx, t, conn, orgID, teamID) - versions; added != 1 {
			t.Errorf("PIN %s: %d versions written, want 1", step.name, added)
		}
		row, updatedAt, createdAt := pinnedTeamRow(ctx, t, conn, orgID, teamID, before)
		before = updatedAt
		if step.name != "delete" {
			body = pinnedBody(t, body, updatedAt, createdAt)
		}
		if row != step.row {
			t.Errorf("PINROW %s %s", step.name, row)
		}
		if body != step.body {
			t.Errorf("PINBODY %s %s", step.name, body)
		}
	}
}
