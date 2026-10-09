//go:build integration

package teamsidentity

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

func readTeamCall(t *testing.T, h handlers, teamID string) (int, map[string]any) {
	t.Helper()
	rec := writeSeamCall(t, h, h.getTeam, http.MethodGet, "/api/v1/admin/teams/"+teamID, teamID, nil)
	if rec.Code != http.StatusOK {
		return rec.Code, nil
	}
	return rec.Code, decodeBody(t, rec)
}

// A read by a bare id resolves with the write seam's rule: the one team
// that holds it, a 409 when two do, the id as given when none does.
func TestAReadByABareIDResolvesLikeAWrite(t *testing.T) {
	t.Run("one carried holder", func(t *testing.T) {
		s, ctx := writeSeamStore(t)
		writeSeamSeed(t, s, ctx, "linear", "ENG")
		if _, err := providersync.CarryTeamIDs(ctx, s.Conn, "org-1", time.Now().UTC(), false); err != nil {
			t.Fatal(err)
		}
		code, body := readTeamCall(t, newTestHandlers(s), "ENG")
		if code != http.StatusOK || body["team_id"] != "linear:ENG" {
			t.Fatalf("GET /teams/ENG = %d %v, want 200 linear:ENG", code, body["team_id"])
		}
	})
	t.Run("an admin team read back by the id the admin gave", func(t *testing.T) {
		s, _ := writeSeamStore(t)
		h := newTestHandlers(s)
		rec := writeSeamCall(t, h, h.createOrUpdateTeam, http.MethodPost, "/api/v1/admin/teams", "",
			map[string]any{"team_id": "written-new", "name": "Written"})
		if rec.Code != http.StatusOK {
			t.Fatalf("create = %d %s", rec.Code, rec.Body.String())
		}
		code, body := readTeamCall(t, h, "written-new")
		if code != http.StatusOK || body["team_id"] != "custom:written-new" {
			t.Fatalf("GET /teams/written-new = %d %v, want 200 custom:written-new", code, body["team_id"])
		}
	})
	t.Run("two holders", func(t *testing.T) {
		s, ctx := writeSeamStore(t)
		writeSeamSeed(t, s, ctx, "linear", "linear:QA")
		writeSeamSeed(t, s, ctx, "gitlab", "gl:QA")
		if code, _ := readTeamCall(t, newTestHandlers(s), "QA"); code != http.StatusConflict {
			t.Fatalf("GET /teams/QA with two holders = %d, want 409", code)
		}
	})
	t.Run("a bare row the carry has not moved", func(t *testing.T) {
		s, ctx := writeSeamStore(t)
		writeSeamSeed(t, s, ctx, "linear", "ENG")
		code, body := readTeamCall(t, newTestHandlers(s), "ENG")
		if code != http.StatusOK || body["team_id"] != "ENG" {
			t.Fatalf("GET /teams/ENG before the carry = %d %v, want 200 ENG", code, body["team_id"])
		}
	})
	t.Run("no team", func(t *testing.T) {
		s, _ := writeSeamStore(t)
		if code, _ := readTeamCall(t, newTestHandlers(s), "nobody"); code != http.StatusNotFound {
			t.Fatalf("GET /teams/nobody = %d, want 404", code)
		}
	})
	t.Run("a prefixed id is read as given", func(t *testing.T) {
		s, ctx := writeSeamStore(t)
		writeSeamSeed(t, s, ctx, "linear", "linear:QA")
		writeSeamSeed(t, s, ctx, "gitlab", "gl:QA")
		code, body := readTeamCall(t, newTestHandlers(s), "gl:QA")
		if code != http.StatusOK || body["team_id"] != "gl:QA" {
			t.Fatalf("GET /teams/gl:QA = %d %v, want 200 gl:QA", code, body["team_id"])
		}
	})
}

// Inferring members needs a project key the team holds: a team with none
// answers 400, and its id is never taken as a Jira project key, also for a
// bare row the carry has not moved.
func TestInferMembersUsesOnlyTheTeamsOwnProjectKey(t *testing.T) {
	for name, seed := range map[string]func(t *testing.T, s Store, ctx context.Context, h handlers){
		"an admin team": func(t *testing.T, s Store, ctx context.Context, h handlers) {
			if rec := writeSeamCall(t, h, h.createOrUpdateTeam, http.MethodPost, "/api/v1/admin/teams", "",
				map[string]any{"team_id": "nokeys", "name": "No keys"}); rec.Code != http.StatusOK {
				t.Fatalf("create = %d %s", rec.Code, rec.Body.String())
			}
		},
		"a bare row not carried": func(t *testing.T, s Store, ctx context.Context, h handlers) {
			writeSeamSeed(t, s, ctx, "jira", "nokeys")
		},
	} {
		t.Run(name, func(t *testing.T) {
			s, ctx := writeSeamStore(t)
			h := newTestHandlers(s)
			seed(t, s, ctx, h)
			rec := writeSeamCall(t, h, h.inferMembers, http.MethodGet, "/api/v1/admin/teams/nokeys/infer-members", "nokeys", nil)
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "Team does not have a Jira project key configured") {
				t.Fatalf("infer for a team without project keys = %d %s, want 400 no Jira project key", rec.Code, rec.Body.String())
			}
		})
	}
}
