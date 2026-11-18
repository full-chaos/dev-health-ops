//go:build integration

package teamsidentity

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// oneTeamStaleCarryConn writes, before every roster read-back of teamID, a
// newer version of teamID that carries the roster held before (a writer that
// read the team earlier and wins on version). Read-backs of other teams pass.
type oneTeamStaleCarryConn struct {
	driver.Conn
	t      *testing.T
	teamID string
	roster []string
}

func (c *oneTeamStaleCarryConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if rosterReadBack.MatchString(query) && namedTeamID(args) == c.teamID {
		if err := c.Conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider)
			SELECT id, team_uuid, name, ?, project_keys, repo_patterns, 1, updated_at + INTERVAL 1 SECOND, org_id, provider
			FROM teams FINAL WHERE org_id = ? AND id = ?`, c.roster, rosterRaceOrg, c.teamID); err != nil {
			c.t.Fatal(err)
		}
	}
	return c.Conn.QueryRow(ctx, query, args...)
}

func namedTeamID(args []any) string {
	for _, arg := range args {
		if named, ok := arg.(driver.NamedValue); ok && named.Name == "team_id" {
			id, _ := named.Value.(string)
			return id
		}
	}
	return ""
}

func storedIdentityTeams(ctx context.Context, t *testing.T, store Store, canonicalID string) []string {
	t.Helper()
	identity, err := store.GetIdentity(ctx, rosterRaceOrg, canonicalID)
	if err != nil || identity == nil {
		t.Fatalf("identity %s: %v %v", canonicalID, identity, err)
	}
	return identity.TeamIDs
}

// CHAOS-14081 (r1 class 2): an identity write refused with 409 while it
// removes the identity's facets from the team it left can be sent again as
// it was, and the second send finishes the removal.
func TestARefusedIdentityMoveCanBeSentAgainAndFinishes(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	plain := Store{Conn: conn}
	const left, joined = "custom:refusal-left", "custom:refusal-joined"
	for _, id := range []string{left, joined} {
		if _, err := plain.CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{Origin: "custom", TeamID: id, Name: id}); err != nil {
			t.Fatal(err)
		}
	}
	h := newTestHandlers(plain)
	body := func(teamID string) map[string]any {
		return map[string]any{"canonical_id": "alice", "email": "alice@example.com", "team_ids": []string{teamID}}
	}
	if first := callWithBody(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", rosterRaceOrg, body(left)); first.Code != http.StatusOK {
		t.Fatalf("seed: %d %s", first.Code, first.Body.String())
	}
	held := storedTeamRoster(ctx, t, conn, left)

	stale := newTestHandlers(Store{Conn: &oneTeamStaleCarryConn{Conn: conn, t: t, teamID: left, roster: held}})
	refused := callWithBody(t, stale, stale.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", rosterRaceOrg, body(joined))
	if refused.Code != http.StatusConflict {
		t.Fatalf("move with the left team overwritten every round: %d %s, want 409", refused.Code, refused.Body.String())
	}
	t.Logf("CELL identity_move first send: %d %s, identity team_ids=%v", refused.Code, refused.Body.String(), storedIdentityTeams(ctx, t, plain, "alice"))

	again := callWithBody(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", rosterRaceOrg, body(joined))
	if again.Code != http.StatusOK {
		t.Fatalf("second send: %d %s", again.Code, again.Body.String())
	}
	leftRoster, joinedRoster := storedTeamRoster(ctx, t, conn, left), storedTeamRoster(ctx, t, conn, joined)
	if slices.Contains(leftRoster, "alice") || slices.Contains(leftRoster, "alice@example.com") {
		t.Errorf("CELL identity_move second send: LOST removal, left team roster=%v", leftRoster)
	} else {
		t.Logf("CELL identity_move second send: KEPT removal, left team roster=%v", leftRoster)
	}
	if !slices.Contains(joinedRoster, "alice") || !slices.Contains(joinedRoster, "alice@example.com") {
		t.Errorf("CELL identity_move second send: joined team roster=%v, want alice's facets", joinedRoster)
	}
	if teams := storedIdentityTeams(ctx, t, plain, "alice"); !slices.Equal(teams, []string{joined}) {
		t.Errorf("identity team_ids=%v, want [%s]", teams, joined)
	}
}

// CHAOS-14081 (r1 class 4): a roster edit whose read-back finds it lost and
// whose team is then deleted is refused (409), not reported as done.
func TestARosterEditLostToADeleteIsRefused(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)
	plain := Store{Conn: conn}
	deleteAtReadBack := func(teamID string) *rosterRaceConn {
		return &rosterRaceConn{Conn: conn, at: rosterReadBack, other: func() {
			// A delete that read the team before the edit's insert and wins on version.
			if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, deleted_at)
				SELECT id, team_uuid, name, ['x'], project_keys, repo_patterns, 0, updated_at + INTERVAL 1 SECOND, org_id, provider, updated_at + INTERVAL 1 SECOND
				FROM teams FINAL WHERE org_id = ? AND id = ?`, rosterRaceOrg, teamID); err != nil {
				t.Error(err)
			}
		}}
	}

	t.Run("store", func(t *testing.T) {
		const teamID = "gh:deleted-store"
		seedRosterTeam(ctx, t, conn, teamID, "github")
		team, err := (Store{Conn: deleteAtReadBack(teamID)}).AddMembers(ctx, rosterRaceOrg, teamID, []string{"a"})
		if !errors.Is(err, ErrTeamChangedConcurrently) {
			t.Errorf("CELL delete_wins/store: team=%v err=%v, want ErrTeamChangedConcurrently", team, err)
		} else {
			t.Logf("CELL delete_wins/store: REFUSED %v", err)
		}
	})

	t.Run("confirm_route", func(t *testing.T) {
		const teamID = "custom:deleted-route"
		if _, err := plain.CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{Origin: "custom", TeamID: teamID, Name: "Deleted"}); err != nil {
			t.Fatal(err)
		}
		if _, err := plain.SetMembers(ctx, rosterRaceOrg, teamID, []string{"x"}); err != nil {
			t.Fatal(err)
		}
		h := newTestHandlers(Store{Conn: deleteAtReadBack(teamID)})
		recorder := callWithBody(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", rosterRaceOrg,
			map[string]any{"canonical_id": "deleted-bob", "email": "bob@example.com", "team_ids": []string{teamID}})
		if recorder.Code != http.StatusConflict {
			t.Errorf("CELL delete_wins/route: %d %s, want 409", recorder.Code, recorder.Body.String())
		} else {
			t.Logf("CELL delete_wins/route: 409 %s", recorder.Body.String())
		}
	})

	t.Run("not_found_first_read_stays_nil", func(t *testing.T) {
		team, err := plain.AddMembers(ctx, rosterRaceOrg, "gh:never-there", []string{"a"})
		if team != nil || err != nil {
			t.Fatalf("a team missing at the first read: team=%v err=%v, want nil, nil", team, err)
		}
		t.Logf("CELL not_found_first_read: nil, nil")
	})
}
