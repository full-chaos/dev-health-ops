//go:build integration

package teamsidentity

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"slices"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var rosterReadBack = regexp.MustCompile(regexp.QuoteMeta(rosterReadBackQuery))

// staleCarryConn writes, before every roster read-back of writer a, a newer
// version of the team that carries the roster [x] (a writer that read the
// team before a's insert and wins on version), and counts a's teams inserts.
type staleCarryConn struct {
	driver.Conn
	t        *testing.T
	teamID   string
	inserts  int
	readBack int
}

func (c *staleCarryConn) Exec(ctx context.Context, query string, args ...any) error {
	if rosterRaceAtInsert.MatchString(query) {
		c.inserts++
	}
	return c.Conn.Exec(ctx, query, args...)
}

func (c *staleCarryConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if rosterReadBack.MatchString(query) {
		c.readBack++
		if err := c.Conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider)
			SELECT id, team_uuid, name, ['x'], project_keys, repo_patterns, 1, updated_at + INTERVAL 1 SECOND, org_id, provider
			FROM teams FINAL WHERE org_id = ? AND id = ?`, rosterRaceOrg, c.teamID); err != nil {
			c.t.Fatal(err)
		}
	}
	return c.Conn.QueryRow(ctx, query, args...)
}

func teamVersions(ctx context.Context, t *testing.T, conn driver.Conn, teamID string) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM teams WHERE org_id = ? AND id = ?`, rosterRaceOrg, teamID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// CHAOS-14081 refusal branch: a roster edit that another writer overwrites
// on every round is written exactly rosterRounds times, then refused with
// ErrTeamChangedConcurrently; nothing is written after the refusal. The
// route answers 409 with the retryable reason code.
func TestARosterEditThatNeverHoldsIsRefusedAfterThreeWrites(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)

	t.Run("store", func(t *testing.T) {
		const teamID = "gh:refused-store"
		// A merge would fold the versions this cell counts.
		if err := conn.Exec(ctx, `SYSTEM STOP MERGES teams`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Exec(context.Background(), `SYSTEM START MERGES teams`) })
		seedRosterTeam(ctx, t, conn, teamID, "github")
		before := teamVersions(ctx, t, conn, teamID)
		stale := &staleCarryConn{Conn: conn, t: t, teamID: teamID}
		_, err := (Store{Conn: stale}).AddMembers(ctx, rosterRaceOrg, teamID, []string{"a"})
		if !errors.Is(err, ErrTeamChangedConcurrently) {
			t.Fatalf("AddMembers error = %v, want ErrTeamChangedConcurrently", err)
		}
		if stale.inserts != rosterRounds || stale.readBack != rosterRounds {
			t.Fatalf("writes = %d, read-backs = %d, want %d each", stale.inserts, stale.readBack, rosterRounds)
		}
		after := teamVersions(ctx, t, conn, teamID)
		if after-before != uint64(2*rosterRounds) {
			t.Fatalf("versions written = %d, want %d (%d edit writes + %d stale carries)", after-before, 2*rosterRounds, rosterRounds, rosterRounds)
		}
		t.Logf("CELL refusal/store: REFUSED after %d writes, %d versions written, roster=%v", stale.inserts, after-before, storedTeamRoster(ctx, t, conn, teamID))
	})

	t.Run("route_409", func(t *testing.T) {
		const teamID = "custom:refused-route"
		if _, err := (Store{Conn: conn}).CreateOrUpdateTeam(ctx, rosterRaceOrg, TeamWrite{Origin: "custom", TeamID: teamID, Name: "Refused"}); err != nil {
			t.Fatal(err)
		}
		stale := &staleCarryConn{Conn: conn, t: t, teamID: teamID}
		h := newTestHandlers(Store{Conn: stale})
		recorder := callWithBody(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", rosterRaceOrg,
			map[string]any{"canonical_id": "refused", "email": "refused@example.com", "team_ids": []string{teamID}})
		if recorder.Code != http.StatusConflict {
			t.Fatalf("status = %d body = %s, want 409", recorder.Code, recorder.Body.String())
		}
		detail, _ := decodeBody(t, recorder)["detail"].(map[string]any)
		if detail["reason_code"] != "team_changed_concurrently" || detail["message"] != "team changed concurrently, retry" {
			t.Fatalf("409 body = %s", recorder.Body.String())
		}
		if stale.inserts != rosterRounds {
			t.Fatalf("roster writes = %d, want %d", stale.inserts, rosterRounds)
		}
		t.Logf("CELL refusal/route: 409 %s after %d writes", recorder.Body.String(), stale.inserts)
	})

	t.Run("no_race_one_write", func(t *testing.T) {
		const teamID = "gh:no-race"
		if err := conn.Exec(ctx, `SYSTEM STOP MERGES teams`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Exec(context.Background(), `SYSTEM START MERGES teams`) })
		seedRosterTeam(ctx, t, conn, teamID, "github")
		before := teamVersions(ctx, t, conn, teamID)
		count := &staleCarryConn{Conn: conn, t: t, teamID: "no-such-team"}
		if _, err := (Store{Conn: count}).AddMembers(ctx, rosterRaceOrg, teamID, []string{"a"}); err != nil {
			t.Fatal(err)
		}
		if count.inserts != 1 || count.readBack != 1 || teamVersions(ctx, t, conn, teamID)-before != 1 {
			t.Fatalf("writes = %d, read-backs = %d, versions = %d, want 1 each", count.inserts, count.readBack, teamVersions(ctx, t, conn, teamID)-before)
		}
		t.Logf("CELL no_race: one write, one read-back")
	})
}

// Named residuals of CHAOS-14081 (RISK-NOTES). They log the residual shape
// and fail only when it changes.
func TestTeamRosterNamedResiduals(t *testing.T) {
	conn, ctx := startMigratedTeamsStore(t)

	// R1: a writer whose roster was read before the admin insert (a sync
	// statement's snapshot) and that commits after the admin read-back, with a
	// newer version, still wins: the read in the statement is not a lock.
	t.Run("R1_carry_after_read_back", func(t *testing.T) {
		const teamID = "gh:residual-r1"
		seedRosterTeam(ctx, t, conn, teamID, "github")
		var snapshot []string
		race := &rosterRaceConn{Conn: conn, at: rosterRaceAtInsert, other: func() {
			snapshot = storedTeamRoster(ctx, t, conn, teamID)
		}}
		if _, err := (Store{Conn: race}).AddMembers(ctx, rosterRaceOrg, teamID, []string{"a"}); err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider)
			SELECT id, team_uuid, name, ?, project_keys, repo_patterns, 1, updated_at + INTERVAL 1 SECOND, org_id, provider
			FROM teams FINAL WHERE org_id = ? AND id = ?`, snapshot, rosterRaceOrg, teamID); err != nil {
			t.Fatal(err)
		}
		got := storedTeamRoster(ctx, t, conn, teamID)
		if !slices.Equal(got, []string{"x"}) {
			t.Fatalf("R1 shape changed: roster=%v", got)
		}
		t.Logf("RESIDUAL R1_carry_after_read_back: roster=%v (the admin add of a is lost)", got)
	})

	// R4: two admins edit the SAME member the opposite way inside one
	// read-back window. a adds m; b removes m and finishes before a's read-back
	// reads; a sees m missing and adds it again, so b's removal is lost.
	t.Run("R4_same_member_inside_read_back", func(t *testing.T) {
		const teamID = "gh:residual-r4"
		seedRosterTeam(ctx, t, conn, teamID, "github")
		race := &rosterRaceConn{Conn: conn, at: rosterReadBack, other: func() {
			if _, err := (Store{Conn: conn}).RemoveMembers(ctx, rosterRaceOrg, teamID, map[string]bool{"m": true}); err != nil {
				t.Errorf("writer b: %v", err)
			}
		}}
		if _, err := (Store{Conn: race}).AddMembers(ctx, rosterRaceOrg, teamID, []string{"m"}); err != nil {
			t.Fatal(err)
		}
		got := storedTeamRoster(ctx, t, conn, teamID)
		if !slices.Equal(got, []string{"m", "x"}) {
			t.Fatalf("R4 shape changed: roster=%v", got)
		}
		t.Logf("RESIDUAL R4_same_member_inside_read_back: roster=%v (b's removal of m is lost; both edits named one member)", got)
	})
}
