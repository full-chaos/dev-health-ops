package providersync

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fakeGitLabGuardConn is a minimal driver.Conn double for the two guard
// read queries (activeManualMembershipsQuery, activeMemberAttributionFallbacksQuery
// -- team_membership_conflict_guard.go) -- no real ClickHouse needed. Every
// method besides Query panics if called: the guard wrappers under test never
// touch them, and a wiring bug that DID call one should fail loudly rather
// than silently return a zero value.
type fakeGitLabGuardConn struct {
	driver.Conn
	manualRows   [][2]string // member_id, team_id
	fallbackRows [][2]string // scope_id, team_id (codex round 3: fallbacks are team-scoped too)
}

func (f *fakeGitLabGuardConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "FROM team_memberships"):
		return &fakeGitLabGuardMembershipRows{rows: f.manualRows, index: -1}, nil
	case strings.Contains(query, "FROM manual_attribution_fallbacks"):
		return &fakeGitLabGuardFallbackRows{rows: f.fallbackRows, index: -1}, nil
	default:
		return nil, fmt.Errorf("fakeGitLabGuardConn: unexpected query: %s", query)
	}
}

// fakeGitLabGuardMembershipRows backs resolveActiveManualMembershipTeams's
// `SELECT member_id, team_id` shape.
type fakeGitLabGuardMembershipRows struct {
	driver.Rows
	rows  [][2]string
	index int
}

func (r *fakeGitLabGuardMembershipRows) Next() bool {
	r.index++
	return r.index < len(r.rows)
}

func (r *fakeGitLabGuardMembershipRows) Scan(dest ...any) error {
	memberID, ok1 := dest[0].(*string)
	teamID, ok2 := dest[1].(*string)
	if !ok1 || !ok2 {
		return fmt.Errorf("fakeGitLabGuardMembershipRows: unexpected Scan dest shape")
	}
	*memberID = r.rows[r.index][0]
	*teamID = r.rows[r.index][1]
	return nil
}

func (r *fakeGitLabGuardMembershipRows) Close() error { return nil }
func (r *fakeGitLabGuardMembershipRows) Err() error   { return nil }

// fakeGitLabGuardFallbackRows backs resolveActiveMemberAttributionFallbackTeams's
// `SELECT scope_id, team_id` shape (codex round 3: fallbacks are team-scoped
// too, same shape as the manual-membership rows).
type fakeGitLabGuardFallbackRows struct {
	driver.Rows
	rows  [][2]string
	index int
}

func (r *fakeGitLabGuardFallbackRows) Next() bool {
	r.index++
	return r.index < len(r.rows)
}

func (r *fakeGitLabGuardFallbackRows) Scan(dest ...any) error {
	scopeID, ok1 := dest[0].(*string)
	teamID, ok2 := dest[1].(*string)
	if !ok1 || !ok2 {
		return fmt.Errorf("fakeGitLabGuardFallbackRows: unexpected Scan dest shape")
	}
	*scopeID = r.rows[r.index][0]
	*teamID = r.rows[r.index][1]
	return nil
}

func (r *fakeGitLabGuardFallbackRows) Close() error { return nil }
func (r *fakeGitLabGuardFallbackRows) Err() error   { return nil }

// TestApplyGitLabTeamMembershipConflictGuardFiltersAndCounts proves
// applyGitLabTeamMembershipConflictGuard against a fake conn (no real
// ClickHouse): a membership row conflicting with an active manual pin to a
// DIFFERENT team is filtered out of the kept batch AND counted, while a
// clean row (and a same-team CONFIRMED row) survive.
func TestApplyGitLabTeamMembershipConflictGuardFiltersAndCounts(t *testing.T) {
	conn := &fakeGitLabGuardConn{
		manualRows: [][2]string{{"gitlab:alice", "gl:org"}}, // alice manually pinned to gl:org
	}
	rows := []gitlabTeamCatalogMembershipRow{
		{MemberID: "gitlab:alice", TeamID: "gl:org/team-a"}, // conflict: different team than the pin
		{MemberID: "gitlab:alice", TeamID: "gl:org"},        // confirmation: same team as the pin
		{MemberID: "gitlab:bob", TeamID: "gl:org"},          // clean: no pin at all for bob
	}
	kept, skipped, err := applyGitLabTeamMembershipConflictGuard(context.Background(), conn, "org-1", "gitlab", rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if skipped != 1 {
		t.Fatalf("skipped=%d want=1", skipped)
	}
	if len(kept) != 2 {
		t.Fatalf("kept=%+v want 2 rows", kept)
	}
	for _, row := range kept {
		if row.MemberID == "gitlab:alice" && row.TeamID == "gl:org/team-a" {
			t.Fatalf("the conflicting row must not survive filtering: kept=%+v", kept)
		}
	}
}

// TestGitLabTeamCatalogCollectorRosterExcludesConflictFilteredMembership is
// the roster-after-filter ordering regression proof team-lead required
// before the slot (2026-08-28): given a membership that the conflict guard
// rejects, that member must NOT appear in the corresponding team's `members`
// roster -- proving the roster is rebuilt from the FILTERED membership set,
// not the raw walk-observed one. Exercised directly at the guard + roster-
// rebuild layer (gitlabRosterFromMemberships), matching exactly what
// GitLabTeamCatalogCollector.CollectTeamCatalog does with keptMemberships,
// without needing the full HTTP walk or a real ClickHouse write path.
func TestGitLabTeamCatalogCollectorRosterExcludesConflictFilteredMembership(t *testing.T) {
	conn := &fakeGitLabGuardConn{
		manualRows: [][2]string{{"gitlab:alice", "gl:org"}}, // alice pinned to gl:org
	}
	// team-a's own membership walk observed alice too (a stale/incorrect
	// discovery -- alice's real, admin-confirmed home is gl:org).
	rows := []gitlabTeamCatalogMembershipRow{
		{MemberID: "gitlab:alice", TeamID: "gl:org/team-a", IdentityFacets: []string{"gitlab:alice"}},
		{MemberID: "gitlab:bob", TeamID: "gl:org/team-a", IdentityFacets: []string{"gitlab:bob"}},
	}
	kept, skipped, err := applyGitLabTeamMembershipConflictGuard(context.Background(), conn, "org-1", "gitlab", rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if skipped != 1 {
		t.Fatalf("skipped=%d want=1", skipped)
	}
	roster := gitlabRosterFromMemberships(kept)
	teamARoster := roster["gl:org/team-a"]
	for _, member := range teamARoster {
		if member == "gitlab:alice" {
			t.Fatalf("team-a's roster must NOT contain alice -- her membership was conflict-filtered: roster=%v", teamARoster)
		}
	}
	if len(teamARoster) != 1 || teamARoster[0] != "gitlab:bob" {
		t.Fatalf("team-a's roster = %v, want only [gitlab:bob]", teamARoster)
	}
}
