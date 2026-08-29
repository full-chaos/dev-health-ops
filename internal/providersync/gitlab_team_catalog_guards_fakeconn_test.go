package providersync

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fakeGitLabGuardConn is a minimal driver.Conn double for every query/insert
// applyGitLabTeamMembershipConflictGuard now issues (CHAOS-4444 added the
// staging/stale-resolution engine on top of the pre-existing exclusion
// queries) -- no real ClickHouse needed. Every method besides Query and
// PrepareBatch panics if called: a wiring bug that DID call one should fail
// loudly rather than silently return a zero value.
type fakeGitLabGuardConn struct {
	driver.Conn
	manualRows   [][2]string // member_id, team_id (legacy 2-col exclusion query)
	fallbackRows [][2]string // scope_id, team_id (legacy 2-col exclusion query)

	richManualRows   []manualMembershipRow // CHAOS-4444 rich fetch for staged-detail content
	richFallbackRows []memberFallbackRow
	existingChanges  []teamDriftChangeRow // pre-seeded team_drift_changes (identity, pending)

	inserted []teamDriftChangeRow // captures every row PrepareBatch/Send wrote, for assertions
}

func (f *fakeGitLabGuardConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "is_primary") && strings.Contains(query, "FROM team_memberships"):
		return &fakeManualMembershipRichRows{rows: f.richManualRows, index: -1}, nil
	case strings.Contains(query, "FROM team_memberships"):
		return &fakeGitLabGuardMembershipRows{rows: f.manualRows, index: -1}, nil
	case strings.Contains(query, "created_by") && strings.Contains(query, "FROM manual_attribution_fallbacks"):
		return &fakeMemberFallbackRichRows{rows: f.richFallbackRows, index: -1}, nil
	case strings.Contains(query, "FROM manual_attribution_fallbacks"):
		return &fakeGitLabGuardFallbackRows{rows: f.fallbackRows, index: -1}, nil
	case strings.Contains(query, "FROM team_drift_changes"):
		return &fakeIdentityChangeRows{rows: f.existingChanges, index: -1}, nil
	default:
		return nil, fmt.Errorf("fakeGitLabGuardConn: unexpected query: %s", query)
	}
}

func (f *fakeGitLabGuardConn) PrepareBatch(_ context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &fakeGitLabGuardBatch{conn: f}, nil
}

// fakeGitLabGuardBatch captures every Append'd row (as its raw positional
// args) and, on Send, decodes it back into a teamDriftChangeRow appended to
// the owning conn's `inserted` slice -- matching insertTeamDriftChanges's
// own Append column order exactly (team_drift_review.go's
// teamDriftChangesInsert constant).
type fakeGitLabGuardBatch struct {
	driver.Batch
	conn *fakeGitLabGuardConn
	rows []teamDriftChangeRow
}

func (b *fakeGitLabGuardBatch) Append(args ...any) error {
	if len(args) != 16 {
		return fmt.Errorf("fakeGitLabGuardBatch: want 16 args, got %d", len(args))
	}
	get := func(i int) string {
		if s, ok := args[i].(string); ok {
			return s
		}
		return ""
	}
	getPtr := func(i int) *string {
		if s, ok := args[i].(*string); ok {
			return s
		}
		return nil
	}
	row := teamDriftChangeRow{
		OrgID: get(0), ChangeID: get(1), EntityType: get(2), EntityID: get(3),
		Provider: get(4), NativeTeamKey: getPtr(5), ChangeType: get(6), Field: getPtr(7),
		OldValueJSON: get(8), NewValueJSON: get(9), Status: get(10),
	}
	if t, ok := args[11].(time.Time); ok {
		row.FirstSeenAt = t
	}
	if t, ok := args[12].(time.Time); ok {
		row.LastSeenAt = t
	}
	if t, ok := args[15].(time.Time); ok {
		row.UpdatedAt = t
	}
	b.rows = append(b.rows, row)
	return nil
}

func (b *fakeGitLabGuardBatch) Send() error {
	b.conn.inserted = append(b.conn.inserted, b.rows...)
	return nil
}

func (b *fakeGitLabGuardBatch) Abort() error { return nil }

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

// fakeManualMembershipRichRows backs fetchManualMembershipRows's 13-column
// SELECT (manualMembershipRowsQuery).
type fakeManualMembershipRichRows struct {
	driver.Rows
	rows  []manualMembershipRow
	index int
}

func (r *fakeManualMembershipRichRows) Next() bool {
	r.index++
	return r.index < len(r.rows)
}

func (r *fakeManualMembershipRichRows) Scan(dest ...any) error {
	if len(dest) != 13 {
		return fmt.Errorf("fakeManualMembershipRichRows: want 13 dest, got %d", len(dest))
	}
	row := r.rows[r.index]
	*dest[0].(*string) = row.OrgID
	*dest[1].(*string) = row.Provider
	*dest[2].(*string) = row.TeamID
	*dest[3].(*string) = row.MemberID
	*dest[4].(**string) = row.RawProviderUserID
	*dest[5].(**string) = row.RawEmail
	*dest[6].(*string) = row.Source
	*dest[7].(*uint8) = row.IsPrimary
	*dest[8].(*uint16) = row.Specificity
	*dest[9].(*int32) = row.Priority
	*dest[10].(*time.Time) = row.ValidFrom
	*dest[11].(**time.Time) = row.ValidTo
	*dest[12].(*time.Time) = row.UpdatedAt
	return nil
}

func (r *fakeManualMembershipRichRows) Close() error { return nil }
func (r *fakeManualMembershipRichRows) Err() error   { return nil }

// fakeMemberFallbackRichRows backs fetchMemberFallbackRows's 13-column
// SELECT (memberFallbackRowsQuery).
type fakeMemberFallbackRichRows struct {
	driver.Rows
	rows  []memberFallbackRow
	index int
}

func (r *fakeMemberFallbackRichRows) Next() bool {
	r.index++
	return r.index < len(r.rows)
}

func (r *fakeMemberFallbackRichRows) Scan(dest ...any) error {
	if len(dest) != 13 {
		return fmt.Errorf("fakeMemberFallbackRichRows: want 13 dest, got %d", len(dest))
	}
	row := r.rows[r.index]
	*dest[0].(*string) = row.OrgID
	*dest[1].(*string) = row.Provider
	*dest[2].(*string) = row.ScopeType
	*dest[3].(*string) = row.ScopeID
	*dest[4].(*string) = row.TeamID
	*dest[5].(*string) = row.TeamName
	*dest[6].(*string) = row.Reason
	*dest[7].(*int32) = row.Priority
	*dest[8].(*time.Time) = row.ValidFrom
	*dest[9].(**time.Time) = row.ValidTo
	*dest[10].(**string) = row.CreatedBy
	*dest[11].(*time.Time) = row.CreatedAt
	*dest[12].(*time.Time) = row.UpdatedAt
	return nil
}

func (r *fakeMemberFallbackRichRows) Close() error { return nil }
func (r *fakeMemberFallbackRichRows) Err() error   { return nil }

// fakeIdentityChangeRows backs fetchIdentityDriftChanges's 9-column SELECT
// (identityDriftChangesQuery).
type fakeIdentityChangeRows struct {
	driver.Rows
	rows  []teamDriftChangeRow
	index int
}

func (r *fakeIdentityChangeRows) Next() bool {
	r.index++
	return r.index < len(r.rows)
}

func (r *fakeIdentityChangeRows) Scan(dest ...any) error {
	if len(dest) != 9 {
		return fmt.Errorf("fakeIdentityChangeRows: want 9 dest, got %d", len(dest))
	}
	row := r.rows[r.index]
	*dest[0].(*string) = row.ChangeID
	*dest[1].(*string) = row.EntityID
	*dest[2].(*string) = row.Provider
	*dest[3].(**string) = row.NativeTeamKey
	*dest[4].(**string) = row.Field
	*dest[5].(*string) = row.OldValueJSON
	*dest[6].(*string) = row.NewValueJSON
	*dest[7].(*string) = row.Status
	*dest[8].(*time.Time) = row.FirstSeenAt
	return nil
}

func (r *fakeIdentityChangeRows) Close() error { return nil }
func (r *fakeIdentityChangeRows) Err() error   { return nil }

// TestApplyGitLabTeamMembershipConflictGuardFiltersAndCounts proves
// applyGitLabTeamMembershipConflictGuard against a fake conn (no real
// ClickHouse): a membership row conflicting with an active manual pin to a
// DIFFERENT team is filtered out of the kept batch AND counted, AND staged
// as a team_drift_changes row, while a clean row (and a same-team CONFIRMED
// row) survive and stage nothing.
func TestApplyGitLabTeamMembershipConflictGuardFiltersAndCounts(t *testing.T) {
	conn := &fakeGitLabGuardConn{
		manualRows:     [][2]string{{"gitlab:alice", "gl:org"}}, // alice manually pinned to gl:org
		richManualRows: []manualMembershipRow{{OrgID: "org-1", Provider: "gitlab", TeamID: "gl:org", MemberID: "gitlab:alice", Source: "manual", ValidFrom: time.Now(), UpdatedAt: time.Now()}},
	}
	rows := []gitlabTeamCatalogMembershipRow{
		{MemberID: "gitlab:alice", TeamID: "gl:org/team-a", Provider: "gitlab", ValidFrom: time.Now(), UpdatedAt: time.Now()}, // conflict: different team than the pin
		{MemberID: "gitlab:alice", TeamID: "gl:org", Provider: "gitlab", ValidFrom: time.Now(), UpdatedAt: time.Now()},        // confirmation: same team as the pin
		{MemberID: "gitlab:bob", TeamID: "gl:org", Provider: "gitlab", ValidFrom: time.Now(), UpdatedAt: time.Now()},          // clean: no pin at all for bob
	}
	kept, skipped, staged, superseded, err := applyGitLabTeamMembershipConflictGuard(context.Background(), conn, "org-1", "gitlab", rows, []string{"gl:org", "gl:org/team-a"}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if skipped != 1 {
		t.Fatalf("skipped=%d want=1", skipped)
	}
	if staged != 1 {
		t.Fatalf("staged=%d want=1", staged)
	}
	if superseded != 0 {
		t.Fatalf("superseded=%d want=0", superseded)
	}
	if len(kept) != 2 {
		t.Fatalf("kept=%+v want 2 rows", kept)
	}
	for _, row := range kept {
		if row.MemberID == "gitlab:alice" && row.TeamID == "gl:org/team-a" {
			t.Fatalf("the conflicting row must not survive filtering: kept=%+v", kept)
		}
	}
	if len(conn.inserted) != 1 {
		t.Fatalf("inserted=%+v want exactly one staged team_drift_changes row", conn.inserted)
	}
	staged1 := conn.inserted[0]
	if staged1.EntityType != identityDriftEntityType || staged1.Status != teamDriftStatusPending {
		t.Fatalf("staged row = %+v, want entity_type=identity status=pending", staged1)
	}
	if staged1.Field == nil || *staged1.Field != identityDriftFieldTeamMembership {
		t.Fatalf("staged row field = %v, want %q", staged1.Field, identityDriftFieldTeamMembership)
	}
	// codex review round 1, P1 (PR #2002): old_value_json must be the WHOLE
	// _conflict_for wrapper ({"field": ..., "manual_membership": {...}}),
	// not just the bare manual row -- apply_identity_membership_change's
	// _expire_conflict reads old_value.get("field") to decide which table
	// to expire; serializing only the inner row makes approval unable to
	// identify (and therefore never expire) the conflicting manual record.
	if !strings.Contains(staged1.OldValueJSON, `"field":"team_memberships"`) ||
		!strings.Contains(staged1.OldValueJSON, `"manual_membership":{`) {
		t.Fatalf("old_value_json = %q, want the wrapper shape {\"field\":\"team_memberships\",\"manual_membership\":{...}}", staged1.OldValueJSON)
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
		manualRows:     [][2]string{{"gitlab:alice", "gl:org"}}, // alice pinned to gl:org
		richManualRows: []manualMembershipRow{{OrgID: "org-1", Provider: "gitlab", TeamID: "gl:org", MemberID: "gitlab:alice", Source: "manual", ValidFrom: time.Now(), UpdatedAt: time.Now()}},
	}
	// team-a's own membership walk observed alice too (a stale/incorrect
	// discovery -- alice's real, admin-confirmed home is gl:org).
	rows := []gitlabTeamCatalogMembershipRow{
		{MemberID: "gitlab:alice", TeamID: "gl:org/team-a", Provider: "gitlab", IdentityFacets: []string{"gitlab:alice"}, ValidFrom: time.Now(), UpdatedAt: time.Now()},
		{MemberID: "gitlab:bob", TeamID: "gl:org/team-a", Provider: "gitlab", IdentityFacets: []string{"gitlab:bob"}, ValidFrom: time.Now(), UpdatedAt: time.Now()},
	}
	kept, skipped, _, _, err := applyGitLabTeamMembershipConflictGuard(context.Background(), conn, "org-1", "gitlab", rows, []string{"gl:org/team-a"}, time.Now())
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
