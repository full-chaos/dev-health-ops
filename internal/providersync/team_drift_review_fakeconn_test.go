package providersync

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// team_drift_review_fakeconn_test.go proves reviewTeamRowsForDrift
// (CHAOS-4444's team-level engine) against a fake driver.Conn -- no real
// ClickHouse needed. Every method besides Query/PrepareBatch panics if
// called, so a wiring bug that reaches one fails loudly.

type fakeTeamDriftConn struct {
	driver.Conn
	policies       map[string]teamDriftPolicy // team_id -> policy
	existingRows   map[string]teamDriftExistingRow
	existingChanges []teamDriftChangeRow // pre-seeded team_drift_changes (team, field_changed)

	observations     []teamProviderObservationCapture
	insertedChanges  []teamDriftChangeRow
}

type teamProviderObservationCapture struct {
	Provider, NativeTeamKey, TeamID string
}

func (f *fakeTeamDriftConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "FROM team_sync_policies"):
		teamIDs := queryArgTeamIDs(args)
		rows := make([][3]any, 0, len(teamIDs))
		for _, id := range teamIDs {
			if p, ok := f.policies[id]; ok {
				rows = append(rows, [3]any{id, uint8(p.Policy), p.ManagedFields})
			}
		}
		return &fakeTeamSyncPolicyRows{rows: rows, index: -1}, nil
	case strings.Contains(query, "FROM teams FINAL"):
		teamIDs := queryArgTeamIDs(args)
		rows := make([]struct {
			id  string
			row teamDriftExistingRow
		}, 0, len(teamIDs))
		for _, id := range teamIDs {
			if row, ok := f.existingRows[id]; ok {
				rows = append(rows, struct {
					id  string
					row teamDriftExistingRow
				}{id, row})
			}
		}
		return &fakeTeamExistingRows{rows: rows, index: -1}, nil
	case strings.Contains(query, "FROM team_drift_changes"):
		return &fakeTeamDriftChangeRows{rows: f.existingChanges, index: -1}, nil
	default:
		return nil, fmt.Errorf("fakeTeamDriftConn: unexpected query: %s", query)
	}
}

func (f *fakeTeamDriftConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	if strings.Contains(query, "INSERT INTO team_provider_observations") {
		return &fakeObservationBatch{conn: f}, nil
	}
	if strings.Contains(query, "INSERT INTO team_drift_changes") {
		return &fakeChangeInsertBatch{conn: f}, nil
	}
	return nil, fmt.Errorf("fakeTeamDriftConn: unexpected PrepareBatch query: %s", query)
}

// queryArgTeamIDs extracts the {team_ids:Array(String)} named-parameter
// value clickhouse.Named(...) produces -- a plain driver.NamedValue{Name,
// Value} struct.
func queryArgTeamIDs(args []any) []string {
	for _, arg := range args {
		named, ok := arg.(driver.NamedValue)
		if !ok || named.Name != "team_ids" {
			continue
		}
		if ids, ok := named.Value.([]string); ok {
			return ids
		}
	}
	return nil
}

type fakeTeamSyncPolicyRows struct {
	driver.Rows
	rows  [][3]any
	index int
}

func (r *fakeTeamSyncPolicyRows) Next() bool { r.index++; return r.index < len(r.rows) }
func (r *fakeTeamSyncPolicyRows) Scan(dest ...any) error {
	row := r.rows[r.index]
	*dest[0].(*string) = row[0].(string)
	*dest[1].(*uint8) = row[1].(uint8)
	*dest[2].(*[]string) = row[2].([]string)
	return nil
}
func (r *fakeTeamSyncPolicyRows) Close() error { return nil }
func (r *fakeTeamSyncPolicyRows) Err() error   { return nil }

type fakeTeamExistingRows struct {
	driver.Rows
	rows []struct {
		id  string
		row teamDriftExistingRow
	}
	index int
}

func (r *fakeTeamExistingRows) Next() bool { r.index++; return r.index < len(r.rows) }
func (r *fakeTeamExistingRows) Scan(dest ...any) error {
	entry := r.rows[r.index]
	*dest[0].(*string) = entry.id
	*dest[1].(**string) = entry.row.Name
	*dest[2].(**string) = entry.row.Description
	*dest[3].(*[]string) = entry.row.Members
	*dest[4].(*[]string) = entry.row.ProjectKeys
	*dest[5].(*[]string) = entry.row.RepoPatterns
	return nil
}
func (r *fakeTeamExistingRows) Close() error { return nil }
func (r *fakeTeamExistingRows) Err() error   { return nil }

type fakeTeamDriftChangeRows struct {
	driver.Rows
	rows  []teamDriftChangeRow
	index int
}

func (r *fakeTeamDriftChangeRows) Next() bool { r.index++; return r.index < len(r.rows) }
func (r *fakeTeamDriftChangeRows) Scan(dest ...any) error {
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
func (r *fakeTeamDriftChangeRows) Close() error { return nil }
func (r *fakeTeamDriftChangeRows) Err() error   { return nil }

type fakeObservationBatch struct {
	driver.Batch
	conn *fakeTeamDriftConn
	rows []teamProviderObservationCapture
}

func (b *fakeObservationBatch) Append(args ...any) error {
	if len(args) != 13 {
		return fmt.Errorf("fakeObservationBatch: want 13 args, got %d", len(args))
	}
	b.rows = append(b.rows, teamProviderObservationCapture{
		Provider: args[1].(string), NativeTeamKey: args[2].(string), TeamID: args[3].(string),
	})
	return nil
}
func (b *fakeObservationBatch) Send() error {
	b.conn.observations = append(b.conn.observations, b.rows...)
	return nil
}
func (b *fakeObservationBatch) Abort() error { return nil }

type fakeChangeInsertBatch struct {
	driver.Batch
	conn *fakeTeamDriftConn
	rows []teamDriftChangeRow
}

func (b *fakeChangeInsertBatch) Append(args ...any) error {
	if len(args) != 16 {
		return fmt.Errorf("fakeChangeInsertBatch: want 16 args, got %d", len(args))
	}
	get := func(i int) string { s, _ := args[i].(string); return s }
	getPtr := func(i int) *string { s, _ := args[i].(*string); return s }
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
func (b *fakeChangeInsertBatch) Send() error {
	b.conn.insertedChanges = append(b.conn.insertedChanges, b.rows...)
	return nil
}
func (b *fakeChangeInsertBatch) Abort() error { return nil }

// TestReviewTeamRowsForDriftAutoApplyWritesThroughAndRecordsObservation
// proves the default policy (0, auto-apply, no team_sync_policies row at
// all) writes through unchanged AND still records an unconditional
// team_provider_observations row (project_team's own behavior, independent
// of policy).
func TestReviewTeamRowsForDriftAutoApplyWritesThroughAndRecordsObservation(t *testing.T) {
	conn := &fakeTeamDriftConn{policies: map[string]teamDriftPolicy{}}
	name := "Platform"
	teams := []teamDriftTeamView{
		{ID: "gh:team-a", Provider: "github", NativeTeamKey: "team-a", Name: &name, Members: []string{"alice"}},
	}
	keptIdx, skipped, staged, superseded, err := reviewTeamRowsForDrift(context.Background(), conn, "org-1", teams, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keptIdx) != 1 || keptIdx[0] != 0 {
		t.Fatalf("keptIdx=%v want [0]", keptIdx)
	}
	if len(skipped) != 0 || staged != 0 || superseded != 0 {
		t.Fatalf("skipped=%v staged=%d superseded=%d want all empty/zero", skipped, staged, superseded)
	}
	if len(conn.observations) != 1 || conn.observations[0].TeamID != "gh:team-a" {
		t.Fatalf("observations=%+v want exactly one for gh:team-a", conn.observations)
	}
}

// TestReviewTeamRowsForDriftFlagForReviewStagesFieldDiff proves a team
// flagged for review (sync_policy=1) is excluded from the write, and its
// changed managed field stages a PENDING team_drift_changes row.
func TestReviewTeamRowsForDriftFlagForReviewStagesFieldDiff(t *testing.T) {
	oldName := "Old Name"
	conn := &fakeTeamDriftConn{
		policies:     map[string]teamDriftPolicy{"gh:team-a": {Policy: teamDriftFlagForReviewPolicy, ManagedFields: teamDriftManagedFields}},
		existingRows: map[string]teamDriftExistingRow{"gh:team-a": {Name: &oldName}},
	}
	newName := "New Name"
	teams := []teamDriftTeamView{
		{ID: "gh:team-a", Provider: "github", NativeTeamKey: "team-a", Name: &newName},
	}
	keptIdx, skipped, staged, superseded, err := reviewTeamRowsForDrift(context.Background(), conn, "org-1", teams, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keptIdx) != 0 {
		t.Fatalf("keptIdx=%v want empty -- flagged team must not write through", keptIdx)
	}
	if len(skipped) != 1 || skipped[0] != "gh:team-a" {
		t.Fatalf("skipped=%v want [gh:team-a]", skipped)
	}
	if staged != 1 {
		t.Fatalf("staged=%d want=1", staged)
	}
	if superseded != 0 {
		t.Fatalf("superseded=%d want=0", superseded)
	}
	if len(conn.insertedChanges) != 1 {
		t.Fatalf("insertedChanges=%+v want exactly one staged row", conn.insertedChanges)
	}
	change := conn.insertedChanges[0]
	if change.Status != teamDriftStatusPending || change.EntityType != teamDriftEntityTypeTeam {
		t.Fatalf("change=%+v want status=pending entity_type=team", change)
	}
	if change.Field == nil || *change.Field != "name" {
		t.Fatalf("change.Field=%v want name", change.Field)
	}
	if change.OldValueJSON != `"Old Name"` || change.NewValueJSON != `"New Name"` {
		t.Fatalf("change old/new = %q/%q, want \"Old Name\"/\"New Name\"", change.OldValueJSON, change.NewValueJSON)
	}
}

// TestReviewTeamRowsForDriftUnchangedFieldResolvesExistingPending proves an
// existing PENDING change for a field that no longer differs from what is
// observed gets RESOLVED, not restaged.
func TestReviewTeamRowsForDriftUnchangedFieldResolvesExistingPending(t *testing.T) {
	name := "Same Name"
	nameField := "name"
	existingChangeID := changeIDForTeamField("org-1", "gh:team-a", "name", `"Same Name"`, `"Same Name"`)
	conn := &fakeTeamDriftConn{
		policies:     map[string]teamDriftPolicy{"gh:team-a": {Policy: teamDriftFlagForReviewPolicy, ManagedFields: teamDriftManagedFields}},
		existingRows: map[string]teamDriftExistingRow{"gh:team-a": {Name: &name}},
		existingChanges: []teamDriftChangeRow{
			{ChangeID: existingChangeID, EntityID: "gh:team-a", Provider: "github", Field: &nameField,
				OldValueJSON: `"Same Name"`, NewValueJSON: `"Same Name"`, Status: teamDriftStatusPending, FirstSeenAt: time.Now()},
		},
	}
	teams := []teamDriftTeamView{
		{ID: "gh:team-a", Provider: "github", NativeTeamKey: "team-a", Name: &name},
	}
	_, _, staged, superseded, err := reviewTeamRowsForDrift(context.Background(), conn, "org-1", teams, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if staged != 0 {
		t.Fatalf("staged=%d want=0 -- name did not actually change", staged)
	}
	if superseded != 0 {
		t.Fatalf("superseded=%d want=0", superseded)
	}
	if len(conn.insertedChanges) != 1 || conn.insertedChanges[0].Status != teamDriftStatusResolved {
		t.Fatalf("insertedChanges=%+v want exactly one RESOLVED status row", conn.insertedChanges)
	}
}

// TestReviewTeamRowsForDriftDifferentDiffSupersedesStalePending proves an
// existing PENDING change whose diff no longer matches the CURRENT diff
// (the field changed again since it was first staged) is SUPERSEDED, and a
// fresh PENDING row for the new diff is staged.
func TestReviewTeamRowsForDriftDifferentDiffSupersedesStalePending(t *testing.T) {
	oldPersisted := "Original"
	nameField := "name"
	staleChangeID := changeIDForTeamField("org-1", "gh:team-a", "name", `"Original"`, `"First Change"`)
	conn := &fakeTeamDriftConn{
		policies:     map[string]teamDriftPolicy{"gh:team-a": {Policy: teamDriftFlagForReviewPolicy, ManagedFields: teamDriftManagedFields}},
		existingRows: map[string]teamDriftExistingRow{"gh:team-a": {Name: &oldPersisted}},
		existingChanges: []teamDriftChangeRow{
			{ChangeID: staleChangeID, EntityID: "gh:team-a", Provider: "github", Field: &nameField,
				OldValueJSON: `"Original"`, NewValueJSON: `"First Change"`, Status: teamDriftStatusPending, FirstSeenAt: time.Now()},
		},
	}
	secondChange := "Second Change"
	teams := []teamDriftTeamView{
		{ID: "gh:team-a", Provider: "github", NativeTeamKey: "team-a", Name: &secondChange},
	}
	_, _, staged, superseded, err := reviewTeamRowsForDrift(context.Background(), conn, "org-1", teams, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if staged != 1 {
		t.Fatalf("staged=%d want=1 (fresh diff)", staged)
	}
	if superseded != 1 {
		t.Fatalf("superseded=%d want=1 (stale diff)", superseded)
	}
	var sawSuperseded, sawPending bool
	for _, change := range conn.insertedChanges {
		switch change.Status {
		case teamDriftStatusSuperseded:
			sawSuperseded = true
			if change.ChangeID != staleChangeID {
				t.Fatalf("superseded change_id=%q want=%q", change.ChangeID, staleChangeID)
			}
		case teamDriftStatusPending:
			sawPending = true
			if change.NewValueJSON != `"Second Change"` {
				t.Fatalf("pending new_value_json=%q want \"Second Change\"", change.NewValueJSON)
			}
		}
	}
	if !sawSuperseded || !sawPending {
		t.Fatalf("insertedChanges=%+v want one superseded + one pending", conn.insertedChanges)
	}
}

// TestReviewTeamRowsForDriftManualPolicyBehavesLikeFlagForReview proves
// MANUAL_POLICY (2) is treated the same as FLAG_FOR_REVIEW_POLICY (1) --
// both stage instead of write, matching Python's `policy !=
// FLAG_FOR_REVIEW_POLICY or not detect_drift: return` guard, which only
// early-returns for values OTHER than FLAG_FOR_REVIEW when detect_drift is
// on, but every non-AUTO_APPLY call from a native collector always has
// detect_drift implicitly on -- MANUAL_POLICY reaches the same diff/stage
// path as FLAG_FOR_REVIEW_POLICY here.
func TestReviewTeamRowsForDriftManualPolicyStagesFieldDiff(t *testing.T) {
	oldName := "Old Name"
	conn := &fakeTeamDriftConn{
		policies:     map[string]teamDriftPolicy{"gh:team-a": {Policy: teamDriftManualPolicy, ManagedFields: teamDriftManagedFields}},
		existingRows: map[string]teamDriftExistingRow{"gh:team-a": {Name: &oldName}},
	}
	newName := "New Name"
	teams := []teamDriftTeamView{
		{ID: "gh:team-a", Provider: "github", NativeTeamKey: "team-a", Name: &newName},
	}
	keptIdx, skipped, staged, _, err := reviewTeamRowsForDrift(context.Background(), conn, "org-1", teams, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keptIdx) != 0 || len(skipped) != 1 || staged != 1 {
		t.Fatalf("keptIdx=%v skipped=%v staged=%d, want empty/[gh:team-a]/1", keptIdx, skipped, staged)
	}
}
