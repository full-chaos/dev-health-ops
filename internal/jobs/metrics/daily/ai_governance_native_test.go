package daily

import (
	"context"
	"errors"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aigovernance"
)

// governanceQueryRecorder embeds the package's panicking stubDriverConn and
// overrides ONLY Query, so any method this executor is not expected to reach
// still panics loudly rather than returning a silently-zero value.
type governanceQueryRecorder struct {
	stubDriverConn
	calls [][]any
	rows  chdriver.Rows
}

func (conn *governanceQueryRecorder) Query(_ context.Context, _ string, args ...any) (chdriver.Rows, error) {
	conn.calls = append(conn.calls, args)
	if conn.rows != nil {
		return conn.rows, nil
	}
	return &emptyGovernanceRows{}, nil
}

type emptyGovernanceRows struct{ chdriver.Rows }

func (*emptyGovernanceRows) Next() bool        { return false }
func (*emptyGovernanceRows) Scan(...any) error { return errors.New("no rows") }
func (*emptyGovernanceRows) Err() error        { return nil }
func (*emptyGovernanceRows) Close() error      { return nil }

func TestNewAIGovernanceExecutorRejectsNilConn(t *testing.T) {
	if _, err := NewAIGovernanceExecutor(nil); !errors.Is(err, errAIGovernanceUnavailable) {
		t.Fatalf("err=%v, want errAIGovernanceUnavailable", err)
	}
}

func TestAIGovernanceComputeFamilyRejectsIncompleteRun(t *testing.T) {
	executor := &AIGovernanceExecutor{conn: &governanceQueryRecorder{}, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	for name, run := range map[string]Run{
		"no organization": {TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)},
		"no target day":   {OrganizationID: "org"},
	} {
		if _, err := executor.ComputeFamily(context.Background(), run, Partition{ID: "p1"}); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("%s: err=%v, want ErrInvalidState", name, err)
		}
	}
}

func TestAIGovernanceComputeFamilyRejectsMalformedRepoIDs(t *testing.T) {
	executor := &AIGovernanceExecutor{conn: &governanceQueryRecorder{}, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	run := Run{OrganizationID: "org", TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}
	partition := Partition{ID: "p1", RepoIDs: []RepositoryID{RepositoryID("not-a-uuid")}}
	if _, err := executor.ComputeFamily(context.Background(), run, partition); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState -- a corrupt recorded scope is still a precondition failure "+
			"even though this family does not USE the scope", err)
	}
}

// TestAIGovernanceComputeFamilyStillRunsWithNoRepoIDs is the single most
// important assertion in this file, because it pins the ONE way this executor
// deliberately differs from every other native daily family.
//
// All eight repo-scoped executors do `if len(repoIDs) == 0 { return 0, nil }`.
// ai_governance MUST NOT: it is org-scoped (build_governance_rows_for_day takes
// only org_id and day), so an empty repo scope is not "nothing to compute" --
// the org's artifacts still exist and Python would still write their rows.
// Copying the repo-scoped guard here would silently produce zero governance
// rows for such a partition, which is precisely the shape of the CHAOS-4269
// class of bug: a family that quietly computes nothing, forever, and looks
// healthy while doing it.
func TestAIGovernanceComputeFamilyStillRunsWithNoRepoIDs(t *testing.T) {
	conn := &governanceQueryRecorder{}
	executor := &AIGovernanceExecutor{conn: conn, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	run := Run{OrganizationID: "org-42", TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}

	written, err := executor.ComputeFamily(context.Background(), run, Partition{ID: "p1"})
	if err != nil {
		t.Fatalf("ComputeFamily: %v", err)
	}
	if written != 0 {
		t.Fatalf("wrote %d rows from an empty artifact set, want 0", written)
	}
	if len(conn.calls) != 1 {
		t.Fatalf("issued %d queries, want exactly 1 -- an empty repo scope must NOT short-circuit this family", len(conn.calls))
	}
}

// TestAIGovernanceWindowIsInclusiveOfTheLastMicrosecond pins the window bound.
// Python uses datetime.combine(day, time.max) with `observed_at <= end`
// (loaders.py:41-42, :297) -- an INCLUSIVE 23:59:59.999999, not an exclusive
// next-midnight. A port that "tidied" this into a half-open window would be a
// silent behaviour change at the boundary, so the operator and the value are
// both asserted here rather than left to the SQL text.
func TestAIGovernanceWindowIsInclusiveOfTheLastMicrosecond(t *testing.T) {
	conn := &governanceQueryRecorder{}
	executor := &AIGovernanceExecutor{conn: conn, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	run := Run{OrganizationID: "org-42", TargetDay: time.Date(2026, 9, 3, 7, 31, 0, 0, time.UTC)}

	if _, err := executor.ComputeFamily(context.Background(), run, Partition{ID: "p1"}); err != nil {
		t.Fatalf("ComputeFamily: %v", err)
	}
	args := conn.calls[0]
	// LoadGovernanceArtifacts binds: org x4, then windowStart, windowEndInclusive.
	start, ok := args[len(args)-2].(time.Time)
	if !ok {
		t.Fatalf("window start arg is %T, want time.Time", args[len(args)-2])
	}
	end, ok := args[len(args)-1].(time.Time)
	if !ok {
		t.Fatalf("window end arg is %T, want time.Time", args[len(args)-1])
	}
	wantStart := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	if !start.Equal(wantStart) {
		t.Fatalf("window start %s, want %s (the target day truncated to UTC midnight)", start, wantStart)
	}
	wantEnd := time.Date(2026, 9, 3, 23, 59, 59, 999000000, time.UTC).Add(999 * time.Microsecond)
	if !end.Equal(wantEnd) {
		t.Fatalf("window end %s, want %s (time.max, inclusive)", end, wantEnd)
	}
	if !end.Before(wantStart.AddDate(0, 0, 1)) {
		t.Fatal("window end reached the next midnight -- the bound must stay inclusive-of-last-microsecond")
	}
}

func TestWriteAIPolicyEventsAppendsPythonColumnOrder(t *testing.T) {
	batch := &recordingBatch{}
	conn := &recordingBatchConn{batch: batch}
	repoID := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	teamID := "team-alpha"
	eventID := uuid.MustParse("11111111-2222-4333-8444-555555555555")
	observedAt := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 4, 1, 2, 3, 0, time.UTC)

	written, err := WriteAIPolicyEvents(context.Background(), conn, []aigovernance.Violation{{
		EventID: eventID, OrgID: "org-42", TeamID: &teamID, RepoID: &repoID,
		RuleID: aigovernance.RuleMissingHumanReview, Severity: aigovernance.SeverityHigh,
		SubjectType: "pull_request", SubjectID: "7", ObservedAt: observedAt,
		Evidence: map[string]any{"subject_id": "7"},
	}}, computedAt)
	if err != nil {
		t.Fatalf("WriteAIPolicyEvents: %v", err)
	}
	if written != 1 || len(batch.appended) != 1 {
		t.Fatalf("written=%d appended=%d, want 1/1", written, len(batch.appended))
	}
	row := batch.appended[0]
	// Order is POLICY_EVENT_COLUMNS (sinks/clickhouse/ai_governance.py:27).
	want := []any{
		eventID, "org-42", &teamID, &repoID,
		"MISSING_HUMAN_REVIEW", "high", "pull_request", "7",
		observedAt, `{"subject_id": "7"}`, computedAt,
	}
	if len(row) != len(want) {
		t.Fatalf("appended %d columns, want %d", len(row), len(want))
	}
	for index := range want {
		if index == 2 || index == 3 {
			continue // pointer columns compared below
		}
		if row[index] != want[index] {
			t.Fatalf("column %d = %#v, want %#v", index, row[index], want[index])
		}
	}
	if got := row[2].(*string); got == nil || *got != teamID {
		t.Fatalf("team_id column = %v, want %q", got, teamID)
	}
	if got := row[3].(*uuid.UUID); got == nil || *got != repoID {
		t.Fatalf("repo_id column = %v, want %s", got, repoID)
	}
	if !batch.sent {
		t.Fatal("batch was never sent")
	}
}

func TestWriteAIGovernanceCoverageDailyAppendsPythonColumnOrder(t *testing.T) {
	batch := &recordingBatch{}
	conn := &recordingBatchConn{batch: batch}
	day := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 4, 1, 2, 3, 0, time.UTC)

	written, err := WriteAIGovernanceCoverageDaily(context.Background(), conn, []aigovernance.CoverageDaily{{
		OrgID: "org-42", TeamID: nil, RepoID: nil, Day: day,
		AIArtifacts: 12, DeclaredArtifacts: 10, HumanReviewedPRs: 9,
		SecurityScannedPRs: 8, InPolicyArtifacts: 5,
	}}, computedAt)
	if err != nil {
		t.Fatalf("WriteAIGovernanceCoverageDaily: %v", err)
	}
	if written != 1 {
		t.Fatalf("written=%d, want 1", written)
	}
	row := batch.appended[0]
	// Order is COVERAGE_COLUMNS (sinks/clickhouse/ai_governance.py:50). The
	// five counters are uint64 to match the columns' UInt64 declaration --
	// asserted by type, since a uint32 would still "look" correct in a diff.
	want := []any{"org-42", (*string)(nil), (*uuid.UUID)(nil), day,
		uint64(12), uint64(10), uint64(9), uint64(8), uint64(5), computedAt}
	if len(row) != len(want) {
		t.Fatalf("appended %d columns, want %d", len(row), len(want))
	}
	for index := range want {
		if row[index] != want[index] {
			t.Fatalf("column %d = %#v (%T), want %#v (%T)", index, row[index], row[index], want[index], want[index])
		}
	}
}

func TestGovernanceWritersAreNoOpsOnEmptyInput(t *testing.T) {
	// A panicking conn: reaching PrepareBatch at all would be the defect.
	conn := &panicBatchConn{}
	if written, err := WriteAIPolicyEvents(context.Background(), conn, nil, time.Now()); err != nil || written != 0 {
		t.Fatalf("WriteAIPolicyEvents(nil) = %d, %v; want 0, nil", written, err)
	}
	if written, err := WriteAIGovernanceCoverageDaily(context.Background(), conn, nil, time.Now()); err != nil || written != 0 {
		t.Fatalf("WriteAIGovernanceCoverageDaily(nil) = %d, %v; want 0, nil", written, err)
	}
}
