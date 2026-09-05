package daily

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiimpact"
)

func TestNewAIImpactExecutorRejectsNilConn(t *testing.T) {
	if _, err := NewAIImpactExecutor(nil); !errors.Is(err, errAIImpactUnavailable) {
		t.Fatalf("err=%v, want errAIImpactUnavailable", err)
	}
}

func TestAIImpactComputeFamilyRejectsIncompleteRun(t *testing.T) {
	executor := &AIImpactExecutor{conn: &governanceQueryRecorder{}, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	for name, run := range map[string]Run{
		"no organization": {TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)},
		"no target day":   {OrganizationID: "org"},
	} {
		if _, err := executor.ComputeFamily(context.Background(), run, Partition{ID: "p1"}); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("%s: err=%v, want ErrInvalidState", name, err)
		}
	}
}

func TestAIImpactComputeFamilyRejectsMalformedRepoIDs(t *testing.T) {
	executor := &AIImpactExecutor{conn: &governanceQueryRecorder{}, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	run := Run{OrganizationID: "org", TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}
	partition := Partition{ID: "p1", RepoIDs: []RepositoryID{RepositoryID("not-a-uuid")}}
	if _, err := executor.ComputeFamily(context.Background(), run, partition); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}

// TestAIImpactComputeFamilyShortCircuitsOnAnEmptyRepoScope is the deliberate
// CONTRAST with ai_governance's TestAIGovernanceComputeFamilyStillRunsWithNoRepoIDs.
//
// ai_impact IS repo-scoped -- Python's compute takes rows already scoped to the
// partition's repos and groups its output by (team_id, repo_id, work_type) --
// so an empty repo scope genuinely means nothing to compute, and the standard
// guard applies. ai_governance is org-scoped and must NOT short-circuit. The
// two executors sit in the same package and look alike; this pair of tests is
// what keeps someone from "harmonising" them in either direction.
func TestAIImpactComputeFamilyShortCircuitsOnAnEmptyRepoScope(t *testing.T) {
	conn := &governanceQueryRecorder{}
	executor := &AIImpactExecutor{conn: conn, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	run := Run{OrganizationID: "org-42", TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}

	written, err := executor.ComputeFamily(context.Background(), run, Partition{ID: "p1"})
	if err != nil {
		t.Fatalf("ComputeFamily: %v", err)
	}
	if written != 0 {
		t.Fatalf("wrote %d rows for an empty repo scope, want 0", written)
	}
	if len(conn.calls) != 0 {
		t.Fatalf("issued %d queries for an empty repo scope, want 0 -- this family is repo-scoped "+
			"and must short-circuit, unlike ai_governance", len(conn.calls))
	}
}

// TestAIImpactComputeFamilyStopsWhenNoPullRequestsAreInWindow pins the early
// return after the first load. Beyond saving queries, it keeps the linkage
// reader from being called with an EMPTY pr_numbers list, which would be an
// `IN ()` against work_graph_pr_commit.
func TestAIImpactComputeFamilyStopsWhenNoPullRequestsAreInWindow(t *testing.T) {
	conn := &governanceQueryRecorder{}
	executor := &AIImpactExecutor{conn: conn, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	run := Run{OrganizationID: "org-42", TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}
	partition := Partition{ID: "p1", RepoIDs: []RepositoryID{RepositoryID(uuid.NewString())}}

	written, err := executor.ComputeFamily(context.Background(), run, partition)
	if err != nil {
		t.Fatalf("ComputeFamily: %v", err)
	}
	if written != 0 {
		t.Fatalf("wrote %d rows with no in-window PRs, want 0", written)
	}
	if len(conn.calls) != 1 {
		t.Fatalf("issued %d queries, want exactly 1 -- the executor must stop after the "+
			"pull-request load returns nothing", len(conn.calls))
	}
}

// TestWriteAIImpactMetricsAppendsPythonColumnOrder pins the write contract
// against sinks/clickhouse/ai_impact.py's _COLUMNS, including the two things
// most easily got wrong silently.
func TestWriteAIImpactMetricsAppendsPythonColumnOrder(t *testing.T) {
	batch := &recordingBatch{}
	conn := &recordingBatchConn{batch: batch}
	repoID := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	day := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 4, 1, 2, 3, 0, time.UTC)
	ratio := 0.25

	written, err := WriteAIImpactMetrics(context.Background(), conn, []aiimpact.Record{{
		OrgID: "org-42", TeamID: nil, RepoID: repoID, WorkType: "pull_request",
		Day: day, AttributionBucket: aiimpact.BucketAIAssisted,
		PRsTotal: 3, PRsMerged: 2, AIAssistedPRs: 3,
		AIAssistedPRRatio: &ratio, LeveragePRsComponent: 5,
	}}, computedAt)
	if err != nil {
		t.Fatalf("WriteAIImpactMetrics: %v", err)
	}
	if written != 1 || len(batch.appended) != 1 {
		t.Fatalf("written=%d appended=%d, want 1/1", written, len(batch.appended))
	}
	row := batch.appended[0]
	if len(row) != 37 {
		t.Fatalf("appended %d columns, want 37 (the _COLUMNS list plus computed_at)", len(row))
	}
	// team_id is a NON-NULLABLE String with an empty-string sentinel, so a nil
	// TeamID must be written as "" -- NOT as a nil pointer, which the driver
	// would reject, and not skipped. Mirrors `row.team_id or ""` (:67).
	if row[1] != "" {
		t.Fatalf("team_id column = %#v, want the empty string for a nil TeamID", row[1])
	}
	if row[0] != "org-42" || row[2] != repoID || row[3] != "pull_request" || row[4] != day {
		t.Fatalf("leading columns wrong: %#v", row[0:5])
	}
	// attribution_bucket is written as a plain string, not the typed constant.
	if row[5] != "ai_assisted" {
		t.Fatalf("attribution_bucket = %#v, want the bare string \"ai_assisted\"", row[5])
	}
	if row[len(row)-1] != computedAt {
		t.Fatalf("last column = %#v, want computed_at", row[len(row)-1])
	}
	if !batch.sent {
		t.Fatal("batch was never sent")
	}
}

func TestWriteAIImpactMetricsIsANoOpOnEmptyInput(t *testing.T) {
	// A panicking conn: reaching PrepareBatch at all would be the defect.
	if written, err := WriteAIImpactMetrics(context.Background(), &panicBatchConn{}, nil, time.Now()); err != nil || written != 0 {
		t.Fatalf("WriteAIImpactMetrics(nil) = %d, %v; want 0, nil", written, err)
	}
}

// TestAIImpactLoadersRefuseAnInvertedWindow guards the precondition on all
// three windowed readers at once. An inverted window is a caller bug, and
// returning rows for it would silently compute a day nobody asked for.
func TestAIImpactLoadersRefuseAnInvertedWindow(t *testing.T) {
	conn := &governanceQueryRecorder{}
	repoIDs := []uuid.UUID{uuid.New()}
	later := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	earlier := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)

	if _, err := LoadAIImpactPullRequests(context.Background(), conn, "org", repoIDs, later, earlier); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("pull requests: err=%v, want ErrInvalidState", err)
	}
	if _, err := LoadAIImpactReviews(context.Background(), conn, "org", repoIDs, later, earlier); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("reviews: err=%v, want ErrInvalidState", err)
	}
	if _, err := LoadAIImpactAttributions(context.Background(), conn, "org", repoIDs, later, earlier); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("attributions: err=%v, want ErrInvalidState", err)
	}
	if len(conn.calls) != 0 {
		t.Fatalf("issued %d queries for an inverted window, want 0", len(conn.calls))
	}
}

// TestPRCommitLinkageReturnsAvailableButEmptyNotNil is the CHAOS-2183
// distinction at the loader boundary, and it is the one assertion here that a
// reader is most likely to get backwards.
//
// With no PR numbers to look up, the linkage is AVAILABLE and contains
// nothing -- a non-nil empty map, exactly as Python's `pr_commit_stats = {}`.
// Returning nil would mean UNAVAILABLE, which makes has_test_change unknown
// for every PR and test_gap_rate null. Both are "no data", and only the
// nil-ness distinguishes them.
func TestPRCommitLinkageReturnsAvailableButEmptyNotNil(t *testing.T) {
	conn := &governanceQueryRecorder{}
	linkage, err := LoadAIImpactPRCommitLinkage(
		context.Background(), conn, "org", []uuid.UUID{uuid.New()}, nil,
	)
	if err != nil {
		t.Fatalf("LoadAIImpactPRCommitLinkage: %v", err)
	}
	if linkage == nil {
		t.Fatal("returned nil for an empty pr_numbers list; nil means the linkage was UNAVAILABLE " +
			"(test_gap_rate null), but it was available and simply had nothing to find")
	}
	if len(linkage) != 0 {
		t.Fatalf("returned %d entries for an empty pr_numbers list", len(linkage))
	}
	if len(conn.calls) != 0 {
		t.Fatalf("issued %d queries with no pr_numbers, want 0", len(conn.calls))
	}
}
