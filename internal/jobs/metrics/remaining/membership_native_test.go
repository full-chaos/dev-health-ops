package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// fakeMembershipEdges is a chqueryEdgeReader returning a fixed edge set,
// ignoring the query options (this package's tests never need scope
// filtering behaviour -- that is chquery's own package to test).
type fakeMembershipEdges struct {
	rows []chquery.EdgeRow
	err  error
}

func (f fakeMembershipEdges) FetchWorkGraphEdges(
	context.Context, chquery.EdgeQueryOptions,
) ([]chquery.EdgeRow, error) {
	return f.rows, f.err
}

// fakeMembershipDistributions is a membershipDistributionFetcher returning a
// fixed map, so ComputeOrg's matched/skipped branching can be tested without
// a live ClickHouse connection.
type fakeMembershipDistributions struct {
	byUnit map[string]membershipDistribution
	err    error
}

func (f fakeMembershipDistributions) FetchLatestDistributions(
	context.Context, string, []string,
) (map[string]membershipDistribution, error) {
	return f.byUnit, f.err
}

// fakeMembershipWriter records every call, in order, so a test can assert
// both WHAT was written and the CHAOS-2433 write-then-marker sequencing.
type fakeMembershipWriter struct {
	calls               []string
	membershipRecords   []units.MembershipRecord
	runRecord           *MembershipRunRecord
	scopedRunRecords    []MembershipScopedRunRecord
	pruneCalled         bool
	pruneKeep           int
	writeMembershipsErr error
	writeRunErr         error
	writeScopedErr      error
	pruneErr            error
}

func (f *fakeMembershipWriter) WriteMemberships(
	_ context.Context, _ string, records []units.MembershipRecord,
) (int, error) {
	f.calls = append(f.calls, "WriteMemberships")
	f.membershipRecords = records
	if f.writeMembershipsErr != nil {
		return 0, f.writeMembershipsErr
	}
	return len(records), nil
}

func (f *fakeMembershipWriter) WriteMembershipRun(_ context.Context, record MembershipRunRecord) error {
	f.calls = append(f.calls, "WriteMembershipRun")
	f.runRecord = &record
	return f.writeRunErr
}

func (f *fakeMembershipWriter) WriteScopedMembershipRuns(
	_ context.Context, records []MembershipScopedRunRecord,
) error {
	f.calls = append(f.calls, "WriteScopedMembershipRuns")
	f.scopedRunRecords = records
	return f.writeScopedErr
}

func (f *fakeMembershipWriter) PruneMembershipRuns(
	_ context.Context, _ string, keep int,
) (int, error) {
	f.calls = append(f.calls, "PruneMembershipRuns")
	f.pruneCalled = true
	f.pruneKeep = keep
	return 0, f.pruneErr
}

var _ MembershipWriter = (*fakeMembershipWriter)(nil)

// fakeMembershipObserver records the outcome passed to it.
type fakeMembershipObserver struct {
	orgID   string
	outcome MembershipOutcome
	called  bool
}

func (f *fakeMembershipObserver) ObserveMembershipRun(orgID string, outcome MembershipOutcome) {
	f.called = true
	f.orgID = orgID
	f.outcome = outcome
}

func newTestMembershipExecutor(
	edges fakeMembershipEdges, distributions fakeMembershipDistributions, writer MembershipWriter,
) *MembershipExecutor {
	fixed := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	return &MembershipExecutor{
		edges:         edges,
		distributions: distributions,
		writer:        writer,
		nowUTC:        func() time.Time { return fixed },
	}
}

// twoDisjointComponentEdges returns edges whose two rows share no nodes, so
// units.BuildComponents produces exactly two components -- one node pair
// each.
func twoDisjointComponentEdges() []chquery.EdgeRow {
	return []chquery.EdgeRow{
		{Edge: units.Edge{
			EdgeID: "e1", SourceType: "pull_request", SourceID: "1",
			TargetType: "issue", TargetID: "1", Confidence: 0.9,
		}},
		{Edge: units.Edge{
			EdgeID: "e2", SourceType: "pull_request", SourceID: "2",
			TargetType: "issue", TargetID: "2", Confidence: 0.9,
		}},
	}
}

func matchedAndSkippedUnitIDs(t *testing.T) (matched, skipped string) {
	t.Helper()
	matched = units.WorkUnitID([]units.NodeKey{
		{Type: "pull_request", ID: "1"}, {Type: "issue", ID: "1"},
	})
	skipped = units.WorkUnitID([]units.NodeKey{
		{Type: "pull_request", ID: "2"}, {Type: "issue", ID: "2"},
	})
	return matched, skipped
}

func TestComputeOrgNoComponentsWritesNothing(t *testing.T) {
	writer := &fakeMembershipWriter{}
	observer := &fakeMembershipObserver{}
	executor := newTestMembershipExecutor(
		fakeMembershipEdges{rows: nil}, fakeMembershipDistributions{}, writer,
	)
	// ComputeOrg's first line refuses a nil executor.conn (ErrMembershipUnavailable);
	// this test exercises the empty-edge-set path past that guard, so it
	// needs a non-nil sentinel, never a real connection.
	executor.conn = fakeDriverConnSentinel{}
	executor.SetObserver(observer)

	outcome, err := executor.ComputeOrg(context.Background(), "org-1", nil, time.Now())
	if err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}
	if outcome != (MembershipOutcome{}) {
		t.Errorf("expected a zero outcome, got %+v", outcome)
	}
	if len(writer.calls) != 0 {
		t.Errorf("expected no writer calls, got %v", writer.calls)
	}
	if !observer.called {
		t.Error("expected the observer to be called even for a no-component run")
	}
}

func TestComputeOrgOrgWideMatchedAndSkipped(t *testing.T) {
	matchedID, _ := matchedAndSkippedUnitIDs(t)
	distribution := membershipDistribution{
		ThemeDistribution:       units.NewDistribution(units.CategoryWeight{Category: "feature_delivery", Weight: 0.8}),
		SubcategoryDistribution: units.NewDistribution(units.CategoryWeight{Category: "backend", Weight: 1.0}),
		CategorizationStatus:    "completed",
	}
	writer := &fakeMembershipWriter{}
	observer := &fakeMembershipObserver{}
	executor := newTestMembershipExecutor(
		fakeMembershipEdges{rows: twoDisjointComponentEdges()},
		fakeMembershipDistributions{byUnit: map[string]membershipDistribution{matchedID: distribution}},
		writer,
	)
	executor.conn = fakeDriverConnSentinel{}
	executor.SetObserver(observer)

	outcome, err := executor.ComputeOrg(context.Background(), "org-1", nil, time.Now())
	if err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}
	if outcome.Components != 2 {
		t.Errorf("Components = %d, want 2", outcome.Components)
	}
	if outcome.Matched != 1 || outcome.Skipped != 1 {
		t.Errorf("Matched=%d Skipped=%d, want 1/1", outcome.Matched, outcome.Skipped)
	}
	// theme (1 category) + subcategory (1 category) per node, 2 nodes in the
	// matched component -> 2 * (1 + 1) = 4 rows.
	if outcome.MembershipRows != 4 {
		t.Errorf("MembershipRows = %d, want 4", outcome.MembershipRows)
	}
	if len(writer.membershipRecords) != 4 {
		t.Fatalf("wrote %d records, want 4", len(writer.membershipRecords))
	}
	for _, record := range writer.membershipRecords {
		if record.WorkUnitID != matchedID {
			t.Errorf("record for unit %q, want only the matched unit %q", record.WorkUnitID, matchedID)
		}
		if record.OrgID != "org-1" {
			t.Errorf("record OrgID = %q, want org-1", record.OrgID)
		}
		if record.RunID == "" {
			t.Error("record has no run_id")
		}
	}

	// CHAOS-2433 protocol: rows written before the marker.
	if len(writer.calls) < 2 || writer.calls[0] != "WriteMemberships" || writer.calls[1] != "WriteMembershipRun" {
		t.Fatalf("call order = %v, want [WriteMemberships WriteMembershipRun ...]", writer.calls)
	}
	if writer.runRecord == nil || writer.runRecord.OrgID != "org-1" {
		t.Fatalf("WriteMembershipRun not called with org-1: %+v", writer.runRecord)
	}
	if !writer.pruneCalled || writer.pruneKeep != membershipRetentionKeep {
		t.Errorf("expected PruneMembershipRuns called with keep=%d, got called=%v keep=%d",
			membershipRetentionKeep, writer.pruneCalled, writer.pruneKeep)
	}
	if len(writer.scopedRunRecords) != 0 {
		t.Errorf("org-wide run must not write scoped markers, got %v", writer.scopedRunRecords)
	}
	if !observer.called || observer.outcome.Matched != 1 {
		t.Errorf("observer not called with the expected outcome: %+v", observer.outcome)
	}
}

func TestComputeOrgRepoScopedNeverPublishesOrgMarker(t *testing.T) {
	matchedID, skippedID := matchedAndSkippedUnitIDs(t)
	distribution := membershipDistribution{
		ThemeDistribution:       units.NewDistribution(units.CategoryWeight{Category: "feature_delivery", Weight: 0.8}),
		SubcategoryDistribution: units.NewDistribution(),
		CategorizationStatus:    "completed",
	}
	writer := &fakeMembershipWriter{}
	executor := newTestMembershipExecutor(
		fakeMembershipEdges{rows: twoDisjointComponentEdges()},
		fakeMembershipDistributions{byUnit: map[string]membershipDistribution{
			matchedID: distribution, skippedID: distribution,
		}},
		writer,
	)
	executor.conn = fakeDriverConnSentinel{}

	_, err := executor.ComputeOrg(context.Background(), "org-1", []string{"repo-a", "repo-a", "repo-b"}, time.Now())
	if err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}

	for _, call := range writer.calls {
		if call == "WriteMembershipRun" || call == "PruneMembershipRuns" {
			t.Errorf("a repo-scoped run must never call %s (CHAOS-2433 finding #2), calls=%v", call, writer.calls)
		}
	}
	if len(writer.scopedRunRecords) != 2 {
		t.Fatalf("scoped run records = %d, want 2 (deduped repo-a, repo-b)", len(writer.scopedRunRecords))
	}
	seenRepos := map[string]bool{}
	for _, record := range writer.scopedRunRecords {
		if record.ScopeKind != "repo" {
			t.Errorf("ScopeKind = %q, want repo", record.ScopeKind)
		}
		seenRepos[record.ScopeID] = true
	}
	if !seenRepos["repo-a"] || !seenRepos["repo-b"] {
		t.Errorf("expected repo-a and repo-b, got %v", seenRepos)
	}
}

// fakeDriverConnSentinel is a non-nil placeholder satisfying driver.Conn's
// identity for ComputeOrg's `executor.conn == nil` guard, without needing a
// real ClickHouse connection -- every method panics if actually called,
// which would mean a test exercised a code path it did not intend to.
type fakeDriverConnSentinel struct{ driverConnStub }
