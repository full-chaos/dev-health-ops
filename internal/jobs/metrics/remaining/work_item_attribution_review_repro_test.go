package remaining

import (
	"reflect"
	"testing"
	"time"
)

// This file is the unit/fake-client half of codex round r1's fix set
// (team-lead's PR-B ruling: "unit/fake-client tests now; the live-ClickHouse
// closure test waits for the container pause to lift"). It covers the PURE
// decision logic each fix depends on, with no ClickHouse connection at all.
// The SQL/write-path proof for the same fixes lives in
// work_item_attribution_closure_integration_test.go (-tags=integration),
// currently PENDING: bigboy's testcontainer/ClickHouse suite runs are
// paused (chris, 09-03 13:48) and local docker is forbidden fleet-wide.

// TestWorkItemAttributionEffectiveChangeSignal covers finding 2's
// future-activation/expiry fix in isolation from the SQL that evaluates the
// same expression server-side (see workItemAttributionEffectiveChangeSignal's
// doc comment for why a Go-side reference exists at all).
func TestWorkItemAttributionEffectiveChangeSignal(t *testing.T) {
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	day := func(n int) time.Time { return base.AddDate(0, 0, n) }
	validTo := func(n int) *time.Time { d := day(n); return &d }

	cases := []struct {
		name                 string
		updatedAt, validFrom time.Time
		validTo              *time.Time
		asOf                 time.Time
		want                 time.Time
	}{
		{
			name:      "immediate row: updated_at is the signal, valid_from not yet a future signal",
			updatedAt: day(0), validFrom: day(0), validTo: nil,
			asOf: day(0),
			want: day(0),
		},
		{
			name:      "future valid_from BEFORE it takes effect contributes nothing",
			updatedAt: day(0), validFrom: day(5), validTo: nil,
			asOf: day(2),
			want: day(0), // updated_at only -- valid_from(5) > asOf(2), excluded
		},
		{
			name:      "future valid_from AFTER it takes effect becomes the signal",
			updatedAt: day(0), validFrom: day(5), validTo: nil,
			asOf: day(6),
			want: day(5), // valid_from(5) <= asOf(6), and 5 > updated_at(0)
		},
		{
			name:      "valid_from taking effect EXACTLY at asOf counts (boundary, <=)",
			updatedAt: day(0), validFrom: day(5), validTo: nil,
			asOf: day(5),
			want: day(5),
		},
		{
			name:      "expired valid_to BEFORE it passes contributes nothing",
			updatedAt: day(0), validFrom: day(0), validTo: validTo(5),
			asOf: day(2),
			want: day(0),
		},
		{
			name:      "expired valid_to AFTER it passes becomes the signal",
			updatedAt: day(0), validFrom: day(0), validTo: validTo(5),
			asOf: day(6),
			want: day(5),
		},
		{
			name:      "valid_to expiring EXACTLY at asOf counts (boundary, <=)",
			updatedAt: day(0), validFrom: day(0), validTo: validTo(5),
			asOf: day(5),
			want: day(5),
		},
		{
			name:      "nil valid_to never contributes, regardless of asOf",
			updatedAt: day(0), validFrom: day(0), validTo: nil,
			asOf: day(100),
			want: day(0),
		},
		{
			name:      "updated_at still wins when it is the LATEST of the three",
			updatedAt: day(10), validFrom: day(1), validTo: validTo(2),
			asOf: day(20),
			want: day(10),
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := workItemAttributionEffectiveChangeSignal(
				testCase.updatedAt, testCase.validFrom, testCase.validTo, testCase.asOf)
			if !got.Equal(testCase.want) {
				t.Errorf("signal = %v, want %v", got, testCase.want)
			}
		})
	}
}

// TestWorkItemAttributionPromotionDecision covers finding 1's fix boundary:
// the SAME 25% bound evaluateClosurePromotion uses to decide promotion,
// isolated from the SQL that produces its inputs (affected count, closure
// size, org total).
func TestWorkItemAttributionPromotionDecision(t *testing.T) {
	cases := []struct {
		name                         string
		affected, closureSize, total int
		wantPromoted                 bool
	}{
		{"below bound: 2/20 = 10%", 1, 1, 20, false},
		{"at the bound exactly: 5/20 = 25%, <= never promotes", 1, 4, 20, false},
		{"just above the bound: 6/20 = 30%", 1, 5, 20, true},
		{"whole-org closure: 20/20 = 100%", 10, 10, 20, true},
		{"zero total (unknown org size) never promotes -- nothing to divide by", 1, 1, 0, false},
		{"negative total (defensive) never promotes", 1, 1, -1, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			promoted, reason := workItemAttributionPromotionDecision(
				testCase.affected, testCase.closureSize, testCase.total)
			if promoted != testCase.wantPromoted {
				t.Errorf("promoted = %v, want %v (reason=%q)", promoted, testCase.wantPromoted, reason)
			}
			if promoted && reason == "" {
				t.Error("promoted=true but reason is empty -- the run marker's promoted_reason column needs this")
			}
			if !promoted && reason != "" {
				t.Errorf("promoted=false but reason = %q, want empty", reason)
			}
		})
	}
}

// TestWorkItemAttributionObserveRunCallsTheObserver is codex round r1's P2
// fix (telemetry never emitted), proven with a fake observer -- no
// ClickHouse connection at all, since observeRun is a pure nil-check-and-
// delegate method.
func TestWorkItemAttributionObserveRunCallsTheObserver(t *testing.T) {
	observer := &fakeWorkItemAttributionObserver{}
	executor := &WorkItemAttributionExecutor{observer: observer}

	outcome := WorkItemAttributionOutcome{OrgWide: true, ItemsSeen: 3, RowsWritten: 5}
	executor.observeRun("org-1", outcome)

	if observer.calls != 1 {
		t.Fatalf("observer called %d time(s), want 1", observer.calls)
	}
	if !reflect.DeepEqual(observer.lastOutcome, outcome) {
		t.Errorf("observer recorded outcome = %+v, want %+v", observer.lastOutcome, outcome)
	}
}

// TestWorkItemAttributionObserveRunToleratesNilObserver mirrors SetObserver's
// own doc comment: a nil observer must never panic.
func TestWorkItemAttributionObserveRunToleratesNilObserver(t *testing.T) {
	executor := &WorkItemAttributionExecutor{}
	executor.observeRun("org-1", WorkItemAttributionOutcome{SkippedNoop: true})
}

// fakeWorkItemAttributionObserver is used by both the unit tests in this
// file and the integration tests in
// work_item_attribution_closure_integration_test.go (no build tag on this
// file, so it's visible to both).
type fakeWorkItemAttributionObserver struct {
	calls       int
	lastOutcome WorkItemAttributionOutcome
}

func (f *fakeWorkItemAttributionObserver) ObserveWorkItemAttributionRun(
	_ string, outcome WorkItemAttributionOutcome,
) {
	f.calls++
	f.lastOutcome = outcome
}
