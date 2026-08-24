package syncdispatchruntime

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// fakeVerifiedDiscoveryInner is a plain-unit-test-safe DiscoveryExecutor
// double for VerifiedDiscoveryExecutor's tests below -- distinct from
// fakeDiscoveryExecutor (native_reference_discovery_integration_test.go),
// which lives behind the integration build tag and is unavailable to a
// plain `go test` run of this file.
type fakeVerifiedDiscoveryInner struct {
	summary map[string]any
	err     error
}

func (executor *fakeVerifiedDiscoveryInner) Discover(context.Context, string, string) (map[string]any, error) {
	return executor.summary, executor.err
}

type fakeReadbackChecker struct {
	teamCalls, sprintCalls int
	missingTeams           [][]string
	missingSprints         [][]string
	err                    error
}

func (checker *fakeReadbackChecker) MissingTeamKeys(_ context.Context, _, _ string, expected []string) ([]string, error) {
	if len(expected) == 0 {
		return nil, nil
	}
	if checker.err != nil {
		return nil, checker.err
	}
	index := checker.teamCalls
	checker.teamCalls++
	if index < len(checker.missingTeams) {
		return checker.missingTeams[index], nil
	}
	if len(checker.missingTeams) == 0 {
		return nil, nil
	}
	return checker.missingTeams[len(checker.missingTeams)-1], nil
}

func (checker *fakeReadbackChecker) MissingSprintIDs(_ context.Context, _, _ string, expected []string) ([]string, error) {
	if len(expected) == 0 {
		return nil, nil
	}
	if checker.err != nil {
		return nil, checker.err
	}
	index := checker.sprintCalls
	checker.sprintCalls++
	if index < len(checker.missingSprints) {
		return checker.missingSprints[index], nil
	}
	if len(checker.missingSprints) == 0 {
		return nil, nil
	}
	return checker.missingSprints[len(checker.missingSprints)-1], nil
}

// TestReferenceReadbackVerifierIsANoOpWithNothingToVerify pins Python's own
// short-circuit: a summary with no claimed team/sprint keys must never call
// the checker at all -- a non-import-capable provider's no-op discovery
// must not depend on ClickHouse being reachable.
func TestReferenceReadbackVerifierIsANoOpWithNothingToVerify(t *testing.T) {
	checker := &fakeReadbackChecker{}
	verifier, err := NewReferenceReadbackVerifier(checker)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.Verify(context.Background(), testOrg, "linear", map[string]any{}); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if checker.teamCalls != 0 || checker.sprintCalls != 0 {
		t.Fatalf("checker calls: teams=%d sprints=%d want=0,0", checker.teamCalls, checker.sprintCalls)
	}
}

// TestReferenceReadbackVerifierSucceedsOnceKeysBecomeVisible pins the
// poll-until-visible behavior: the first check reports a miss, the second
// (after one simulated sleep) reports everything visible, and Verify
// returns nil rather than treating the transient miss as a hard failure.
func TestReferenceReadbackVerifierSucceedsOnceKeysBecomeVisible(t *testing.T) {
	checker := &fakeReadbackChecker{
		missingTeams: [][]string{{"ENG"}, nil},
	}
	verifier, err := NewReferenceReadbackVerifier(checker)
	if err != nil {
		t.Fatal(err)
	}
	verifier.timeout = time.Second
	var slept []time.Duration
	verifier.sleep = func(d time.Duration) { slept = append(slept, d) }

	err = verifier.Verify(context.Background(), testOrg, "linear", map[string]any{
		"reference_team_keys": []any{"ENG"},
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if checker.teamCalls != 2 {
		t.Fatalf("teamCalls=%d want=2", checker.teamCalls)
	}
	if len(slept) != 1 || slept[0] != 250*time.Millisecond {
		t.Fatalf("slept=%v want=[250ms]", slept)
	}
}

// TestReferenceReadbackVerifierFailsAfterDeadline pins the other half:
// a key that never becomes visible produces a genuine error once the
// injected clock crosses the deadline, not an infinite loop.
func TestReferenceReadbackVerifierFailsAfterDeadline(t *testing.T) {
	checker := &fakeReadbackChecker{missingTeams: [][]string{{"ENG"}}}
	verifier, err := NewReferenceReadbackVerifier(checker)
	if err != nil {
		t.Fatal(err)
	}
	verifier.timeout = time.Second
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	current := start
	verifier.now = func() time.Time { return current }
	sleeps := 0
	verifier.sleep = func(time.Duration) {
		sleeps++
		current = current.Add(600 * time.Millisecond)
		if sleeps > 10 {
			t.Fatal("Verify did not respect the deadline")
		}
	}

	err = verifier.Verify(context.Background(), testOrg, "linear", map[string]any{
		"reference_team_keys": []any{"ENG"},
	})
	if err == nil {
		t.Fatal("Verify: want an error once the deadline elapses, got nil")
	}
}

// TestReferenceReadbackVerifierPropagatesCheckerErrors pins that a genuine
// ClickHouse query failure is a hard error, not silently retried forever.
func TestReferenceReadbackVerifierPropagatesCheckerErrors(t *testing.T) {
	checkerErr := errors.New("clickhouse unavailable")
	checker := &fakeReadbackChecker{err: checkerErr}
	verifier, err := NewReferenceReadbackVerifier(checker)
	if err != nil {
		t.Fatal(err)
	}
	err = verifier.Verify(context.Background(), testOrg, "linear", map[string]any{
		"reference_team_keys": []any{"ENG"},
	})
	if !errors.Is(err, checkerErr) {
		t.Fatalf("Verify error=%v want=%v", err, checkerErr)
	}
}

func TestNewReferenceReadbackVerifierRejectsANilChecker(t *testing.T) {
	if _, err := NewReferenceReadbackVerifier(nil); !errors.Is(err, ErrReferenceDiscoveryUnavailable) {
		t.Fatalf("error=%v want=%v", err, ErrReferenceDiscoveryUnavailable)
	}
}

// TestStringsFromSummaryFieldMatchesPythonStrings pins `_strings`'s
// tolerance: absent/empty is nil, a bare string is a singleton, a mixed
// list stringifies and drops empties, and duplicates collapse.
func TestStringsFromSummaryFieldMatchesPythonStrings(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  []string
	}{
		{"nil value", nil, nil},
		{"empty string", "", nil},
		{"bare string", "ENG", []string{"ENG"}},
		{"list with duplicates and empties", []any{"ENG", "ENG", "", 42}, []string{"42", "ENG"}},
		{"not iterable", 7, nil},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := stringsFromSummaryField(testCase.value)
			if !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("stringsFromSummaryField(%#v)=%#v want=%#v", testCase.value, got, testCase.want)
			}
		})
	}
}

func TestVerifiedDiscoveryExecutorVerifiesReadbackAfterPopulating(t *testing.T) {
	inner := &fakeVerifiedDiscoveryInner{summary: map[string]any{"provider": "linear", "reference_team_keys": []any{"ENG"}}}
	checker := &fakeReadbackChecker{}
	readbackVerifier, err := NewReferenceReadbackVerifier(checker)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewVerifiedDiscoveryExecutor(inner, readbackVerifier)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := executor.Discover(context.Background(), testOrg, testRun)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if summary["provider"] != "linear" {
		t.Fatalf("summary=%#v", summary)
	}
	if checker.teamCalls != 1 {
		t.Fatalf("teamCalls=%d want=1 (readback must run after a successful populate)", checker.teamCalls)
	}
}

// TestVerifiedDiscoveryExecutorSkipsReadbackWhenPopulateFails pins ordering:
// a populate failure must never reach the readback check at all.
func TestVerifiedDiscoveryExecutorSkipsReadbackWhenPopulateFails(t *testing.T) {
	populateErr := errors.New("populate failed")
	inner := &fakeVerifiedDiscoveryInner{err: populateErr}
	checker := &fakeReadbackChecker{}
	readbackVerifier, err := NewReferenceReadbackVerifier(checker)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewVerifiedDiscoveryExecutor(inner, readbackVerifier)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Discover(context.Background(), testOrg, testRun); !errors.Is(err, populateErr) {
		t.Fatalf("Discover error=%v want=%v", err, populateErr)
	}
	if checker.teamCalls != 0 || checker.sprintCalls != 0 {
		t.Fatalf("checker was called after a populate failure: teams=%d sprints=%d", checker.teamCalls, checker.sprintCalls)
	}
}

// TestVerifiedDiscoveryExecutorRejectsASummaryWithNoProvider pins a
// defensive guard: a populate summary missing "provider" cannot be
// verified (there is nothing to scope the readback query to), so this
// must fail loudly rather than silently skip verification.
func TestVerifiedDiscoveryExecutorRejectsASummaryWithNoProvider(t *testing.T) {
	inner := &fakeVerifiedDiscoveryInner{summary: map[string]any{"status": "success"}}
	checker := &fakeReadbackChecker{}
	readbackVerifier, err := NewReferenceReadbackVerifier(checker)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewVerifiedDiscoveryExecutor(inner, readbackVerifier)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Discover(context.Background(), testOrg, testRun); !errors.Is(err, ErrReferenceDiscoveryUnavailable) {
		t.Fatalf("Discover error=%v want=%v", err, ErrReferenceDiscoveryUnavailable)
	}
}

func TestNewVerifiedDiscoveryExecutorRejectsMissingDependencies(t *testing.T) {
	checker := &fakeReadbackChecker{}
	readbackVerifier, err := NewReferenceReadbackVerifier(checker)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewVerifiedDiscoveryExecutor(nil, readbackVerifier); !errors.Is(err, ErrReferenceDiscoveryUnavailable) {
		t.Fatalf("nil inner error=%v want=%v", err, ErrReferenceDiscoveryUnavailable)
	}
	if _, err := NewVerifiedDiscoveryExecutor(&fakeVerifiedDiscoveryInner{}, nil); !errors.Is(err, ErrReferenceDiscoveryUnavailable) {
		t.Fatalf("nil verifier error=%v want=%v", err, ErrReferenceDiscoveryUnavailable)
	}
}
