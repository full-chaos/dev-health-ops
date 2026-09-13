package daily

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// The share must hand out a stored record only for the exact key that
// produced it, and must never store a failure: a family whose read failed
// has to leave the next family free to make its own attempt.
func TestTestopsTestMetricShareStoresOnlySuccessPerKey(t *testing.T) {
	ctx := withTestopsTestMetricShare(context.Background())
	share, _ := ctx.Value(testopsTestMetricShareContextKey{}).(*testopsTestMetricShare)
	if share == nil {
		t.Fatal("withTestopsTestMetricShare did not attach a share")
	}
	key := testopsTestMetricKey{orgID: "org", repoID: uuid.MustParse("00000000-0000-4000-8000-0000000000e5"), start: 1, end: 2}
	otherRepo := key
	otherRepo.repoID = uuid.MustParse("00000000-0000-4000-8000-0000000000e6")
	otherWindow := key
	otherWindow.historyStart = -1

	calls := 0
	readFails := errors.New("read failed")
	failing := func() ([]testops.TestMetric, error) { calls++; return nil, readFails }
	succeeding := func() ([]testops.TestMetric, error) {
		calls++
		return []testops.TestMetric{{TotalCases: calls}}, nil
	}

	if _, err := share.recordFor(key, failing); !errors.Is(err, readFails) {
		t.Fatalf("first family's failure = %v, want it returned unchanged", err)
	}
	record, err := share.recordFor(key, succeeding)
	if err != nil || calls != 2 || len(record) != 1 || record[0].TotalCases != 2 {
		t.Fatalf("after a failure the next caller must compute: record=%+v err=%v calls=%d", record, err, calls)
	}
	record, err = share.recordFor(key, failing)
	if err != nil || calls != 2 || record[0].TotalCases != 2 {
		t.Fatalf("a stored success must be reused without computing: record=%+v err=%v calls=%d", record, err, calls)
	}
	for _, other := range []testopsTestMetricKey{otherRepo, otherWindow} {
		before := calls
		if _, err := share.recordFor(other, succeeding); err != nil || calls != before+1 {
			t.Fatalf("key %+v reused another key's record: err=%v calls=%d", other, err, calls)
		}
	}

	var absent *testopsTestMetricShare
	before := calls
	for range 2 {
		if _, err := absent.recordFor(key, succeeding); err != nil {
			t.Fatalf("without a share: %v", err)
		}
	}
	if calls != before+2 {
		t.Fatalf("without a share every call must compute: %d computations for 2 lookups", calls-before)
	}
}
