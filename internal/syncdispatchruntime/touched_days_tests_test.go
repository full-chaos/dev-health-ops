package syncdispatchruntime

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// The fan-out records the days of the test suites of a run (CHAOS-14006) exactly when a successful unit of the run has a
// dataset whose writer stores test_suite_results, for every provider of the matrix: the answer is read from the dataset
// capability (the cicd or tests target), and it agrees with the destinations of the route that writes the rows.
func TestWritesTestReportRowsFollowsTheDatasetCapabilityForEveryProvider(t *testing.T) {
	writers := map[string]bool{}
	for _, provider := range providersync.MatrixProviders() {
		for _, capability := range providersync.Capabilities(provider) {
			targets := map[string]struct{}{}
			for _, target := range capability.LegacyTargets {
				targets[target] = struct{}{}
			}
			descriptor, ok := providersync.Descriptor(provider, capability.Dataset)
			if !ok {
				t.Fatalf("%s/%s: no route descriptor", provider, capability.Dataset)
			}
			want := slices.Contains(descriptor.Destinations, "test_suite_results")
			if got := writesTestReportRows(targets); got != want {
				t.Errorf("%s/%s: writesTestReportRows = %t, want %t (targets %v, destinations %v)",
					provider, capability.Dataset, got, want, capability.LegacyTargets, descriptor.Destinations)
			}
			// The touched record runs only in a plan that triggers the daily run.
			if want && !dailyRelevantTargets(targets) {
				t.Errorf("%s/%s writes test_suite_results but does not trigger the daily run (targets %v)",
					provider, capability.Dataset, capability.LegacyTargets)
			}
			if want {
				writers[provider] = true
			}
		}
	}
	for _, provider := range []string{"github", "gitlab"} {
		if !writers[provider] {
			t.Errorf("provider %s has no dataset that writes test_suite_results: the matrix does not reach it", provider)
		}
	}
	if !dailyRelevantTargets(map[string]struct{}{"tests": {}}) || !writesTestReportRows(map[string]struct{}{"tests": {}}) {
		t.Error("a tests-only run writes test_suite_results: it must trigger the daily run and the test report record")
	}
	if writesTestReportRows(map[string]struct{}{"git": {}, "prs": {}, "work-items": {}}) {
		t.Error("writesTestReportRows must be false for a mix with neither cicd nor tests")
	}
}

// recordingTouchedDaysStore records the RecordTouchedTests bounds and counts every other record call.
type recordingTouchedDaysStore struct {
	calls      int
	tests      []time.Time
	testsError error
}

func (store *recordingTouchedDaysStore) RecordTouched(context.Context, string, time.Time, []time.Time) (uint64, error) {
	store.calls++
	return 0, nil
}

func (store *recordingTouchedDaysStore) RecordTouchedGit(context.Context, string, time.Time) (uint64, error) {
	store.calls++
	return 0, nil
}

func (store *recordingTouchedDaysStore) PendingDays(context.Context, string, int) (TouchedDaysPending, error) {
	return TouchedDaysPending{}, nil
}

func (store *recordingTouchedDaysStore) PendingRepositories(context.Context, string, []time.Time, int) (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (store *recordingTouchedDaysStore) MarkDispatched(context.Context, string, time.Time, []time.Time, []TouchedDayKey) error {
	return nil
}

func (store *recordingTouchedDaysStore) RecordTouchedTests(_ context.Context, _ string, since time.Time) (uint64, error) {
	store.tests = append(store.tests, since)
	return 2, store.testsError
}

// A plan with test report rows records their days from the same bound as the other kinds, adds the keys to the take,
// and fails the take when the record fails. A plan without them never reads test_suite_results.
func TestRecordPlanTouchedDaysRecordsTheTestReportDays(t *testing.T) {
	started := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	plan := &PostSyncPlan{OrganizationID: "org", TestReports: true, RunStartedAt: started}
	store := &recordingTouchedDaysStore{}
	take := &touchedDaysTake{}
	if err := recordPlanTouchedDays(context.Background(), store, plan, take); err != nil {
		t.Fatal(err)
	}
	if want := []time.Time{started.Add(-postSyncTouchedClockMargin)}; !slices.Equal(store.tests, want) || take.recorded != 2 {
		t.Fatalf("test report record calls %v recorded %d, want %v and 2", store.tests, take.recorded, want)
	}
	if store.calls != 0 {
		t.Fatalf("a test-report-only plan called the work-item or git record %d times", store.calls)
	}

	failing := &recordingTouchedDaysStore{testsError: ErrTouchedDaysUnavailable}
	if err := recordPlanTouchedDays(context.Background(), failing, plan, &touchedDaysTake{}); !errors.Is(err, ErrTouchedDaysUnavailable) {
		t.Fatalf("a failed test report record returned %v, want ErrTouchedDaysUnavailable", err)
	}

	none := &recordingTouchedDaysStore{}
	if err := recordPlanTouchedDays(context.Background(), none, &PostSyncPlan{OrganizationID: "org", Git: true, RunStartedAt: started}, &touchedDaysTake{}); err != nil || len(none.tests) != 0 {
		t.Fatalf("a git-only plan: err %v, test report record calls %v, want none", err, none.tests)
	}
	if err := recordPlanTouchedDays(context.Background(), none, &PostSyncPlan{OrganizationID: "org", TestReports: true}, &touchedDaysTake{}); err == nil || len(none.tests) != 0 {
		t.Fatalf("a plan with no run start: err %v, calls %v, want an error and no record", err, none.tests)
	}
}
