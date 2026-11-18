package daily

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// The families a finalize of a run with no partition skips. A family joins the
// list only when it reads the partitions of the run and refuses a run with
// none; every other recognised family runs on such a run (the production
// family set is driven end to end by
// TestFinalizeOfAWholeOrganizationRunWithNoPartitionRetiresTheRowsOfItsDay in
// internal/workerservice).
func TestFinalizeFamiliesRefusingZeroPartitionsIsBenchmarkingOnly(t *testing.T) {
	if !slices.Equal(finalizeFamiliesRefusingZeroPartitions, []string{BenchmarkingFamilyName}) {
		t.Fatalf("finalizeFamiliesRefusingZeroPartitions = %v, want [%s]", finalizeFamiliesRefusingZeroPartitions, BenchmarkingFamilyName)
	}
	for _, name := range finalizeFamiliesRefusingZeroPartitions {
		if !slices.Contains(pythonRecognisedFinalizeFamilies, name) {
			t.Errorf("%s is not a recognised finalize family", name)
		}
	}
	executor, err := NewBenchmarkingFinalizeExecutor(&fakeStore{noPartitions: true}, poisonedConn{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{ID: testRunID, OrganizationID: testOrgID, TargetDay: time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)}
	if _, err := executor.ComputeFinalizeFamily(context.Background(), run); !errors.Is(err, ErrInvalidState) {
		t.Errorf("benchmarking on a run with no partition gave %v, want its refusal (ErrInvalidState)", err)
	}
}

func zeroPartitionFamilies() map[string]*stubFinalizeFamily {
	families := map[string]*stubFinalizeFamily{}
	for _, name := range pythonRecognisedFinalizeFamilies {
		families[name] = &stubFinalizeFamily{}
	}
	families[BenchmarkingFamilyName].err = errors.New("benchmarking must not run on a run with no partition")
	return families
}

func registerZeroPartitionFamilies(t *testing.T, handler *FinalizeHandler, families map[string]*stubFinalizeFamily) {
	t.Helper()
	registered := map[string]NativeFinalizeFamilyExecutor{}
	for name, family := range families {
		registered[name] = family
	}
	if err := handler.SetNativeFinalizeFamilies(registered); err != nil {
		t.Fatal(err)
	}
}

func TestFinalizeOfAWholeOrganizationRunWithNoPartitionSkipsOnlyTheRefusingFamilies(t *testing.T) {
	store := finalizeStoreWithClaim()
	store.noPartitions = true
	store.run.FullOrg = true
	handler, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	families := zeroPartitionFamilies()
	registerZeroPartitionFamilies(t, handler, families)
	if err := handler.Work(context.Background(), finalizeExecution()); err != nil {
		t.Fatalf("finalize = %v, want nil", err)
	}
	for name, family := range families {
		want := 1
		if slices.Contains(finalizeFamiliesRefusingZeroPartitions, name) {
			want = 0
		}
		if family.calls != want {
			t.Errorf("%s ran %d time(s), want %d", name, family.calls, want)
		}
	}
	if store.finalizeCompletions != 1 {
		t.Errorf("completions = %d, want 1", store.finalizeCompletions)
	}
}

func TestFinalizeOfARunWithPartitionsRunsEveryFamily(t *testing.T) {
	store := finalizeStoreWithClaim()
	store.partitionTotal, store.partitionSucceeded = 2, 2
	handler, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	families := zeroPartitionFamilies()
	families[BenchmarkingFamilyName].err = nil
	registerZeroPartitionFamilies(t, handler, families)
	if err := handler.Work(context.Background(), finalizeExecution()); err != nil {
		t.Fatalf("finalize = %v, want nil", err)
	}
	for name, family := range families {
		if family.calls != 1 {
			t.Errorf("%s ran %d time(s), want 1", name, family.calls)
		}
	}
}

func TestFinalizeOfARunOfSomeRepositoriesWithNoPartitionIsRefusedPermanently(t *testing.T) {
	store := finalizeStoreWithClaim()
	store.noPartitions = true
	handler, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	families := zeroPartitionFamilies()
	registerZeroPartitionFamilies(t, handler, families)
	err = handler.Work(context.Background(), finalizeExecution())
	if err == nil || !errors.Is(err, ErrInvalidState) || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("finalize = %v, want a permanent ErrInvalidState", err)
	}
	for name, family := range families {
		if family.calls != 0 {
			t.Errorf("%s ran %d time(s) on a refused run, want 0", name, family.calls)
		}
	}
	if store.finalizeCompletions != 0 || store.finalizeReleases != 1 || store.failedFinalizePermanently != 0 {
		t.Errorf("completions=%d releases=%d terminal=%d, want 0/1/0",
			store.finalizeCompletions, store.finalizeReleases, store.failedFinalizePermanently)
	}
}
