package jobrescue

import (
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

func TestRegisterMissingWorkersCoversEveryRuntimeKind(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "contracts", "jobs", "v1"))
	registry, err := jobruntime.Load(root)
	if err != nil {
		t.Fatal(err)
	}

	coverage, err := RegisterMissingWorkers(river.NewWorkers(), registry, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]string, 0, len(registry.Descriptors())+4)
	for _, descriptor := range registry.Descriptors() {
		want = append(want, descriptor.Kind)
	}
	want = append(want,
		syncdispatchcontract.KindDispatchSyncRun,
		syncdispatchcontract.KindFinalizeSyncRun,
		syncdispatchcontract.KindPostSync,
		syncdispatchcontract.KindReferenceDiscovery,
	)
	sort.Strings(want)
	if !reflect.DeepEqual(coverage, want) {
		t.Fatalf("rescue coverage = %v, want %v", coverage, want)
	}
}

func TestRescueOnlyWorkerUsesCheckedInRetryPolicy(t *testing.T) {
	descriptor := jobruntime.Descriptor{
		Kind:        "test",
		Timeout:     3 * time.Minute,
		RetryPolicy: "bounded_exponential_jitter",
	}
	attempted := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	row := &rivertype.JobRow{ID: 42, Attempt: 2, AttemptedAt: &attempted}
	worker := rescueOnlyWorker[jobruntime.RetentionCleanupArgs]{descriptor: descriptor}
	got := worker.NextRetry(&river.Job[jobruntime.RetentionCleanupArgs]{JobRow: row})
	want := jobruntime.NextRetryAt(descriptor, row)
	if !got.Equal(want) || got.IsZero() {
		t.Fatalf("rescue retry = %v, want checked-in %v", got, want)
	}
	if worker.Timeout(nil) != descriptor.Timeout {
		t.Fatalf("rescue timeout = %v, want %v", worker.Timeout(nil), descriptor.Timeout)
	}
}
