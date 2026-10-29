package syncdispatchruntime

import (
	"testing"

	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

func TestOccurrenceOfBackfillTask(t *testing.T) {
	for _, tc := range []struct{ task, want string }{
		{"occurrence:sha256:abc", "sha256:abc"},
		{"idem:0123456789abcdef;occurrence:sha256:abc", "sha256:abc"},
		{"occurrence:sha256:abc;extra", "sha256:abc"},
		{"sync_run:00000000-0000-4000-8000-000000000001", ""},
		{"", ""},
	} {
		if got := occurrenceOfBackfillTask(tc.task); got != tc.want {
			t.Errorf("occurrenceOfBackfillTask(%q) = %q, want %q", tc.task, got, tc.want)
		}
	}
}

// The finalizer matches a manual backfill job to its run by the materializer's deterministic run id, so the two must
// stay one function: a different derivation would leave every manual job pending again.
func TestDeterministicSyncRunIDIsStableAndPerOccurrence(t *testing.T) {
	first, err := schedulersync.DeterministicSyncRunID("sha256:abc")
	if err != nil {
		t.Fatal(err)
	}
	again, _ := schedulersync.DeterministicSyncRunID("sha256:abc")
	other, _ := schedulersync.DeterministicSyncRunID("sha256:abd")
	if first == "" || first != again || first == other {
		t.Fatalf("run ids: %q, %q, %q", first, again, other)
	}
	if _, err := schedulersync.DeterministicSyncRunID(""); err == nil {
		t.Fatal("an empty occurrence id has no run id")
	}
}
