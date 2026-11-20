package fixed

import (
	"testing"
	"time"

	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// The River discarded-job retention must stay exactly one day below the
// prune_worker_job_terminal default (platform/config DefaultDiscardedJobRetention).
// It checks defaults only, not a runtime WORKER_JOB_TERMINAL_RETENTION_DAYS override.
func TestOutboxPruneDefaultIsOneDayAboveRiverDiscardedRetention(t *testing.T) {
	producer, ok := NewRetentionProducer().(*RetentionProducer)
	if !ok {
		t.Fatal("NewRetentionProducer did not return *RetentionProducer")
	}
	spec, ok := producer.byScheduleID["prune_worker_job_terminal"]
	if !ok {
		t.Fatal("prune_worker_job_terminal spec missing")
	}
	prune := time.Duration(spec.DefaultDays) * 24 * time.Hour
	want := platformconfig.DefaultDiscardedJobRetention + 24*time.Hour
	if prune != want {
		t.Fatalf("prune_worker_job_terminal default = %s, want River discarded retention %s + 24h = %s",
			prune, platformconfig.DefaultDiscardedJobRetention, want)
	}
}
