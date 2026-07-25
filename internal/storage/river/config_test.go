package riverstore

import (
	"errors"
	"testing"
	"time"

	"github.com/riverqueue/river"
)

func TestMaintenanceDefaultsAreBoundedAndApplied(t *testing.T) {
	t.Parallel()

	maintenance := DefaultMaintenanceConfig()
	if err := maintenance.Validate(); err != nil {
		t.Fatal(err)
	}
	target := &river.Config{}
	if err := ApplyMaintenance(target, maintenance); err != nil {
		t.Fatal(err)
	}
	if target.CompletedJobRetentionPeriod != 7*24*time.Hour {
		t.Fatalf("completed retention = %s", target.CompletedJobRetentionPeriod)
	}
	if target.CancelledJobRetentionPeriod != 30*24*time.Hour || target.DiscardedJobRetentionPeriod != 30*24*time.Hour {
		t.Fatalf("unexpected long retention: cancelled=%s discarded=%s", target.CancelledJobRetentionPeriod, target.DiscardedJobRetentionPeriod)
	}
	if target.JobCleanerTimeout != 30*time.Second {
		t.Fatalf("cleaner timeout = %s", target.JobCleanerTimeout)
	}
}

func TestMaintenanceRejectsUnboundedValues(t *testing.T) {
	t.Parallel()

	for _, candidate := range []MaintenanceConfig{
		{},
		{CompletedJobRetention: 23 * time.Hour, CancelledJobRetention: 24 * time.Hour, DiscardedJobRetention: 24 * time.Hour, JobCleanerTimeout: 30 * time.Second},
		{CompletedJobRetention: 24 * time.Hour, CancelledJobRetention: 366 * 24 * time.Hour, DiscardedJobRetention: 24 * time.Hour, JobCleanerTimeout: 30 * time.Second},
		{CompletedJobRetention: 24 * time.Hour, CancelledJobRetention: 24 * time.Hour, DiscardedJobRetention: 24 * time.Hour, JobCleanerTimeout: 4 * time.Second},
	} {
		if err := candidate.Validate(); !errors.Is(err, ErrInvalidMaintenanceConfig) {
			t.Fatalf("Validate() error = %v", err)
		}
	}
	if err := ApplyMaintenance(nil, DefaultMaintenanceConfig()); !errors.Is(err, ErrInvalidMaintenanceConfig) {
		t.Fatalf("ApplyMaintenance(nil) error = %v", err)
	}
}
