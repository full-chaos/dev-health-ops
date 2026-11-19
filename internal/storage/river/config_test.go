package riverstore

import (
	"errors"
	"testing"
	"time"

	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
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
	if target.CompletedJobRetentionPeriod != 24*time.Hour {
		t.Fatalf("completed retention = %s", target.CompletedJobRetentionPeriod)
	}
	if target.CancelledJobRetentionPeriod != 24*time.Hour || target.DiscardedJobRetentionPeriod != 29*24*time.Hour {
		t.Fatalf("unexpected terminal retention: cancelled=%s discarded=%s", target.CancelledJobRetentionPeriod, target.DiscardedJobRetentionPeriod)
	}
	if target.JobCleanerTimeout != 30*time.Second {
		t.Fatalf("cleaner timeout = %s", target.JobCleanerTimeout)
	}
}

// The client config takes the platform defaults (config.Load); this keeps
// DefaultMaintenanceConfig from drifting away from them.
func TestDefaultMaintenanceConfigEqualsLoadedPlatformDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := platformconfig.Load(platformconfig.Spec{
		Service:   "dev-health-worker",
		LookupEnv: func(string) (string, bool) { return "", false },
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := DefaultMaintenanceConfig(), MaintenanceConfigFromPlatform(cfg); got != want {
		t.Fatalf("DefaultMaintenanceConfig() = %+v, config.Load defaults = %+v", got, want)
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
