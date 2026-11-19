package workerservice

import (
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/riverqueue/river"
)

var riverRetentionCases = []struct {
	name                            string
	env                             map[string]string
	completed, cancelled, discarded time.Duration
	cleanerTimeout                  time.Duration
}{
	{
		// River's own defaults, so wiring the config changes no behavior.
		name:      "defaults",
		env:       map[string]string{},
		completed: 24 * time.Hour, cancelled: 24 * time.Hour, discarded: 168 * time.Hour,
		cleanerTimeout: 30 * time.Second,
	},
	{
		name: "operator values",
		env: map[string]string{
			"RIVER_COMPLETED_JOB_RETENTION": "49h",
			"RIVER_CANCELLED_JOB_RETENTION": "100h",
			"RIVER_DISCARDED_JOB_RETENTION": "200h",
			"RIVER_JOB_CLEANER_TIMEOUT":     "45s",
		},
		completed: 49 * time.Hour, cancelled: 100 * time.Hour, discarded: 200 * time.Hour,
		cleanerTimeout: 45 * time.Second,
	},
}

func loadRiverRetentionConfig(t *testing.T, env map[string]string) config.Config {
	t.Helper()
	cfg, err := config.Load(config.Spec{
		Service: "dev-health-worker",
		LookupEnv: func(key string) (string, bool) {
			value, ok := env[key]
			return value, ok
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg.WorkerInstanceID = "w"
	return cfg
}

func shippedRiverClientConfig(cfg config.Config) *river.Config {
	return riverWorkerClientConfig(
		cfg, map[string]river.QueueConfig{"heartbeat": {MaxWorkers: 1}},
		river.NewWorkers(), slog.Default(), time.Minute)
}

func TestRiverWorkerClientConfigCarriesTheConfiguredRetention(t *testing.T) {
	for _, tc := range riverRetentionCases {
		t.Run(tc.name, func(t *testing.T) {
			clientConfig := shippedRiverClientConfig(loadRiverRetentionConfig(t, tc.env))
			if clientConfig.CompletedJobRetentionPeriod != tc.completed {
				t.Errorf("CompletedJobRetentionPeriod = %s, want %s", clientConfig.CompletedJobRetentionPeriod, tc.completed)
			}
			if clientConfig.CancelledJobRetentionPeriod != tc.cancelled {
				t.Errorf("CancelledJobRetentionPeriod = %s, want %s", clientConfig.CancelledJobRetentionPeriod, tc.cancelled)
			}
			if clientConfig.DiscardedJobRetentionPeriod != tc.discarded {
				t.Errorf("DiscardedJobRetentionPeriod = %s, want %s", clientConfig.DiscardedJobRetentionPeriod, tc.discarded)
			}
			if clientConfig.JobCleanerTimeout != tc.cleanerTimeout {
				t.Errorf("JobCleanerTimeout = %s, want %s", clientConfig.JobCleanerTimeout, tc.cleanerTimeout)
			}
		})
	}
}

func TestRiverWorkerClientConfigRetentionMatchesApplyMaintenance(t *testing.T) {
	for _, tc := range riverRetentionCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadRiverRetentionConfig(t, tc.env)
			shipped := shippedRiverClientConfig(cfg)
			applied := &river.Config{}
			if err := riverstore.ApplyMaintenance(applied, riverstore.MaintenanceConfigFromPlatform(cfg)); err != nil {
				t.Fatal(err)
			}
			if shipped.CompletedJobRetentionPeriod != applied.CompletedJobRetentionPeriod ||
				shipped.CancelledJobRetentionPeriod != applied.CancelledJobRetentionPeriod ||
				shipped.DiscardedJobRetentionPeriod != applied.DiscardedJobRetentionPeriod ||
				shipped.JobCleanerTimeout != applied.JobCleanerTimeout {
				t.Fatalf("shipped %s/%s/%s/%s != ApplyMaintenance %s/%s/%s/%s",
					shipped.CompletedJobRetentionPeriod, shipped.CancelledJobRetentionPeriod,
					shipped.DiscardedJobRetentionPeriod, shipped.JobCleanerTimeout,
					applied.CompletedJobRetentionPeriod, applied.CancelledJobRetentionPeriod,
					applied.DiscardedJobRetentionPeriod, applied.JobCleanerTimeout)
			}
		})
	}
}
