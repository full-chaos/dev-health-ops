// Package riverstore owns the River database boundary. It deliberately keeps
// schema migration APIs separate from long-running client configuration.
package riverstore

import (
	"errors"
	"fmt"
	"time"

	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/riverqueue/river"
)

var ErrInvalidMaintenanceConfig = errors.New("invalid River maintenance configuration")

type MaintenanceConfig struct {
	CompletedJobRetention time.Duration
	CancelledJobRetention time.Duration
	DiscardedJobRetention time.Duration
	JobCleanerTimeout     time.Duration
}

func MaintenanceConfigFromPlatform(configValue platformconfig.Config) MaintenanceConfig {
	return MaintenanceConfig{
		CompletedJobRetention: configValue.CompletedJobRetention,
		CancelledJobRetention: configValue.CancelledJobRetention,
		DiscardedJobRetention: configValue.DiscardedJobRetention,
		JobCleanerTimeout:     configValue.RiverJobCleanerTimeout,
	}
}

func DefaultMaintenanceConfig() MaintenanceConfig {
	return MaintenanceConfig{
		CompletedJobRetention: 7 * 24 * time.Hour,
		CancelledJobRetention: 30 * 24 * time.Hour,
		DiscardedJobRetention: 30 * 24 * time.Hour,
		JobCleanerTimeout:     30 * time.Second,
	}
}

func (c MaintenanceConfig) Validate() error {
	for _, retention := range []time.Duration{
		c.CompletedJobRetention,
		c.CancelledJobRetention,
		c.DiscardedJobRetention,
	} {
		if retention < 24*time.Hour || retention > 365*24*time.Hour {
			return ErrInvalidMaintenanceConfig
		}
	}
	if c.JobCleanerTimeout < 5*time.Second || c.JobCleanerTimeout > 5*time.Minute {
		return ErrInvalidMaintenanceConfig
	}
	return nil
}

// ApplyMaintenance copies the bounded policy into a River client config. The
// caller remains responsible for workers, queues, middleware, and startup.
func ApplyMaintenance(target *river.Config, maintenance MaintenanceConfig) error {
	if target == nil {
		return ErrInvalidMaintenanceConfig
	}
	if err := maintenance.Validate(); err != nil {
		return err
	}
	target.CompletedJobRetentionPeriod = maintenance.CompletedJobRetention
	target.CancelledJobRetentionPeriod = maintenance.CancelledJobRetention
	target.DiscardedJobRetentionPeriod = maintenance.DiscardedJobRetention
	target.JobCleanerTimeout = maintenance.JobCleanerTimeout
	return nil
}

func (c MaintenanceConfig) SafeAttributes() map[string]any {
	return map[string]any{
		"completed_job_retention": c.CompletedJobRetention.String(),
		"cancelled_job_retention": c.CancelledJobRetention.String(),
		"discarded_job_retention": c.DiscardedJobRetention.String(),
		"job_cleaner_timeout":     c.JobCleanerTimeout.String(),
	}
}

func (c MaintenanceConfig) String() string {
	return fmt.Sprint(c.SafeAttributes())
}
