package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// unscheduledConfigsSQL lists the configs the handoff query
// (schedulerHandoffCandidatesSQL) can never select: active and planner-managed,
// but without sync_options.schedule_cron. Such a config is skipped by a WHERE
// clause, so without this read nothing says it exists (CHAOS-8768: a Jira
// config sat unscheduled for weeks while its work items went stale).
// It is a plain read: no lock, no write.
const unscheduledConfigsSQL = `
SELECT config.provider, config.id::text
FROM public.sync_configurations AS config
WHERE config.is_active = TRUE
    AND config.planner_managed = TRUE
    AND COALESCE(config.sync_options->>'schedule_cron', '') = ''
ORDER BY config.provider, config.id
LIMIT $1
`

// unscheduledConfigReportLimit bounds one pass's read and log volume.
const unscheduledConfigReportLimit = 200

// unscheduledReportEvery is the scheduler pass length for this report. The
// loop polls every second; the schedules it serves are hourly, so one pass per
// hour keeps the WARN loud without one line per config per second.
const unscheduledReportEvery = time.Hour

// UnscheduledConfig names an active, planner-managed config that the
// scheduler skips because it has no schedule_cron. Hash is a short digest of
// the config id: the id itself is a customer object and never reaches a log.
type UnscheduledConfig struct {
	Provider string
	Hash     string
}

// UnscheduledConfigLister is optional on a HandoffStepper. Repository
// implements it.
type UnscheduledConfigLister interface {
	UnscheduledConfigs(context.Context) ([]UnscheduledConfig, error)
}

// ConfigHash is the short, stable digest a log line uses for a config id.
func ConfigHash(configID string) string {
	sum := sha256.Sum256([]byte(configID))
	return hex.EncodeToString(sum[:])[:12]
}

// UnscheduledConfigs reads the configs the handoff query silently skips.
func (repository *Repository) UnscheduledConfigs(ctx context.Context) ([]UnscheduledConfig, error) {
	if repository == nil || repository.pool == nil {
		return nil, ErrInvalidTransactionRequest
	}
	rows, err := repository.pool.Query(ctx, unscheduledConfigsSQL, unscheduledConfigReportLimit)
	if err != nil {
		return nil, fmt.Errorf("read configs without a schedule: %w", err)
	}
	defer rows.Close()
	var out []UnscheduledConfig
	for rows.Next() {
		var provider, id string
		if err := rows.Scan(&provider, &id); err != nil {
			return nil, fmt.Errorf("scan config without a schedule: %w", err)
		}
		out = append(out, UnscheduledConfig{Provider: provider, Hash: ConfigHash(id)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read configs without a schedule: %w", err)
	}
	return out, nil
}

// reportUnscheduledConfigs logs one WARN per unscheduled config, once per
// scheduler pass (unscheduledReportEvery). A read failure is logged and never
// fails the handoff window: the report is diagnostic, not a gate.
func (loop *Loop) reportUnscheduledConfigs(ctx context.Context, now time.Time) {
	lister, ok := loop.stepper.(UnscheduledConfigLister)
	if !ok {
		return
	}
	loop.mu.Lock()
	due := loop.lastUnscheduledReport.IsZero() || now.Sub(loop.lastUnscheduledReport) >= unscheduledReportEvery
	if due {
		loop.lastUnscheduledReport = now
	}
	loop.mu.Unlock()
	if !due {
		return
	}
	configs, err := lister.UnscheduledConfigs(ctx)
	if err != nil {
		// The error text can carry SQL or connection detail; log its Go type only.
		errorType := fmt.Sprintf("%T", err)
		loop.logger().WarnContext(ctx, "sync.scheduler.unscheduled_config_read_failed", "error_type", errorType)
		return
	}
	loop.mu.Lock()
	loop.unscheduledConfigs = uint64(len(configs))
	loop.mu.Unlock()
	for _, config := range configs {
		loop.logger().WarnContext(ctx, "sync.scheduler.config_without_schedule",
			"provider", config.Provider,
			"config_hash", config.Hash,
			"reason", "active planner-managed config has no sync_options.schedule_cron; the scheduler never plans it",
		)
	}
}
