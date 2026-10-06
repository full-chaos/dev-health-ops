package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// unscheduledConfigsSQL lists the configs the handoff query
// (schedulerHandoffCandidatesSQL) can never select: active and planner-managed,
// but without sync_options.schedule_cron. Such a config is skipped by a WHERE
// clause, so without this read nothing says it exists (CHAOS-8768: a Jira
// config sat unscheduled for weeks while its work items went stale).
// It is a plain read: no lock, no write. count(*) OVER () is the TRUE total of
// matches (computed before LIMIT), so the gauge is exact even when the listed rows are capped.
const unscheduledConfigsSQL = `
SELECT config.provider, config.id::text, count(*) OVER () AS total
FROM public.sync_configurations AS config
WHERE config.is_active = TRUE
    AND config.planner_managed = TRUE
    AND COALESCE(config.sync_options->>'schedule_cron', '') = ''
ORDER BY config.provider, config.id
LIMIT $1
`

// unscheduledConfigReportLimit bounds one pass's read and log volume.
const unscheduledConfigReportLimit = 200

// unscheduledReadTimeout bounds the diagnostic read on its OWN context, well
// under the window deadline (15s default), so a slow read can neither fail nor
// shorten the handoff window (CHAOS-8768 r1 P1-a). A var so a test can shrink it.
var unscheduledReadTimeout = 2 * time.Second

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
	// UnscheduledConfigs returns the listed configs (capped) and the true total.
	UnscheduledConfigs(context.Context) ([]UnscheduledConfig, int, error)
}

// ConfigHash is the short, stable digest a log line uses for a config id.
func ConfigHash(configID string) string {
	sum := sha256.Sum256([]byte(configID))
	return hex.EncodeToString(sum[:])[:12]
}

// UnscheduledConfigs reads the configs the handoff query silently skips.
func (repository *Repository) UnscheduledConfigs(ctx context.Context) ([]UnscheduledConfig, int, error) {
	if repository == nil || repository.pool == nil {
		return nil, 0, ErrInvalidTransactionRequest
	}
	rows, err := repository.pool.Query(ctx, unscheduledConfigsSQL, unscheduledConfigReportLimit)
	if err != nil {
		return nil, 0, fmt.Errorf("read configs without a schedule: %w", err)
	}
	defer rows.Close()
	var out []UnscheduledConfig
	total := 0
	for rows.Next() {
		var provider, id string
		if err := rows.Scan(&provider, &id, &total); err != nil {
			return nil, 0, fmt.Errorf("scan config without a schedule: %w", err)
		}
		out = append(out, UnscheduledConfig{Provider: provider, Hash: ConfigHash(id)})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("read configs without a schedule: %w", err)
	}
	return out, total, nil
}

// reportUnscheduledConfigs logs one WARN per unscheduled config, once per
// scheduler pass (unscheduledReportEvery). A read failure is logged and never
// fails the handoff window: the report is diagnostic, not a gate. The read runs
// on its own bounded context derived from the loop's parent context (never the
// window's step context), after the window's work, so it cannot consume the
// window's deadline. When more configs match than the list cap, one extra WARN
// states the true total and that the list is truncated.
func (loop *Loop) reportUnscheduledConfigs(parent context.Context, now time.Time) {
	if parent.Err() != nil {
		return // shutting down: no diagnostic read, no noise
	}
	ctx, cancel := context.WithTimeout(parent, unscheduledReadTimeout)
	defer cancel()
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
	configs, total, err := lister.UnscheduledConfigs(ctx)
	if err != nil {
		// The error text can carry SQL or connection detail: log the innermost
		// Go type and, for a Postgres error, its SQLSTATE; never the text.
		errorType, sqlState := unscheduledReadFailureCause(err)
		loop.logger().WarnContext(ctx, "sync.scheduler.unscheduled_config_read_failed",
			"error_type", errorType, "sqlstate", sqlState)
		return
	}
	loop.mu.Lock()
	loop.unscheduledConfigs = uint64(total)
	loop.mu.Unlock()
	for _, config := range configs {
		loop.logger().WarnContext(ctx, "sync.scheduler.config_without_schedule",
			"provider", config.Provider,
			"config_hash", config.Hash,
			"reason", "active planner-managed config has no sync_options.schedule_cron; the scheduler never plans it",
		)
	}
	if total > len(configs) {
		loop.logger().WarnContext(ctx, "sync.scheduler.config_without_schedule_truncated",
			"total", total, "listed", len(configs),
			"reason", "more active planner-managed configs lack sync_options.schedule_cron than the list cap; the rest are not named",
		)
	}
}

// unscheduledReadFailureCause names a failed read without its text: the Go
// type at the end of the Unwrap chain (the repository wraps with %w, so the
// outer type is always the same wrapper) and, when a *pgconn.PgError is in the
// chain, its SQLSTATE code ("" otherwise).
func unscheduledReadFailureCause(err error) (errorType, sqlState string) {
	innermost := err
	for {
		next := errors.Unwrap(innermost)
		if next == nil {
			break
		}
		innermost = next
	}
	errorType = fmt.Sprintf("%T", innermost)
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		sqlState = pgError.Code
	}
	return errorType, sqlState
}
