// Package synchandoff is the seam the sync and backfill triggers use to start a
// run: it writes one scheduled_sync_occurrences row and its sync_manual_triggers
// payload in the caller's transaction, and waits a bounded time for the scheduler
// to materialize the run. The Go scheduler is the one planner; nothing here plans.
// The `dho backfill run` verb and the integration sync and backfill admin routes
// share it.
package synchandoff

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	occurrenceIdentityVersion = "sync_scheduler_occurrence_v1"
	jobStatusActive           = 0 // JobStatus.ACTIVE
	jobStatusPaused           = 1 // JobStatus.PAUSED
)

// Config is the part of a sync configuration the verb reads.
type Config struct {
	ID             string
	OrgID          string
	Provider       string
	Name           string
	SyncTargets    []string
	IntegrationID  *string
	PlannerManaged bool
	SourceID       *string
	IsActive       bool
	SyncOptions    map[string]any
}

// Trigger is what the verb wrote.
type Trigger struct {
	OccurrenceID    string
	ScheduledFor    time.Time
	JobID           string
	DatasetKeys     []string
	EnabledSources  int
	Provider        string
	OrgID           string
	SyncConfigID    string
	SyncTargets     []string
	SinceInstant    time.Time
	BeforeInstant   time.Time
	ScheduledJobNew bool
}

// OccurrenceIdentity is scheduled_sync_occurrence_identity, byte for byte.
func OccurrenceIdentity(configID string, scheduledFor time.Time) string {
	fields := [][2]string{
		{"identity_version", occurrenceIdentityVersion},
		{"config_id", configID},
		{"scheduled_for", scheduledFor.UTC().Format("2006-01-02T15:04:05.000000") + "000Z"},
	}
	digest := sha256.New()
	for _, item := range fields {
		name, value := []byte(item[0]), []byte(item[1])
		fmt.Fprintf(digest, "%d:", len(name))
		digest.Write(name)
		fmt.Fprintf(digest, "%d:", len(value))
		digest.Write(value)
		digest.Write([]byte("\n"))
	}
	return "sha256:" + hex.EncodeToString(digest.Sum(nil))
}

// LoadConfig reads the configuration (nil when there is none).
func LoadConfig(ctx context.Context, tx pgx.Tx, configID string) (*Config, error) {
	var config Config
	var targets, options []byte
	err := tx.QueryRow(ctx, `
SELECT id::text, org_id, provider, name, sync_targets::jsonb, integration_id::text, planner_managed,
       source_id::text, is_active, sync_options::jsonb
FROM public.sync_configurations WHERE id = $1::uuid`, configID).Scan(
		&config.ID, &config.OrgID, &config.Provider, &config.Name, &targets, &config.IntegrationID,
		&config.PlannerManaged, &config.SourceID, &config.IsActive, &options)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load sync configuration: %w", err)
	}
	if len(targets) > 0 && string(targets) != "null" {
		if err := json.Unmarshal(targets, &config.SyncTargets); err != nil {
			return nil, fmt.Errorf("decode sync targets: %w", err)
		}
	}
	if len(options) > 0 && string(options) != "null" {
		if err := json.Unmarshal(options, &config.SyncOptions); err != nil {
			return nil, fmt.Errorf("decode sync options: %w", err)
		}
	}
	return &config, nil
}

// MintInput is what one trigger asks the scheduler for: the manual trigger row.
type MintInput struct {
	// Mode is the manual trigger's mode ("backfill", "incremental", ...).
	Mode string
	// Since and Before bound the window (nil: none).
	Since, Before *time.Time
	// SourceIDs and DatasetKeys scope the run (nil: the scheduler's default).
	SourceIDs, DatasetKeys []string
	// TriggeredBy is "manual" or "backfill" (the table's CHECK constraint).
	TriggeredBy string
}

// Mint writes the occurrence and its manual trigger in the transaction.
func Mint(ctx context.Context, tx pgx.Tx, config *Config, input MintInput, now time.Time) (Trigger, error) {
	scheduledFor := now.UTC().Truncate(time.Microsecond)
	occurrenceID := OccurrenceIdentity(config.ID, scheduledFor)
	jobID, created, err := ensureScheduledJob(ctx, tx, config, scheduledFor)
	if err != nil {
		return Trigger{}, err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.scheduled_sync_occurrences
	(occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id, scheduled_for)
VALUES ($1, $2, $3, $4::uuid, $5::uuid, $6)`,
		occurrenceID, occurrenceIdentityVersion, config.OrgID, config.ID, jobID, scheduledFor); err != nil {
		return Trigger{}, fmt.Errorf("write the scheduled occurrence: %w", err)
	}
	var keys any
	if len(input.DatasetKeys) > 0 {
		keys = input.DatasetKeys
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.sync_manual_triggers (occurrence_id, mode, since, before, source_ids, dataset_keys, triggered_by)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		occurrenceID, input.Mode, input.Since, input.Before, input.SourceIDs, keys, input.TriggeredBy); err != nil {
		return Trigger{}, fmt.Errorf("write the manual trigger: %w", err)
	}
	trigger := Trigger{
		OccurrenceID: occurrenceID, ScheduledFor: scheduledFor, JobID: jobID, DatasetKeys: input.DatasetKeys,
		Provider: config.Provider, OrgID: config.OrgID, SyncConfigID: config.ID, SyncTargets: config.SyncTargets,
		ScheduledJobNew: created,
	}
	if input.Since != nil {
		trigger.SinceInstant = *input.Since
	}
	if input.Before != nil {
		trigger.BeforeInstant = *input.Before
	}
	return trigger, nil
}

// ensureScheduledJob is _ensure_scheduled_job_for_config: the sync job marker
// of the configuration, created paused when the configuration has no schedule.
func ensureScheduledJob(ctx context.Context, tx pgx.Tx, config *Config, now time.Time) (string, bool, error) {
	var jobID string
	err := tx.QueryRow(ctx, `
SELECT id::text FROM public.scheduled_jobs WHERE org_id = $1 AND sync_config_id = $2::uuid AND job_type = 'sync'`,
		config.OrgID, config.ID).Scan(&jobID)
	if err == nil {
		return jobID, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", false, fmt.Errorf("find the scheduled job: %w", err)
	}
	cron := "0 * * * *"
	if truthy(config.SyncOptions["schedule_cron"]) {
		cron = fmt.Sprint(config.SyncOptions["schedule_cron"])
	}
	timezone := "UTC"
	if truthy(config.SyncOptions["timezone"]) {
		timezone = fmt.Sprint(config.SyncOptions["timezone"])
	}
	status := jobStatusPaused
	if config.IsActive && truthy(config.SyncOptions["schedule_cron"]) {
		status = jobStatusActive
	}
	jobConfig, err := json.Marshal(map[string]string{"provider": config.Provider, "sync_config_id": config.ID})
	if err != nil {
		return "", false, err
	}
	if err := tx.QueryRow(ctx, `
INSERT INTO public.scheduled_jobs
	(id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, sync_config_id, status, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, 'sync', $3, $4, $5, $6::jsonb, $7::uuid, $8, $9, $9)
RETURNING id::text`,
		config.OrgID, "sync-config-"+config.ID, config.Provider, cron, timezone, jobConfig, config.ID, status, now).Scan(&jobID); err != nil {
		return "", false, fmt.Errorf("create the scheduled job: %w", err)
	}
	return jobID, true, nil
}

// truthy is Python truthiness for a JSON value (`x or default`).
func truthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return typed != ""
	case bool:
		return typed
	case float64:
		return typed != 0
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	}
	return true
}

// State is what the scheduler did with the occurrence.
type State int

const (
	StatePending State = iota
	StateMaterialized
	StateQuarantined
	StateTerminal
)

// Outcome is the result of Wait.
type Outcome struct {
	State      State
	SyncRunID  string
	TotalUnits int
	ErrorCode  string
	Reason     string
}

// Wait polls the occurrence until the scheduler has planned it, quarantined it,
// or the wait is over. A pending outcome is not an error.
func Wait(ctx context.Context, pool *pgxpool.Pool, occurrenceID string, wait, poll time.Duration) (Outcome, error) {
	deadline := time.Now().Add(wait)
	for {
		var status string
		var syncRunID, errorCode *string
		err := pool.QueryRow(ctx, `
SELECT reconcile_status, sync_run_id::text, reconcile_error_code
FROM public.scheduled_sync_occurrences WHERE occurrence_id = $1`, occurrenceID).Scan(&status, &syncRunID, &errorCode)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return Outcome{}, fmt.Errorf("read the occurrence: %w", err)
		}
		if err == nil {
			switch status {
			case "completed":
				if syncRunID != nil {
					return readRun(ctx, pool, *syncRunID)
				}
			case "quarantined":
				code := "unknown"
				if errorCode != nil && *errorCode != "" {
					code = *errorCode
				}
				return Outcome{State: StateQuarantined, ErrorCode: code}, nil
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return Outcome{State: StatePending}, nil
		}
		select {
		case <-ctx.Done():
			return Outcome{}, ctx.Err()
		case <-time.After(min(poll, remaining)):
		}
	}
}

// readRun is _materialized_trigger_result: the planned run's unit count, or its
// terminal reason when the run ended as a disabled PagerDuty sync.
func readRun(ctx context.Context, pool *pgxpool.Pool, syncRunID string) (Outcome, error) {
	var units int
	var status string
	var runError *string
	var category *string
	err := pool.QueryRow(ctx, `
SELECT COALESCE(total_units, 0), status::text, error, result->>'error_category'
FROM public.sync_runs WHERE id = $1::uuid`, syncRunID).Scan(&units, &status, &runError, &category)
	if errors.Is(err, pgx.ErrNoRows) {
		return Outcome{State: StatePending}, nil
	}
	if err != nil {
		return Outcome{}, fmt.Errorf("read the sync run: %w", err)
	}
	if strings.EqualFold(status, "failed") && category != nil && *category == "pagerduty_sync_disabled" {
		reason := ""
		if runError != nil {
			reason = *runError
		}
		return Outcome{State: StateTerminal, SyncRunID: syncRunID, Reason: reason}, nil
	}
	return Outcome{State: StateMaterialized, SyncRunID: syncRunID, TotalUnits: units}, nil
}
