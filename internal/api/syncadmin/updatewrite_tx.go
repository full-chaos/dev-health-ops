package syncadmin

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// pyIterate is `for item in (value or [])` with no validation: a falsy value
// yields nothing, a list its items, a string its characters, a dict its
// keys; any other value is iteration's TypeError (a bare 500).
func pyIterate(value pyjson.Value) ([]pyjson.Value, error) {
	if !pyjson.Truthy(value) {
		return nil, nil
	}
	switch typed := value.(type) {
	case []pyjson.Value:
		return typed, nil
	case string:
		runes := pyjson.Runes(typed)
		out := make([]pyjson.Value, len(runes))
		for index, r := range runes {
			out[index] = pyjson.FromRunes([]rune{r})
		}
		return out, nil
	case *pyjson.Object:
		keys := typed.Keys()
		out := make([]pyjson.Value, len(keys))
		for index, key := range keys {
			out[index] = key
		}
		return out, nil
	}
	return nil, fmt.Errorf("%w: %T is not iterable", errUnrenderable, value)
}

// upsertScheduledJob is sync.py's _upsert_scheduled_job for the config as
// stored in this transaction: its provider (lower-cased), options and
// activity. The job carries name "sync-config-<id>", job_type "sync", the
// options' schedule_cron or the hourly default, the options' timezone or
// UTC, job_config {provider, sync_config_id}, and is ACTIVE when the config
// is active and has an explicit schedule, else PAUSED. Without a sync job it
// inserts one; with one it sets those fields, writing (and stamping
// updated_at) only when one of them changes, as the ORM flush does. The
// schema allows one sync job per config (uq_scheduled_job_org_sync_config_type).
func upsertScheduledJob(ctx context.Context, tx pgx.Tx, orgID string, configID uuid.UUID, now time.Time) error {
	var provider string
	var optionsText *string
	var active bool
	if err := tx.QueryRow(ctx, `SELECT provider, sync_options::text, is_active FROM sync_configurations WHERE id = $1`, configID).
		Scan(&provider, &optionsText, &active); err != nil {
		return fmt.Errorf("read the config for its sync job: %w", err)
	}
	stored, err := decodeStored(optionsText)
	if err != nil {
		return err
	}
	options, err := plainDict(stored)
	if err != nil {
		return err
	}
	lower := pythonparity.Lower(provider)
	cron := "0 * * * *"
	status := int64(1) // JobStatus.PAUSED
	if value, _ := options.Get("schedule_cron"); pyjson.Truthy(value) {
		cron = pyjson.Str(value)
		if active {
			status = 0 // JobStatus.ACTIVE
		}
	}
	timezone := "UTC"
	if value, _ := options.Get("timezone"); pyjson.Truthy(value) {
		timezone = pyjson.Str(value)
	}
	jobConfig := pyjson.NewObject()
	jobConfig.Set("provider", lower)
	jobConfig.Set("sync_config_id", configID.String())
	jobConfigText, err := pyjson.Dumps(jobConfig)
	if err != nil {
		return err
	}

	rows, err := tx.Query(ctx, `SELECT id, schedule_cron, provider, timezone, job_config::text, status FROM scheduled_jobs
WHERE org_id = $1 AND sync_config_id = $2 AND job_type = 'sync'`, orgID, configID)
	if err != nil {
		return fmt.Errorf("read the config's sync job: %w", err)
	}
	type job struct {
		id                       uuid.UUID
		cron, provider, timezone string
		config                   *string
		status                   int64
	}
	var jobs []job
	for rows.Next() {
		var row job
		if err := rows.Scan(&row.id, &row.cron, &row.provider, &row.timezone, &row.config, &row.status); err != nil {
			rows.Close()
			return fmt.Errorf("read the config's sync job: %w", err)
		}
		jobs = append(jobs, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read the config's sync job: %w", err)
	}
	switch len(jobs) {
	case 0:
		if _, err := tx.Exec(ctx, `INSERT INTO scheduled_jobs
(id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, sync_config_id, status, is_running,
 run_count, failure_count, created_at, updated_at)
VALUES ($1, $2, $3, 'sync', $4, $5, $6, $7::json, $8, $9, false, 0, 0, $10, $10)`,
			uuid.New(), orgID, "sync-config-"+configID.String(), lower, cron, timezone, jobConfigText, configID, status, now); err != nil {
			return fmt.Errorf("insert scheduled job: %w", err)
		}
		return nil
	case 1:
	default:
		return fmt.Errorf("%d sync jobs for config %s: MultipleResultsFound", len(jobs), configID)
	}
	current := jobs[0]
	storedConfig, err := decodeStored(current.config)
	if err != nil {
		return err
	}
	set := []string{}
	args := []any{current.id}
	add := func(column string, value any) {
		args = append(args, value)
		set = append(set, fmt.Sprintf("%s = $%d", column, len(args)))
	}
	if current.cron != cron {
		add("schedule_cron", cron)
	}
	if current.provider != lower {
		add("provider", lower)
	}
	if current.timezone != timezone {
		add("timezone", timezone)
	}
	if !pyjson.Equal(storedConfig, jobConfig) {
		args = append(args, jobConfigText)
		set = append(set, fmt.Sprintf("job_config = $%d::json", len(args)))
	}
	if current.status != status {
		add("status", status)
	}
	if len(set) == 0 {
		return nil
	}
	add("updated_at", now)
	if _, err := tx.Exec(ctx, `UPDATE scheduled_jobs SET `+strings.Join(set, ", ")+` WHERE id = $1`, args...); err != nil {
		return fmt.Errorf("update scheduled job: %w", err)
	}
	return nil
}
