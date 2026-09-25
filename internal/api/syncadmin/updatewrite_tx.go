package syncadmin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
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

// plannerTargets is planner_dataset_keys' own reading of a target list:
// str() of every item that is not None.
func plannerTargets(items []pyjson.Value) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item != nil {
			out = append(out, pyjson.Str(item))
		}
	}
	return out
}

// reconcileDatasetRowsForSyncTargets is sync.py's
// _reconcile_dataset_rows_for_sync_targets: the integration's shared
// dataset rows follow the edited selection in both directions. Desired is
// planner_dataset_keys of the new targets (a PagerDuty refusal skips the
// whole call) plus that of every other whole-integration config of the
// integration (source_id NULL, any activity); the controlled universe is
// operator_controlled_dataset_keys. Desired rows are created enabled or
// re-enabled; controlled rows outside desired that are enabled are
// disabled, never deleted. A sibling whose targets cannot be mapped drops
// the disable pass. Serialised per integration by a transaction-scoped
// advisory lock taken before the sibling read.
func reconcileDatasetRowsForSyncTargets(ctx context.Context, tx pgx.Tx, logger *slog.Logger, orgID string, integrationID uuid.UUID,
	provider string, syncTargets []string, previousSyncTargets []pyjson.Value, configID uuid.UUID) error {
	desiredList, err := providersync.PlannerDatasetKeys(provider, syncTargets)
	if errors.Is(err, providersync.ErrPagerDutyTargetNotOperational) {
		return nil
	}
	if err != nil {
		return err
	}
	desired := map[string]bool{}
	for _, key := range desiredList {
		desired[key] = true
	}
	previouslyDesired := map[string]bool{}
	if keys, err := providersync.PlannerDatasetKeys(provider, plannerTargets(previousSyncTargets)); err == nil {
		for _, key := range keys {
			previouslyDesired[key] = true
		}
	}
	controlled := map[string]bool{}
	for _, key := range providersync.OperatorControlledDatasetKeys(provider) {
		controlled[key] = true
	}
	if len(desired) == 0 && len(controlled) == 0 {
		return nil
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		fmt.Sprintf("sync-target-dataset-reconcile:%s:%s", orgID, integrationID)); err != nil {
		return fmt.Errorf("dataset reconcile lock: %w", err)
	}
	rows, err := tx.Query(ctx, `SELECT provider, sync_targets::text FROM sync_configurations
WHERE org_id = $1 AND integration_id = $2 AND source_id IS NULL AND id != $3`, orgID, integrationID, configID)
	if err != nil {
		return fmt.Errorf("read sibling configs: %w", err)
	}
	type sibling struct {
		provider string
		targets  *string
	}
	var siblings []sibling
	for rows.Next() {
		var row sibling
		if err := rows.Scan(&row.provider, &row.targets); err != nil {
			rows.Close()
			return fmt.Errorf("read sibling config: %w", err)
		}
		siblings = append(siblings, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read sibling configs: %w", err)
	}
	for _, row := range siblings {
		stored, err := decodeStored(row.targets)
		if err != nil {
			return err
		}
		items, err := pyIterate(stored)
		if err != nil {
			return err
		}
		// [str(target) for target in (sibling_targets or [])]: None becomes
		// "None" here, before planner_dataset_keys sees it.
		targets := make([]string, len(items))
		for index, item := range items {
			targets[index] = pyjson.Str(item)
		}
		keys, err := providersync.PlannerDatasetKeys(row.provider, targets)
		if err != nil {
			logger.WarnContext(ctx, "sync_target_reconcile_skipped_unreadable_sibling", "org_id", orgID,
				"integration_id", integrationID.String(), "sibling_provider", row.provider,
				"reason", "a sibling whole-integration config's sync_targets could not be mapped to dataset keys; "+
					"shared rows are left enabled rather than disabled on a guess")
			controlled = map[string]bool{}
			break
		}
		for _, key := range keys {
			desired[key] = true
		}
	}

	for _, key := range sortedKeys(desired) {
		if _, err := tx.Exec(ctx, `INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES ($1, $2, $3, $4, true, '{}')
ON CONFLICT (org_id, integration_id, dataset_key) DO UPDATE SET is_enabled = true
WHERE integration_datasets.is_enabled IS NOT true`, uuid.New(), orgID, integrationID, key); err != nil {
			return fmt.Errorf("enable dataset %s: %w", key, err)
		}
	}
	var disabled []string
	for _, key := range sortedKeys(controlled) {
		if desired[key] {
			continue
		}
		tag, err := tx.Exec(ctx, `UPDATE integration_datasets SET is_enabled = false
WHERE org_id = $1 AND integration_id = $2 AND dataset_key = $3 AND is_enabled IS true`, orgID, integrationID, key)
		if err != nil {
			return fmt.Errorf("disable dataset %s: %w", key, err)
		}
		if tag.RowsAffected() > 0 {
			disabled = append(disabled, key)
		}
	}
	var drifted []string
	for _, key := range disabled {
		if !(previouslyDesired[key] && !desired[key]) {
			drifted = append(drifted, key)
		}
	}
	if len(drifted) > 0 {
		logger.WarnContext(ctx, "sync_target_dataset_drift_repaired", "org_id", orgID, "integration_id", integrationID.String(),
			"provider", provider, "drifted_dataset_keys", strings.Join(drifted, ","), "drifted_count", len(drifted),
			"reason", "integration_datasets rows were enabled that this config's sync_targets cannot account for; "+
				"the planner was syncing datasets the operator had deselected")
	}
	return nil
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
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
