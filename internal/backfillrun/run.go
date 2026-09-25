// Package backfillrun is `dho backfill run`: the operator's way to start a
// historical backfill of one sync configuration. It replaces the Python
// `dev-hops backfill run`, which planned the run in the CLI process and
// dispatched it.
//
// The Go scheduler is the one planner. The verb therefore does what the admin
// "Backfill" trigger does at the seam the scheduler reads: it validates the
// configuration as the Python verb did, then writes one scheduled_sync_occurrences
// row and its sync_manual_triggers payload (mode backfill, the window, the
// dataset keys the configuration's sync_targets select) in one transaction, and
// waits a bounded time for the scheduler to materialize the run.
package backfillrun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
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

// Params are the verb's inputs after flag handling.
type Params struct {
	ConfigID     string
	RequestedOrg string
	Window       Window
}

// RefusedError is a refusal the operator can act on: the run is not started.
type RefusedError struct{ Message string }

func (err RefusedError) Error() string { return err.Message }

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

// occurrenceIdentity is scheduled_sync_occurrence_identity, byte for byte.
func occurrenceIdentity(configID string, scheduledFor time.Time) string {
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

// normalizeUUID is uuid.UUID(text) in Python: it drops "urn:" and "uuid:",
// trims braces, drops every hyphen (wherever it stands), and wants exactly 32
// hexadecimal digits.
func normalizeUUID(text string) (string, error) {
	digits := strings.ReplaceAll(strings.ReplaceAll(text, "urn:", ""), "uuid:", "")
	digits = strings.ReplaceAll(strings.Trim(digits, "{}"), "-", "")
	if len(digits) != 32 {
		return "", errors.New("badly formed hexadecimal UUID string")
	}
	for _, character := range digits {
		if !strings.ContainsRune("0123456789abcdefABCDEF", character) {
			return "", errors.New("badly formed hexadecimal UUID string")
		}
	}
	digits = strings.ToLower(digits)
	return digits[0:8] + "-" + digits[8:12] + "-" + digits[12:16] + "-" + digits[16:20] + "-" + digits[20:], nil
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

// Validate is the validation _cmd_backfill_run ran before it planned, in its
// order and with its messages, then the two refusals of this seam. It returns
// the dataset keys the sync_targets select (nil when there are no targets).
func Validate(ctx context.Context, tx pgx.Tx, config *Config, params Params) (datasetKeys []string, enabledSources int, err error) {
	if params.RequestedOrg != "" && params.RequestedOrg != config.OrgID {
		return nil, 0, fmt.Errorf("Org mismatch: --org %s does not own sync config %s (owned by %s)",
			params.RequestedOrg, params.ConfigID, config.OrgID)
	}
	provider := strings.ToLower(strings.TrimSpace(config.Provider))
	syncTargets := slices.Clone(config.SyncTargets)
	if len(syncTargets) > 0 {
		known := schedsync.SupportedLegacyTargets(provider)
		var unresolved []string
		for _, target := range syncTargets {
			if !slices.Contains(known, target) && !slices.Contains(unresolved, target) {
				unresolved = append(unresolved, target)
			}
		}
		if len(unresolved) > 0 {
			sort.Strings(unresolved)
			all := slices.Clone(syncTargets)
			sort.Strings(all)
			return nil, 0, fmt.Errorf("backfill run: sync configuration %s (%s) has sync_targets %s that provider %s "+
				"does not recognize as a legacy target (checked against sync.datasets.supported_legacy_targets) -- "+
				"refusing rather than silently planning a narrower or empty dataset_keys set. Full sync_targets on "+
				"this config: %s.", params.ConfigID, pyRepr(config.Name), pyList(unresolved), pyRepr(provider), pyList(all))
		}
		keys, err := schedsync.PlannerDatasetKeys(provider, syncTargets)
		if err != nil {
			return nil, 0, err
		}
		datasetKeys = keys
	}
	if config.IntegrationID == nil {
		return nil, 0, fmt.Errorf("Sync configuration %s has no integration_id; cannot plan a backfill", params.ConfigID)
	}
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM public.integration_sources
WHERE org_id = $1 AND integration_id = $2::uuid AND is_enabled`, config.OrgID, *config.IntegrationID).Scan(&enabledSources); err != nil {
		return nil, 0, fmt.Errorf("count enabled sources: %w", err)
	}
	// The canonical-parent check (canonical_sync_config_for_sync_run).
	rows, err := tx.Query(ctx, `
SELECT id::text, name FROM public.sync_configurations
WHERE org_id = $1 AND integration_id = $2::uuid AND parent_id IS NULL
ORDER BY created_at ASC, id ASC LIMIT 2`, config.OrgID, *config.IntegrationID)
	if err != nil {
		return nil, 0, fmt.Errorf("resolve the canonical configuration: %w", err)
	}
	var canonicalID, canonicalName string
	found := false
	for rows.Next() {
		if !found {
			if err := rows.Scan(&canonicalID, &canonicalName); err != nil {
				rows.Close()
				return nil, 0, err
			}
			found = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if !found || canonicalID != config.ID {
		resolved := "no parent SyncConfiguration at all"
		if found {
			resolved = fmt.Sprintf("%s (%s)", canonicalID, pyRepr(canonicalName))
		}
		return nil, 0, fmt.Errorf("backfill run: --config-id %s (%s) is not the config the shared reference-discovery "+
			"resolver (canonical_sync_config_for_sync_run) would use for integration %s -- it would resolve %s instead. "+
			"This is a child config (parent_id set) or one of several parent configs for this integration; the "+
			"discovery seam has no way to honour --config-id specifically (CHAOS-4500). Point --config-id at the "+
			"integration's sole/canonical parent SyncConfiguration, or resolve CHAOS-4500 first.",
			params.ConfigID, pyRepr(config.Name), *config.IntegrationID, resolved)
	}
	// This seam's own refusal: the scheduler materializes only a planner-managed
	// configuration or a child pinned to one source, and would quarantine any
	// other, so the run is not started.
	if !config.PlannerManaged && config.SourceID == nil {
		return nil, 0, RefusedError{Message: fmt.Sprintf("backfill run: sync configuration %s (%s) is neither planner-managed "+
			"nor pinned to one source, and the scheduler materializes only those: nothing was started", params.ConfigID, pyRepr(config.Name))}
	}
	return datasetKeys, enabledSources, nil
}

func pyRepr(text string) string {
	if !strings.Contains(text, "'") {
		return "'" + strings.ReplaceAll(text, `\`, `\\`) + "'"
	}
	if !strings.Contains(text, `"`) {
		return `"` + strings.ReplaceAll(text, `\`, `\\`) + `"`
	}
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(text) + "'"
}

func pyList(items []string) string {
	quoted := make([]string, len(items))
	for index, item := range items {
		quoted[index] = pyRepr(item)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// Mint writes the occurrence and its manual trigger in the transaction.
func Mint(ctx context.Context, tx pgx.Tx, config *Config, params Params, datasetKeys []string, now time.Time) (Trigger, error) {
	scheduledFor := now.UTC().Truncate(time.Microsecond)
	occurrenceID := occurrenceIdentity(config.ID, scheduledFor)
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
	if len(datasetKeys) > 0 {
		keys = datasetKeys
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.sync_manual_triggers (occurrence_id, mode, since, before, source_ids, dataset_keys, triggered_by)
VALUES ($1, 'backfill', $2, $3, NULL, $4, 'backfill')`,
		occurrenceID, params.Window.Since, params.Window.Before, keys); err != nil {
		return Trigger{}, fmt.Errorf("write the manual trigger: %w", err)
	}
	return Trigger{
		OccurrenceID: occurrenceID, ScheduledFor: scheduledFor, JobID: jobID, DatasetKeys: datasetKeys,
		Provider: config.Provider, OrgID: config.OrgID, SyncConfigID: config.ID, SyncTargets: config.SyncTargets,
		SinceInstant: params.Window.Since, BeforeInstant: params.Window.Before, ScheduledJobNew: created,
	}, nil
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
