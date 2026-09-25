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
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

const (
	occurrenceIdentityVersion = "sync_scheduler_occurrence_v1"
	jobStatusActive           = 0 // JobStatus.ACTIVE
	jobStatusPaused           = 1 // JobStatus.PAUSED
)

// Params are the verb's inputs after flag handling.
type Params struct {
	ConfigID     string
	RequestedOrg string
	Window       Window
}

// RefusedError is a refusal the operator can act on: the run is not started.
type RefusedError struct{ Message string }

func (err RefusedError) Error() string { return err.Message }

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

// Validated is what validation resolved.
type Validated struct {
	DatasetKeys    []string
	EnabledSources int
	// SourceIDs is the explicit source list the trigger carries (nil: none).
	SourceIDs []string
}

// sourceIDsFor is the source scope of the trigger. Python's planner took
// source_ids=None to mean every enabled source of the integration. The scheduler
// reads a NULL list on a planner-managed configuration as "the sources tagged for
// this configuration", which can be none: a run over untagged sources would
// complete with zero units. So the enabled sources are named. A configuration
// pinned to one source is scoped by that source and carries no list.
func sourceIDsFor(config *Config, enabled []string) []string {
	if config.PlannerManaged && config.SourceID == nil {
		return enabled
	}
	return nil
}

// Validate is the validation _cmd_backfill_run ran before it planned, in its
// order and with its messages, then the two refusals of this seam. It returns
// the dataset keys the sync_targets select (nil when there are no targets).
func Validate(ctx context.Context, tx pgx.Tx, config *Config, params Params) (validated Validated, err error) {
	var datasetKeys []string
	if params.RequestedOrg != "" && params.RequestedOrg != config.OrgID {
		return Validated{}, fmt.Errorf("Org mismatch: --org %s does not own sync config %s (owned by %s)",
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
			return Validated{}, fmt.Errorf("backfill run: sync configuration %s (%s) has sync_targets %s that provider %s "+
				"does not recognize as a legacy target (checked against sync.datasets.supported_legacy_targets) -- "+
				"refusing rather than silently planning a narrower or empty dataset_keys set. Full sync_targets on "+
				"this config: %s.", params.ConfigID, pyRepr(config.Name), pyList(unresolved), pyRepr(provider), pyList(all))
		}
		keys, err := schedsync.PlannerDatasetKeys(provider, syncTargets)
		if err != nil {
			return Validated{}, err
		}
		datasetKeys = keys
	}
	if config.IntegrationID == nil {
		return Validated{}, fmt.Errorf("Sync configuration %s has no integration_id; cannot plan a backfill", params.ConfigID)
	}
	// The integration must be there, in the configuration's organization, and
	// active: the scheduler materializes no other (it would retry the occurrence
	// five times and quarantine it).
	var integrationActive bool
	err = tx.QueryRow(ctx, `
SELECT is_active FROM public.integrations WHERE id = $1::uuid AND org_id = $2`, *config.IntegrationID, config.OrgID).Scan(&integrationActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return Validated{}, RefusedError{Message: fmt.Sprintf("backfill run: sync configuration %s (%s) names integration %s, which does not exist in organization %s: nothing was started",
			params.ConfigID, pyRepr(config.Name), *config.IntegrationID, config.OrgID)}
	}
	if err != nil {
		return Validated{}, fmt.Errorf("read the integration: %w", err)
	}
	if !integrationActive {
		return Validated{}, RefusedError{Message: fmt.Sprintf("backfill run: integration %s of sync configuration %s (%s) is not active, and the scheduler materializes only an active integration: nothing was started",
			*config.IntegrationID, params.ConfigID, pyRepr(config.Name))}
	}
	sourceRows, err := tx.Query(ctx, `
SELECT id::text FROM public.integration_sources
WHERE org_id = $1 AND integration_id = $2::uuid AND is_enabled
ORDER BY full_name, id`, config.OrgID, *config.IntegrationID)
	if err != nil {
		return Validated{}, fmt.Errorf("list enabled sources: %w", err)
	}
	enabled := []string{}
	for sourceRows.Next() {
		var id string
		if err := sourceRows.Scan(&id); err != nil {
			sourceRows.Close()
			return Validated{}, err
		}
		enabled = append(enabled, id)
	}
	sourceRows.Close()
	if err := sourceRows.Err(); err != nil {
		return Validated{}, err
	}
	// The canonical-parent check (canonical_sync_config_for_sync_run).
	rows, err := tx.Query(ctx, `
SELECT id::text, name FROM public.sync_configurations
WHERE org_id = $1 AND integration_id = $2::uuid AND parent_id IS NULL
ORDER BY created_at ASC, id ASC LIMIT 2`, config.OrgID, *config.IntegrationID)
	if err != nil {
		return Validated{}, fmt.Errorf("resolve the canonical configuration: %w", err)
	}
	var canonicalID, canonicalName string
	found := false
	for rows.Next() {
		if !found {
			if err := rows.Scan(&canonicalID, &canonicalName); err != nil {
				rows.Close()
				return Validated{}, err
			}
			found = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Validated{}, err
	}
	if !found || canonicalID != config.ID {
		resolved := "no parent SyncConfiguration at all"
		if found {
			resolved = fmt.Sprintf("%s (%s)", canonicalID, pyRepr(canonicalName))
		}
		return Validated{}, fmt.Errorf("backfill run: --config-id %s (%s) is not the config the shared reference-discovery "+
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
		return Validated{}, RefusedError{Message: fmt.Sprintf("backfill run: sync configuration %s (%s) is neither planner-managed "+
			"nor pinned to one source, and the scheduler materializes only those: nothing was started", params.ConfigID, pyRepr(config.Name))}
	}
	return Validated{DatasetKeys: datasetKeys, EnabledSources: len(enabled), SourceIDs: sourceIDsFor(config, enabled)}, nil
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
