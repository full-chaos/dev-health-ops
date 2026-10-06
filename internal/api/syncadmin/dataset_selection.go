package syncadmin

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// The dataset row is the single owner of "which datasets of an integration
// sync" (CHAOS-8816; Linear project document "Sync configuration: the dataset
// row is the single owner"). integration_datasets.is_enabled decides at plan
// time. A whole-integration config's sync_targets is therefore not a store:
// the api computes it from the enabled rows on every read, and a save changes
// only the rows of the targets the user changed. The sync_configurations
// column stays as a mirror, written on a save; no code may compute a row
// from it.
//
// This file is NOT a port of the Python route: the recorded Python answer
// rebuilt the rows from the submitted list on every save, in both directions.

// rowsOwnSelection reports whether the config's sync_targets is derived from
// its integration's dataset rows (providersync.RowsOwnSyncSelection: a
// whole-integration config of any provider but PagerDuty). Every other
// config keeps its stored list and a save of it writes no dataset row.
func rowsOwnSelection(config *syncConfig) bool {
	return providersync.RowsOwnSyncSelection(config.Provider, config.IntegrationID != nil, config.SourceID != nil)
}

// incidentGateTargets is the items of the config's stored list the
// canonical-incident gate reads (providersync.IncidentGateTargets, over the
// list as the routes decode it): an item that mirrors a dataset row is left
// out, every other item stays, a non-string item included, so the gate
// answers it as before. Every route that runs the gate on the stored list
// takes its targets from here.
func incidentGateTargets(config *syncConfig, stored []pyjson.Value) []pyjson.Value {
	out := make([]pyjson.Value, 0, len(stored))
	for _, target := range stored {
		if text, ok := target.(string); ok &&
			providersync.StoredTargetIsMirrored(config.Provider, config.IntegrationID != nil, config.SourceID != nil, text) {
			continue
		}
		out = append(out, target)
	}
	return out
}

// storedListItems is the string items of a stored sync_targets JSON list, in
// order. Any other stored value has no items.
func storedListItems(stored pyjson.Value) []string {
	list, ok := stored.([]pyjson.Value)
	if !ok {
		return nil
	}
	items := make([]string, 0, len(list))
	for _, item := range list {
		if text, ok := item.(string); ok {
			items = append(items, text)
		}
	}
	return items
}

// shownTargets is the list a whole-integration config shows: the targets
// derived from the enabled dataset keys plus the passthrough targets. Order:
// the targets of preferred that are in the result keep preferred's order (so
// a list that agrees with the rows is returned as it is), then every other
// derived target in the registry's target order, then every other
// passthrough target.
func shownTargets(provider string, enabledKeys, passthrough, preferred []string) []string {
	derived := providersync.DerivedSyncTargets(provider, enabledKeys)
	in := map[string]bool{}
	for _, target := range derived {
		in[target] = true
	}
	for _, target := range passthrough {
		in[target] = true
	}
	out := make([]string, 0, len(in))
	done := map[string]bool{}
	for _, group := range [][]string{preferred, derived, passthrough} {
		for _, target := range group {
			if in[target] && !done[target] {
				done[target] = true
				out = append(out, target)
			}
		}
	}
	return out
}

// selectionChange is what one save does to the selection: the dataset keys
// to switch on and off, and the passthrough targets after the save.
type selectionChange struct {
	added, removed          []string
	enableKeys, disableKeys []string
	passthrough             []string
	// baseDiffers: the request carried the list the form was shown and it is
	// not the list the rows show now (another save, the dataset endpoint or
	// a backfill changed a row while the form was open).
	baseDiffers bool
}

// planSelectionChange is the save rule for a whole-integration config. The
// reference is the list the form was shown (base) when the request carries
// it, else the list the rows show now. Only a target the submitted list adds
// to, or drops from, the reference writes rows: a target in neither set
// keeps its rows whatever they are. A target with no dataset moves in and
// out of the passthrough list. A target the form never offers ("blame",
// "security") writes no row.
func planSelectionChange(provider string, enabledKeys, storedPassthrough, submitted, base []string, baseSet bool) (selectionChange, error) {
	shown := shownTargets(provider, enabledKeys, storedPassthrough, nil)
	reference := shown
	if baseSet {
		reference = base
	}
	inReference, inSubmitted := stringSet(reference), stringSet(submitted)
	change := selectionChange{baseDiffers: baseSet && !sameStringSet(inReference, stringSet(shown))}
	for _, target := range uniqueStrings(submitted) {
		if !inReference[target] {
			change.added = append(change.added, target)
		}
	}
	for _, target := range uniqueStrings(reference) {
		if !inSubmitted[target] {
			change.removed = append(change.removed, target)
		}
	}
	keysOf := func(target string) ([]string, error) {
		if !providersync.OperatorSelectableSyncTarget(target) {
			return nil, nil
		}
		return providersync.PlannerDatasetKeys(provider, []string{target})
	}
	passthrough := append([]string{}, storedPassthrough...)
	for _, target := range change.added {
		if !providersync.SyncTargetHasDataset(provider, target) {
			passthrough = append(passthrough, target)
			continue
		}
		keys, err := keysOf(target)
		if err != nil {
			return selectionChange{}, err
		}
		change.enableKeys = append(change.enableKeys, keys...)
	}
	dropped := map[string]bool{}
	for _, target := range change.removed {
		if !providersync.SyncTargetHasDataset(provider, target) {
			dropped[target] = true
			continue
		}
		keys, err := keysOf(target)
		if err != nil {
			return selectionChange{}, err
		}
		change.disableKeys = append(change.disableKeys, keys...)
	}
	for _, target := range uniqueStrings(passthrough) {
		if !dropped[target] {
			change.passthrough = append(change.passthrough, target)
		}
	}
	change.enableKeys, change.disableKeys = uniqueStrings(change.enableKeys), uniqueStrings(change.disableKeys)
	return change, nil
}

// gatedTargets is the targets the canonical-incident gate reads for this
// save: the ones the user adds and the passthrough targets the list keeps. A
// gated target that is in the list only because its row is on does not
// refuse the save; the plan-time gate on the rows stays.
func (change selectionChange) gatedTargets() []string {
	return uniqueStrings(append(append([]string{}, change.added...), change.passthrough...))
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

func sameStringSet(left, right map[string]bool) bool {
	if len(left) != len(right) {
		return false
	}
	for value := range left {
		if !right[value] {
			return false
		}
	}
	return true
}

func uniqueStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}

func stringValues(values []string) []pyjson.Value {
	out := make([]pyjson.Value, len(values))
	for index, value := range values {
		out[index] = value
	}
	return out
}

// rowQuerier is the read both the pool and a transaction serve.
type rowQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// enabledDatasetKeysByIntegration reads the ENABLED dataset keys of the
// org's integrations in one statement: integration id -> keys in key order.
// A row that exists with is_enabled false is not read: it does not sync.
func enabledDatasetKeysByIntegration(ctx context.Context, db rowQuerier, orgID string, integrationIDs []uuid.UUID) (map[uuid.UUID][]string, error) {
	found := map[uuid.UUID][]string{}
	if len(integrationIDs) == 0 {
		return found, nil
	}
	rows, err := db.Query(ctx, `SELECT integration_id, dataset_key FROM integration_datasets
WHERE org_id = $1 AND integration_id = ANY($2) AND is_enabled IS true
ORDER BY integration_id, dataset_key`, orgID, integrationIDs)
	if err != nil {
		return nil, fmt.Errorf("read enabled datasets: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var integrationID uuid.UUID
		var key string
		if err := rows.Scan(&integrationID, &key); err != nil {
			return nil, fmt.Errorf("read enabled dataset: %w", err)
		}
		found[integrationID] = append(found[integrationID], key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read enabled datasets: %w", err)
	}
	return found, nil
}

// deriveShownTargets sets, on every config whose rows own its selection, the
// sync_targets list its response carries: derived from the integration's
// enabled rows plus the stored passthrough targets. One read serves all the
// configs. Every other config keeps its stored list.
func deriveShownTargets(ctx context.Context, read func(context.Context, string, []uuid.UUID) (map[uuid.UUID][]string, error),
	orgID string, configs ...*syncConfig) error {
	var integrationIDs []uuid.UUID
	seen := map[uuid.UUID]bool{}
	for _, config := range configs {
		if rowsOwnSelection(config) && !seen[*config.IntegrationID] {
			seen[*config.IntegrationID] = true
			integrationIDs = append(integrationIDs, *config.IntegrationID)
		}
	}
	if len(integrationIDs) == 0 {
		return nil
	}
	enabled, err := read(ctx, orgID, integrationIDs)
	if err != nil {
		return err
	}
	for _, config := range configs {
		if !rowsOwnSelection(config) {
			continue
		}
		stored, err := decodeStored(config.SyncTargets)
		if err != nil {
			return err
		}
		items := storedListItems(stored)
		config.shownTargets = shownTargets(config.Provider, enabled[*config.IntegrationID], providersync.PassthroughSyncTargets(config.Provider, items), items)
		config.shownTargetsSet = true
	}
	return nil
}

// selectionLock serialises the saves of one integration's selection for the
// rest of the transaction, so the rows a save reads are the rows it changes.
func selectionLock(ctx context.Context, tx pgx.Tx, orgID string, integrationID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		fmt.Sprintf("sync-target-dataset-reconcile:%s:%s", orgID, integrationID)); err != nil {
		return fmt.Errorf("dataset selection lock: %w", err)
	}
	return nil
}

// applySelectionChange writes the rows of one save: every key of an added
// target is created enabled or switched on, every enabled key of a removed
// target is switched off (never deleted). No other row is written. It
// returns the keys whose state changed.
func applySelectionChange(ctx context.Context, tx pgx.Tx, orgID string, integrationID uuid.UUID, change selectionChange) (enabled, disabled []string, err error) {
	keys := append([]string{}, change.enableKeys...)
	sort.Strings(keys)
	for _, key := range keys {
		tag, err := tx.Exec(ctx, `INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES ($1, $2, $3, $4, true, '{}')
ON CONFLICT (org_id, integration_id, dataset_key) DO UPDATE SET is_enabled = true
WHERE integration_datasets.is_enabled IS NOT true`, uuid.New(), orgID, integrationID, key)
		if err != nil {
			return nil, nil, fmt.Errorf("enable dataset %s: %w", key, err)
		}
		if tag.RowsAffected() > 0 {
			enabled = append(enabled, key)
		}
	}
	keys = append([]string{}, change.disableKeys...)
	sort.Strings(keys)
	for _, key := range keys {
		tag, err := tx.Exec(ctx, `UPDATE integration_datasets SET is_enabled = false
WHERE org_id = $1 AND integration_id = $2 AND dataset_key = $3 AND is_enabled IS true`, orgID, integrationID, key)
		if err != nil {
			return nil, nil, fmt.Errorf("disable dataset %s: %w", key, err)
		}
		if tag.RowsAffected() > 0 {
			disabled = append(disabled, key)
		}
	}
	return enabled, disabled, nil
}

// saveSelection runs the row writes of a save and returns the mirror list:
// the list the rows show after the writes plus the passthrough targets, in
// the submitted list's order where it names them. The caller holds the
// selection lock.
func saveSelection(ctx context.Context, tx pgx.Tx, logger *slog.Logger, orgID string, config *syncConfig, submitted []string,
	change selectionChange) ([]string, error) {
	integrationID := *config.IntegrationID
	enabled, disabled, err := applySelectionChange(ctx, tx, orgID, integrationID, change)
	if err != nil {
		return nil, err
	}
	recordSelectionChange(ctx, logger, orgID, integrationID, config.Provider, enabled, disabled, change.baseDiffers)
	after, err := enabledDatasetKeysByIntegration(ctx, tx, orgID, []uuid.UUID{integrationID})
	if err != nil {
		return nil, err
	}
	return shownTargets(config.Provider, after[integrationID], change.passthrough, submitted), nil
}

// recordSelectionChange counts and logs what a save did to the rows, and a
// save whose base list was not the list the rows showed. Dataset keys and
// counts only: no target list of the request is logged.
func recordSelectionChange(ctx context.Context, logger *slog.Logger, orgID string, integrationID uuid.UUID, provider string,
	enabled, disabled []string, baseDiffers bool) {
	selectionMetrics.observeRows(provider, "enabled", len(enabled))
	selectionMetrics.observeRows(provider, "disabled", len(disabled))
	if len(enabled)+len(disabled) > 0 {
		logger.InfoContext(ctx, "sync_config_dataset_rows_changed", "org_id", orgID, "integration_id", integrationID.String(),
			"provider", provider, "enabled_dataset_keys", strings.Join(enabled, ","), "disabled_dataset_keys", strings.Join(disabled, ","))
	}
	if baseDiffers {
		selectionMetrics.observeStaleBase(provider)
		logger.InfoContext(ctx, "sync_config_save_stale_base", "org_id", orgID, "integration_id", integrationID.String(),
			"provider", provider, "reason", "the list the form was shown is not the list the dataset rows show now; "+
				"only the targets this save changed were written")
	}
}
