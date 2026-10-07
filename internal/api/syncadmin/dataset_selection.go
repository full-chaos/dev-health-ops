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
// time. The stored sync_configurations.sync_targets list holds what requests
// asked for, as before: a save never stores a target because a row is on, and
// no code computes a row from the stored list. What a whole-integration
// config shows is the state of the rows: a target that has a dataset of the
// provider is shown when a row of it is on and is not shown when its rows are
// off, whatever the stored list says; a target with no dataset is shown as
// stored (shownTargets, the one derive function). A save changes only the
// rows of the targets it adds to, or drops from, that list.
//
// This file is NOT a port of the Python route: the recorded Python answer
// rebuilt the rows from the submitted list on every save, in both directions.

// rowsOwnSelection reports whether the config's shown list is derived from
// its integration's dataset rows (providersync.RowsOwnSyncSelection: a
// whole-integration config of any provider but PagerDuty). Every other
// config shows its stored list and a save of it writes no dataset row.
func rowsOwnSelection(config *syncConfig) bool {
	return providersync.RowsOwnSyncSelection(config.Provider, config.IntegrationID != nil, config.SourceID != nil)
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

// shownTargets is the list a whole-integration config shows. A stored target
// is shown, in the stored order and once, when the provider has no dataset
// for it (no row can speak for it) or when a row of it is on. That holds for
// a stored target the form does not offer too ("blame", "security"). Then
// comes every other target the enabled keys derive, in the registry's target
// order: form targets only, so a target the form does not offer is shown
// only when it is stored. A stored target that has a dataset and no enabled
// row is not shown: the row is off, so it does not sync.
func shownTargets(provider string, enabledKeys, stored []string) []string {
	derived := providersync.DerivedSyncTargets(provider, enabledKeys)
	out := make([]string, 0, len(stored)+len(derived))
	inOut := map[string]bool{}
	for _, target := range stored {
		if inOut[target] {
			continue
		}
		if providersync.SyncTargetHasEnabledDataset(provider, target, enabledKeys) || !providersync.SyncTargetHasDataset(provider, target) {
			inOut[target] = true
			out = append(out, target)
		}
	}
	for _, target := range derived {
		if !inOut[target] {
			inOut[target] = true
			out = append(out, target)
		}
	}
	return out
}

// selectionChange is what one save does to the selection: the targets it
// adds to and drops from the shown list, the dataset keys to switch on and
// off, and the list the save stores.
type selectionChange struct {
	added, removed          []string
	enableKeys, disableKeys []string
	// stored is the new stored list: the items of the submitted list that
	// were stored before or that this save adds, in the submitted order. An
	// item the submitted list names only because a dataset row is on is not
	// in it.
	stored []string
}

// planSelectionChange is the save rule for a whole-integration config. The
// reference is the list the server shows now (shownTargets over the rows and
// the stored list), read in the save's transaction; nothing the
// request says changes it. Only a target the submitted list adds to, or drops
// from, the reference writes rows: a target in neither set keeps its rows
// whatever they are. A target with no dataset writes no row.
//
// The rows after the save are computed from sets, never from an order of
// writes. On: the keys of every added target, whether the form offers it or
// not ("blame", "security"), as the create writes them for the same list.
// Off: the keys of the dropped targets that an operator controls
// (OperatorControlledDatasetKeys: "blame" is one, "security" is not, so no
// save switches "security" off) and that no target of the submitted list
// names. So a key that belongs to a target the request keeps or adds is never
// switched off by that request: dropping "git" while "blame" is submitted
// leaves the blame row alone, and dropping "blame" while "git" is submitted
// does too ("git" names the blame key).
func planSelectionChange(provider string, enabledKeys, stored, submitted []string) (selectionChange, error) {
	shown := shownTargets(provider, enabledKeys, stored)
	inShown, inSubmitted, inStored := stringSet(shown), stringSet(submitted), stringSet(stored)
	var change selectionChange
	for _, target := range uniqueStrings(submitted) {
		if !inShown[target] {
			change.added = append(change.added, target)
		}
	}
	for _, target := range uniqueStrings(shown) {
		if !inSubmitted[target] {
			change.removed = append(change.removed, target)
		}
	}
	keysOf := func(targets []string) ([]string, error) {
		var withDataset []string
		for _, target := range targets {
			if providersync.SyncTargetHasDataset(provider, target) {
				withDataset = append(withDataset, target)
			}
		}
		if len(withDataset) == 0 {
			return nil, nil
		}
		return providersync.PlannerDatasetKeys(provider, withDataset)
	}
	var err error
	if change.enableKeys, err = keysOf(change.added); err != nil {
		return selectionChange{}, err
	}
	removedKeys, err := keysOf(change.removed)
	if err != nil {
		return selectionChange{}, err
	}
	submittedKeys, err := keysOf(submitted)
	if err != nil {
		return selectionChange{}, err
	}
	controlled, keptOn := stringSet(providersync.OperatorControlledDatasetKeys(provider)), stringSet(submittedKeys)
	for _, key := range removedKeys {
		if controlled[key] && !keptOn[key] {
			change.disableKeys = append(change.disableKeys, key)
		}
	}
	inAdded := stringSet(change.added)
	change.stored = make([]string, 0, len(submitted))
	for _, target := range submitted {
		if inStored[target] || inAdded[target] {
			change.stored = append(change.stored, target)
		}
	}
	return change, nil
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
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
// sync_targets list its response carries (shownTargets). One read serves all
// the configs. Every other config keeps its stored list, as stored.
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
		config.shownTargets = shownTargets(config.Provider, enabled[*config.IntegrationID], storedListItems(stored))
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

// saveSelection runs the row writes of a save and records them. The caller
// holds the selection lock and stores change.stored.
func saveSelection(ctx context.Context, tx pgx.Tx, logger *slog.Logger, orgID string, config *syncConfig, change selectionChange) error {
	integrationID := *config.IntegrationID
	enabled, disabled, err := applySelectionChange(ctx, tx, orgID, integrationID, change)
	if err != nil {
		return err
	}
	recordSelectionChange(ctx, logger, orgID, integrationID, config.Provider, enabled, disabled)
	return nil
}

// recordSelectionChange counts and logs what a save did to the rows. Dataset
// keys and counts only: no target list of the request is logged.
func recordSelectionChange(ctx context.Context, logger *slog.Logger, orgID string, integrationID uuid.UUID, provider string,
	enabled, disabled []string) {
	selectionMetrics.observeRows(provider, "enabled", len(enabled))
	selectionMetrics.observeRows(provider, "disabled", len(disabled))
	if len(enabled)+len(disabled) > 0 {
		logger.InfoContext(ctx, "sync_config_dataset_rows_changed", "org_id", orgID, "integration_id", integrationID.String(),
			"provider", provider, "enabled_dataset_keys", strings.Join(enabled, ","), "disabled_dataset_keys", strings.Join(disabled, ","))
	}
}
