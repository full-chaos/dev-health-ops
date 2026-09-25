package syncadmin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// plannerCreate is _create_planner_managed_config's input.
type plannerCreate struct {
	orgID, name, provider string
	// credentialID is the request's credential_id; nil or "" means none.
	credentialID *string
	syncTargets  []string
	// parentOptions are the config's sync options (the top-level fields
	// already merged in).
	parentOptions *pyjson.Object
	// scheduleCron and timezone are the request's own top-level fields,
	// stamped on the integration row as given.
	scheduleCron, timezone *string
	// buildSourceRows is the caller's build_source_rows(integration_id,
	// config_id).
	buildSourceRows func(integrationID, configID uuid.UUID) []newSourceRow
}

// plannerCreated is what _create_planner_managed_config returns: the
// planner parent config and its integration's id and credential.
type plannerCreated struct {
	config        *syncConfig
	integrationID uuid.UUID
	credentialID  *uuid.UUID
}

// errPlannerParentInvariant is _assert_single_planner_parent_for_integration's
// RuntimeError (a bare 500).
var errPlannerParentInvariant = errors.New("planner-managed integration invariant violated")

// errCredentialIDNotUUID is uuid.UUID(credential_id)'s ValueError (a bare
// 500: the create route does not catch it).
var errCredentialIDNotUUID = errors.New("credential_id is not a UUID")

// createPlannerManagedConfig is _create_planner_managed_config inside the
// route's transaction, in its order:
//
//  1. a GitHub config's parent options gain the work-item runtime snapshot;
//  2. the Integration row (credential_id parsed as a UUID);
//  3. the planner-managed parent SyncConfiguration;
//  4. the single-planner-parent assert;
//  5. the caller's source rows, then the planner dataset rows and their
//     options (a PagerDuty selection other than {"operational"} and a
//     malformed GitHub work-item option are ValueErrors: nothing of this
//     step is written);
//  6. repair_pagerduty_operational_integration for a PagerDuty config;
//  7. the scheduled-job anchor (_upsert_scheduled_job; a new config has
//     none, so it is inserted).
//
// Every row carries the request's instant (now) where the ORM stamps
// datetime.now(timezone.utc), and a fresh uuid4 id.
func createPlannerManagedConfig(ctx context.Context, tx pgx.Tx, in plannerCreate, now time.Time, lookupEnv func(string) (string, bool)) (*plannerCreated, error) {
	parentOptions, err := plannerParentOptions(in.provider, in.parentOptions, lookupEnv)
	if err != nil {
		return nil, err
	}
	var credentialID *uuid.UUID
	if in.credentialID != nil && *in.credentialID != "" {
		parsed, err := pythonparity.ParseUUID(*in.credentialID)
		if err != nil {
			return nil, errCredentialIDNotUUID
		}
		credentialID = &parsed
	}
	optionsText, err := pyjson.Dumps(parentOptions)
	if err != nil {
		return nil, err
	}
	integrationID := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO integrations
(id, org_id, provider, credential_id, name, config, is_active, schedule_cron, timezone, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6::json, true, $7, $8, $9, $9)`,
		integrationID, in.orgID, in.provider, credentialID, in.name, optionsText, in.scheduleCron, in.timezone, now); err != nil {
		return nil, fmt.Errorf("insert integration: %w", err)
	}

	targets := make([]pyjson.Value, len(in.syncTargets))
	for index, target := range in.syncTargets {
		targets[index] = target
	}
	targetsText, err := pyjson.Dumps(targets)
	if err != nil {
		return nil, err
	}
	configID := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO sync_configurations
(id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5::json, $6::json, true, true, $7, $8, $8)`,
		configID, in.orgID, in.name, in.provider, targetsText, optionsText, integrationID, now); err != nil {
		if isDuplicateSyncConfigName(err) {
			// Named divergence (CHAOS-6726, D2473): Python lets the unique
			// violation escape as a 500. Go answers 409 in FastAPI's detail
			// shape, and the caller's transaction rolls back every write of
			// the request (a batch create is refused whole).
			return nil, refuse(http.StatusConflict, duplicateSyncConfigNameDetail)
		}
		return nil, fmt.Errorf("insert sync configuration: %w", err)
	}

	var parents int64
	if err := tx.QueryRow(ctx, `SELECT count(id) FROM sync_configurations
WHERE org_id = $1 AND planner_managed IS true AND integration_id = $2 AND parent_id IS NULL`,
		in.orgID, integrationID).Scan(&parents); err != nil {
		return nil, fmt.Errorf("count planner parents: %w", err)
	}
	if parents > 1 {
		return nil, errPlannerParentInvariant
	}

	var sources []newSourceRow
	if in.buildSourceRows != nil {
		sources = in.buildSourceRows(integrationID, configID)
	}
	keys, err := plannerDatasetKeys(in.provider, in.syncTargets)
	if err != nil {
		return nil, err
	}
	datasetOptions := make([]string, len(keys))
	for index, key := range keys {
		options, err := plannerDatasetOptions(in.provider, key, in.syncTargets, parentOptions)
		if err != nil {
			return nil, err
		}
		if datasetOptions[index], err = pyjson.Dumps(options); err != nil {
			return nil, err
		}
	}
	for _, source := range sources {
		if err := insertSourceRow(ctx, tx, in.orgID, integrationID, source, now); err != nil {
			return nil, err
		}
	}
	for index, key := range keys {
		if _, err := tx.Exec(ctx, `INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES ($1, $2, $3, $4, true, $5::json)`, uuid.New(), in.orgID, integrationID, key, datasetOptions[index]); err != nil {
			return nil, fmt.Errorf("insert integration dataset: %w", err)
		}
	}

	if pythonparity.Lower(in.provider) == "pagerduty" {
		if err := repairPagerDutyOperationalIntegration(ctx, tx, in.orgID, integrationID, credentialID, now); err != nil {
			return nil, err
		}
	}

	if err := upsertScheduledJob(ctx, tx, in.orgID, configID, now); err != nil {
		return nil, err
	}

	config, err := scanSyncConfig(tx.QueryRow(ctx, `SELECT `+syncConfigColumns+` FROM sync_configurations WHERE id = $1`, configID))
	if err != nil {
		return nil, fmt.Errorf("read the created config: %w", err)
	}
	return &plannerCreated{config: config, integrationID: integrationID, credentialID: credentialID}, nil
}

func insertSourceRow(ctx context.Context, tx pgx.Tx, orgID string, integrationID uuid.UUID, source newSourceRow, now time.Time) error {
	metadata, err := pyjson.Dumps(source.metadata)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO integration_sources
(id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::json, true, $10, $10)`,
		uuid.New(), orgID, integrationID, source.provider, source.sourceType, source.externalID, source.name, source.fullName, metadata, now); err != nil {
		return fmt.Errorf("insert integration source: %w", err)
	}
	return nil
}

// repairPagerDutyOperationalIntegration is
// sync/pagerduty_repair.repair_pagerduty_operational_integration right
// after a create. The config's targets are already exactly
// {"operational"} (planner_dataset_keys refused anything else), so the
// malformed-target branch cannot be reached here. Without an active
// PagerDuty credential of the org (by the integration's credential_id)
// nothing changes. A credential without a usable account identity stamps
// the integration's PagerDuty configs disabled with the reason. Otherwise
// the integration gets its canonical "account" source keyed on the account
// id (the create path has added none), and every operational dataset row
// is enabled with legacy_targets ["operational"] (added when missing;
// other rows disabled).
func repairPagerDutyOperationalIntegration(ctx context.Context, tx pgx.Tx, orgID string, integrationID uuid.UUID, credentialID *uuid.UUID, now time.Time) error {
	if credentialID == nil {
		return nil
	}
	var configText *string
	err := tx.QueryRow(ctx, `SELECT config::text FROM integration_credentials
WHERE id = $1 AND org_id = $2 AND provider = 'pagerduty' AND is_active IS true`, *credentialID, orgID).Scan(&configText)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read pagerduty credential: %w", err)
	}
	var config pyjson.Value
	if configText != nil {
		if config, err = pyjson.DecodeString(*configText); err != nil {
			return fmt.Errorf("decode pagerduty credential config: %w", err)
		}
	}
	accountID, err := pagerDutyProviderInstanceID(config)
	if errors.Is(err, errPagerDutyConfigNotMapping) {
		return err
	}
	if err != nil {
		return stampPagerDutyConfigsDisabled(ctx, tx, orgID, integrationID, "PagerDuty credential account identity is invalid", now)
	}

	metadata := pyjson.NewObject()
	if err := repairPagerDutySources(ctx, tx, orgID, integrationID, accountID, metadata, now); err != nil {
		return err
	}
	return repairPagerDutyDatasets(ctx, tx, orgID, integrationID)
}

// stampPagerDutyConfigsDisabled is _stamp_disabled over
// _linked_pagerduty_configs: every config of the integration whose
// provider is exactly "pagerduty".
func stampPagerDutyConfigsDisabled(ctx context.Context, tx pgx.Tx, orgID string, integrationID uuid.UUID, reason string, now time.Time) error {
	stats := pyjson.NewObject()
	stats.Set("phase", "pagerduty_repair")
	stats.Set("error", reason)
	statsText, err := pyjson.Dumps(stats)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE sync_configurations
SET is_active = false, last_sync_at = $4, last_sync_success = false, last_sync_error = $3, last_sync_stats = $5::json, updated_at = $4
WHERE org_id = $1 AND integration_id = $2 AND provider = 'pagerduty'`, orgID, integrationID, reason, now, statsText); err != nil {
		return fmt.Errorf("stamp pagerduty configs: %w", err)
	}
	return nil
}

// repairPagerDutySources is _repair_sources on an integration the create
// path gave no PagerDuty source: the canonical account source is added,
// enabled. Existing PagerDuty sources (none here) would be disabled or
// reshaped; they are read so the rule stays whole.
func repairPagerDutySources(ctx context.Context, tx pgx.Tx, orgID string, integrationID uuid.UUID, accountID string, metadata *pyjson.Object, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT id, external_id FROM integration_sources
WHERE org_id = $1 AND integration_id = $2 AND provider = 'pagerduty' ORDER BY id`, orgID, integrationID)
	if err != nil {
		return fmt.Errorf("read pagerduty sources: %w", err)
	}
	type existing struct {
		id         uuid.UUID
		externalID string
	}
	var sources []existing
	for rows.Next() {
		var row existing
		if err := rows.Scan(&row.id, &row.externalID); err != nil {
			rows.Close()
			return err
		}
		sources = append(sources, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	var canonical *uuid.UUID
	for _, source := range sources {
		if source.externalID == accountID {
			id := source.id
			canonical = &id
			break
		}
	}
	if canonical == nil {
		if err := insertSourceRow(ctx, tx, orgID, integrationID, newSourceRow{provider: "pagerduty", sourceType: "account",
			externalID: accountID, name: accountID, fullName: accountID, metadata: metadata}, now); err != nil {
			return err
		}
	} else if _, err := tx.Exec(ctx, `UPDATE integration_sources SET source_type = 'account', external_id = $2, name = $2, full_name = $2, is_enabled = true
WHERE id = $1`, *canonical, accountID); err != nil {
		return fmt.Errorf("reshape the canonical pagerduty source: %w", err)
	}
	for _, source := range sources {
		if canonical != nil && source.id == *canonical {
			continue
		}
		if _, err := tx.Exec(ctx, `UPDATE integration_sources SET is_enabled = false WHERE id = $1 AND is_enabled`, source.id); err != nil {
			return fmt.Errorf("disable a stale pagerduty source: %w", err)
		}
	}
	return nil
}

// pagerDutyOperationalDatasetKeys is PAGERDUTY_OPERATIONAL_DATASET_KEYS.
func pagerDutyOperationalDatasetKeys() []string {
	keys, _ := providersync.PlannerDatasetKeys("pagerduty", []string{"operational"})
	return keys
}

// repairPagerDutyDatasets is _repair_datasets: every operational dataset
// row enabled with legacy_targets ["operational"] merged into its options
// (written only when the options change, as the ORM compares the JSON
// value), a missing one added, every other row disabled.
func repairPagerDutyDatasets(ctx context.Context, tx pgx.Tx, orgID string, integrationID uuid.UUID) error {
	rows, err := tx.Query(ctx, `SELECT id, dataset_key, is_enabled, options::text FROM integration_datasets
WHERE org_id = $1 AND integration_id = $2 ORDER BY id`, orgID, integrationID)
	if err != nil {
		return fmt.Errorf("read integration datasets: %w", err)
	}
	type datasetRow struct {
		id       uuid.UUID
		key      string
		enabled  bool
		options  string
		expected bool
	}
	byKey := map[string]*datasetRow{}
	var all []*datasetRow
	for rows.Next() {
		row := &datasetRow{}
		if err := rows.Scan(&row.id, &row.key, &row.enabled, &row.options); err != nil {
			rows.Close()
			return err
		}
		byKey[row.key] = row
		all = append(all, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	expected := pagerDutyOperationalDatasetKeys()
	sort.Strings(expected)
	operational := []pyjson.Value{"operational"}
	for _, key := range expected {
		row, ok := byKey[key]
		if !ok {
			options := pyjson.NewObject()
			options.Set("legacy_targets", operational)
			text, err := pyjson.Dumps(options)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES ($1, $2, $3, $4, true, $5::json)`, uuid.New(), orgID, integrationID, key, text); err != nil {
				return fmt.Errorf("insert pagerduty dataset: %w", err)
			}
			continue
		}
		row.expected = true
		current, err := pyjson.DecodeString(row.options)
		if err != nil {
			return fmt.Errorf("decode dataset options: %w", err)
		}
		merged := pyjson.NewObject()
		if object, ok := current.(*pyjson.Object); ok {
			for _, name := range object.Keys() {
				value, _ := object.Get(name)
				merged.Set(name, value)
			}
		}
		merged.Set("legacy_targets", operational)
		changed := !pyjson.Equal(current, merged)
		if !changed && row.enabled {
			continue
		}
		text, err := pyjson.Dumps(merged)
		if err != nil {
			return err
		}
		if !changed {
			text = row.options
		}
		if _, err := tx.Exec(ctx, `UPDATE integration_datasets SET is_enabled = true, options = $2::json WHERE id = $1`, row.id, text); err != nil {
			return fmt.Errorf("repair pagerduty dataset: %w", err)
		}
	}
	for _, row := range all {
		if row.expected || !row.enabled {
			continue
		}
		if _, err := tx.Exec(ctx, `UPDATE integration_datasets SET is_enabled = false WHERE id = $1`, row.id); err != nil {
			return fmt.Errorf("disable a non-operational pagerduty dataset: %w", err)
		}
	}
	return nil
}

// duplicateSyncConfigNameDetail is the 409 detail for a create whose
// (org, provider, name) is already taken. No Python admin route has a
// duplicate-name message to reuse, so the text is the one D2473 names.
const duplicateSyncConfigNameDetail = "Sync configuration with this name already exists for this provider"

// isDuplicateSyncConfigName reports the unique violation of
// sync_configurations' (org_id, provider, name) key, and nothing else.
func isDuplicateSyncConfigName(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "uq_sync_config_org_provider_name"
}
