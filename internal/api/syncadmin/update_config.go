package syncadmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/synccoverage"
)

// syncConfigUpdate is the SyncConfigUpdate body. Every field is optional;
// provided records the fields the body carried (model_fields_set), null
// included.
type syncConfigUpdate struct {
	syncTargets            []string
	syncTargetsSet         bool
	syncOptions            *pyjson.Object
	isActive               *bool
	scheduleCron, timezone *string
	initialSyncDepth       *big.Int
	provided               map[string]bool
}

// decodeSyncConfigUpdate validates SyncConfigUpdate in its field order:
// sync_targets list[str] | None, sync_options dict | None, is_active
// bool | None (pydantic's lax bool), schedule_cron and timezone str | None,
// initial_sync_depth int | None (lax).
func decodeSyncConfigUpdate(body pybody.Body) (syncConfigUpdate, pybody.Errors) {
	var problems pybody.Errors
	in := syncConfigUpdate{provided: map[string]bool{}}
	object, ok := problems.Object(body)
	if !ok {
		return in, problems
	}
	for _, key := range []string{"sync_targets", "sync_options", "is_active", "schedule_cron", "timezone", "initial_sync_depth"} {
		if _, present := object.Get(key); present {
			in.provided[key] = true
		}
	}
	in.syncTargets, in.syncTargetsSet = problems.OptionalStringList(object, "sync_targets")
	if options, present := problems.OptionalAnyDict(object, "sync_options"); present {
		in.syncOptions = options
	}
	if value, present := problems.OptionalBool(object, "is_active"); present {
		in.isActive = &value
	}
	if value, present := problems.OptionalString(object, "schedule_cron", 0, 0); present {
		in.scheduleCron = &value
	}
	if value, present := problems.OptionalString(object, "timezone", 0, 0); present {
		in.timezone = &value
	}
	if value, present := problems.OptionalLaxInt(object, "initial_sync_depth"); present {
		in.initialSyncDepth = value
	}
	return in, problems
}

// updateSyncConfig is sync.py's update_sync_config, in its order: the body
// (422, before the guard); the config (404); the canonical incident gate on
// the new targets or the stored ones; the top-level schedule fields folded
// into the body's options (an explicit null clears); for a truthy
// schedule_cron the schedule checks (the scheduled_jobs feature, the cron
// interval, the timezone, the tier's minimum interval); the targets and,
// for a whole-integration config, the dataset reconciliation; the merged
// options and their auto-import checks; the GitHub work-item runtime
// options (config, integration and work-items dataset); the PagerDuty
// services mappings; is_active; the sync job; the legacy children; the
// coverage invalidation. Then, for a Jira config whose options changed or
// that was reactivated, best-effort project discovery. 200 with the config.
func (h *handlers) updateSyncConfig(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	in, problems := decodeSyncConfigUpdate(body)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	ctx := r.Context()
	org := orgID(r)
	id, err := pythonparity.ParseUUID(r.PathValue("config_id"))
	if err != nil {
		writeConfigNotFound(w)
		return
	}
	var result *updatedConfig
	err = pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		var err error
		result, err = h.updateSyncConfigTx(ctx, tx, org, id, in)
		return err
	})
	if errors.Is(err, errConfigNotFound) {
		writeConfigNotFound(w)
		return
	}
	if err != nil {
		h.answerOrFail(w, r, "update_sync_config", err)
		return
	}
	if result.discover {
		h.discoverIntegrationSources(ctx, org, *result.config.IntegrationID, "jira_project_discovery_on_update",
			"config_id", r.PathValue("config_id"))
	}
	credentialID, err := h.credentialForConfig(ctx, org, result.config)
	if err != nil {
		h.fail(w, r, "sync_config_credential", err)
		return
	}
	out, err := syncConfigResponse(result.config, nil, credentialID)
	if err != nil {
		h.fail(w, r, "sync_config_response", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

var errConfigNotFound = errors.New("sync configuration not found")

type updatedConfig struct {
	config   *syncConfig
	discover bool
}

func (h *handlers) updateSyncConfigTx(ctx context.Context, tx pgx.Tx, org string, id uuid.UUID, in syncConfigUpdate) (*updatedConfig, error) {
	config, err := scanSyncConfig(tx.QueryRow(ctx,
		`SELECT `+syncConfigColumns+` FROM sync_configurations WHERE org_id = $1 AND id = $2`, org, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errConfigNotFound
	}
	if err != nil {
		return nil, err
	}
	storedTargetsValue, err := decodeStored(config.SyncTargets)
	if err != nil {
		return nil, err
	}
	storedOptionsValue, err := decodeStored(config.SyncOptions)
	if err != nil {
		return nil, err
	}

	var gateTargets []pyjson.Value
	if in.syncTargetsSet {
		for _, target := range in.syncTargets {
			gateTargets = append(gateTargets, target)
		}
	} else if gateTargets, err = pyIterate(storedTargetsValue); err != nil {
		return nil, err
	}
	if err := h.requireCanonicalIncident(ctx, org, gateTargets); err != nil {
		return nil, err
	}

	// The body's options with the top-level schedule fields folded in: a
	// provided null clears the key, an omitted field leaves it alone.
	options := pyjson.NewObject()
	if in.syncOptions != nil {
		for _, key := range in.syncOptions.Keys() {
			value, _ := in.syncOptions.Get(key)
			options.Set(key, value)
		}
	}
	cleared := map[string]bool{}
	for _, field := range []struct {
		key   string
		value pyjson.Value
	}{
		{"schedule_cron", optionalStringValue(in.scheduleCron)},
		{"timezone", optionalStringValue(in.timezone)},
		{"initial_sync_depth", optionalIntValue(in.initialSyncDepth)},
	} {
		if !in.provided[field.key] {
			continue
		}
		if field.value == nil {
			cleared[field.key] = true
			options.Delete(field.key)
		} else {
			options.Set(field.key, field.value)
		}
	}
	optionsProvided := in.syncOptions != nil || in.provided["schedule_cron"] || in.provided["timezone"] || in.provided["initial_sync_depth"]

	if cron, _ := options.Get("schedule_cron"); pyjson.Truthy(cron) {
		orgUUID, err := pythonparity.ParseUUID(org)
		if err != nil {
			return nil, fmt.Errorf("org id is not a UUID: %w", err)
		}
		inputs, err := licensing.LoadTierLimitInputs(ctx, tx, orgUUID)
		if err != nil {
			return nil, err
		}
		if err := h.checkSchedule(ctx, tx, org, inputs, cron, options); err != nil {
			return nil, err
		}
	}

	now := h.now().UTC()
	newTargets := storedTargetsValue
	if in.syncTargetsSet {
		list := make([]pyjson.Value, len(in.syncTargets))
		for index, target := range in.syncTargets {
			list[index] = target
		}
		newTargets = list
		if config.IntegrationID != nil && config.SourceID == nil {
			previous, err := pyIterate(storedTargetsValue)
			if err != nil {
				return nil, err
			}
			if err := reconcileDatasetRowsForSyncTargets(ctx, tx, h.logger, org, *config.IntegrationID, config.Provider,
				in.syncTargets, previous, config.ID); err != nil {
				return nil, err
			}
		}
	}

	var newOptions pyjson.Value = storedOptionsValue
	if optionsProvided {
		merged, err := plainDict(storedOptionsValue)
		if err != nil {
			return nil, err
		}
		for _, key := range options.Keys() {
			value, _ := options.Get(key)
			merged.Set(key, value)
		}
		for key := range cleared {
			merged.Delete(key)
		}
		if malformed := malformedAutoImportCategoryValues(merged); malformed.Len() > 0 {
			detail := pyjson.NewObject()
			detail.Set("message", "auto-import category flags must be true or false")
			detail.Set("malformed_auto_import_category_values", malformed)
			return nil, refuse(http.StatusUnprocessableEntity, detail)
		}
		if unsupported := unsupportedAutoImportCategories(config.Provider, merged); unsupported.Len() > 0 {
			detail := pyjson.NewObject()
			detail.Set("message", config.Provider+" does not support the requested auto-import categories")
			detail.Set("unsupported_auto_import_categories", unsupported)
			return nil, refuse(http.StatusUnprocessableEntity, detail)
		}
		newOptions = merged
	}

	lower := pythonparity.Lower(config.Provider)
	if lower == "github" && config.IntegrationID != nil {
		current, err := plainDict(newOptions)
		if err != nil {
			return nil, err
		}
		written, err := h.writeGitHubRuntimeOptions(ctx, tx, org, *config.IntegrationID, current, options, now)
		if err != nil {
			return nil, err
		}
		newOptions = written
	}
	if lower == "pagerduty" && config.IntegrationID != nil {
		if mappings, ok := options.Get("service_repository_mappings"); ok {
			if _, isDict := mappings.(*pyjson.Object); isDict {
				if err := updateServicesDatasetMappings(ctx, tx, org, *config.IntegrationID, mappings); err != nil {
					return nil, err
				}
			}
		}
	}
	wasInactive := !config.IsActive
	newActive := config.IsActive
	if in.isActive != nil {
		newActive = *in.isActive
	}

	if err := writeConfigChanges(ctx, tx, config, storedTargetsValue, newTargets, storedOptionsValue, newOptions, newActive, now); err != nil {
		return nil, err
	}
	discover := config.IntegrationID != nil && jiraKeyNorm(config.Provider) == "jira" &&
		(optionsProvided || (wasInactive && config.IsActive))

	if err := upsertScheduledJob(ctx, tx, org, config.ID, now); err != nil {
		return nil, err
	}
	if config.ParentID == nil {
		if err := h.cascadeToChildren(ctx, tx, org, config.ID, in, options, cleared, optionsProvided, now); err != nil {
			return nil, err
		}
	}
	if config.IntegrationID != nil {
		err = synccoverage.InvalidateForIntegration(ctx, tx, org, config.IntegrationID.String())
	} else {
		err = synccoverage.InvalidateForConfig(ctx, tx, org, config.ID.String())
	}
	if err != nil {
		return nil, err
	}
	return &updatedConfig{config: config, discover: discover}, nil
}

func optionalStringValue(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}

func optionalIntValue(value *big.Int) pyjson.Value {
	if value == nil {
		return nil
	}
	return pyjson.Int{Int: value}
}

// jiraKeyNorm is discovery/repos.py's jira_key_norm: strip().lower().
func jiraKeyNorm(value string) string {
	return pythonparity.Lower(pythonparity.Strip(value))
}

// writeConfigChanges sets the config's targets, options and activity to the
// new values, writing only the columns whose value changed (Python ==) and
// stamping updated_at only when one did, as the ORM flush does. config is
// updated in place to the stored result.
func writeConfigChanges(ctx context.Context, tx pgx.Tx, config *syncConfig, oldTargets, newTargets, oldOptions, newOptions pyjson.Value,
	newActive bool, now time.Time) error {
	set := []string{}
	args := []any{config.ID}
	add := func(column string, value any) {
		args = append(args, value)
		set = append(set, fmt.Sprintf("%s = $%d", column, len(args)))
	}
	var targetsText, optionsText *string
	if !pyjson.Equal(oldTargets, newTargets) {
		text, err := pyjson.Dumps(newTargets)
		if err != nil {
			return err
		}
		targetsText = &text
		args = append(args, text)
		set = append(set, fmt.Sprintf("sync_targets = $%d::json", len(args)))
	}
	if !pyjson.Equal(oldOptions, newOptions) {
		text, err := pyjson.Dumps(newOptions)
		if err != nil {
			return err
		}
		optionsText = &text
		args = append(args, text)
		set = append(set, fmt.Sprintf("sync_options = $%d::json", len(args)))
	}
	if newActive != config.IsActive {
		add("is_active", newActive)
	}
	if len(set) == 0 {
		return nil
	}
	add("updated_at", now)
	if _, err := tx.Exec(ctx, `UPDATE sync_configurations SET `+strings.Join(set, ", ")+` WHERE id = $1`, args...); err != nil {
		return fmt.Errorf("update sync configuration: %w", err)
	}
	if targetsText != nil {
		config.SyncTargets = targetsText
	}
	if optionsText != nil {
		config.SyncOptions = optionsText
	}
	config.IsActive = newActive
	config.UpdatedAt = now
	return nil
}

// writeGitHubRuntimeOptions is update_sync_config's GitHub branch: the
// canonical work-item runtime options snapshot from, in rising precedence,
// the config's current options, its integration's config (the org's own
// integration only), the work-items dataset's options and the body's
// options; written onto the config's options (returned), the integration's
// config and the dataset's options, each only when it changes.
func (h *handlers) writeGitHubRuntimeOptions(ctx context.Context, tx pgx.Tx, org string, integrationID uuid.UUID,
	current, bodyOptions *pyjson.Object, now time.Time) (*pyjson.Object, error) {
	var integrationOrg string
	var integrationConfigText *string
	integrationFound := true
	err := tx.QueryRow(ctx, `SELECT org_id, config::text FROM integrations WHERE id = $1`, integrationID).Scan(&integrationOrg, &integrationConfigText)
	if errors.Is(err, pgx.ErrNoRows) {
		integrationFound = false
	} else if err != nil {
		return nil, fmt.Errorf("read integration: %w", err)
	}
	scoped := integrationFound && integrationOrg == org
	integrationOptions := pyjson.NewObject()
	var storedIntegrationConfig pyjson.Value
	if scoped {
		if storedIntegrationConfig, err = decodeStored(integrationConfigText); err != nil {
			return nil, err
		}
		if integrationOptions, err = plainDict(storedIntegrationConfig); err != nil {
			return nil, err
		}
	}
	var datasetID *uuid.UUID
	var datasetOptionsText *string
	var id uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id, options::text FROM integration_datasets WHERE org_id = $1 AND integration_id = $2 AND dataset_key = 'work-items'`,
		org, integrationID).Scan(&id, &datasetOptionsText)
	if err == nil {
		datasetID = &id
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("read work-items dataset: %w", err)
	}
	datasetOptions := pyjson.NewObject()
	var storedDatasetOptions pyjson.Value
	if datasetID != nil {
		if storedDatasetOptions, err = decodeStored(datasetOptionsText); err != nil {
			return nil, err
		}
		if datasetOptions, err = plainDict(storedDatasetOptions); err != nil {
			return nil, err
		}
	}
	source := pyjson.NewObject()
	for _, layer := range []*pyjson.Object{current, integrationOptions, datasetOptions, bodyOptions} {
		for _, key := range layer.Keys() {
			value, _ := layer.Get(key)
			source.Set(key, value)
		}
	}
	canonical, err := snapshotGitHubWorkItemRuntimeOptions(source, h.lookupEnv)
	if err != nil {
		return nil, err
	}
	overlay := func(base *pyjson.Object) *pyjson.Object {
		out := pyjson.NewObject()
		for _, key := range base.Keys() {
			value, _ := base.Get(key)
			out.Set(key, value)
		}
		for _, key := range canonical.Keys() {
			value, _ := canonical.Get(key)
			out.Set(key, value)
		}
		return out
	}
	if scoped {
		next := overlay(integrationOptions)
		if !pyjson.Equal(storedIntegrationConfig, next) {
			text, err := pyjson.Dumps(next)
			if err != nil {
				return nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE integrations SET config = $2::json, updated_at = $3 WHERE id = $1`, integrationID, text, now); err != nil {
				return nil, fmt.Errorf("update integration config: %w", err)
			}
		}
	}
	if datasetID != nil {
		next := overlay(datasetOptions)
		if !pyjson.Equal(storedDatasetOptions, next) {
			text, err := pyjson.Dumps(next)
			if err != nil {
				return nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE integration_datasets SET options = $2::json WHERE id = $1`, *datasetID, text); err != nil {
				return nil, fmt.Errorf("update work-items dataset options: %w", err)
			}
		}
	}
	return overlay(current), nil
}

// updateServicesDatasetMappings is update_sync_config's PagerDuty branch:
// the integration's "services" dataset (when there is one) gets the body's
// service_repository_mappings over its options.
func updateServicesDatasetMappings(ctx context.Context, tx pgx.Tx, org string, integrationID uuid.UUID, mappings pyjson.Value) error {
	var id uuid.UUID
	var optionsText *string
	err := tx.QueryRow(ctx, `SELECT id, options::text FROM integration_datasets WHERE org_id = $1 AND integration_id = $2 AND dataset_key = 'services'`,
		org, integrationID).Scan(&id, &optionsText)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read services dataset: %w", err)
	}
	stored, err := decodeStored(optionsText)
	if err != nil {
		return err
	}
	current, err := plainDict(stored)
	if err != nil {
		return err
	}
	current.Set("service_repository_mappings", mappings)
	if pyjson.Equal(stored, current) {
		return nil
	}
	text, err := pyjson.Dumps(current)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE integration_datasets SET options = $2::json WHERE id = $1`, id, text); err != nil {
		return fmt.Errorf("update services dataset options: %w", err)
	}
	return nil
}

// cascadeToChildren is update_sync_config's legacy child cascade: every
// config whose parent is this one (by parent_id alone, as Python selects
// them) takes the body's targets and activity, and the schedule keys the
// body set or cleared; then each child's sync job is upserted.
func (h *handlers) cascadeToChildren(ctx context.Context, tx pgx.Tx, org string, parentID uuid.UUID, in syncConfigUpdate,
	options *pyjson.Object, cleared map[string]bool, optionsProvided bool, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT `+syncConfigColumns+` FROM sync_configurations WHERE parent_id = $1`, parentID)
	if err != nil {
		return fmt.Errorf("read child configs: %w", err)
	}
	var children []*syncConfig
	for rows.Next() {
		child, err := scanSyncConfig(rows)
		if err != nil {
			rows.Close()
			return err
		}
		children = append(children, child)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read child configs: %w", err)
	}
	for _, child := range children {
		storedTargets, err := decodeStored(child.SyncTargets)
		if err != nil {
			return err
		}
		storedOptions, err := decodeStored(child.SyncOptions)
		if err != nil {
			return err
		}
		newTargets := storedTargets
		if in.syncTargetsSet {
			list := make([]pyjson.Value, len(in.syncTargets))
			for index, target := range in.syncTargets {
				list[index] = target
			}
			newTargets = list
		}
		newActive := child.IsActive
		if in.isActive != nil {
			newActive = *in.isActive
		}
		newOptions := storedOptions
		if optionsProvided {
			childOptions, err := plainDict(storedOptions)
			if err != nil {
				return err
			}
			changed := false
			for _, key := range []string{"schedule_cron", "timezone", "initial_sync_depth"} {
				if cleared[key] {
					if _, ok := childOptions.Get(key); ok {
						childOptions.Delete(key)
						changed = true
					}
				} else if value, ok := options.Get(key); ok {
					childOptions.Set(key, value)
					changed = true
				}
			}
			if changed {
				newOptions = childOptions
			}
		}
		if err := writeConfigChanges(ctx, tx, child, storedTargets, newTargets, storedOptions, newOptions, newActive, now); err != nil {
			return err
		}
	}
	for _, child := range children {
		if err := upsertScheduledJob(ctx, tx, org, child.ID, now); err != nil {
			return err
		}
	}
	return nil
}

// discoverIntegrationSources is discover_sources_for_integration for the
// update path, run after the update commits through the scheduler's
// source discovery (the one implementation): the integration's provider
// and credential, its planner-managed parent config's id and options (the
// integration's own config when it has none). A failure is logged at Error
// as <event>_failed (Python's own event) and swallowed; a success is logged
// at Info as <event> with the outcome and counts (Go-only, as on create: a
// discovery that finds nothing still answers 200).
func (h *handlers) discoverIntegrationSources(ctx context.Context, org string, integrationID uuid.UUID, event string, attrs ...any) {
	fail := func(err any) {
		h.logger.ErrorContext(ctx, event+"_failed", append([]any{"org_id", org, "error", err}, attrs...)...)
	}
	if h.discovery == nil {
		fail("source discovery is unavailable in this process")
		return
	}
	var provider string
	var credentialID *uuid.UUID
	var integrationConfig *string
	if err := h.pool.QueryRow(ctx, `SELECT provider, credential_id, config::text FROM integrations WHERE id = $1`, integrationID).
		Scan(&provider, &credentialID, &integrationConfig); err != nil {
		fail(err)
		return
	}
	var configID string
	var plannerOptions *string
	err := h.pool.QueryRow(ctx, `SELECT id::text, sync_options::text FROM sync_configurations
WHERE integration_id = $1 AND planner_managed IS true AND parent_id IS NULL`, integrationID).Scan(&configID, &plannerOptions)
	plannerFound := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		fail(err)
		return
	}
	optionsText := integrationConfig
	if plannerFound {
		optionsText = plannerOptions
	}
	var syncOptions map[string]any
	if optionsText != nil {
		if err := json.Unmarshal([]byte(*optionsText), &syncOptions); err != nil {
			syncOptions = nil
		}
	}
	var credential *string
	if credentialID != nil {
		text := credentialID.String()
		credential = &text
	}
	h.runIntegrationDiscovery(ctx, schedsync.SourceDiscoveryArgs{
		OrgID: org, IntegrationID: integrationID.String(), CredentialID: credential,
		Provider: provider, SyncOptions: syncOptions, ConfigID: configID, PlannerManaged: plannerFound,
	}, event, attrs...)
}

// runIntegrationDiscovery runs one discovery and logs its result: the
// Error event <event>_failed on failure, else the Info event <event> with
// the outcome and counts. Exactly one event per run.
func (h *handlers) runIntegrationDiscovery(ctx context.Context, args schedsync.SourceDiscoveryArgs, event string, attrs ...any) {
	report, err := h.discovery.Discover(ctx, args)
	if err != nil {
		h.logger.ErrorContext(ctx, event+"_failed", append([]any{"org_id", args.OrgID, "error", err}, attrs...)...)
		return
	}
	h.logger.InfoContext(ctx, event, append([]any{"org_id", args.OrgID, "integration_id", args.IntegrationID,
		"outcome", report.Outcome, "created", report.Created, "existing", report.Existing}, attrs...)...)
}
