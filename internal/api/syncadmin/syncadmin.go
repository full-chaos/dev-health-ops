// Package syncadmin serves dho api's sync admin reads, ported route for
// route from api/admin/routers/sync.py and the sync-run read of
// api/admin/routers/integrations.py:
//
//	GET /api/v1/admin/sync-configs/auto-import-capabilities
//	GET /api/v1/admin/sync-targets
//	GET /api/v1/admin/sync-configs
//	GET /api/v1/admin/sync-configs/{config_id}
//	GET /api/v1/admin/sync-configs/{config_id}/repositories
//	GET /api/v1/admin/sync-configs/{config_id}/jobs
//	GET /api/v1/admin/sync-configs/{config_id}/coverage
//	GET /api/v1/admin/backfill-jobs
//	GET /api/v1/admin/backfill-jobs/{job_id}
//	GET /api/v1/admin/sync-runs/{run_id}
//	GET /api/v1/admin/sync-runs/{run_id}/units
//
// Every route is Depends(get_admin_org_id): policy.AdminOrg, and the org is
// the caller's own org_id claim. As in FastAPI, the dependency answers
// first, then the query parameters are validated (every error in one 422),
// then the handler runs.
package syncadmin

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

const prefix = "/api/v1/admin"

// Deps is this area's dependency set. Pool is the api role's pool; Guard
// the protected-route runtime. LookupEnv reads HIDE_MIGRATED_CHILD_CONFIGS
// per request as the Python route does (nil means os.LookupEnv).
type Deps struct {
	Pool *pgxpool.Pool
	// ClickHouse is the api's own ClickHouse login; nil when the api has
	// none (the backfill job detail then carries no metrics diagnostics,
	// as the Python route does without a ClickHouse URI).
	ClickHouse driver.Conn
	Guard      *policy.Guard
	Logger     *slog.Logger
	LookupEnv  func(string) (string, bool)
	// Decryptor reads stored integration credentials (the api's Fernet
	// key); nil reads every credential as undecryptable.
	Decryptor providerfoundation.CredentialDecryptor
	// Now is the clock of the run units freshness read and of the writes
	// (nil: time.Now).
	Now func() time.Time
	// GitLabHTTP is the GitLab listing's HTTP client (nil: the client's
	// default, 15 s timeout, no redirects followed).
	GitLabHTTP *http.Client
	// JiraHTTP is the create path's Jira project discovery HTTP client
	// (nil: 45 s timeout, no redirects followed, as the scheduler's).
	JiraHTTP *http.Client
}

// Routes is the area's route set.
func Routes(deps Deps) []httpapi.Route {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	lookup := deps.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	// One clock for the run units freshness read and the writes: Python
	// reads datetime.now in both; nil means the wall clock.
	clock := deps.Now
	if clock == nil {
		clock = time.Now
	}
	h := &handlers{
		store:      store{pool: deps.Pool},
		writes:     store{pool: deps.Pool},
		features:   licensing.PostgresStore{Pool: deps.Pool},
		logger:     logger,
		lookupEnv:  lookup,
		pool:       deps.Pool,
		decryptor:  deps.Decryptor,
		clock:      clock,
		gitlabHTTP: deps.GitLabHTTP,
	}
	if deps.ClickHouse != nil {
		h.diagnostics = clickhouseDiagnostics{conn: deps.ClickHouse}
		h.clickhouse = deps.ClickHouse
	}
	h.discovery = newCreateDiscovery(deps, logger, clock)
	wrap := func(handler http.HandlerFunc) http.Handler { return deps.Guard.Wrap(policy.AdminOrg, handler) }
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: prefix + "/sync-configs/auto-import-capabilities", Handler: wrap(h.autoImportCapabilities)},
		{Method: http.MethodGet, Pattern: prefix + "/sync-targets", Handler: wrap(h.syncTargets)},
		{Method: http.MethodGet, Pattern: prefix + "/sync-configs", Handler: wrap(h.listSyncConfigs)},
		{Method: http.MethodPost, Pattern: prefix + "/sync-configs",
			Handler: deps.Guard.BodyFirst(policy.AdminOrg, http.HandlerFunc(h.createSyncConfig))},
		{Method: http.MethodPost, Pattern: prefix + "/sync-configs/batch",
			Handler: deps.Guard.BodyFirst(policy.AdminOrg, http.HandlerFunc(h.batchCreateSyncConfigs))},
		{Method: http.MethodGet, Pattern: prefix + "/sync-configs/{config_id}", Handler: wrap(h.getSyncConfig)},
		{Method: http.MethodPatch, Pattern: prefix + "/sync-configs/{config_id}",
			Handler: deps.Guard.BodyFirst(policy.AdminOrg, http.HandlerFunc(h.updateSyncConfig))},
		{Method: http.MethodDelete, Pattern: prefix + "/sync-configs/{config_id}", Handler: wrap(h.deleteSyncConfig)},
		{Method: http.MethodPost, Pattern: prefix + "/sync-configs/{config_id}/trigger", Handler: wrap(h.triggerSyncConfig)},
		{Method: http.MethodPost, Pattern: prefix + "/sync-configs/{config_id}/backfill",
			Handler: deps.Guard.BodyFirst(policy.AdminOrg, http.HandlerFunc(h.backfillSyncConfig))},
		{Method: http.MethodGet, Pattern: prefix + "/sync-configs/{config_id}/repositories", Handler: wrap(h.getRepositories)},
		{Method: http.MethodPut, Pattern: prefix + "/sync-configs/{config_id}/repositories",
			Handler: deps.Guard.BodyFirst(policy.AdminOrg, http.HandlerFunc(h.replaceRepositories))},
		{Method: http.MethodGet, Pattern: prefix + "/sync-configs/{config_id}/jobs", Handler: wrap(h.listJobs)},
		{Method: http.MethodGet, Pattern: prefix + "/sync-configs/{config_id}/coverage", Handler: wrap(h.getCoverage)},
		{Method: http.MethodGet, Pattern: prefix + "/backfill-jobs", Handler: wrap(h.listBackfillJobs)},
		{Method: http.MethodGet, Pattern: prefix + "/backfill-jobs/{job_id}", Handler: wrap(h.getBackfillJob)},
		{Method: http.MethodGet, Pattern: prefix + "/sync-runs/{run_id}", Handler: wrap(h.getSyncRun)},
		{Method: http.MethodGet, Pattern: prefix + "/sync-runs/{run_id}/units", Handler: wrap(h.getRunUnits)},
	}
}

type handlers struct {
	store     reader
	writes    writer
	features  licensing.Store
	logger    *slog.Logger
	lookupEnv func(string) (string, bool)
	pool      *pgxpool.Pool
	decryptor providerfoundation.CredentialDecryptor
	// clock is build_dataset_freshness's datetime.now(timezone.utc) and
	// the writes' clock.
	clock      func() time.Time
	gitlabHTTP *http.Client
	// clickhouse is the api's ClickHouse login (nil when it has none): the
	// trigger route's work items count.
	clickhouse driver.Conn
	// diagnostics is nil when the api has no ClickHouse login.
	diagnostics diagnosticsReader
	// discovery is the create path's Jira project discovery; nil when the
	// process has no pool or credential decryptor.
	discovery schedsync.SourceDiscoveryExecutor
}

// now is the writes' clock.
func (h *handlers) now() time.Time {
	if h.clock != nil {
		return h.clock()
	}
	return time.Now()
}

// orgID is get_admin_org_id's value; the guard already refused an empty
// claim.
func orgID(r *http.Request) string { return policy.UserFrom(r.Context()).OrgID }

// fail logs a failure with the route's fields and answers the Python api's
// bare 500.
func (h *handlers) fail(w http.ResponseWriter, r *http.Request, step string, err error) {
	h.logger.ErrorContext(r.Context(), "sync admin: request failed",
		slog.String("route", r.Pattern), slog.String("step", step), slog.String("error", err.Error()))
	policy.WriteInternal(w)
}

func writeQueryErrors(w http.ResponseWriter, errs pybody.Errors) {
	policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
}

func writeConfigNotFound(w http.ResponseWriter) {
	policy.WriteDetail(w, http.StatusNotFound, "Sync configuration not found", nil)
}

// autoImportCapabilities is get_auto_import_capabilities: the static
// per-provider table of providers/team_capabilities.py.
func (h *handlers) autoImportCapabilities(w http.ResponseWriter, _ *http.Request) {
	policy.WriteModel(w, http.StatusOK, autoImportCapabilityTable(), nil)
}

// autoImportCapabilityTable is _AUTO_IMPORT_CAPABILITIES in its dict order,
// rendered from the one capability table the write checks read.
func autoImportCapabilityTable() *pyjson.Object {
	table := pyjson.NewObject()
	for _, provider := range autoImportCapabilityProviders {
		capability := autoImportCapabilityByProvider[provider]
		entry := pyjson.NewObject()
		for _, category := range autoImportCategories {
			entry.Set(category.category, capability.supports[category.category])
		}
		reasons := pyjson.NewObject()
		for _, reason := range capability.reasons {
			reasons.Set(reason[0], reason[1])
		}
		entry.Set("reasons", reasons)
		table.Set(provider, entry)
	}
	return table
}

// syncTargetProviders is PROVIDER_SYNC_TARGETS's key order.
var syncTargetProviders = []string{"github", "gitlab", "jira", "linear", "launchdarkly", "pagerduty"}

// canonicalIncidentFeatureKey and gatedSyncTargets are
// sync/canonical_incident_gate.py's CANONICAL_INCIDENT_FEATURE_KEY and
// _GATED_SYNC_TARGETS.
const canonicalIncidentFeatureKey = "canonical_incident_ingestion"

var gatedSyncTargets = map[string]bool{"incidents": true, "operational": true}

// syncTargets is get_provider_sync_targets: every provider's supported
// legacy targets, without the canonical-incident targets unless the org's
// canonical_incident_ingestion feature decision allows them.
func (h *handlers) syncTargets(w http.ResponseWriter, r *http.Request) {
	enabled, err := h.canonicalIncidentEnabled(r.Context(), orgID(r))
	if err != nil {
		h.fail(w, r, "canonical_incident_feature", err)
		return
	}
	out := pyjson.NewObject()
	for _, provider := range syncTargetProviders {
		targets := []pyjson.Value{}
		for _, target := range providersync.SupportedLegacyTargets(provider) {
			if !enabled && gatedSyncTargets[pythonparity.Lower(target)] {
				continue
			}
			targets = append(targets, target)
		}
		out.Set(provider, targets)
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// canonicalIncidentEnabled is is_canonical_incident_feature_enabled_async:
// false for an org id uuid.UUID() refuses, else the feature decision.
func (h *handlers) canonicalIncidentEnabled(ctx context.Context, org string) (bool, error) {
	parsed, err := pythonparity.ParseUUID(org)
	if err != nil {
		return false, nil
	}
	decision, err := h.features.Decide(ctx, parsed.String(), canonicalIncidentFeatureKey)
	if err != nil {
		return false, err
	}
	return decision.Allowed, nil
}

// hideMigratedChildConfigs is list_sync_configs's
// HIDE_MIGRATED_CHILD_CONFIGS read: stripped, lower-cased, one of
// 1/true/yes/on.
func (h *handlers) hideMigratedChildConfigs() bool {
	raw, _ := h.lookupEnv("HIDE_MIGRATED_CHILD_CONFIGS")
	switch pythonparity.Lower(pythonparity.Strip(raw)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// listSyncConfigs is list_sync_configs.
func (h *handlers) listSyncConfigs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	query := r.URL.Query()
	var errs pybody.Errors
	activeOnly, _ := errs.QueryBool("active_only", pybody.LastQuery(query, "active_only"), false)
	parentOnly, _ := errs.QueryBool("parent_only", pybody.LastQuery(query, "parent_only"), false)
	includeMigrated, _ := errs.QueryBool("include_migrated", pybody.LastQuery(query, "include_migrated"), false)
	if len(errs) > 0 {
		writeQueryErrors(w, errs)
		return
	}
	org := orgID(r)
	configs, err := h.store.listConfigs(ctx, org, activeOnly)
	if err != nil {
		h.fail(w, r, "list_configs", err)
		return
	}
	if parentOnly {
		configs = filterConfigs(configs, func(c *syncConfig) bool { return c.ParentID == nil })
	}
	if h.hideMigratedChildConfigs() && !includeMigrated {
		configs = filterConfigs(configs, func(c *syncConfig) bool { return c.ParentID == nil && c.SourceID == nil })
	}
	var parentIDs []uuid.UUID
	var integrationIDs []uuid.UUID
	for _, config := range configs {
		if config.ParentID == nil {
			parentIDs = append(parentIDs, config.ID)
		}
		if config.IntegrationID != nil {
			integrationIDs = append(integrationIDs, *config.IntegrationID)
		}
	}
	counts, err := h.store.childrenCounts(ctx, parentIDs)
	if err != nil {
		h.fail(w, r, "children_counts", err)
		return
	}
	credentials, err := h.store.credentialIDs(ctx, org, integrationIDs)
	if err != nil {
		h.fail(w, r, "credential_ids", err)
		return
	}
	list := make([]pyjson.Value, 0, len(configs))
	for _, config := range configs {
		var childrenCount pyjson.Value
		if count, ok := counts[config.ID]; ok {
			childrenCount = count
		}
		var credentialID *uuid.UUID
		if config.IntegrationID != nil {
			credentialID = credentials[*config.IntegrationID]
		}
		body, err := syncConfigResponse(config, childrenCount, credentialID)
		if err != nil {
			h.fail(w, r, "render_config", err)
			return
		}
		list = append(list, body)
	}
	policy.WriteModel(w, http.StatusOK, list, nil)
}

func filterConfigs(configs []*syncConfig, keep func(*syncConfig) bool) []*syncConfig {
	var out []*syncConfig
	for _, config := range configs {
		if keep(config) {
			out = append(out, config)
		}
	}
	return out
}

// configFromPath is SyncConfigurationService.get_by_id on the path's
// config_id: an id uuid.UUID() refuses is not found, like a missing row.
func (h *handlers) configFromPath(w http.ResponseWriter, r *http.Request) (*syncConfig, bool) {
	id, err := pythonparity.ParseUUID(r.PathValue("config_id"))
	if err != nil {
		writeConfigNotFound(w)
		return nil, false
	}
	config, err := h.store.configByID(r.Context(), orgID(r), id)
	if err != nil {
		h.fail(w, r, "get_config", err)
		return nil, false
	}
	if config == nil {
		writeConfigNotFound(w)
		return nil, false
	}
	return config, true
}

// credentialForConfig is _integration_credential_id_for_config.
func (h *handlers) credentialForConfig(ctx context.Context, org string, config *syncConfig) (*uuid.UUID, error) {
	if config.IntegrationID == nil {
		return nil, nil
	}
	found, err := h.store.credentialIDs(ctx, org, []uuid.UUID{*config.IntegrationID})
	if err != nil {
		return nil, err
	}
	return found[*config.IntegrationID], nil
}

// getSyncConfig is get_sync_config.
func (h *handlers) getSyncConfig(w http.ResponseWriter, r *http.Request) {
	config, ok := h.configFromPath(w, r)
	if !ok {
		return
	}
	credentialID, err := h.credentialForConfig(r.Context(), orgID(r), config)
	if err != nil {
		h.fail(w, r, "credential_id", err)
		return
	}
	body, err := syncConfigResponse(config, nil, credentialID)
	if err != nil {
		h.fail(w, r, "render_config", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, body, nil)
}

// syncConfigResponse is _sync_config_to_response.
func syncConfigResponse(config *syncConfig, childrenCount pyjson.Value, credentialID *uuid.UUID) (*pyjson.Object, error) {
	targetsValue, err := decodeStored(config.SyncTargets)
	if err != nil {
		return nil, err
	}
	targets, err := pyStringList(targetsValue)
	if err != nil {
		return nil, err
	}
	optionsValue, err := decodeStored(config.SyncOptions)
	if err != nil {
		return nil, err
	}
	options, err := pyDict(optionsValue)
	if err != nil {
		return nil, err
	}
	out := pyjson.NewObject()
	out.Set("id", config.ID.String())
	out.Set("name", config.Name)
	out.Set("provider", config.Provider)
	out.Set("credential_id", uuidOrNone(credentialID))
	out.Set("sync_targets", targets)
	out.Set("sync_options", options)
	out.Set("is_active", config.IsActive)
	out.Set("parent_id", uuidOrNone(config.ParentID))
	out.Set("children_count", childrenCount)
	out.Set("last_sync_at", pyTimeOrNone(config.LastSyncAt))
	if config.LastSyncSuccess == nil {
		out.Set("last_sync_success", nil)
	} else {
		out.Set("last_sync_success", *config.LastSyncSuccess)
	}
	out.Set("last_sync_error", stringOrNone(config.LastSyncError))
	out.Set("created_at", pyTime(config.CreatedAt))
	out.Set("updated_at", pyTime(config.UpdatedAt))
	return out, nil
}

func uuidOrNone(id *uuid.UUID) pyjson.Value {
	if id == nil {
		return nil
	}
	return id.String()
}
