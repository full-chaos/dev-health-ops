package admin

import (
	"context"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// JobRunStatus values (models/settings.py JobRunStatus).
const (
	jobRunPending   = 0
	jobRunRunning   = 1
	jobRunSuccess   = 2
	jobRunFailed    = 3
	jobRunCancelled = 4
)

func (h *handlers) setupStatusRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: governancePrefix + "/setup/status", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getSetupStatus))},
	}
}

// setupConfig is the columns of one sync_configurations row this route reads.
type setupConfig struct {
	ID              uuid.UUID
	Provider        string
	ParentID        *uuid.UUID
	IsActive        bool
	SyncOptions     pyjson.Value
	IntegrationID   *uuid.UUID
	LastSyncSuccess *bool
	LastSyncError   *string
	Created         string // str(created_at), the sort key setup.py compares
}

// setupRun is the columns of one job_runs row this route reads.
type setupRun struct {
	Status int
	Result pyjson.Value
	Error  *string
}

// syncStatus is setup.py's _sync_status_from_job_run.
func (r setupRun) syncStatus() string {
	var status string
	switch r.Status {
	case jobRunPending:
		status = "pending"
	case jobRunRunning:
		status = "running"
	case jobRunSuccess:
		status = "complete"
	case jobRunFailed, jobRunCancelled:
		status = "failed"
	default:
		status = "running"
	}
	if object, ok := r.Result.(*pyjson.Object); ok {
		if value, present := object.Get("sync_run_status"); present {
			if text, isText := value.(string); isText && (text == "partial_failed" || text == "partial") {
				status = "partial"
			}
		}
	}
	return status
}

func isRepoProvider(provider string) bool { return provider == "github" || provider == "gitlab" }

// getSetupStatus is setup.py's get_setup_status: a read-only projection of the
// first-run state over credentials, sync configurations and planner runs.
func (h *handlers) getSetupStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	status, err := h.store.setupStatus(ctx, orgID)
	if err != nil {
		h.internalError(ctx, w, "read setup status", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, status, nil)
}

// setupStatus computes the response object in SetupStatusResponse's field order.
func (s pgStore) setupStatus(ctx context.Context, orgID string) (*pyjson.Object, error) {
	providers, err := s.activeCredentialProviders(ctx, orgID)
	if err != nil {
		return nil, err
	}
	hasIntegration := len(providers) > 0
	configs, err := s.setupConfigs(ctx, orgID)
	if err != nil {
		return nil, err
	}
	primary := selectPrimaryConfig(configs)
	hasSyncConfig := primary != nil

	var syncConfigID *string
	syncStatus := "none"
	firstSyncStarted := false
	firstSyncCompleted := false
	for _, config := range configs {
		if config.ParentID == nil && config.LastSyncSuccess != nil && *config.LastSyncSuccess {
			firstSyncCompleted = true
			break
		}
	}
	if !firstSyncCompleted {
		completed, err := s.hasCompletedParentSync(ctx, orgID, configs)
		if err != nil {
			return nil, err
		}
		firstSyncCompleted = completed
	}
	selectedRepositories := int64(0)
	var lastSyncError *string
	canStartSync := false
	hasRepoSelection := true

	if primary != nil {
		id := primary.ID.String()
		syncConfigID = &id
		provider := strings.ToLower(primary.Provider)
		if primary.IntegrationID != nil {
			if err := s.Pool.QueryRow(ctx, `SELECT count(id) FROM integration_sources WHERE org_id = $1 AND integration_id = $2 AND is_enabled IS true`,
				orgID, *primary.IntegrationID).Scan(&selectedRepositories); err != nil {
				return nil, err
			}
		}
		if isRepoProvider(provider) {
			allRepos := false
			if options, ok := primary.SyncOptions.(*pyjson.Object); ok {
				if value, present := options.Get("all_repos"); present {
					allRepos = pyjson.Truthy(value)
				}
			}
			hasRepoSelection = selectedRepositories > 0 || allRepos
		}

		latest, latestConfig, err := s.latestActiveParentRun(ctx, orgID, configs)
		if err != nil {
			return nil, err
		}
		if latest == nil {
			latest, err = s.latestJobRun(ctx, orgID, primary.ID)
			if err != nil {
				return nil, err
			}
			latestConfig = primary
		}
		if latest != nil {
			firstSyncStarted = true
			syncStatus = latest.syncStatus()
			if syncStatus == "complete" {
				firstSyncCompleted = true
			}
			if syncStatus == "failed" {
				// `run.error or config.last_sync_error`: an empty error text is falsy.
				if latest.Error != nil && *latest.Error != "" {
					lastSyncError = latest.Error
				} else {
					lastSyncError = latestConfig.LastSyncError
				}
			}
		} else {
			// No planner run yet: fall back to the config-level last-sync facts.
			switch {
			case primary.LastSyncSuccess != nil && *primary.LastSyncSuccess:
				syncStatus = "complete"
				firstSyncStarted = true
				firstSyncCompleted = true
			case (primary.LastSyncError != nil && *primary.LastSyncError != "") || (primary.LastSyncSuccess != nil && !*primary.LastSyncSuccess):
				syncStatus = "failed"
				firstSyncStarted = true
				lastSyncError = primary.LastSyncError
			default:
				syncStatus = "none"
			}
		}
		inFlight := syncStatus == "pending" || syncStatus == "running"
		canStartSync = primary.IsActive && !inFlight && hasRepoSelection
	}

	var nextAction string
	var blocker *string
	text := func(value string) *string { return &value }
	switch {
	case !hasIntegration:
		nextAction, blocker = "connect_integration", text("No integration connected")
	case !hasSyncConfig:
		nextAction = "create_sync_config"
		for _, provider := range providers {
			if isRepoProvider(provider) {
				nextAction = "select_repositories"
				break
			}
		}
		blocker = text("No sync configuration")
	case syncStatus == "failed":
		nextAction = "start_sync"
		if lastSyncError != nil && *lastSyncError != "" {
			blocker = lastSyncError
		} else {
			blocker = text("Last sync failed")
		}
	case syncStatus == "pending" || syncStatus == "running" || syncStatus == "complete" || syncStatus == "partial":
		nextAction = "complete"
	case !hasRepoSelection:
		nextAction, blocker = "select_repositories", text("No repositories selected")
	default:
		nextAction = "start_sync"
	}

	out := pyjson.NewObject()
	out.Set("has_integration", hasIntegration)
	providerValues := make([]pyjson.Value, len(providers))
	for index, provider := range providers {
		providerValues[index] = provider
	}
	out.Set("providers", providerValues)
	out.Set("has_sync_config", hasSyncConfig)
	out.Set("sync_config_id", optionalString(syncConfigID))
	out.Set("first_sync_started", firstSyncStarted)
	out.Set("first_sync_completed", firstSyncCompleted)
	out.Set("sync_status", syncStatus)
	out.Set("selected_repositories_count", pyjson.Int{Int: big.NewInt(selectedRepositories)})
	out.Set("last_sync_error", optionalString(lastSyncError))
	out.Set("can_start_sync", canStartSync)
	out.Set("next_action", nextAction)
	out.Set("blocker", optionalString(blocker))
	return out, nil
}

// activeCredentialProviders is sorted({c.provider for c in active credentials}).
func (s pgStore) activeCredentialProviders(ctx context.Context, orgID string) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `SELECT DISTINCT provider FROM integration_credentials WHERE org_id = $1 AND is_active IS true`, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	providers := []string{}
	for rows.Next() {
		var provider string
		if err := rows.Scan(&provider); err != nil {
			return nil, err
		}
		providers = append(providers, provider)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(providers)
	return providers, nil
}

func (s pgStore) setupConfigs(ctx context.Context, orgID string) ([]setupConfig, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id, provider, parent_id, is_active, sync_options, integration_id, last_sync_success, last_sync_error, created_at
FROM sync_configurations WHERE org_id = $1`, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var configs []setupConfig
	for rows.Next() {
		var (
			config  setupConfig
			options []byte
			created time.Time
		)
		if err := rows.Scan(&config.ID, &config.Provider, &config.ParentID, &config.IsActive, &options, &config.IntegrationID,
			&config.LastSyncSuccess, &config.LastSyncError, &created); err != nil {
			return nil, err
		}
		if len(options) > 0 {
			decoded, err := pyjson.Decode(options)
			if err != nil {
				return nil, err
			}
			config.SyncOptions = decoded
		}
		config.Created = strings.Replace(pythonparity.IsoformatUTC(created), "T", " ", 1)
		configs = append(configs, config)
	}
	return configs, rows.Err()
}

// selectPrimaryConfig is setup.py's _select_primary_config: the parent config
// sorted by (is_active, str(created_at)) descending, stably.
func selectPrimaryConfig(configs []setupConfig) *setupConfig {
	var parents []*setupConfig
	for index := range configs {
		if configs[index].ParentID == nil {
			parents = append(parents, &configs[index])
		}
	}
	if len(parents) == 0 {
		return nil
	}
	key := func(config *setupConfig) (int, string) {
		if config.IsActive {
			return 1, config.Created
		}
		return 0, config.Created
	}
	sort.SliceStable(parents, func(a, b int) bool {
		activeA, createdA := key(parents[a])
		activeB, createdB := key(parents[b])
		if activeA != activeB {
			return activeA > activeB
		}
		return createdA > createdB
	})
	return parents[0]
}

func scanSetupRun(rows pgx.Rows, configID *uuid.UUID) (setupRun, error) {
	var (
		run    setupRun
		result []byte
	)
	var err error
	if configID != nil {
		err = rows.Scan(&run.Status, &result, &run.Error, configID)
	} else {
		err = rows.Scan(&run.Status, &result, &run.Error)
	}
	if err != nil {
		return run, err
	}
	if len(result) > 0 {
		decoded, err := pyjson.Decode(result)
		if err != nil {
			return run, err
		}
		run.Result = decoded
	}
	return run, nil
}

// hasCompletedParentSync is setup.py's _has_completed_parent_sync.
func (s pgStore) hasCompletedParentSync(ctx context.Context, orgID string, configs []setupConfig) (bool, error) {
	var ids []uuid.UUID
	for _, config := range configs {
		if config.ParentID == nil {
			ids = append(ids, config.ID)
		}
	}
	if len(ids) == 0 {
		return false, nil
	}
	rows, err := s.Pool.Query(ctx, `SELECT jr.status, jr.result, jr.error FROM job_runs jr JOIN scheduled_jobs sj ON jr.job_id = sj.id
WHERE sj.org_id = $1 AND sj.sync_config_id = ANY($2) AND jr.status = $3 ORDER BY jr.created_at DESC`, orgID, ids, jobRunSuccess)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		run, err := scanSetupRun(rows, nil)
		if err != nil {
			return false, err
		}
		if run.syncStatus() == "complete" {
			return true, nil
		}
	}
	return false, rows.Err()
}

// latestJobRun is setup.py's _latest_job_run: the newest run of one config.
func (s pgStore) latestJobRun(ctx context.Context, orgID string, configID uuid.UUID) (*setupRun, error) {
	rows, err := s.Pool.Query(ctx, `SELECT jr.status, jr.result, jr.error FROM job_runs jr JOIN scheduled_jobs sj ON jr.job_id = sj.id
WHERE sj.org_id = $1 AND sj.sync_config_id = $2 ORDER BY jr.created_at DESC LIMIT 1`, orgID, configID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	run, err := scanSetupRun(rows, nil)
	if err != nil {
		return nil, err
	}
	return &run, rows.Err()
}

// latestActiveParentRun is setup.py's _latest_active_parent_run: the newest run
// of each ACTIVE parent config, then the first of running, pending, failed,
// cancelled among them (in newest-first order within a status).
func (s pgStore) latestActiveParentRun(ctx context.Context, orgID string, configs []setupConfig) (*setupRun, *setupConfig, error) {
	byID := map[uuid.UUID]*setupConfig{}
	var ids []uuid.UUID
	for index := range configs {
		if configs[index].ParentID == nil && configs[index].IsActive {
			byID[configs[index].ID] = &configs[index]
			ids = append(ids, configs[index].ID)
		}
	}
	if len(ids) == 0 {
		return nil, nil, nil
	}
	rows, err := s.Pool.Query(ctx, `SELECT jr.status, jr.result, jr.error, sj.sync_config_id FROM job_runs jr JOIN scheduled_jobs sj ON jr.job_id = sj.id
WHERE sj.org_id = $1 AND sj.sync_config_id = ANY($2) ORDER BY jr.created_at DESC`, orgID, ids)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var order []uuid.UUID
	latest := map[uuid.UUID]setupRun{}
	for rows.Next() {
		var configID uuid.UUID
		run, err := scanSetupRun(rows, &configID)
		if err != nil {
			return nil, nil, err
		}
		if _, seen := latest[configID]; !seen {
			latest[configID] = run
			order = append(order, configID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	for _, status := range []int{jobRunRunning, jobRunPending, jobRunFailed, jobRunCancelled} {
		for _, configID := range order {
			if run := latest[configID]; run.Status == status {
				return &run, byID[configID], nil
			}
		}
	}
	return nil, nil, nil
}
