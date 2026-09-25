package syncadmin

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/synclimits"
)

// syncConfigBatchCreate is the SyncConfigBatchCreate body: SyncConfigCreate's
// fields plus repos.
type syncConfigBatchCreate struct {
	syncConfigCreate
	repos []string
}

// decodeSyncConfigBatchCreate validates SyncConfigBatchCreate in its field
// order: name, provider, credential_id, sync_targets, sync_options, repos
// (a list of str, default []), schedule_cron, timezone, initial_sync_depth.
func decodeSyncConfigBatchCreate(body pybody.Body) (syncConfigBatchCreate, pybody.Errors) {
	var problems pybody.Errors
	var in syncConfigBatchCreate
	object, ok := problems.Object(body)
	if !ok {
		return in, problems
	}
	in.name, _ = problems.RequiredString(object, "name", 1, 0)
	in.provider, _ = problems.RequiredString(object, "provider", 1, 0)
	if value, present := problems.OptionalString(object, "credential_id", 0, 0); present {
		in.credentialID = &value
	}
	in.syncTargets, _ = problems.DefaultedStringList(object, "sync_targets")
	if options, present := problems.DefaultedAnyDict(object, "sync_options"); present {
		in.syncOptions = options
	} else {
		in.syncOptions = pyjson.NewObject()
	}
	in.repos, _ = problems.DefaultedStringList(object, "repos")
	if value, present := problems.OptionalString(object, "schedule_cron", 0, 0); present {
		in.scheduleCron = &value
	}
	if value, present := problems.OptionalString(object, "timezone", 0, 0); present {
		in.timezone = &value
	}
	if value, present := problems.OptionalLaxInt(object, "initial_sync_depth"); present {
		in.initialSyncDepth = value
	}
	if in.syncTargets == nil {
		in.syncTargets = []string{}
	}
	if in.repos == nil {
		in.repos = []string{}
	}
	return in, problems
}

// batchCreateSyncConfigs is sync.py's batch_create_sync_configs, in its
// order: the body (422, before the guard), the canonical incident gate,
// the repo limit under the org's advisory lock (every repos entry charges
// one, duplicates included), the parent options, the auto-import flag
// checks, the GitLab project resolution and effective gitlab_url, and the
// planner-managed create with one source row per repos entry. 201 with the
// parent config and no children. It also runs the single create's backfill
// and schedule checks (checkDepthAndSchedule) before any write, which
// Python's batch create does not: a named divergence (CHAOS-6719). Python
// stores an out-of-range cron, an unknown timezone or an over-tier depth on
// an active config; Go answers 422/403 exactly as its single create does
// and persists nothing.
func (h *handlers) batchCreateSyncConfigs(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	in, problems := decodeSyncConfigBatchCreate(body)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	ctx := r.Context()
	org := orgID(r)
	targets := make([]pyjson.Value, len(in.syncTargets))
	for index, target := range in.syncTargets {
		targets[index] = target
	}
	if err := h.requireCanonicalIncident(ctx, org, targets); err != nil {
		h.answerOrFail(w, r, "canonical_incident_feature", err)
		return
	}
	var created *plannerCreated
	err := pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		var err error
		created, err = h.batchCreateSyncConfigsTx(ctx, tx, org, in)
		return err
	})
	if err != nil {
		h.answerOrFail(w, r, "batch_create_sync_configs", err)
		return
	}
	parent, err := syncConfigResponse(created.config, int64(0), created.credentialID)
	if err != nil {
		h.fail(w, r, "sync_config_response", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("parent", parent)
	out.Set("children", []pyjson.Value{})
	out.Set("total_created", int64(0))
	policy.WriteModel(w, http.StatusCreated, out, nil)
}

func (h *handlers) batchCreateSyncConfigsTx(ctx context.Context, tx pgx.Tx, org string, in syncConfigBatchCreate) (*plannerCreated, error) {
	orgUUID, err := pythonparity.ParseUUID(org)
	if err != nil {
		return nil, fmt.Errorf("org id is not a UUID: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, synclimits.AdvisoryLockKey(org)); err != nil {
		return nil, fmt.Errorf("repo limit lock: %w", err)
	}
	current, err := synclimits.ActiveRepoUsageCount(ctx, tx, org)
	if err != nil {
		return nil, err
	}
	inputs, err := licensing.LoadTierLimitInputs(ctx, tx, orgUUID)
	if err != nil {
		return nil, err
	}
	newCount := len(in.repos)
	allowed, reason, err := licensing.CheckLimitFrom(inputs, "max_repos", int64(current+newCount))
	if err != nil {
		return nil, err
	}
	if !allowed {
		if reason == "" {
			reason = fmt.Sprintf("Repo limit exceeded (adding %d repos)", newCount)
		}
		return nil, refuse(http.StatusForbidden, reason)
	}

	provider := pythonparity.Lower(in.provider)
	parentOptions := pyjson.NewObject()
	for _, key := range in.syncOptions.Keys() {
		if key == "repo" {
			continue
		}
		value, _ := in.syncOptions.Get(key)
		parentOptions.Set(key, value)
	}
	if in.scheduleCron != nil {
		parentOptions.Set("schedule_cron", *in.scheduleCron)
	}
	if in.timezone != nil {
		parentOptions.Set("timezone", *in.timezone)
	}
	if in.initialSyncDepth != nil {
		parentOptions.Set("initial_sync_depth", pyjson.Int{Int: in.initialSyncDepth})
	}

	if malformed := malformedAutoImportCategoryValues(parentOptions); malformed.Len() > 0 {
		detail := pyjson.NewObject()
		detail.Set("message", "auto-import category flags must be true or false")
		detail.Set("malformed_auto_import_category_values", malformed)
		return nil, refuse(http.StatusUnprocessableEntity, detail)
	}
	if unsupported := unsupportedAutoImportCategories(provider, parentOptions); unsupported.Len() > 0 {
		detail := pyjson.NewObject()
		detail.Set("message", provider+" does not support the requested auto-import categories")
		detail.Set("unsupported_auto_import_categories", unsupported)
		return nil, refuse(http.StatusUnprocessableEntity, detail)
	}
	if err := h.checkDepthAndSchedule(ctx, tx, org, inputs, parentOptions); err != nil {
		return nil, err
	}

	var gitlabProjects map[string]gitlabProject
	if provider == "gitlab" && len(in.repos) > 0 {
		projects, effectiveURL, err := h.resolveGitLabProjects(ctx, tx, org, in.credentialID, in.syncOptions, in.repos)
		if err != nil {
			return nil, err
		}
		gitlabProjects = projects
		if effectiveURL != defaultGitLabURL {
			parentOptions.Set("gitlab_url", effectiveURL)
		}
	}

	var buildErr error
	created, err := createPlannerManagedConfig(ctx, tx, plannerCreate{
		orgID: org, name: in.name, provider: in.provider, credentialID: in.credentialID,
		syncTargets: in.syncTargets, parentOptions: parentOptions, scheduleCron: in.scheduleCron, timezone: in.timezone,
		buildSourceRows: func(_, configID uuid.UUID) []newSourceRow {
			// _planner_source_rows reads the payload's own sync_options
			// (not the parent options) for the owner.
			desired, err := plannerSourceRows(&syncConfig{ID: configID, Name: in.name, Provider: in.provider},
				in.syncOptions, in.repos, gitlabProjects, org)
			if err != nil {
				buildErr = err
				return nil
			}
			rows := make([]newSourceRow, len(desired))
			for index, row := range desired {
				rows[index] = newSourceRow{provider: row.Provider, sourceType: row.SourceType, externalID: row.ExternalID,
					name: row.Name, fullName: row.FullName, metadata: row.Metadata}
			}
			return rows
		},
	}, h.now().UTC(), h.lookupEnv)
	if buildErr != nil {
		return nil, buildErr
	}
	if err != nil {
		return nil, err
	}
	return created, nil
}
