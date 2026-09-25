package syncadmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/synclimits"
)

// syncConfigCreate is the SyncConfigCreate body.
type syncConfigCreate struct {
	name, provider         string
	credentialID           *string
	syncTargets            []string
	syncOptions            *pyjson.Object
	scheduleCron, timezone *string
	initialSyncDepth       *big.Int
}

// decodeSyncConfigCreate validates SyncConfigCreate: name and provider are
// str with at least one character; credential_id, schedule_cron and
// timezone are optional str; sync_targets a list of str (default []);
// sync_options a dict (default {}); initial_sync_depth an optional lax int.
func decodeSyncConfigCreate(body pybody.Body) (syncConfigCreate, pybody.Errors) {
	var problems pybody.Errors
	var in syncConfigCreate
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
	return in, problems
}

// createSyncConfig is sync.py's create_sync_config, in its order: the
// body (422, before the guard), the canonical incident gate, the repo
// limit under the org's advisory lock (a Jira config without explicit
// scope charges 0), the merged sync options, the auto-import flag checks,
// the backfill limit, the schedule checks (the scheduled_jobs feature, the
// cron interval, the timezone, the tier's minimum interval), the
// GitHub/GitLab repository selection rule, and the planner-managed
// create; then, for a Jira config without explicit scope, best-effort
// project discovery. 201 with the config.
func (h *handlers) createSyncConfig(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	in, problems := decodeSyncConfigCreate(body)
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
	var options *pyjson.Object
	err := pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		var err error
		created, options, err = h.createSyncConfigTx(ctx, tx, org, in)
		return err
	})
	if err != nil {
		h.answerOrFail(w, r, "create_sync_config", err)
		return
	}
	if jiraConfigMaterializesZeroSources(in.provider, options) {
		h.discoverJiraProjects(ctx, org, created, options)
	}
	out, err := syncConfigResponse(created.config, nil, created.credentialID)
	if err != nil {
		h.fail(w, r, "sync_config_response", err)
		return
	}
	policy.WriteModel(w, http.StatusCreated, out, nil)
}

func (h *handlers) createSyncConfigTx(ctx context.Context, tx pgx.Tx, org string, in syncConfigCreate) (*plannerCreated, *pyjson.Object, error) {
	orgUUID, err := pythonparity.ParseUUID(org)
	if err != nil {
		return nil, nil, fmt.Errorf("org id is not a UUID: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, synclimits.AdvisoryLockKey(org)); err != nil {
		return nil, nil, fmt.Errorf("repo limit lock: %w", err)
	}
	current, err := synclimits.ActiveRepoUsageCount(ctx, tx, org)
	if err != nil {
		return nil, nil, err
	}
	increment := 1
	if jiraConfigMaterializesZeroSources(in.provider, in.syncOptions) {
		increment = 0
	}
	inputs, err := licensing.LoadTierLimitInputs(ctx, tx, orgUUID)
	if err != nil {
		return nil, nil, err
	}
	allowed, reason, err := licensing.CheckLimitFrom(inputs, "max_repos", int64(current+increment))
	if err != nil {
		return nil, nil, err
	}
	if !allowed {
		if reason == "" {
			reason = "Repo limit exceeded"
		}
		return nil, nil, refuse(http.StatusForbidden, reason)
	}

	options := syncOptionsWithTopLevelFields(in)
	if malformed := malformedAutoImportCategoryValues(options); malformed.Len() > 0 {
		detail := pyjson.NewObject()
		detail.Set("message", "auto-import category flags must be true or false")
		detail.Set("malformed_auto_import_category_values", malformed)
		return nil, nil, refuse(http.StatusUnprocessableEntity, detail)
	}
	if unsupported := unsupportedAutoImportCategories(in.provider, options); unsupported.Len() > 0 {
		detail := pyjson.NewObject()
		detail.Set("message", in.provider+" does not support the requested auto-import categories")
		detail.Set("unsupported_auto_import_categories", unsupported)
		return nil, nil, refuse(http.StatusUnprocessableEntity, detail)
	}

	if depth, _ := options.Get("initial_sync_depth"); depth != nil {
		requested, err := pyInt(depth)
		if err != nil {
			return nil, nil, err
		}
		allowed, reason, err := licensing.CheckBackfillLimitFrom(inputs, requested)
		if err != nil {
			return nil, nil, err
		}
		if !allowed {
			if reason == "" {
				reason = "initial_sync_depth exceeds tier limit"
			}
			return nil, nil, refuse(http.StatusForbidden, reason)
		}
	}

	if cron, _ := options.Get("schedule_cron"); pyjson.Truthy(cron) {
		if err := h.checkSchedule(ctx, tx, org, inputs, cron, options); err != nil {
			return nil, nil, err
		}
	}

	lower := pythonparity.Lower(in.provider)
	if lower == "github" || lower == "gitlab" {
		if allRepos, _ := options.Get("all_repos"); !pyjson.Truthy(allRepos) {
			return nil, nil, refuse(http.StatusBadRequest,
				"github/gitlab sync configs require repository selection via POST /sync-configs/batch, or sync_options.all_repos=true")
		}
	}

	created, err := createPlannerManagedConfig(ctx, tx, plannerCreate{
		orgID: org, name: in.name, provider: in.provider, credentialID: in.credentialID,
		syncTargets: in.syncTargets, parentOptions: options, scheduleCron: in.scheduleCron, timezone: in.timezone,
		buildSourceRows: func(_, configID uuid.UUID) []newSourceRow {
			if lower == "github" || lower == "gitlab" || lower == "pagerduty" {
				return nil
			}
			return nonGitSourceRows(in.provider, options, in.name, configID.String())
		},
	}, h.now().UTC(), h.lookupEnv)
	if err != nil {
		return nil, nil, err
	}
	return created, options, nil
}

// syncOptionsWithTopLevelFields is _sync_options_with_top_level_fields:
// the body's sync_options with schedule_cron, timezone and
// initial_sync_depth set from the body's own fields when they are not None.
func syncOptionsWithTopLevelFields(in syncConfigCreate) *pyjson.Object {
	merged := pyjson.NewObject()
	for _, key := range in.syncOptions.Keys() {
		value, _ := in.syncOptions.Get(key)
		merged.Set(key, value)
	}
	if in.scheduleCron != nil {
		merged.Set("schedule_cron", *in.scheduleCron)
	}
	if in.timezone != nil {
		merged.Set("timezone", *in.timezone)
	}
	if in.initialSyncDepth != nil {
		merged.Set("initial_sync_depth", pyjson.Int{Int: in.initialSyncDepth})
	}
	return merged
}

// scheduledJobsFeature is the feature the schedule checks gate on.
const scheduledJobsFeature = "scheduled_jobs"

// checkSchedule is create_sync_config's schedule block for a truthy
// schedule_cron: the org's scheduled_jobs feature (else 403; the
// process-wide LicenseManager fallback is the community tier, which lacks
// it), the cron interval (422 "Invalid cron expression: ..."), the
// timezone (422 with its text; a non-str raises), and the tier's
// min_sync_interval_hours (403 when the interval is below it).
func (h *handlers) checkSchedule(ctx context.Context, tx pgx.Tx, org string, inputs licensing.TierLimitInputs, cron pyjson.Value, options *pyjson.Object) error {
	allowed, err := licensing.OrgHasFeature(ctx, tx, org, scheduledJobsFeature)
	if err != nil {
		h.logger.ErrorContext(ctx, "sync_config_create: scheduled_jobs feature check failed; denying", "org_id", org, "error", err)
	}
	if !allowed {
		return refuse(http.StatusForbidden, "scheduled_jobs feature requires Team tier or higher")
	}
	interval, err := cronIntervalHours(cron, h.now().UTC())
	if err != nil {
		return refuse(http.StatusUnprocessableEntity, cronInvalidDetail(err))
	}
	timezone, _ := options.Get("timezone")
	detail, err := validateTimezoneName(timezone)
	if err != nil {
		return err
	}
	if detail != "" {
		return refuse(http.StatusUnprocessableEntity, detail)
	}
	limit, err := licensing.GetLimitFrom(inputs, "min_sync_interval_hours")
	if err != nil {
		return err
	}
	if limit == nil {
		return nil
	}
	minimum, err := pyFloat(limit)
	if err != nil {
		return err
	}
	if interval < minimum {
		return refuse(http.StatusForbidden, cronIntervalRefusal(interval, minimum))
	}
	return nil
}

// errPyIntArgument is int(value)'s TypeError or ValueError for a value
// int() refuses (the create route does not catch it: a bare 500).
var errPyIntArgument = errors.New("int() refuses the initial_sync_depth value")

// pyInt is Python's int(value) of a decoded JSON value: a bool is 0 or 1,
// an int itself, a float truncated (an infinity or NaN raises), a string
// parsed as int() parses it; anything else raises.
func pyInt(value pyjson.Value) (*big.Int, error) {
	switch typed := value.(type) {
	case bool:
		if typed {
			return big.NewInt(1), nil
		}
		return big.NewInt(0), nil
	case pyjson.Int:
		return new(big.Int).Set(typed.Int), nil
	case pyjson.Float:
		f := float64(typed)
		if math.IsInf(f, 0) || math.IsNaN(f) {
			return nil, errPyIntArgument
		}
		whole, _ := new(big.Float).SetFloat64(math.Trunc(f)).Int(nil)
		return whole, nil
	case string:
		parsed, err := pythonparity.ParseInt(typed)
		if err != nil {
			return nil, errPyIntArgument
		}
		return parsed, nil
	}
	return nil, errPyIntArgument
}

// pyFloat is float(val) of get_limit's value (None is handled by the
// caller): a bool is 0.0 or 1.0, an int its nearest float (one past the
// float range raises OverflowError), a float itself.
func pyFloat(value pyjson.Value) (float64, error) {
	switch typed := value.(type) {
	case bool:
		if typed {
			return 1, nil
		}
		return 0, nil
	case pyjson.Int:
		f, _ := new(big.Float).SetInt(typed.Int).Float64()
		if math.IsInf(f, 0) {
			return 0, errors.New("float() of the min_sync_interval_hours limit overflows")
		}
		return f, nil
	case pyjson.Float:
		return float64(typed), nil
	}
	return 0, fmt.Errorf("float() of a %T min_sync_interval_hours limit", value)
}

// discoverJiraProjects is create_sync_config's best-effort
// discover_sources_for_integration for a Jira config without explicit
// scope, through the scheduler's port of it (NativeSourceDiscoveryService,
// the one implementation). A failure is logged and swallowed; the config
// stands.
//
// It runs after the create commits, in its own transaction on the api
// pool, where Python runs it inside the request's session under a
// SAVEPOINT that is committed with the create. The stored outcome is the
// same: the discovered sources on success, none on a failure (Python's
// SAVEPOINT rolls back; here the discovery transaction does).
//
// Named limit: an integration without a credential is skipped here (the Go
// resolver has no environment-credential path), where Python would try the
// process's JIRA_* environment credentials; neither deployment sets them.
func (h *handlers) discoverJiraProjects(ctx context.Context, org string, created *plannerCreated, options *pyjson.Object) {
	if h.discovery == nil {
		h.logger.ErrorContext(ctx, "jira_project_discovery_at_creation_failed", "org_id", org,
			"integration_id", created.integrationID.String(), "error", "source discovery is unavailable in this process")
		return
	}
	var credentialID *string
	if created.credentialID != nil {
		text := created.credentialID.String()
		credentialID = &text
	}
	encoded, err := pyjson.Dumps(options)
	if err != nil {
		h.logger.ErrorContext(ctx, "jira_project_discovery_at_creation_failed", "org_id", org, "error", err)
		return
	}
	var syncOptions map[string]any
	if err := json.Unmarshal([]byte(encoded), &syncOptions); err != nil {
		h.logger.ErrorContext(ctx, "jira_project_discovery_at_creation_failed", "org_id", org, "error", err)
		return
	}
	report, err := h.discovery.Discover(ctx, schedsync.SourceDiscoveryArgs{
		OrgID: org, IntegrationID: created.integrationID.String(), CredentialID: credentialID,
		Provider: created.config.Provider, SyncOptions: syncOptions,
		ConfigID: created.config.ID.String(), PlannerManaged: true,
	})
	if err != nil {
		h.logger.ErrorContext(ctx, "jira_project_discovery_at_creation_failed", "org_id", org,
			"integration_id", created.integrationID.String(), "error", err)
		return
	}
	// Go-only event (Python logs nothing on success): a discovery that
	// succeeds with zero projects, or is skipped, still answers 201, so its
	// outcome and counts are the only sign of it at Info.
	h.logger.InfoContext(ctx, "jira_project_discovery_at_creation", "org_id", org,
		"integration_id", created.integrationID.String(), "outcome", report.Outcome,
		"created", report.Created, "existing", report.Existing)
}

// newCreateDiscovery builds the create path's discovery on the api pool:
// the stored credential through the api's decryptor, the given (or the
// scheduler's default) HTTP client. nil without a pool or a decryptor.
func newCreateDiscovery(deps Deps, logger *slog.Logger, clock func() time.Time) schedsync.SourceDiscoveryExecutor {
	if deps.Pool == nil || deps.Decryptor == nil {
		return nil
	}
	client := deps.JiraHTTP
	if client == nil {
		client = &http.Client{Timeout: 45 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}
	}
	discovery, err := schedsync.NewNativeSourceDiscoveryService(deps.Pool, providerfoundation.CredentialResolver{
		Repository: providerfoundation.PostgresCredentialRepository{Pool: deps.Pool},
		Decryptor:  deps.Decryptor,
	}, client, logger)
	if err != nil {
		logger.Error("sync_config_create: jira project discovery is unavailable", "error", err)
		return nil
	}
	return discovery.WithClock(clock)
}
