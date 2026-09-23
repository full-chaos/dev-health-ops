package syncbudget

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Querier is the one pgx method the loader needs; *pgxpool.Pool and pgx.Tx
// both satisfy it.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Batch errors. Each one refuses the WHOLE batch, as the bridge endpoint
// refused it before any credential was decrypted.
var (
	// ErrStaleRun: the run does not exist in this organization (the
	// endpoint's 409 "Sync run reference is stale").
	ErrStaleRun = errors.New("sync run reference is stale")
	// ErrUnitsOutsideRun: a unit id does not belong to this run and
	// organization (the endpoint's 409 tenant fence).
	ErrUnitsOutsideRun = errors.New("one or more units do not belong to this sync run")
	// ErrInvalidReference: an empty unit list or an id that is not a UUID
	// (the endpoint's 422 request validation).
	ErrInvalidReference = errors.New("invalid budget estimate reference")
	// ErrLoaderUnavailable: the loader is missing a dependency, or a batch
	// query failed (the endpoint's 500 or an unreachable bridge).
	ErrLoaderUnavailable = errors.New("budget estimate loader unavailable")
)

// Loader is the in-process port of SyncTaskBootstrap.load plus
// batch_estimate_provider_budget_for_units and the endpoint's reference
// checks (api/internal/worker_sync.py): identifiers in, estimates out.
type Loader struct {
	DB        Querier
	Decryptor providerfoundation.CredentialDecryptor
	// Getenv is os.getenv for the reads Python made from the process
	// environment: environment credentials, SYNC_RUN_AUTH_STRICT and the
	// Jira estimator's flags and base URL.
	Getenv func(string) string
	Logger *slog.Logger
	// PagerDutyOAuth and PagerDutyDoer run PagerDuty credential hydration
	// (see hydratePagerDuty); a PagerDuty unit whose mode needs one that is
	// nil fails its estimate.
	PagerDutyOAuth providerfoundation.CredentialHydrator
	PagerDutyDoer  providerfoundation.HTTPDoer
}

// UnitResult is one unit's outcome: its estimates, or the exception its
// bootstrap or estimator raised. Python logged that exception and used an
// empty estimate for the unit.
type UnitResult struct {
	Estimates []Estimate
	Err       error
}

// EstimateUnits checks the reference, then bootstraps and estimates each
// unit on its own, in order. A batch error refuses the whole reference.
func (loader Loader) EstimateUnits(ctx context.Context, orgID, runID string, unitIDs []string) (map[string]UnitResult, error) {
	if loader.DB == nil || loader.Decryptor == nil {
		return nil, ErrLoaderUnavailable
	}
	// The endpoint's request model parsed all three as UUIDs and the
	// queries used their canonical str() form.
	orgUUID, orgErr := uuid.Parse(orgID)
	runUUID, runErr := uuid.Parse(runID)
	if len(unitIDs) == 0 || orgErr != nil || runErr != nil {
		return nil, ErrInvalidReference
	}
	orgID, runID = orgUUID.String(), runUUID.String()
	distinct := map[string]bool{}
	canonical := make([]string, len(unitIDs))
	for index, unitID := range unitIDs {
		parsed, err := uuid.Parse(unitID)
		if err != nil {
			return nil, ErrInvalidReference
		}
		canonical[index] = parsed.String()
		distinct[canonical[index]] = true
	}
	var present bool
	if err := loader.DB.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM public.sync_runs WHERE id = $1::uuid AND org_id = $2)`,
		runID, orgID).Scan(&present); err != nil {
		return nil, fmt.Errorf("%w: run reference: %v", ErrLoaderUnavailable, err)
	}
	if !present {
		return nil, ErrStaleRun
	}
	ids := make([]string, 0, len(distinct))
	for id := range distinct {
		ids = append(ids, id)
	}
	var matched int
	if err := loader.DB.QueryRow(ctx,
		`SELECT count(*) FROM public.sync_run_units WHERE id = ANY($1::uuid[]) AND sync_run_id = $2::uuid AND org_id = $3`,
		ids, runID, orgID).Scan(&matched); err != nil {
		return nil, fmt.Errorf("%w: unit fence: %v", ErrLoaderUnavailable, err)
	}
	if matched != len(distinct) {
		return nil, ErrUnitsOutsideRun
	}
	credentials := credentialCache{}
	hydrations := hydrationCache{}
	results := make(map[string]UnitResult, len(unitIDs))
	for index, unitID := range unitIDs {
		unitContext, err := loader.load(ctx, canonical[index], credentials, hydrations)
		if err != nil {
			results[unitID] = UnitResult{Estimates: []Estimate{}, Err: err}
			continue
		}
		estimates, err := EstimateProviderBudget(unitContext)
		if err != nil {
			results[unitID] = UnitResult{Estimates: []Estimate{}, Err: err}
			continue
		}
		results[unitID] = UnitResult{Estimates: estimates}
	}
	return results, nil
}

func (loader Loader) getenv(name string) string {
	if loader.Getenv == nil {
		return ""
	}
	return loader.Getenv(name)
}

// credentialCache keeps one decrypted mapping per credential row for one
// batch. Decryption derives the key with 600,000 PBKDF2 rounds; the Python
// endpoint paid that per unit.
type credentialCache map[string]credentialRow

type credentialRow struct {
	provider string
	active   bool
	mapping  any
	err      error
}

var errBootstrap = errors.New("sync task bootstrap failed")

func bootstrapError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errBootstrap, fmt.Sprintf(format, args...))
}

// load is SyncTaskBootstrap.load, reduced to the context the estimators
// read. Every exception path it keeps is one that made Python's estimate
// for the unit empty.
func (loader Loader) load(ctx context.Context, unitID string, credentials credentialCache, hydrations hydrationCache) (Context, error) {
	var (
		runID, orgID, integrationID, sourceID, provider, datasetKey string
		sinceAt, beforeAt                                           *time.Time
		processorFlagsText                                          *string
	)
	err := loader.DB.QueryRow(ctx, `
SELECT sync_run_id::text, org_id, integration_id::text, source_id::text, provider, dataset_key,
       since_at, before_at, processor_flags::text
FROM public.sync_run_units WHERE id = $1::uuid`, unitID).Scan(
		&runID, &orgID, &integrationID, &sourceID, &provider, &datasetKey, &sinceAt, &beforeAt, &processorFlagsText)
	if errors.Is(err, pgx.ErrNoRows) {
		return Context{}, bootstrapError("sync run unit not found")
	}
	if err != nil {
		return Context{}, err
	}

	var (
		integrationCredentialID *string
		integrationProvider     string
		integrationConfigText   *string
	)
	err = loader.DB.QueryRow(ctx, `
SELECT id::text, provider, credential_id::text, config::text
FROM public.integrations WHERE id = $1::uuid AND org_id = $2`, integrationID, orgID).Scan(
		&integrationID, &integrationProvider, &integrationCredentialID, &integrationConfigText)
	if errors.Is(err, pgx.ErrNoRows) {
		return Context{}, bootstrapError("integration not found for unit")
	}
	if err != nil {
		return Context{}, err
	}

	var (
		sourceProvider, sourceExternalID string
		sourceMetadataText               *string
	)
	err = loader.DB.QueryRow(ctx, `
SELECT provider, external_id, metadata::text
FROM public.integration_sources WHERE id = $1::uuid AND org_id = $2 AND integration_id = $3::uuid`,
		sourceID, orgID, integrationID).Scan(&sourceProvider, &sourceExternalID, &sourceMetadataText)
	if errors.Is(err, pgx.ErrNoRows) {
		return Context{}, bootstrapError("integration source not found for unit")
	}
	if err != nil {
		return Context{}, err
	}

	var datasetRows int
	var datasetOptionsText *string
	err = loader.DB.QueryRow(ctx, `
SELECT count(*) OVER (), options::text
FROM public.integration_datasets WHERE org_id = $1 AND integration_id = $2::uuid AND dataset_key = $3
LIMIT 1`, orgID, integrationID, datasetKey).Scan(&datasetRows, &datasetOptionsText)
	datasetOptions := newObject()
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return Context{}, err
	case datasetRows > 1:
		// .one_or_none() raises MultipleResultsFound.
		return Context{}, bootstrapError("multiple integration datasets for unit")
	default:
		decoded, err := decodeColumn(datasetOptionsText)
		if err != nil {
			return Context{}, err
		}
		if datasetOptions, err = dictOrEmpty(decoded); err != nil {
			return Context{}, err
		}
	}

	var (
		runFound                                   = true
		authSource, runCredentialID, runWitnessPtr *string
	)
	err = loader.DB.QueryRow(ctx, `
SELECT auth_source, credential_id::text, credential_fingerprint
FROM public.sync_runs WHERE id = $1::uuid AND org_id = $2`, runID, orgID).Scan(&authSource, &runCredentialID, &runWitnessPtr)
	if errors.Is(err, pgx.ErrNoRows) {
		runFound = false
	} else if err != nil {
		return Context{}, err
	}

	credentialID, mapping, err := loader.resolveRunAuth(ctx, credentials, runAuth{
		found: runFound, authSource: authSource, credentialID: runCredentialID, witness: runWitnessPtr,
	}, orgID, integrationID, integrationCredentialID, provider)
	if err != nil {
		return Context{}, err
	}
	// resolve_run_auth hydrates a PagerDuty mapping (provider compared
	// exactly, as in Python).
	if provider == "pagerduty" {
		if pagerDutyMapping, isObject := mapping.(*object); isObject {
			if err := loader.hydratePagerDuty(ctx, hydrations, orgID, credentialID, pagerDutyMapping); err != nil {
				return Context{}, err
			}
		}
	}

	flagsValue, err := decodeColumn(processorFlagsText)
	if err != nil {
		return Context{}, err
	}
	flagsObject, err := dictOrEmpty(flagsValue)
	if err != nil {
		return Context{}, err
	}
	processorFlags := make(map[string]bool, len(flagsObject.keys))
	for _, key := range flagsObject.keys {
		processorFlags[key] = truthy(flagsObject.values[key])
	}

	if err := checkLinearPlaceholderInputs(sourceProvider, integrationProvider, sourceExternalID, sourceMetadataText, integrationConfigText); err != nil {
		return Context{}, err
	}

	return Context{
		Provider: provider, DatasetKey: datasetKey, OrgID: orgID, IntegrationID: integrationID,
		CredentialID: credentialID, Credentials: mapping, ProcessorFlags: processorFlags,
		WindowStart: sinceAt, WindowEnd: beforeAt, DatasetOptions: datasetOptions,
		Getenv: loader.Getenv,
	}, nil
}

func decodeColumn(text *string) (any, error) {
	if text == nil {
		return nil, nil
	}
	return decodeJSON([]byte(*text))
}

type runAuth struct {
	found        bool
	authSource   *string
	credentialID *string
	witness      *string
}

var errPagerDutyCredential = bootstrapError("PagerDuty sync requires an active organization-scoped credential")

// resolveRunAuth is resolve_run_auth up to the PagerDuty hydration, which
// load runs next: it returns (credential_id, decrypted mapping).
func (loader Loader) resolveRunAuth(
	ctx context.Context, credentials credentialCache, run runAuth,
	orgID, integrationID string, integrationCredentialID *string, provider string,
) (*string, any, error) {
	isPagerDuty := provider == "pagerduty"
	var credentialID *string
	var mapping any
	if !run.found || run.authSource == nil {
		if integrationCredentialID == nil {
			if isPagerDuty {
				return nil, nil, errPagerDutyCredential
			}
			return nil, loader.environmentCredentials(provider), nil
		}
		row, err := loader.credential(ctx, credentials, *integrationCredentialID, orgID)
		if err != nil {
			return nil, nil, err
		}
		if isPagerDuty && (pythonparity.Lower(row.provider) != "pagerduty" || !row.active) {
			return nil, nil, errPagerDutyCredential
		}
		return integrationCredentialID, row.mapping, nil
	}
	if run.credentialID == nil {
		if isPagerDuty {
			return nil, nil, errPagerDutyCredential
		}
		mapping = loader.environmentCredentials(provider)
	} else {
		row, err := loader.credential(ctx, credentials, *run.credentialID, orgID)
		if err != nil {
			return nil, nil, err
		}
		if isPagerDuty && (pythonparity.Lower(row.provider) != "pagerduty" || !row.active) {
			return nil, nil, errPagerDutyCredential
		}
		mapping = row.mapping
		credentialID = run.credentialID
	}
	if run.witness != nil && *run.witness != "" {
		current := RunAuthFingerprint(mapping, credentialID, integrationID)
		if current != *run.witness {
			strict := pythonparity.Lower(pythonparity.Strip(loader.getenv("SYNC_RUN_AUTH_STRICT")))
			if in(strict, "1", "true", "yes", "on") {
				return nil, nil, bootstrapError("sync run auth fingerprint mismatch: stamped credential content changed mid-run")
			}
			if loader.Logger != nil {
				loader.Logger.WarnContext(ctx, "sync_run_auth.fingerprint_mismatch",
					slog.String("integration_id", integrationID), slog.Bool("strict", false))
			}
		}
	}
	return credentialID, mapping, nil
}

// credential loads and decrypts one integration_credentials row
// (_load_credential + _credential_mapping), once per batch.
func (loader Loader) credential(ctx context.Context, cache credentialCache, credentialID, orgID string) (credentialRow, error) {
	if cached, ok := cache[credentialID]; ok {
		return cached, cached.err
	}
	row := credentialRow{}
	var encrypted, configText *string
	err := loader.DB.QueryRow(ctx, `
SELECT provider, is_active, credentials_encrypted, config::text
FROM public.integration_credentials WHERE id = $1::uuid AND org_id = $2`, credentialID, orgID).Scan(
		&row.provider, &row.active, &encrypted, &configText)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		row.err = bootstrapError("credential not found for unit")
	case err != nil:
		// Not cached: a transient read error must not stick for the batch.
		return credentialRow{}, err
	default:
		row.mapping, row.err = loader.credentialMapping(encrypted, configText)
	}
	cache[credentialID] = row
	return row, row.err
}

// credentialMapping is _credential_mapping: {**config, **decrypted}, or the
// decrypted value alone when config is not a non-empty dict.
func (loader Loader) credentialMapping(encrypted, configText *string) (any, error) {
	var decrypted any = newObject()
	if encrypted != nil && *encrypted != "" {
		plaintext, err := loader.Decryptor.Decrypt(secrets.NewValue(*encrypted))
		if err != nil {
			return nil, bootstrapError("credential decryption failed")
		}
		if decrypted, err = decodeJSON(plaintext); err != nil {
			return nil, bootstrapError("decrypted credential is not JSON")
		}
	}
	config, err := decodeColumn(configText)
	if err != nil {
		return nil, err
	}
	configObject, isObject := config.(*object)
	if !isObject || len(configObject.keys) == 0 {
		return decrypted, nil
	}
	decryptedObject, isMapping := decrypted.(*object)
	if !isMapping {
		// {**config, **decrypted} raises TypeError for a non-mapping.
		return nil, bootstrapError("decrypted credential is not a mapping")
	}
	return merged(configObject, decryptedObject), nil
}

// providerEnvVars is credentials/resolver.py PROVIDER_ENV_VARS, in its order.
var providerEnvVars = map[string][][2]string{
	"github": {
		{"token", "GITHUB_TOKEN"}, {"base_url", "GITHUB_URL"}, {"app_id", "GITHUB_APP_ID"},
		{"private_key_path", "GITHUB_APP_PRIVATE_KEY_PATH"}, {"installation_id", "GITHUB_APP_INSTALLATION_ID"},
	},
	"gitlab": {{"token", "GITLAB_TOKEN"}, {"base_url", "GITLAB_URL"}},
	"jira": {
		{"api_token", "JIRA_API_TOKEN"}, {"email", "JIRA_EMAIL"}, {"base_url", "JIRA_BASE_URL"},
	},
	"linear": {{"api_key", "LINEAR_API_KEY"}},
	"atlassian": {
		{"api_token", "ATLASSIAN_API_TOKEN"}, {"email", "ATLASSIAN_EMAIL"}, {"cloud_id", "ATLASSIAN_CLOUD_ID"},
	},
	"launchdarkly": {{"api_key", "LAUNCHDARKLY_API_KEY"}},
	"telemetry":    {{"api_key", "TELEMETRY_API_KEY"}},
	"pagerduty": {
		{"client_id", "PAGER_DUTY_CLIENT_ID"}, {"client_secret", "PAGER_DUTY_SECRET"},
		{"api_token", "PAGERDUTY_API_TOKEN"}, {"subdomain", "PAGERDUTY_SUBDOMAIN"}, {"region", "PAGERDUTY_REGION"},
	},
}

// environmentCredentials is _resolve_env_credentials: every set, non-empty
// variable of the provider's table.
func (loader Loader) environmentCredentials(provider string) *object {
	result := newObject()
	for _, pair := range providerEnvVars[pythonparity.Lower(provider)] {
		if value := loader.getenv(pair[1]); value != "" {
			result.set(pair[0], value)
		}
	}
	return result
}

// checkLinearPlaceholderInputs keeps the exceptions
// _linear_org_wide_placeholder_source can raise (its result is unused by
// the estimators): dict() of a non-mapping metadata or config.
func checkLinearPlaceholderInputs(sourceProvider, integrationProvider, externalID string, metadataText, configText *string) error {
	provider := sourceProvider
	if provider == "" {
		provider = integrationProvider
	}
	if pythonparity.Lower(provider) != "linear" {
		return nil
	}
	metadataValue, err := decodeColumn(metadataText)
	if err != nil {
		return err
	}
	metadata, err := dictOrEmpty(metadataValue)
	if err != nil {
		return err
	}
	if placeholder, _ := metadata.get("org_wide_placeholder"); placeholder == true {
		return nil
	}
	if pythonparity.Lower(pythonparity.Strip(externalID)) != pythonparity.Lower(provider) {
		return nil
	}
	if !truthy(get(metadata, "planner_managed_sync_config_id")) {
		return nil
	}
	configValue, err := decodeColumn(configText)
	if err != nil {
		return err
	}
	_, err = dictOrEmpty(configValue)
	return err
}

// RunAuthFingerprint is credentials/fingerprint.py credential_fingerprint
// over a decrypted mapping.
func RunAuthFingerprint(credentials any, credentialID *string, integrationID string) string {
	fallback := func() *object {
		scope := newObject()
		id := "env"
		if credentialID != nil && *credentialID != "" {
			id = *credentialID
		}
		scope.set("credential_id", id)
		scope.set("integration_id", integrationID)
		return scope
	}
	mapping, ok := credentials.(*object)
	scope := newObject()
	if ok {
		copyPresent(scope, mapping, "app_id", "installation_id", "email", "cloud_id", "cloudId",
			"client_id", "clientId", "user_id", "username", "group_id", "project_id", "project_key",
			"environment", "schema_version", "organization_id", "workspace_id", "team_id", "oauth_binding_id")
		if baseURL := baseURLOf(mapping); baseURL != nil {
			scope.set("base_url", strings.TrimRight(pythonparity.Strip(pyStr(baseURL)), "/"))
		}
		for _, key := range []string{"token", "private_token", "access_token", "accessToken",
			"refresh_token", "refreshToken", "api_token", "apiToken", "api_key", "apiKey",
			"private_key", "privateKey", "client_secret", "clientSecret"} {
			if value := get(mapping, key); truthy(value) {
				scope.set(key+"_sha256", sha256Hex(pyStr(value)))
			}
		}
	}
	if !ok || len(scope.keys) == 0 {
		scope = fallback()
	}
	return fingerprintOf(scope)
}
