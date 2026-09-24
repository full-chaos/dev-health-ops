// PagerDuty admin: status and preflight (api/admin/routers/pagerduty.py's
// get_pagerduty_status/preflight_pagerduty). Disconnect and the OAuth/
// non-OAuth credential write routes are a separate slice (CHAOS-6591):
// they make a live outbound PagerDuty call this one does not need.
package admin

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// pagerDutyPrefix is the mount prefix for every PagerDuty admin route.
const pagerDutyPrefix = governancePrefix + "/integrations/pagerduty"

func (h *handlers) pagerDutyRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: pagerDutyPrefix + "/status", Allow: "GET", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getPagerDutyStatus))},
		{Method: http.MethodPost, Pattern: pagerDutyPrefix + "/preflight", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.preflightPagerDuty))},
	}
}

// pagerDutyDatasetFamilies is DATASET_OAUTH_FAMILIES (providers/pagerduty/
// oauth.py): a sync-registry dataset key normalized to its OAuth scope
// family. A key mapping to itself (incidents, services, ...) is still
// listed explicitly so the "known dataset" membership check below reads
// directly off this one map, matching Python's `.difference(DATASET_OAUTH_FAMILIES)`.
var pagerDutyDatasetFamilies = map[string]string{
	"incidents":            "incidents",
	"services":             "services",
	"business-services":    "business_services",
	"escalation-policies":  "escalation_policies",
	"schedules":            "schedules",
	"on-calls":             "oncalls",
	"users":                "users",
	"teams":                "teams",
	"incident-alerts":      "incidents",
	"incident-log-entries": "incidents",
	"incident-notes":       "incidents",
}

// pagerDutyDatasetScopes is DATASET_SCOPES: the OAuth read scope(s) each
// family requires.
var pagerDutyDatasetScopes = map[string][]string{
	"incidents":           {"incidents.read"},
	"services":            {"services.read"},
	"business_services":   {"services.read"},
	"escalation_policies": {"escalation_policies.read"},
	"schedules":           {"schedules.read"},
	"oncalls":             {"oncalls.read"},
	"users":               {"users.read"},
	"teams":               {"teams.read"},
}

// pagerDutyIntegrationCredential is the integration_credentials row this
// package reads (IntegrationCredential, filtered to provider='pagerduty').
type pagerDutyIntegrationCredential struct {
	ID       uuid.UUID
	IsActive bool
	Config   *pyjson.Object // nil = SQL NULL config
}

// loadPagerDutyIntegrationCredential is IntegrationCredentialsService.get("pagerduty", name).
func loadPagerDutyIntegrationCredential(ctx context.Context, db pgExecer, orgID, name string) (*pagerDutyIntegrationCredential, error) {
	var (
		row    pagerDutyIntegrationCredential
		config []byte
	)
	err := db.QueryRow(ctx,
		`SELECT id, is_active, config::text FROM integration_credentials WHERE org_id = $1 AND provider = 'pagerduty' AND name = $2`,
		orgID, name).Scan(&row.ID, &row.IsActive, &config)
	if pgx.ErrNoRows == err {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	value, err := jsonColumnValue(config)
	if err != nil {
		return nil, err
	}
	if object, ok := value.(*pyjson.Object); ok {
		row.Config = object
	}
	return &row, nil
}

// pagerDutyOAuthStatus is OAuthStatusMetadata (oauth_storage.py): the
// non-secret provider_oauth_credentials columns get_status_metadata reads,
// never the encrypted token.
type pagerDutyOAuthStatus struct {
	ExpiresAt       *time.Time
	GrantedScopes   []string // deduplicated and sorted, as frozenset(...) then sorted() leaves them
	HasRefreshToken bool
	AccountID       *string
	AccountDisplay  *string
}

// loadPagerDutyOAuthStatus is PagerDutyOAuthCredentialRepository.get_status_metadata.
func loadPagerDutyOAuthStatus(ctx context.Context, db pgExecer, orgID, credentialName string) (*pagerDutyOAuthStatus, error) {
	var (
		row           pagerDutyOAuthStatus
		expiresAt     *time.Time
		grantedScopes []byte
	)
	err := db.QueryRow(ctx,
		`SELECT expires_at, granted_scopes::text, has_refresh_token, account_id, account_display
FROM provider_oauth_credentials WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2`,
		orgID, credentialName).Scan(&expiresAt, &grantedScopes, &row.HasRefreshToken, &row.AccountID, &row.AccountDisplay)
	if pgx.ErrNoRows == err {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	row.ExpiresAt = expiresAt
	scopes, err := decodePagerDutyScopeSet(grantedScopes)
	if err != nil {
		return nil, err
	}
	row.GrantedScopes = scopes
	return &row, nil
}

// decodePagerDutyScopeSet is `frozenset(row.granted_scopes or ())` then, at
// every call site, `sorted(...)`: a SQL NULL or JSON null decodes to no
// scopes; duplicates in the stored list collapse exactly as a Python set
// would.
func decodePagerDutyScopeSet(raw []byte) ([]string, error) {
	value, err := jsonColumnValue(raw)
	if err != nil {
		return nil, err
	}
	list, ok := value.([]pyjson.Value)
	if !ok {
		return nil, nil
	}
	seen := map[string]bool{}
	var out []string
	for _, item := range list {
		text, ok := item.(string)
		if !ok || seen[text] {
			continue
		}
		seen[text] = true
		out = append(out, text)
	}
	sort.Strings(out)
	return out, nil
}

// pagerDutyConfigString is _config_string: a present string value of
// config[key], nil for a missing key, a non-string value, or a nil config.
func pagerDutyConfigString(config *pyjson.Object, key string) *string {
	if config == nil {
		return nil
	}
	raw, ok := config.Get(key)
	if !ok {
		return nil
	}
	text, ok := raw.(string)
	if !ok {
		return nil
	}
	return &text
}

// pagerDutyStatusObject is PagerDutyStatusResponse for a loaded descriptor
// and its optional OAuth metadata; credentialName is always the response's
// own credential_name field (the request's own value, not read back from a row).
func pagerDutyStatusObject(credentialName string, descriptor *pagerDutyIntegrationCredential, metadata *pagerDutyOAuthStatus) *pyjson.Object {
	// Field order matches PagerDutyStatusResponse's DECLARED order exactly
	// (pydantic serializes in declaration order, not insertion order of
	// whatever built the response): connected, credential_name, auth_mode,
	// region, subdomain, account_id, account_display, granted_scopes,
	// expires_at, has_refresh_token.
	out := pyjson.NewObject()
	out.Set("connected", descriptor != nil && descriptor.IsActive)
	out.Set("credential_name", credentialName)
	authMode := pagerDutyConfigString(descriptorConfig(descriptor), "auth_mode")
	out.Set("auth_mode", optionalStringValue(authMode))
	out.Set("region", optionalStringValue(pagerDutyConfigString(descriptorConfig(descriptor), "region")))
	out.Set("subdomain", optionalStringValue(pagerDutyConfigString(descriptorConfig(descriptor), "subdomain")))
	if metadata != nil {
		out.Set("account_id", optionalStringValue(metadata.AccountID))
		out.Set("account_display", optionalStringValue(metadata.AccountDisplay))
	} else {
		out.Set("account_id", optionalStringValue(pagerDutyConfigString(descriptorConfig(descriptor), "account_id")))
		out.Set("account_display", nil)
	}
	grantedScopes := pagerDutyEffectiveScopes(authMode, metadata)
	scopeList := make([]pyjson.Value, len(grantedScopes))
	for index, scope := range grantedScopes {
		scopeList[index] = scope
	}
	out.Set("granted_scopes", scopeList)
	if metadata != nil && metadata.ExpiresAt != nil {
		out.Set("expires_at", pyTimeString(*metadata.ExpiresAt))
	} else {
		out.Set("expires_at", nil)
	}
	if metadata != nil {
		out.Set("has_refresh_token", metadata.HasRefreshToken)
	} else {
		out.Set("has_refresh_token", false)
	}
	return out
}

func descriptorConfig(descriptor *pagerDutyIntegrationCredential) *pyjson.Object {
	if descriptor == nil {
		return nil
	}
	return descriptor.Config
}

// pagerDutyEffectiveScopes is `metadata.granted_scopes if auth_mode ==
// "oauth" and metadata else frozenset()`.
func pagerDutyEffectiveScopes(authMode *string, metadata *pagerDutyOAuthStatus) []string {
	if metadata == nil || authMode == nil || *authMode != "oauth" {
		return nil
	}
	return metadata.GrantedScopes
}

func optionalStringValue(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}

// getPagerDutyStatus is pagerduty.py's get_pagerduty_status.
func (h *handlers) getPagerDutyStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	credentialName := "default"
	if value, present := queryLastValue(r.URL.Query(), "credential_name"); present {
		credentialName = value
	}
	descriptor, err := loadPagerDutyIntegrationCredential(ctx, h.store.Pool, orgID, credentialName)
	if err != nil {
		h.internalError(ctx, w, "load pagerduty credential", err)
		return
	}
	if descriptor == nil {
		policy.WriteModel(w, http.StatusOK, pagerDutyStatusObject(credentialName, nil, nil), nil)
		return
	}
	metadata, err := loadPagerDutyOAuthStatus(ctx, h.store.Pool, orgID, credentialName)
	if err != nil {
		h.internalError(ctx, w, "load pagerduty oauth status", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, pagerDutyStatusObject(credentialName, descriptor, metadata), nil)
}

// preflightPagerDuty is pagerduty.py's preflight_pagerduty.
func (h *handlers) preflightPagerDuty(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var (
		credentialName  = "default"
		enabledDatasets []string
	)
	if ok {
		if raw, present := errs.DefaultedString(object, "credential_name", 0, 0); present {
			normalized := pythonparity.Strip(raw)
			if normalized == "" {
				errs = append(errs, pydanticValueError([]pyjson.Value{"body", "credential_name"}, raw, "value must not be blank"))
			} else {
				credentialName = normalized
			}
		}
		enabledDatasets, _ = errs.RequiredStringList(object, "enabled_datasets")
		errs.ForbidExtra(object, "credential_name", "enabled_datasets")
	}
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	// set(enabled_datasets).difference(...): each unknown name once.
	var unknown []string
	seenUnknown := map[string]struct{}{}
	for _, dataset := range enabledDatasets {
		if _, known := pagerDutyDatasetFamilies[dataset]; known {
			continue
		}
		if _, dup := seenUnknown[dataset]; !dup {
			seenUnknown[dataset] = struct{}{}
			unknown = append(unknown, dataset)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		policy.WriteDetail(w, http.StatusBadRequest, "Unknown PagerDuty datasets: "+strings.Join(unknown, ", "), nil)
		return
	}

	descriptor, err := loadPagerDutyIntegrationCredential(ctx, h.store.Pool, orgID, credentialName)
	if err != nil {
		h.internalError(ctx, w, "load pagerduty credential", err)
		return
	}
	metadata, err := loadPagerDutyOAuthStatus(ctx, h.store.Pool, orgID, credentialName)
	if err != nil {
		h.internalError(ctx, w, "load pagerduty oauth status", err)
		return
	}
	authMode := pagerDutyConfigString(descriptorConfig(descriptor), "auth_mode")
	grantedScopes := pagerDutyEffectiveScopes(authMode, metadata)
	grantedSet := map[string]bool{}
	for _, scope := range grantedScopes {
		grantedSet[scope] = true
	}
	grantable := authMode != nil && (*authMode == "api_token" || *authMode == "client_credentials")

	datasets := make([]pyjson.Value, 0, len(enabledDatasets))
	for _, dataset := range enabledDatasets {
		family := pagerDutyDatasetFamilies[dataset]
		required := append([]string(nil), pagerDutyDatasetScopes[family]...)
		sort.Strings(required)
		var missing []string
		if !grantable {
			for _, scope := range required {
				if !grantedSet[scope] {
					missing = append(missing, scope)
				}
			}
			sort.Strings(missing)
		}
		entry := pyjson.NewObject()
		entry.Set("requested", dataset)
		requiredList := make([]pyjson.Value, len(required))
		for index, scope := range required {
			requiredList[index] = scope
		}
		entry.Set("required_scopes", requiredList)
		entry.Set("granted", len(missing) == 0)
		missingList := make([]pyjson.Value, len(missing))
		for index, scope := range missing {
			missingList[index] = scope
		}
		entry.Set("missing", missingList)
		datasets = append(datasets, entry)
	}

	connected := descriptor != nil && descriptor.IsActive && (authMode == nil || *authMode != "oauth" || metadata != nil)
	out := pyjson.NewObject()
	out.Set("connected", connected)
	out.Set("credential_name", credentialName)
	out.Set("datasets", datasets)
	policy.WriteModel(w, http.StatusOK, out, nil)
}
