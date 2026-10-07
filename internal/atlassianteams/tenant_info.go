package atlassianteams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"atlassian/atlassian"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TenantInfoPath is Atlassian's own unauthenticated, per-tenant endpoint
// that maps a site's base URL to its cloud id -- the same mechanism
// Atlassian Connect/Forge apps use to resolve cloudId without a stored
// value. Shared by every caller that needs a Params.SiteID and does not
// already have one stored (D2770/CHAOS-7002): the CLI verb and the
// automatic post-sync team-catalog collector both call ResolveCloudID
// rather than each deriving it their own way.
const TenantInfoPath = "/_edge/tenant_info"

// ResolveCloudID derives the Atlassian cloud (site) id for tenant: it tries
// TenantInfoPath first (a real Atlassian cloud id), falling back to the
// tenant's own subdomain -- the historical env-only path's approximation --
// only when that call fails, so an outage of that endpoint never regresses
// what an explicit override already worked around.
func ResolveCloudID(ctx context.Context, doer providerfoundation.HTTPDoer, tenant *url.URL) (string, error) {
	if tenant == nil {
		return "", errors.New("atlassianteams: no tenant URL to resolve a cloud id from")
	}
	if id, err := fetchTenantCloudID(ctx, doer, tenant); err == nil && id != "" {
		return id, nil
	}
	host := tenant.Hostname()
	if i := strings.Index(host, "."); i > 0 {
		return host[:i], nil
	}
	return "", fmt.Errorf("atlassianteams: the tenant_info endpoint failed and %q has no subdomain to fall back to", host)
}

func fetchTenantCloudID(ctx context.Context, doer providerfoundation.HTTPDoer, tenant *url.URL) (string, error) {
	if doer == nil {
		doer = &http.Client{Timeout: 15 * time.Second}
	}
	target := *tenant
	target.Path = TenantInfoPath
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return "", err
	}
	response, err := doer.Do(request)
	if err != nil {
		return "", logging.TransportFailure(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("tenant_info returned status %d", response.StatusCode)
	}
	var payload struct {
		CloudID string `json:"cloudId"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return "", logging.DecodeFailure(err)
	}
	cloudID := strings.TrimSpace(payload.CloudID)
	if cloudID == "" {
		return "", errors.New("tenant_info response had no cloudId")
	}
	return cloudID, nil
}

// tenantContextsQuery is the AGG (Atlassian GraphQL gateway) query Atlassian's
// own CLIs use to resolve a cloud id's organization id: the same
// email+API-token credential ResolveCloudID's caller already holds is
// sufficient (D2817/CHAOS-7020) -- no admin-scoped key, no new credential
// shape.
const tenantContextsQuery = `query TenantContexts($cloudIds: [ID!]!) {
  tenantContexts(cloudIds: $cloudIds) {
    orgId
    cloudId
  }
}
`

// OrganizationResolver is the raw-GraphQL-execution capability
// ResolveOrganizationID needs from the AGG gateway client -- deliberately not
// the Teams-specific Client interface Collect uses (SearchTeams/
// IterTeamUsers/IterTeamConnectedContainers), so a caller that only wants to
// resolve an organization id (and a test that only wants to fake that) never
// has to also satisfy Collect's larger surface. *graph.Client (production)
// satisfies both interfaces from one instance.
type OrganizationResolver interface {
	Execute(ctx context.Context, query string, variables map[string]any, operationName string, experimentalAPIs []string, estimatedCost int) (*atlassian.Result, error)
}

// ErrOrganizationNotFound and ErrOrganizationPermission are named so a caller
// can tell "this tenant has no organization context" (real, if unusual, data)
// from "not permitted to read it" (a scope/credential problem) from a generic
// transport failure, without leaking provider response text into a
// user-facing error body (D2742).
var (
	ErrOrganizationNotFound   = errors.New("atlassianteams: the tenant has no organization context")
	ErrOrganizationPermission = errors.New("atlassianteams: not permitted to read the tenant's organization id")
)

// ResolveOrganizationID derives the Atlassian organization id for cloudID via
// the AGG tenantContexts query -- the discovery path atlassian_organization_id
// never had (unlike ResolveCloudID's tenant_info endpoint), confirmed absent
// by CHAOS-7020's own trace. executor is an already-authenticated gateway
// client (the same atlassian.BasicAPITokenAuth{Email, Token} credential
// SearchTeams uses); this function makes exactly one request and returns the
// first tenant context's orgId.
func ResolveOrganizationID(ctx context.Context, executor OrganizationResolver, cloudID string) (string, error) {
	cloudID = strings.TrimSpace(cloudID)
	if cloudID == "" {
		return "", errors.New("atlassianteams: no cloud id to resolve an organization id from")
	}
	if executor == nil {
		return "", errors.New("atlassianteams: no gateway client to resolve an organization id")
	}
	result, err := executor.Execute(ctx, tenantContextsQuery, map[string]any{"cloudIds": []string{cloudID}}, "TenantContexts", nil, 1)
	if err != nil {
		var transportErr *atlassian.TransportError
		if errors.As(err, &transportErr) && (transportErr.StatusCode == http.StatusForbidden || transportErr.StatusCode == http.StatusUnauthorized) {
			return "", fmt.Errorf("%w (http %d)", ErrOrganizationPermission, transportErr.StatusCode)
		}
		var gqlErr *atlassian.GraphQLOperationError
		if errors.As(err, &gqlErr) && graphQLErrorsNameAPermissionProblem(gqlErr.Errors) {
			return "", ErrOrganizationPermission
		}
		return "", fmt.Errorf("resolve the atlassian organization id: %w", err)
	}
	if result == nil || result.Data == nil {
		return "", ErrOrganizationNotFound
	}
	contexts, _ := result.Data["tenantContexts"].([]any)
	if len(contexts) == 0 {
		return "", ErrOrganizationNotFound
	}
	first, _ := contexts[0].(map[string]any)
	orgID, _ := first["orgId"].(string)
	orgID = strings.TrimSpace(orgID)
	if orgID == "" {
		return "", ErrOrganizationNotFound
	}
	return orgID, nil
}

// graphQLErrorsNameAPermissionProblem mirrors GraphQLOperationError.Error's
// own scope-extension check: an AGG error carrying a required-scopes
// extension is a permission problem, not a missing-data one.
func graphQLErrorsNameAPermissionProblem(errs []atlassian.GraphQLError) bool {
	for _, gqlErr := range errs {
		for _, key := range []string{"requiredScopes", "required_scopes", "required_scopes_any", "required_scopes_all"} {
			if val, ok := gqlErr.Extensions[key]; ok && val != nil {
				return true
			}
		}
	}
	return false
}
