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
