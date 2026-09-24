package teamsidentity

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/externalurl"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// alwaysValidLease satisfies providerfoundation.LeaseGuard for a live admin
// HTTP request. providerfoundation's lease contract exists for a
// background sync worker's CLAIMED sync unit (CHAOS-3045/3046) -- a
// request handled synchronously, start to finish, inside one incoming
// HTTP request has no claim to lose partway through, so this never
// refuses.
type alwaysValidLease struct{}

func (alwaysValidLease) Assert(context.Context) error { return nil }

// discoverCredentials is what GET /teams/discover resolves a stored
// provider credential from: the SAME Postgres integration_credentials
// table and Fernet decryptor (SETTINGS_ENCRYPTION_KEY) internal/apiservice
// already builds into Deps for webhookintake's own credential use.
type discoverCredentials struct {
	Pool      *pgxpool.Pool
	Decryptor providerfoundation.CredentialDecryptor
}

// resolve mirrors IntegrationCredentialsService.resolve_with_fallback's id
// -> name -> "default" -> single-active order (integration_credentials.py:
// 245-297) via providerfoundation.PostgresCredentialRepository.
// ResolveEncrypted, then decrypts. credentialID/credentialName empty means
// "not given", matching the route's own optional query parameters.
func (d discoverCredentials) resolve(ctx context.Context, orgID, provider, credentialID, credentialName string) (providerfoundation.Credential, error) {
	resolver := providerfoundation.CredentialResolver{
		Repository: providerfoundation.PostgresCredentialRepository{Pool: d.Pool},
		Decryptor:  d.Decryptor,
	}
	scope := providerfoundation.TenantScope{
		OrgID:    orgID,
		Provider: provider,
		// IntegrationID is validated non-empty by TenantScope.Validate but
		// never referenced by ResolveEncrypted's own query (confirmed by
		// reading it) -- it exists for a claimed sync unit's real identity,
		// which a live admin discover request has no equivalent of. A
		// fixed sentinel satisfies the shape without inventing a fake
		// sync-unit identity that could be confused for a real one.
		IntegrationID:  "admin-discover",
		CredentialID:   credentialID,
		CredentialName: credentialName,
	}
	return resolver.Resolve(ctx, alwaysValidLease{}, scope)
}

// discoveryPerAttemptTimeout bounds one HTTP attempt; providerfoundation.
// HTTPClient's own retry/backoff policy governs how many attempts a
// discovery call makes, so this only guards against one hung TCP
// connection stalling a single attempt.
const discoveryPerAttemptTimeout = 30 * time.Second

// discoveryHTTPClient is the *http.Client every discovery call shares.
var discoveryHTTPClient providerfoundation.HTTPDoer = &http.Client{Timeout: discoveryPerAttemptTimeout, Transport: externalurl.GuardedTransport()}
