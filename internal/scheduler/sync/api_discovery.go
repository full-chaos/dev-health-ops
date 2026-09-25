package sync

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// NewAPISourceDiscovery builds source discovery for the api process: the
// upserts run on the api pool (its role holds the integration_sources
// grants), the stored credential resolves through the api's decryptor, and
// client carries the provider calls (nil: a 45 s client that follows no
// redirects). The sync config create path and the integration discover route
// share it, so both run the one discovery implementation. nil without a pool
// or a decryptor.
func NewAPISourceDiscovery(
	pool *pgxpool.Pool, decryptor providerfoundation.CredentialDecryptor, client providerfoundation.HTTPDoer,
	logger *slog.Logger, clock func() time.Time,
) (*NativeSourceDiscoveryService, error) {
	if pool == nil || decryptor == nil {
		return nil, ErrSourceDiscoveryUnavailable
	}
	if client == nil {
		client = &http.Client{Timeout: 45 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}
	}
	discovery, err := NewNativeSourceDiscoveryService(pool, providerfoundation.CredentialResolver{
		Repository: providerfoundation.PostgresCredentialRepository{Pool: pool},
		Decryptor:  decryptor,
	}, client, logger)
	if err != nil {
		return nil, err
	}
	return discovery.WithClock(clock), nil
}
