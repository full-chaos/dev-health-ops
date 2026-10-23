package authruntime

import (
	"context"
	"log/slog"
	"sync"

	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/ratelimitvalkey"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
)

// CheckValkey is the readiness check of the shared rate-limit store.
const CheckValkey = "valkey"

// metricsSourceRateLimit names the store-error counter's source on /metrics.
const metricsSourceRateLimit = "auth_rate_limit_store"

// valkeyClientName labels this service's connection in CLIENT LIST.
const valkeyClientName = "auth-service"

// lazyStore is the shared counter store, dialled on first use. Nothing dials
// at startup: an unreachable Valkey leaves the process running with /readyz
// failing on the "valkey" check, as an unreachable database does, and every
// limited route answers its closed 500 until the store answers.
type lazyStore struct {
	config valkey.Config

	mu     sync.Mutex
	client valkeygo.Client
	store  *ratelimitvalkey.Store
}

var _ httpapi.CounterStore = (*lazyStore)(nil)

func (l *lazyStore) open(ctx context.Context) (*ratelimitvalkey.Store, valkeygo.Client, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.store != nil {
		return l.store, l.client, nil
	}
	client, err := valkey.Open(ctx, l.config)
	if err != nil {
		return nil, nil, err
	}
	store, err := ratelimitvalkey.New(client)
	if err != nil {
		client.Close()
		return nil, nil, err
	}
	l.client, l.store = client, store
	return store, client, nil
}

func (l *lazyStore) Increment(ctx context.Context, hit httpapi.Hit) (int64, error) {
	store, _, err := l.open(ctx)
	if err != nil {
		return 0, err
	}
	return store.Increment(ctx, hit)
}

func (l *lazyStore) Peek(ctx context.Context, hit httpapi.Hit) (int64, error) {
	store, _, err := l.open(ctx)
	if err != nil {
		return 0, err
	}
	return store.Peek(ctx, hit)
}

func (l *lazyStore) Backend() string { return "redis" }

func (l *lazyStore) ping(ctx context.Context) error {
	_, client, err := l.open(ctx)
	if err != nil {
		return err
	}
	return client.Do(ctx, client.B().Ping().Build()).Error()
}

func (l *lazyStore) Name() string                { return "auth-valkey" }
func (l *lazyStore) Start(context.Context) error { return nil }
func (l *lazyStore) Shutdown(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.client != nil {
		l.client.Close()
		l.client, l.store = nil, nil
	}
	return nil
}

// limitStore returns the limits (the shared counter store and the digest key,
// derived from the session signing secret AUTH_JWT_SECRET_KEY) when a limited
// route mounts, and nil (no key derived, nothing built, nothing dialled) when
// none does: a value in AUTH_VALKEY_URI beside no limited route is inert. The
// login is this service's own (an ACL user limited to the auth_svc_ key
// prefix), never go-api's.
func limitStore(
	cfg authconfig.Config, logger *slog.Logger, registry *health.Registry,
) (*authapi.Limits, []lifecycle.Component, error) {
	if !cfg.LimitedRoutesMounted() {
		return nil, nil, nil
	}
	config := valkey.DefaultConfig(cfg.ValkeyURI.Reveal())
	config.ClientName = valkeyClientName
	store := &lazyStore{config: config}
	if err := registry.RegisterRequired(CheckValkey, func(ctx context.Context) error {
		if pingErr := store.ping(ctx); pingErr != nil {
			logger.ErrorContext(ctx, "readiness check failed", "check", CheckValkey, "reason", "valkey_unavailable")
			return pingErr
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	// The store-error counter is process-wide; only go-api registered it, so
	// without this it counted into nothing on this service's /metrics.
	if err := registry.RegisterMetrics(metricsSourceRateLimit, httpapi.RateLimitStoreErrors); err != nil {
		return nil, nil, err
	}
	limits, err := authapi.NewLimits(store, cfg.Session.AccessSecret.Reveal())
	if err != nil {
		return nil, nil, err
	}
	return limits, []lifecycle.Component{store}, nil
}
