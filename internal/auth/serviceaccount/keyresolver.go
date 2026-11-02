package serviceaccount

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// KeyResolverConfig configures a KeyResolver.
type KeyResolverConfig struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	Logger *slog.Logger
	Meter  metric.Meter
}

// KeyResolver is the read-only ResolveKey: no write verb and no row locker, so
// a caller that only resolves keys needs neither.
type KeyResolver struct{ store *Store }

// NewKeyResolver returns a KeyResolver over pool.
func NewKeyResolver(cfg KeyResolverConfig) (*KeyResolver, error) {
	switch {
	case cfg.Pool == nil:
		return nil, errors.New("serviceaccount: no connection pool")
	case cfg.Schema.String() == "":
		return nil, errors.New("serviceaccount: no auth schema")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount")
	}
	resolves, err := registerKeyResolves(meter)
	if err != nil {
		return nil, err
	}
	return &KeyResolver{store: &Store{pool: cfg.Pool, q: authschema.Quote(cfg.Schema), logger: logger, keyResolves: resolves}}, nil
}

// ResolveKey is Store.ResolveKey.
func (r *KeyResolver) ResolveKey(ctx context.Context, digest [sha256.Size]byte, now time.Time) (ResolvedKey, error) {
	return r.store.ResolveKey(ctx, digest, now)
}

func registerKeyResolves(meter metric.Meter) (metric.Int64Counter, error) {
	c, err := meter.Int64Counter("auth.serviceaccount.key_resolves",
		metric.WithDescription("Service account key resolves, by outcome."))
	if err != nil {
		return nil, fmt.Errorf("serviceaccount: registering the key resolve counter: %w", err)
	}
	return c, nil
}
