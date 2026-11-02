package serviceaccount

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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

// KeyResolver is the read-only ResolveKey: no write verb, no row lock.
type KeyResolver struct{}

// NewKeyResolver returns a KeyResolver.
func NewKeyResolver(KeyResolverConfig) (*KeyResolver, error) {
	return nil, errors.New("serviceaccount: not implemented")
}

// ResolveKey is Store.ResolveKey.
func (r *KeyResolver) ResolveKey(context.Context, [sha256.Size]byte, time.Time) (ResolvedKey, error) {
	return ResolvedKey{}, errors.New("serviceaccount: not implemented")
}
