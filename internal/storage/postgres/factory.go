package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

var (
	ErrInvalidConfig = errors.New("invalid PostgreSQL connection configuration")
	ErrUnavailable   = errors.New("PostgreSQL readiness check failed")
)

// Config defines a bounded PostgreSQL pool. URI is intentionally excluded from
// SafeAttributes and from every returned error.
type Config struct {
	URI               string
	MinConns          int32
	MaxConns          int32
	ConnectTimeout    time.Duration
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	// Tracer is optional and unused by most callers (the zero value is nil,
	// preserving today's behavior). NewRuntimePools sets it to instrument
	// Acquire for the domain and queue-control pools.
	Tracer pgx.QueryTracer
}

func DefaultConfig(uri string) Config {
	return Config{
		URI:               uri,
		MinConns:          0,
		MaxConns:          4,
		ConnectTimeout:    5 * time.Second,
		MaxConnLifetime:   30 * time.Minute,
		MaxConnIdleTime:   5 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
	}
}

func (c Config) Validate() error {
	if c.URI == "" || c.MinConns < 0 || c.MaxConns <= 0 || c.MinConns > c.MaxConns {
		return ErrInvalidConfig
	}
	if c.ConnectTimeout <= 0 || c.MaxConnLifetime <= 0 || c.MaxConnIdleTime <= 0 || c.HealthCheckPeriod <= 0 {
		return ErrInvalidConfig
	}
	if _, err := parseConfig(c.URI); err != nil {
		return ErrInvalidConfig
	}
	return nil
}

func (c Config) SafeAttributes() map[string]any {
	return map[string]any{
		"configured":          c.URI != "",
		"min_connections":     c.MinConns,
		"max_connections":     c.MaxConns,
		"connect_timeout":     c.ConnectTimeout.String(),
		"max_connection_age":  c.MaxConnLifetime.String(),
		"max_connection_idle": c.MaxConnIdleTime.String(),
		"health_check_period": c.HealthCheckPeriod.String(),
	}
}

// New creates a bounded pool without performing network I/O. Long-running
// processes use this form so readiness can remain false and recover when a
// configured database endpoint is temporarily unavailable.
func New(ctx context.Context, config Config) (*pgxpool.Pool, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	poolConfig, err := parseConfig(config.URI)
	if err != nil {
		return nil, ErrInvalidConfig
	}
	poolConfig.MinConns = config.MinConns
	poolConfig.MaxConns = config.MaxConns
	poolConfig.MaxConnLifetime = config.MaxConnLifetime
	poolConfig.MaxConnIdleTime = config.MaxConnIdleTime
	poolConfig.HealthCheckPeriod = config.HealthCheckPeriod
	poolConfig.ConnConfig.ConnectTimeout = config.ConnectTimeout
	if config.Tracer != nil {
		poolConfig.ConnConfig.Tracer = config.Tracer
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, secrets.WithRedactedCauseAlso(ErrUnavailable, config.URI, err, resolvedCredentials(poolConfig)...)
	}
	return pool, nil
}

// Open creates and verifies a pool. A driver error is returned under a stable
// category (errors.Is ErrUnavailable) with the driver's text appended after
// every credential component of the URI is redacted, so an operator can tell a
// refused dial from an authentication failure while the URI and its password
// never appear in logs.
func Open(ctx context.Context, config Config) (*pgxpool.Pool, error) {
	pool, err := New(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		resolved := resolvedCredentials(pool.Config())
		pool.Close()
		return nil, secrets.WithRedactedCauseAlso(ErrUnavailable, config.URI, err, resolved...)
	}
	return pool, nil
}

// resolvedCredentials is the login and password pgx settled on. It reads them
// from the DSN, PGUSER and PGPASSWORD, and service files, so the effective login
// can be absent from the DSN and still appear in the server's failure text.
func resolvedCredentials(poolConfig *pgxpool.Config) []string {
	return []string{poolConfig.ConnConfig.User, poolConfig.ConnConfig.Password}
}

// Boundary is the secrets.Boundary of a PostgreSQL URI: the URI's own
// credential components and the login and password the driver settles on. A
// boundary built from the URI alone (secrets.NewBoundary) cannot know a login or
// password that came from PGUSER, PGPASSWORD, a password file or a service file,
// which never appear in the URI yet appear in a server's failure text. A CLI verb
// that opens PostgreSQL itself (a raw pgx connection or pool, not Open) builds its
// boundary here. A URI pgx cannot parse gets the URI's own components (the parse
// error, which the driver returns again, carries no resolved credential).
func Boundary(uri string) secrets.Boundary {
	poolConfig, err := parseConfig(uri)
	if err != nil {
		return secrets.NewBoundary(uri)
	}
	return secrets.NewBoundaryWith(uri, resolvedCredentials(poolConfig)...)
}

func parseConfig(uri string) (*pgxpool.Config, error) {
	return pgxpool.ParseConfig(normalizeURI(uri))
}

// normalizeURI accepts the documented SQLAlchemy driver-qualified aliases at
// the Go boundary without changing credentials, host, path, or query options.
func normalizeURI(uri string) string {
	for _, driverScheme := range []string{
		"postgresql+asyncpg://",
		"postgresql+psycopg://",
		"postgresql+psycopg2://",
		"postgres+asyncpg://",
	} {
		if strings.HasPrefix(strings.ToLower(uri), driverScheme) {
			return "postgresql://" + uri[len(driverScheme):]
		}
	}
	return uri
}

// ConnectionUser returns only the PostgreSQL role name. It is suitable for
// privilege-policy validation; callers must never log the source URI.
func ConnectionUser(uri string) (string, error) {
	config, err := parseConfig(uri)
	if err != nil || config.ConnConfig.User == "" {
		return "", ErrInvalidConfig
	}
	return config.ConnConfig.User, nil
}
