// Package authstore is the Auth Control Plane's PostgreSQL boundary.
//
// Everything above it -- handlers, readiness, and the domain packages later
// waves add -- depends on the interfaces declared here, never on pgx, so the
// storage driver can be replaced without reshaping the domain.
//
// # Ownership
//
// ACP-ADR-04 (Accepted 2026-09-02) gives the auth control plane a separate
// PostgreSQL schema and a separate role on the same instance, and states that
// "auth-migrate is a separate binary and the runtime never auto-migrates: the
// runtime role owns no DDL." Nothing in this package issues DDL, and nothing
// in it runs a migration. The schema's existence is a READINESS question here,
// answered by observation -- if the schema is absent the service reports
// not-ready rather than creating it.
package authstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// Reason is a bounded, DSN-free classification of a storage failure. pgx dial
// errors render host:port and can render a user name, so no error from this
// package ever carries the driver's text outward -- only one of these codes.
type Reason string

const (
	ReasonUnreachable   Reason = "postgres_unreachable"
	ReasonSchemaMissing Reason = "auth_schema_missing"
	ReasonInvalidConfig Reason = "postgres_config_invalid"
)

// Error is a storage failure carrying only its Reason outwards.
type Error struct {
	Reason Reason
	cause  error
}

func (e *Error) Error() string { return string(e.Reason) }

func (e *Error) Unwrap() error { return e.cause }

// DependencyReason exposes the bounded code for logging without redaction.
func (e *Error) DependencyReason() string { return string(e.Reason) }

func failure(reason Reason, cause error) error { return &Error{Reason: reason, cause: cause} }

// ReasonOf extracts the bounded reason from an error produced by this package.
// It never falls back to err.Error(): a call path that forgets to wrap fails
// to a less specific label, never open to a raw DSN-bearing string.
func ReasonOf(err error) string {
	var storage *Error
	if errors.As(err, &storage) {
		return string(storage.Reason)
	}
	return "storage_failed"
}

// Prober reports whether the auth store is fit to serve.
//
// It is the interface the readiness check depends on, so a test can drive
// readiness without a database and a later wave can swap the adapter without
// touching the runtime.
type Prober interface {
	Probe(ctx context.Context) error
}

// Config describes the auth-owned connection.
type Config struct {
	// URI is the DSN. It is never logged, never returned in an error, and
	// never rendered by SafeAttributes.
	URI string
	// Schema is the auth-owned schema whose presence Probe verifies.
	Schema string
	// MaxConns bounds connections held by this replica.
	MaxConns int32
	// ConnectTimeout bounds a single dial attempt.
	ConnectTimeout time.Duration
}

// Postgres is the pgx-backed adapter. It is also a lifecycle component so the
// runtime owns its shutdown rather than a handler or a package-level variable.
type Postgres struct {
	pool   *pgxpool.Pool
	schema string
}

var _ Prober = (*Postgres)(nil)

// Open constructs a bounded pool WITHOUT performing network I/O.
//
// This is what lets readiness stay false and then recover: a configured
// database that is temporarily unavailable must not prevent the process from
// starting, or /readyz has nothing to report and an orchestrator sees a
// crash-loop instead of an unready replica. It is also what makes CHAOS-4881's
// executed negative proof possible at all -- a deliberately broken DSN has to
// reach a readiness response, not abort main().
func Open(ctx context.Context, cfg Config) (*Postgres, error) {
	if cfg.Schema == "" {
		return nil, failure(ReasonInvalidConfig, errors.New("auth schema is required"))
	}
	poolConfig := postgres.DefaultConfig(cfg.URI)
	if cfg.MaxConns > 0 {
		poolConfig.MaxConns = cfg.MaxConns
	}
	if cfg.ConnectTimeout > 0 {
		poolConfig.ConnectTimeout = cfg.ConnectTimeout
	}
	// internal/storage/postgres.New is reused rather than reimplemented: it
	// already parses the DSN, applies the bounded pool settings, and -- the
	// part that matters here -- replaces every driver error with a stable
	// category so a malformed or unreachable URI cannot appear in a log line.
	pool, err := postgres.New(ctx, poolConfig)
	if err != nil {
		if errors.Is(err, postgres.ErrInvalidConfig) {
			return nil, failure(ReasonInvalidConfig, err)
		}
		return nil, failure(ReasonUnreachable, err)
	}
	return &Postgres{pool: pool, schema: cfg.Schema}, nil
}

// Name identifies the component to the lifecycle runtime.
func (*Postgres) Name() string { return "auth-postgres" }

// Start performs no I/O on purpose. Connectivity is a readiness question,
// re-answered live on every probe; see Open's doc comment.
func (*Postgres) Start(context.Context) error { return nil }

// Shutdown closes the pool. pgxpool.Close waits for every acquired connection
// to be released, so it is dispatched by lifecycle.Runtime under that
// component's bounded share of the shutdown budget rather than being allowed
// to block the process indefinitely.
func (p *Postgres) Shutdown(context.Context) error {
	if p == nil || p.pool == nil {
		return nil
	}
	p.pool.Close()
	return nil
}

// Probe verifies, under the caller's deadline, that the database answers AND
// that the auth-owned schema is visible to this role.
//
// Both halves are required. A reachable instance whose auth schema has not
// been migrated yet (or which this role cannot see) is not a database this
// service can serve from, and answering "ready" there would hand traffic to a
// replica whose every query is about to fail. ACP-ADR-04 puts the schema's
// creation in auth-migrate, so from the runtime's side its absence is an
// observation, never something to repair.
//
// The schema name is bound as a QUERY PARAMETER, not interpolated: this
// compares against pg_namespace.nspname rather than naming an identifier, so
// no operator-supplied value ever reaches the SQL text. authconfig
// additionally bounds the value to an unquoted-identifier character set, which
// is belt-and-braces for the day a call site does need it as an identifier.
func (p *Postgres) Probe(ctx context.Context) error {
	if p == nil || p.pool == nil {
		return failure(ReasonInvalidConfig, errors.New("pool is not constructed"))
	}
	if err := p.pool.Ping(ctx); err != nil {
		return failure(ReasonUnreachable, err)
	}
	var present bool
	err := p.pool.QueryRow(
		ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1)`,
		p.schema,
	).Scan(&present)
	if err != nil {
		return failure(ReasonUnreachable, err)
	}
	if !present {
		return failure(ReasonSchemaMissing, fmt.Errorf("schema %q is not visible", p.schema))
	}
	return nil
}
