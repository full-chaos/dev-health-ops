package authschema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/pgident"
)

// advisoryLockKey serialises concurrent auth-migrate runs against one
// database. Two migrators racing would each see an out-of-date version table
// and try to apply the same migration; the loser would fail on an object that
// already exists, which is recoverable but reads as a real failure. The key is
// an arbitrary constant, chosen once, and must never change: changing it would
// stop new binaries from excluding old ones during a rollout, which is exactly
// when two migrators are most likely to run at the same time.
const advisoryLockKey int64 = 0x4155_5448_0001 // "AUTH" + lineage 1

// versionTable is the auth lineage's own bookkeeping, inside the auth schema.
// It is deliberately NOT alembic's table and deliberately not in public: the
// lineage is independent (ACP-ADR-04, ruled 2026-09-02), so its state lives
// with the objects it describes and travels with them.
const versionTable = "schema_migrations"

// Options configures one migration run.
type Options struct {
	// Schema is the auth-owned schema to create and populate.
	Schema string
	// RuntimeRole is the least-privilege role auth-service connects as. It is
	// only ever the SUBJECT of a GRANT here; it never executes anything.
	RuntimeRole string
	// SessionReaderRoles are the roles of the services that end an access
	// token when its session is revoked (go-api and query-api read
	// SessionReaderColumns, and nothing else in the schema). Empty means none.
	SessionReaderRoles []string
	// Logger receives one structured line per applied migration. Optional.
	Logger *slog.Logger
	// AcknowledgeNarrowing is the one-shot operator acknowledgement of a
	// deliberate narrowing of the runtime role's grants: the Digest printed by
	// the refusal (ErrRuntimeGrantsNarrowed). It binds to that exact set of
	// lost grants, so any other narrowing refuses again. Empty means none.
	AcknowledgeNarrowing string
}

// Result reports what a run did.
type Result struct {
	// CurrentVersion is the lineage position the database holds afterwards.
	CurrentVersion int
	// AppliedVersions lists what this run applied, empty when already current.
	AppliedVersions []int
}

var (
	// ErrInvalidOptions reports a configuration this package refuses to act
	// on, including the case that matters most: a migration connection whose
	// role IS the runtime role.
	ErrInvalidOptions = errors.New("invalid auth migration options")
	// ErrMigrationFailed reports a migration that did not apply. The
	// underlying driver error is wrapped for errors.Is/As but never rendered
	// into this message, because a pgx error can carry the DSN's host and user.
	ErrMigrationFailed = errors.New("auth migration failed")
)

// ValidateIdentifier reports whether a schema or role name may be emitted as
// a bare identifier by this package. The rule is internal/platform/pgident's,
// shared with the ops migrator so the two cannot disagree about which schema
// name the auth migrator can own.
func ValidateIdentifier(name string) error {
	switch pgident.Check(name) {
	case pgident.BadShape:
		return fmt.Errorf(
			"%w: %q must match [a-z][a-z0-9_]* and be at most 63 characters",
			ErrInvalidOptions, name,
		)
	case pgident.ReservedKeyword:
		return fmt.Errorf("%w: %q is a PostgreSQL reserved keyword", ErrInvalidOptions, name)
	}
	return nil
}

// Validate checks the options without touching the database.
func (o Options) Validate(migrationRole string) error {
	if err := ValidateIdentifier(o.Schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(o.RuntimeRole); err != nil {
		return err
	}
	// ACP-ADR-04: the runtime role owns no DDL. If the migration connection
	// authenticates AS the runtime role, then every object this run creates is
	// owned by the runtime role, which holds DDL over its own objects
	// permanently -- the exact posture the ADR forbids, established silently
	// by a successful-looking migration. Refuse instead.
	if migrationRole != "" && migrationRole == o.RuntimeRole {
		return fmt.Errorf(
			"%w: the migration role and the runtime role must be distinct "+
				"(both are %q); the runtime role must own no DDL (ACP-ADR-04)",
			ErrInvalidOptions, o.RuntimeRole,
		)
	}
	return nil
}

// Apply brings the database up to the embedded lineage head.
//
// It takes an advisory lock for the whole run, applies each pending migration
// in its OWN transaction together with that migration's version row, and
// returns what it did. Per-migration transactions mean a failure leaves the
// database at the last fully-applied version rather than half-way through one,
// and the version row committing with the DDL means the two cannot disagree.
func Apply(ctx context.Context, pool *pgxpool.Pool, options Options) (Result, error) {
	if pool == nil {
		return Result{}, fmt.Errorf("%w: no connection pool", ErrInvalidOptions)
	}
	return apply(ctx, poolAdapter{pool}, options)
}

// querier is the slice of a connection the migration steps need; *pgx.Conn,
// *pgxpool.Conn and a recording stub all satisfy it.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// pooledConn is one acquired migration connection.
type pooledConn interface {
	querier
	Release()
	Conn() *pgx.Conn
}

// migrationPool is what apply needs from the pool.
type migrationPool interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Acquire(ctx context.Context) (pooledConn, error)
}

type poolAdapter struct{ *pgxpool.Pool }

func (p poolAdapter) Acquire(ctx context.Context) (pooledConn, error) {
	conn, err := p.Pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func apply(ctx context.Context, pool migrationPool, options Options) (Result, error) {
	migrations, err := Migrations()
	if err != nil {
		return Result{}, err
	}

	migrationRole, err := currentUser(ctx, pool)
	if err != nil {
		return Result{}, err
	}
	if err := options.Validate(migrationRole); err != nil {
		return Result{}, err
	}
	// One construction, carried onward. Everything downstream that interpolates
	// the schema name takes ValidatedIdentifier, so this is the only place the
	// raw string can become an identifier.
	schemaID, err := NewValidatedIdentifier(options.Schema)
	if err != nil {
		return Result{}, err
	}

	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	// The lock is held on ONE connection for the whole run; releasing it is
	// tied to that connection's release, so an abandoned run cannot strand it.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("%w: acquiring a migration connection", ErrMigrationFailed)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey); err != nil {
		return Result{}, fmt.Errorf("%w: acquiring the migration lock", ErrMigrationFailed)
	}
	defer func() {
		// Best effort: a failure here means the connection is already gone,
		// which releases the lock anyway.
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, advisoryLockKey)
	}()

	if err := ensureSchemaAndVersionTable(ctx, conn, options); err != nil {
		return Result{}, err
	}
	applied, err := appliedVersions(ctx, conn, schemaID)
	if err != nil {
		return Result{}, err
	}

	result := Result{CurrentVersion: highest(applied)}
	for _, migration := range migrations {
		if _, done := applied[migration.Version]; done {
			continue
		}
		if err := applyOne(ctx, conn, options, migration); err != nil {
			return result, err
		}
		result.CurrentVersion = migration.Version
		result.AppliedVersions = append(result.AppliedVersions, migration.Version)
		logger.InfoContext(
			ctx, "applied auth migration",
			"version", migration.Version, "name", migration.Name, "schema", options.Schema,
		)
	}

	// Grants are re-applied on EVERY run, not only when a migration was
	// applied. ApplyRuntimeGrants revokes and re-grants from the manifest, so
	// running it unconditionally is what makes the manifest authoritative:
	// a posture that drifted for any reason -- a hand-run GRANT, a privilege
	// removed from the manifest in a later release, a restored backup -- is
	// corrected by the next migrate run rather than persisting because no new
	// migration happened to be pending.
	if err := ApplyRuntimeGrants(ctx, conn.Conn(), options); err != nil {
		return result, err
	}
	logger.InfoContext(
		ctx, "applied auth runtime grants",
		"schema", options.Schema, "runtime_role", options.RuntimeRole,
		"tables", len(RuntimePosture()),
	)

	// Grants applied without error proves the statements succeeded. It does
	// NOT prove the runtime role holds only what the manifest declares:
	// object-level grants are not the only route to authority, and codex
	// round 1 demonstrated two that survive a successful reapply -- membership
	// in the object-owning migration role, and CREATE on the database. So the
	// posture is RE-DERIVED from the live database here and the run FAILS if
	// the runtime role can still escalate. A migrate that reports success over
	// a runtime role which can SET ROLE to the schema's owner is worse than
	// one that fails, because it is the report an operator will trust.
	//
	// The migrations themselves stay applied: they are additive and correct,
	// and the escalation is a pre-existing cluster-level condition this tool
	// deliberately does not rewrite (see EscalationPath's doc comment). What
	// fails is the RUN, with the offending grant and its remedy named.
	violations, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
	if err != nil {
		return result, err
	}
	if len(violations) > 0 {
		logger.ErrorContext(
			ctx, "runtime role can escalate beyond its declared posture",
			"schema", options.Schema, "runtime_role", options.RuntimeRole,
			"violations", len(violations),
		)
		return result, fmt.Errorf(
			"%w (schema %q, role %q):\n%s",
			ErrRuntimeRoleCanEscalate, options.Schema, options.RuntimeRole, describeViolations(violations),
		)
	}
	logger.InfoContext(
		ctx, "verified auth runtime posture",
		"schema", options.Schema, "runtime_role", options.RuntimeRole,
		"extra_privileges", 0,
	)
	return result, nil
}

// applyOne runs a single migration and records it, atomically.
func applyOne(ctx context.Context, conn querier, options Options, migration Migration) error {
	schemaID, err := NewValidatedIdentifier(options.Schema)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: beginning version %d", ErrMigrationFailed, migration.Version)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// search_path is set for the transaction so migration SQL can name its
	// own tables unqualified and cannot accidentally create anything in
	// public. pg_temp is excluded deliberately: a temp object shadowing a
	// real one during DDL is a genuinely confusing failure.
	if _, err := tx.Exec(ctx, `SELECT set_config('search_path', $1, true)`, options.Schema); err != nil {
		return fmt.Errorf("%w: setting search_path for version %d", ErrMigrationFailed, migration.Version)
	}
	if _, err := tx.Exec(ctx, migration.SQL); err != nil {
		// The driver error carries the failing statement, which is our own
		// embedded SQL and therefore safe -- but it can also carry connection
		// context, so only the version and name are surfaced.
		return fmt.Errorf(
			"%w: applying version %d (%s): %v",
			ErrMigrationFailed, migration.Version, migration.Name, redactPGError(err),
		)
	}
	if _, err := tx.Exec(
		ctx,
		fmt.Sprintf(
			`INSERT INTO %s.%s (version, name, applied_at) VALUES ($1, $2, now())`,
			quoteIdentifier(schemaID), quoteIdentifier(versionTableIdentifier),
		),
		migration.Version, migration.Name,
	); err != nil {
		return fmt.Errorf("%w: recording version %d", ErrMigrationFailed, migration.Version)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: committing version %d", ErrMigrationFailed, migration.Version)
	}
	return nil
}

func highest(applied map[int]struct{}) int {
	current := 0
	for version := range applied {
		if version > current {
			current = version
		}
	}
	return current
}
