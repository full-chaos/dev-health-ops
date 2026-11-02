package authschema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

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

// The lock policy of one migration's transaction (CHAOS-12200).
//
// A migration that needs a strong table lock -- 0037's ALTER TABLE
// credential_registry takes ACCESS EXCLUSIVE -- waits behind every open
// transaction on that table, and while it waits every NEW reader of the table
// queues behind it. Without a bound, auth paths that read the table stall for
// as long as the oldest open reader stays open.
//
// lock_timeout bounds that wait. After LockTimeout the attempt rolls back and
// gives up its place in the queue, so the queued readers are served. The
// migration is tried again after a pause of the same length, at most
// LockAttempts times in all. Every timed-out attempt logs a warning, and the
// last one fails the run with ErrMigrationLockTimeout, with the version table
// at the last migration that committed.
//
// The setting is transaction-local (set_config(..., true)) on purpose. The
// session-level advisory lock that serialises auth-migrate runs is a lock wait
// too, so a session-level lock_timeout would turn a concurrent run's wait for
// the first run into a failure.
const (
	DefaultMigrationLockTimeout  = 5 * time.Second
	DefaultMigrationLockAttempts = 3
)

// lockNotAvailable is the SQLSTATE of a lock wait that lock_timeout ended.
const lockNotAvailable = "55P03"

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
	// RequirePublicTables makes a PublicReadColumns table or column that does
	// not exist an error instead of a logged skip (DEFERRED for a column).
	// Grants sets it: the grants pass runs after the ops migration, so a
	// missing table or column is a failed release, not an auth-only database
	// or a schema before the revision that adds the column.
	RequirePublicTables bool
	// UsersOwned is the users-owned switch (AUTH_USERS_OWNED): the runtime role
	// holds UsersWriteGrant on public.users. Off, it holds no write privilege
	// there, and the grants pass revokes any it held.
	UsersOwned bool
	// DeferNarrowing keeps, with the acknowledgement, every lost grant the
	// users-owned switch gives (the 4->3 flip-back release, whose old owned
	// pods still serve); the rest narrows. See defer_narrowing.go.
	DeferNarrowing bool
	// PlatformRolesOwned is the platform roles switch (AUTH_PLATFORM_ROLES_OWNED):
	// each session reader role also holds PlatformRoleReaderColumns. Off, it
	// holds the session read alone; turning it off is a narrowing.
	PlatformRolesOwned bool
	// SchemaOwnerRole pins the owner of the auth schema and of every object in it (AUTH_SCHEMA_OWNER_ROLE). Set, each
	// apply and grants run refuses a schema with an object owned by any other role, and a pinned role that is a
	// superuser or can create roles. Empty means the pin is off (a dev stack whose migrator is a superuser).
	SchemaOwnerRole string

	// PushLifecycleWriterRole is the dedicated NOLOGIN role that owns the one
	// function go-api calls to append External Push lifecycle events
	// (CHAOS-10895). Empty means the function stays owned by the migration role
	// and nobody can execute it. It must already exist, must have no login and
	// no membership, and the migration role must be a member of it (the one
	// thing ALTER FUNCTION OWNER needs); see applyPushLifecycleWriter.
	PushLifecycleWriterRole string
	// PushLifecycleCallerRoles are the logins that may execute that function:
	// go-api's, and no other. Required when PushLifecycleWriterRole is set.
	PushLifecycleCallerRoles []string
	// LockTimeout bounds how long one migration's transaction waits for a
	// lock. Zero means DefaultMigrationLockTimeout; see the lock policy.
	LockTimeout time.Duration
	// LockAttempts is how many times in all a migration whose lock wait timed
	// out is tried. Zero means DefaultMigrationLockAttempts.
	LockAttempts int
}

// lockPolicy is the lock timeout and the attempt count a migration runs with.
// It never returns a timeout under 1ms: lock_timeout is set in whole
// milliseconds, and 0ms would turn the bound off.
func (o Options) lockPolicy() (time.Duration, int) {
	timeout, attempts := o.LockTimeout, o.LockAttempts
	if timeout < time.Millisecond {
		timeout = DefaultMigrationLockTimeout
	}
	if attempts < 1 {
		attempts = DefaultMigrationLockAttempts
	}
	return timeout, attempts
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
	// ErrMigrationLockTimeout reports a migration whose lock wait timed out on
	// every attempt (the lock policy). It always comes with ErrMigrationFailed.
	ErrMigrationLockTimeout = errors.New("auth migration could not take its locks in time")
	// ErrPublicTableMissing reports a PublicReadColumns table absent from the
	// ops schema in a run that requires it (the grants pass).
	ErrPublicTableMissing = errors.New("auth public read table is missing")
	// ErrPublicColumnMissing reports a PublicReadColumns column absent from an
	// existing ops table in a run that requires it (the grants pass).
	ErrPublicColumnMissing = errors.New("auth public read column is missing")
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
	if o.LockTimeout < 0 || (o.LockTimeout > 0 && o.LockTimeout < time.Millisecond) {
		return fmt.Errorf("%w: LockTimeout %s must be zero (the default) or at least 1ms", ErrInvalidOptions, o.LockTimeout)
	}
	if o.LockAttempts < 0 {
		return fmt.Errorf("%w: LockAttempts %d must be zero (the default) or positive", ErrInvalidOptions, o.LockAttempts)
	}
	// ACP-ADR-04: the runtime role owns no DDL. If the migration connection
	// authenticates AS the runtime role, then every object this run creates is
	// owned by the runtime role, which holds DDL over its own objects
	// permanently -- the exact posture the ADR forbids, established silently
	// by a successful-looking migration. Refuse instead.
	seen := map[string]bool{}
	for _, reader := range o.SessionReaderRoles {
		if err := ValidateIdentifier(reader); err != nil {
			return err
		}
		if reader == o.RuntimeRole || (migrationRole != "" && reader == migrationRole) || seen[reader] {
			return fmt.Errorf("%w: session reader role %q must differ from the runtime role, the migration role and every other reader",
				ErrInvalidOptions, reader)
		}
		seen[reader] = true
	}
	if err := o.validateSchemaOwnerRole(); err != nil {
		return err
	}
	if err := o.validatePushLifecycle(migrationRole); err != nil {
		return err
	}
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

	return applyGrantsAndVerify(ctx, conn, options, logger, result)
}

// Grants re-applies the runtime and public read grants and re-verifies the
// posture, and does nothing else: no DDL, no lineage rows. It requires the
// auth lineage to be at the embedded head and every PublicReadColumns table
// to exist, so it runs after the ops migration (CHAOS-10581).
func Grants(ctx context.Context, pool *pgxpool.Pool, options Options) (Result, error) {
	if pool == nil {
		return Result{}, fmt.Errorf("%w: no connection pool", ErrInvalidOptions)
	}
	return grants(ctx, poolAdapter{pool}, options)
}

func grants(ctx context.Context, pool migrationPool, options Options) (Result, error) {
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
	schemaID, err := NewValidatedIdentifier(options.Schema)
	if err != nil {
		return Result{}, err
	}
	options.RequirePublicTables = true
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("%w: acquiring a migration connection", ErrMigrationFailed)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey); err != nil {
		return Result{}, fmt.Errorf("%w: acquiring the migration lock", ErrMigrationFailed)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, advisoryLockKey)
	}()
	applied, err := appliedVersions(ctx, conn, schemaID)
	if err != nil {
		return Result{}, err
	}
	result := Result{CurrentVersion: highest(applied)}
	for _, migration := range migrations {
		if _, done := applied[migration.Version]; !done {
			return result, fmt.Errorf("%w: the auth schema is at version %d, embedded head is %d; run auth-migrate first",
				ErrMigrationFailed, result.CurrentVersion, migrations[len(migrations)-1].Version)
		}
	}
	return applyGrantsAndVerify(ctx, conn, options, logger, result)
}

// applyGrantsAndVerify is the grant half of a run, shared by Apply and Grants.
func applyGrantsAndVerify(ctx context.Context, conn pooledConn, options Options, logger *slog.Logger, result Result) (Result, error) {
	// Grants are re-applied on EVERY run, not only when a migration was
	// applied. ApplyRuntimeGrants revokes and re-grants from the manifest, so
	// running it unconditionally is what makes the manifest authoritative:
	// a posture that drifted for any reason -- a hand-run GRANT, a privilege
	// removed from the manifest in a later release, a restored backup -- is
	// corrected by the next migrate run rather than persisting because no new
	// migration happened to be pending.
	if err := verifySchemaOwnerPin(ctx, conn.Conn(), options); err != nil {
		return result, err
	}
	var retained []GrantRecord
	if err := applyRuntimeGrants(ctx, conn.Conn(), RuntimePosture(), options, &retained); err != nil {
		return result, err
	}
	logger.InfoContext(
		ctx, "applied auth runtime grants",
		"schema", options.Schema, "runtime_role", options.RuntimeRole,
		"tables", len(RuntimePosture()),
	)

	if err := applyPushLifecycleWriter(ctx, conn, options, logger); err != nil {
		return result, err
	}

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
	violations = excuseRetained(violations, retained)
	writerViolations, err := pushWriterViolations(ctx, conn, options)
	if err != nil {
		return result, err
	}
	violations = append(violations, writerViolations...)
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

// applyOne runs a single migration and records it, atomically. An attempt
// whose lock wait timed out rolls back, and the migration is tried again after
// a pause, at most LockAttempts times in all (the lock policy).
func applyOne(ctx context.Context, conn querier, options Options, migration Migration) error {
	lockTimeout, attempts := options.lockPolicy()
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	for attempt := 1; ; attempt++ {
		lockTimedOut, err := applyOnce(ctx, conn, options, migration, lockTimeout)
		if !lockTimedOut {
			return err
		}
		final := attempt >= attempts
		outcome := "retrying"
		if final {
			outcome = "giving_up"
		}
		logger.WarnContext(
			ctx, "auth migration lock wait timed out",
			"version", migration.Version, "name", migration.Name, "schema", options.Schema,
			"attempt", attempt, "attempts", attempts, "lock_timeout", lockTimeout.String(),
			"outcome", outcome,
		)
		if final {
			return fmt.Errorf(
				"%w: %w: version %d (%s) waited %s for a lock on each of %d attempts; "+
					"an open transaction holds a lock on a table it changes, so run auth-migrate again when that transaction ends",
				ErrMigrationFailed, ErrMigrationLockTimeout, migration.Version, migration.Name, lockTimeout, attempts,
			)
		}
		// The pause lets the readers that queued behind the wait through, and
		// gives the transaction that holds the lock time to end.
		pause := time.NewTimer(lockTimeout)
		select {
		case <-ctx.Done():
			pause.Stop()
			return fmt.Errorf("%w: version %d (%s) stopped between lock attempts: %w",
				ErrMigrationFailed, migration.Version, migration.Name, ctx.Err())
		case <-pause.C:
		}
	}
}

// applyOnce is one attempt of applyOne. lockTimedOut reports an attempt that
// rolled back because lock_timeout ended a lock wait; err is nil then.
func applyOnce(ctx context.Context, conn querier, options Options, migration Migration, lockTimeout time.Duration) (lockTimedOut bool, err error) {
	schemaID, err := NewValidatedIdentifier(options.Schema)
	if err != nil {
		return false, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("%w: beginning version %d", ErrMigrationFailed, migration.Version)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// search_path is set for the transaction so migration SQL can name its
	// own tables unqualified and cannot accidentally create anything in
	// public. pg_temp is excluded deliberately: a temp object shadowing a
	// real one during DDL is a genuinely confusing failure.
	if _, err := tx.Exec(ctx, `SELECT set_config('search_path', $1, true)`, options.Schema); err != nil {
		return false, fmt.Errorf("%w: setting search_path for version %d", ErrMigrationFailed, migration.Version)
	}
	// lock_timeout for this transaction only (the lock policy). The value is
	// a bound parameter, never SQL text.
	if _, err := tx.Exec(
		ctx, `SELECT set_config('lock_timeout', $1, true)`, strconv.FormatInt(lockTimeout.Milliseconds(), 10)+"ms",
	); err != nil {
		return false, fmt.Errorf("%w: setting lock_timeout for version %d", ErrMigrationFailed, migration.Version)
	}
	if _, err := tx.Exec(ctx, migration.SQL); err != nil {
		if isLockTimeout(err) {
			return true, nil
		}
		// The driver error carries the failing statement, which is our own
		// embedded SQL and therefore safe -- but it can also carry connection
		// context, so only the version and name are surfaced.
		return false, fmt.Errorf(
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
		if isLockTimeout(err) {
			return true, nil
		}
		return false, fmt.Errorf("%w: recording version %d", ErrMigrationFailed, migration.Version)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("%w: committing version %d", ErrMigrationFailed, migration.Version)
	}
	return false, nil
}

// isLockTimeout reports a statement that lock_timeout ended.
func isLockTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == lockNotAvailable
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
