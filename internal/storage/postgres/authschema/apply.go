package authschema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
	// Logger receives one structured line per applied migration. Optional.
	Logger *slog.Logger
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

// identifierPattern bounds every identifier this package interpolates.
//
// Interpolation is unavoidable: PostgreSQL has no bind placeholder for a
// schema or role name, so CREATE SCHEMA and GRANT cannot be parameterised.
// The mitigation is that nothing reaches the SQL text without matching this
// pattern first, and the pattern excludes quotes, whitespace, semicolons and
// backslashes by construction rather than by escaping them afterwards.
//
// This is a stricter bound than authconfig's: it additionally rejects
// PostgreSQL reserved keywords via reservedKeywords below, because unlike the
// runtime -- which only ever binds the schema as a query parameter -- this
// package genuinely does emit it as an identifier. That gap is what codex
// round 3 found in CHAOS-4881's validator, one layer up.
var identifierPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// reservedKeywords are the PostgreSQL reserved words that cannot appear as a
// bare identifier. The list is deliberately short and covers the words a
// human might plausibly choose for a schema or role; it is a usability guard
// against a confusing runtime syntax error, not a security control -- the
// security comes from identifierPattern excluding quotes and whitespace, and
// from quoteIdentifier double-quoting whatever survives.
var reservedKeywords = map[string]struct{}{
	"all": {}, "analyse": {}, "analyze": {}, "and": {}, "any": {}, "array": {},
	"as": {}, "asc": {}, "authorization": {}, "between": {}, "both": {},
	"case": {}, "cast": {}, "check": {}, "collate": {}, "column": {},
	"constraint": {}, "create": {}, "current_date": {}, "current_role": {},
	"current_time": {}, "current_timestamp": {}, "current_user": {},
	"default": {}, "deferrable": {}, "desc": {}, "distinct": {}, "do": {},
	"else": {}, "end": {}, "except": {}, "false": {}, "for": {}, "foreign": {},
	"from": {}, "grant": {}, "group": {}, "having": {}, "in": {}, "initially": {},
	"intersect": {}, "into": {}, "leading": {}, "limit": {}, "localtime": {},
	"localtimestamp": {}, "new": {}, "not": {}, "null": {}, "off": {},
	"offset": {}, "old": {}, "on": {}, "only": {}, "or": {}, "order": {},
	"placing": {}, "primary": {}, "references": {}, "returning": {},
	"select": {}, "session_user": {}, "some": {}, "symmetric": {}, "table": {},
	"then": {}, "to": {}, "trailing": {}, "true": {}, "union": {}, "unique": {},
	"user": {}, "using": {}, "when": {}, "where": {}, "with": {},
}

// ValidateIdentifier reports whether a schema or role name may be emitted as
// a bare identifier by this package.
func ValidateIdentifier(name string) error {
	if !identifierPattern.MatchString(name) {
		return fmt.Errorf(
			"%w: %q must match [a-z][a-z0-9_]* and be at most 63 characters",
			ErrInvalidOptions, name,
		)
	}
	if _, reserved := reservedKeywords[name]; reserved {
		return fmt.Errorf("%w: %q is a PostgreSQL reserved keyword", ErrInvalidOptions, name)
	}
	return nil
}

// quoteIdentifier renders a validated identifier for interpolation.
//
// ValidateIdentifier has already excluded everything that would need escaping,
// so the quoting is belt-and-braces -- and, unlike the comment this pattern
// replaces in CHAOS-4881, that is an accurate description of what it is: the
// double quotes are what make the emitted SQL correct if the pattern is ever
// widened, not what makes the current input safe.
func quoteIdentifier(name string) string { return `"` + name + `"` }

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

	if err := ensureSchemaAndVersionTable(ctx, conn.Conn(), options); err != nil {
		return Result{}, err
	}
	applied, err := appliedVersions(ctx, conn.Conn(), options.Schema)
	if err != nil {
		return Result{}, err
	}

	result := Result{CurrentVersion: highest(applied)}
	for _, migration := range migrations {
		if _, done := applied[migration.Version]; done {
			continue
		}
		if err := applyOne(ctx, conn.Conn(), options, migration); err != nil {
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
	paths, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
	if err != nil {
		return result, err
	}
	if len(paths) > 0 {
		logger.ErrorContext(
			ctx, "runtime role can escalate beyond its declared posture",
			"schema", options.Schema, "runtime_role", options.RuntimeRole,
			"paths", len(paths),
		)
		return result, fmt.Errorf(
			"%w (schema %q, role %q):\n%s",
			ErrRuntimeRoleCanEscalate, options.Schema, options.RuntimeRole, describeEscalation(paths),
		)
	}
	logger.InfoContext(
		ctx, "verified auth runtime posture",
		"schema", options.Schema, "runtime_role", options.RuntimeRole,
		"escalation_paths", 0,
	)
	return result, nil
}

// applyOne runs a single migration and records it, atomically.
func applyOne(ctx context.Context, conn *pgx.Conn, options Options, migration Migration) error {
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
			quoteIdentifier(options.Schema), quoteIdentifier(versionTable),
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
