package pgmigrate

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// ResolveDSN finds the elevated migration DSN. The migrate group passes the
// River migrator's resolver, so both verbs read MIGRATION_DATABASE_URI (or its
// component form) and fall back to POSTGRES_URI the same way. ok=false means
// the resolver already wrote the error to stderr.
type ResolveDSN func(lookup secrets.LookupEnv, stderr io.Writer) (dsn secrets.Value, source string, ok bool)

// Command is the `postgres` group of `dho migrate`.
func Command(resolve ResolveDSN) cli.Command {
	return cli.Command{
		Name:    "postgres",
		Summary: "bring the application PostgreSQL schema to its head, or report where it stands",
		Kind:    cli.Group,
		Children: []cli.Command{
			{
				Name:    "upgrade",
				Summary: "apply the head baseline to an empty database, then every revision after the head",
				Kind:    cli.Verb,
				Run: func(ctx context.Context, env cli.Env) int {
					return run(ctx, "upgrade", resolve, env)
				},
			},
			{
				Name:    "status",
				Summary: "report recorded, missing and pending revisions without changing the database",
				Kind:    cli.Verb,
				Run: func(ctx context.Context, env cli.Env) int {
					return run(ctx, "status", resolve, env)
				},
			},
			{
				Name:    "current",
				Summary: "print the revisions the database records, as `alembic current` does",
				Kind:    cli.Verb,
				Run: func(ctx context.Context, env cli.Env) int {
					return revisions(ctx, "current", resolve, env)
				},
			},
			{
				Name:    "heads",
				Summary: "print the head revisions, as `alembic heads` does",
				Kind:    cli.Verb,
				Run: func(ctx context.Context, env cli.Env) int {
					return revisions(ctx, "heads", resolve, env)
				},
			},
			{
				Name:    "history",
				Summary: "print the revision history, as `alembic history` does",
				Kind:    cli.Verb,
				Run:     history,
			},
			{
				Name:    "downgrade",
				Summary: "refused: the PostgreSQL migrator is forward-only",
				Kind:    cli.Verb,
				Run:     downgrade,
			},
		},
	}
}

func run(ctx context.Context, verb string, resolve ResolveDSN, env cli.Env) int {
	flags := flag.NewFlagSet("dho migrate postgres "+verb, flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	defaultUsage := flags.Usage
	flags.Usage = func() {
		defaultUsage()
		fmt.Fprint(env.Stderr, "\nEnvironment:\n"+
			"  MIGRATION_DATABASE_URI (or _FILE, or the DEV_HEALTH_MIGRATION_PG_* component form)   elevated DSN, direct to PostgreSQL\n"+
			"  POSTGRES_URI (or _FILE)                   used when MIGRATION_DATABASE_URI is not configured\n"+
			"  DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER     must be 1: the head has the River cutover (0066) applied, as production does\n"+
			"  RIVER_DATABASE_SCHEMA                     must be the head's River schema (river), as production's\n")
	}
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted")
		return cli.ExitUsage
	}

	baseline, err := LoadBaseline()
	if err != nil {
		return writeError(env.Stderr, "baseline_unavailable", err.Error())
	}
	if err := CheckSettings(ReadSettings(env.Lookup), baseline); err != nil {
		return writeError(env.Stderr, "settings_mismatch", err.Error())
	}
	chain, err := LoadChain()
	if err != nil {
		return writeError(env.Stderr, "chain_unavailable", err.Error())
	}
	dsn, _, ok := resolve(env.Lookup, env.Stderr)
	if !ok {
		return cli.ExitFailure
	}
	boundary := pgstorage.Boundary(dsn.Reveal())
	conn, err := pgx.Connect(ctx, dsn.Reveal())
	if err != nil {
		return writeError(env.Stderr, "postgres_unavailable", boundary.Redact(err).Error())
	}
	defer conn.Close(context.Background())

	if verb == "status" {
		status, err := ReadStatus(ctx, conn, baseline, chain)
		if err != nil {
			return writeError(env.Stderr, "status_failed", boundary.Redact(err).Error())
		}
		return writeResult(env.Stdout, env.Stderr, status)
	}
	result, err := UpgradeLogged(ctx, conn, baseline, chain, logging.NewJSON(env.Stderr, slog.LevelInfo))
	if err != nil {
		var below BelowHeadError
		var foreign ForeignDatabaseError
		var mismatch SchemaMismatchError
		switch {
		case errors.As(err, &mismatch):
			return writeError(env.Stderr, "schema_mismatch", err.Error())
		case errors.As(err, &below):
			return writeError(env.Stderr, "below_head", err.Error())
		case errors.As(err, &foreign):
			return writeError(env.Stderr, "foreign_database", err.Error())
		}
		return writeError(env.Stderr, "migration_failed", boundary.Redact(err).Error())
	}
	return writeResult(env.Stdout, env.Stderr, result)
}

func writeResult(stdout, stderr io.Writer, value any) int {
	if err := json.NewEncoder(stdout).Encode(value); err != nil {
		fmt.Fprintln(stderr, "could not write the result")
		return cli.ExitFailure
	}
	return cli.ExitOK
}

func writeError(stderr io.Writer, code, detail string) int {
	_ = json.NewEncoder(stderr).Encode(map[string]any{"error": map[string]string{"code": code, "detail": detail}})
	return cli.ExitFailure
}

// revisions is `current` and `heads`. The verbose forms of `alembic current` and
// `alembic heads` print each script's docstring and path, which dho does not
// carry (the Go migrator has the head baseline, not the Alembic scripts):
// --verbose is refused.
func revisions(ctx context.Context, verb string, resolve ResolveDSN, env cli.Env) int {
	flags := flag.NewFlagSet("dho migrate postgres "+verb, flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	verbose := flags.Bool("verbose", false, "not supported: the Alembic script details are not carried in dho")
	flags.BoolVar(verbose, "v", false, "not supported: the Alembic script details are not carried in dho")
	defaultUsage := flags.Usage
	flags.Usage = func() {
		defaultUsage()
		if verb == "current" {
			fmt.Fprint(env.Stderr, "\nEnvironment:\n"+
				"  MIGRATION_DATABASE_URI (or _FILE, or the DEV_HEALTH_MIGRATION_PG_* component form)   elevated DSN, direct to PostgreSQL\n"+
				"  POSTGRES_URI (or _FILE)                   used when MIGRATION_DATABASE_URI is not configured\n")
		}
	}
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted")
		return cli.ExitUsage
	}
	if *verbose {
		fmt.Fprintln(env.Stderr, "argument error: --verbose is not supported: dho does not carry the Alembic script details (use alembic for them)")
		return cli.ExitUsage
	}
	baseline, err := LoadBaseline()
	if err != nil {
		return writeError(env.Stderr, "baseline_unavailable", err.Error())
	}
	chain, err := LoadChain()
	if err != nil {
		return writeError(env.Stderr, "chain_unavailable", err.Error())
	}
	if verb == "heads" {
		if err := WriteHeads(env.Stdout, baseline, chain); err != nil {
			return cli.ExitFailure
		}
		return cli.ExitOK
	}
	recorded, code, ok := recordedRevisions(ctx, resolve, env)
	if !ok {
		return code
	}
	if err := WriteCurrent(env.Stdout, recorded, baseline, chain); err != nil {
		return cli.ExitFailure
	}
	return cli.ExitOK
}

// recordedRevisions connects to the elevated migration database and reads the
// revisions alembic_version records (sorted). ok false means the error is printed
// and code is the exit code. `current` and the flat `status` alias both read this way.
func recordedRevisions(ctx context.Context, resolve ResolveDSN, env cli.Env) (recorded []string, code int, ok bool) {
	dsn, _, resolved := resolve(env.Lookup, env.Stderr)
	if !resolved {
		return nil, cli.ExitFailure, false
	}
	boundary := pgstorage.Boundary(dsn.Reveal())
	conn, err := pgx.Connect(ctx, dsn.Reveal())
	if err != nil {
		return nil, writeError(env.Stderr, "postgres_unavailable", boundary.Redact(err).Error()), false
	}
	defer conn.Close(context.Background())
	recorded, err = Recorded(ctx, conn)
	if err != nil {
		return nil, writeError(env.Stderr, "current_failed", boundary.Redact(err).Error()), false
	}
	return recorded, cli.ExitOK, true
}
