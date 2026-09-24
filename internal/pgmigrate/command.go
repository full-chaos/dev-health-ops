package pgmigrate

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
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
			"  DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER     1 selects the cutover head (revision 0066 as a second head)\n")
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

	raw, present := env.Lookup(CutoverEnv)
	baseline, err := LoadBaseline(CutoverAuthorized(raw, present))
	if err != nil {
		return writeError(env.Stderr, "baseline_unavailable", err.Error())
	}
	chain, err := LoadChain()
	if err != nil {
		return writeError(env.Stderr, "chain_unavailable", err.Error())
	}
	dsn, _, ok := resolve(env.Lookup, env.Stderr)
	if !ok {
		return cli.ExitFailure
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
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
	result, err := Upgrade(ctx, conn, baseline, chain)
	if err != nil {
		var below BelowHeadError
		var foreign ForeignDatabaseError
		switch {
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
