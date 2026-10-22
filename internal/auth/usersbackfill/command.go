package usersbackfill

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// The auth-service runtime role: it writes auth.* and reads the PublicReadColumns of public.users.
const (
	schemaEnv = authconfig.EnvDatabaseSchema
	dsnEnv    = authconfig.EnvDatabaseURI
)

// Command is the `auth` group of dho.
func Command() cli.Command {
	return cli.Command{
		Name:    "auth",
		Kind:    cli.Group,
		Summary: "auth control plane operator verbs: users-backfill",
		Children: []cli.Command{{
			Name:    "users-backfill",
			Kind:    cli.Verb,
			Summary: "copy public.users into the auth schema (idempotent; counts only)",
			Run:     run,
		}},
	}
}

func run(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho auth users-backfill", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	dryRun := flags.Bool("dry-run", false, "read and report; write nothing")
	batch := flags.Int("batch", 500, "users read per page")
	if err := flags.Parse(env.Args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if flags.NArg() != 0 || *batch < 1 || *batch > 10000 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted and -batch must be 1..10000")
		return cli.ExitUsage
	}
	if owned, err := authconfig.UsersOwnedFromLookup(env.Lookup); err != nil {
		fmt.Fprintln(env.Stderr, "users-backfill: AUTH_USERS_OWNED must be true or false")
		return cli.ExitFailure
	} else if owned {
		fmt.Fprintln(env.Stderr, "users-backfill: refused: AUTH_USERS_OWNED is true, auth.users is the authority and this command copies from public.users")
		return cli.ExitRefused
	}
	schemaName := "auth"
	if v, ok := env.Lookup(schemaEnv); ok && v != "" {
		schemaName = v
	}
	schema, err := authschema.NewValidatedIdentifier(schemaName)
	if err != nil {
		fmt.Fprintln(env.Stderr, "users-backfill: AUTH_DATABASE_SCHEMA is not a valid schema name")
		return cli.ExitFailure
	}
	dsn, configured, err := secrets.ResolveDSNSetting(dsnEnv, env.Lookup)
	if err != nil || !configured {
		fmt.Fprintln(env.Stderr, "users-backfill: AUTH_DATABASE_URI (or AUTH_DATABASE_URI_FILE) is required")
		return cli.ExitFailure
	}
	boundary := pgstorage.Boundary(dsn.Reveal())
	pool, err := pgxpool.New(ctx, dsn.Reveal())
	if err != nil {
		fmt.Fprintf(env.Stderr, "users-backfill: could not open the database pool: %v\n", boundary.Redact(err))
		return cli.ExitFailure
	}
	defer pool.Close()
	store, err := identity.New(identity.Config{Pool: pool, Schema: schema, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		fmt.Fprintln(env.Stderr, "users-backfill: could not build the identity store")
		return cli.ExitFailure
	}
	rep, err := Run(ctx, PGSource{Pool: pool}, store, Options{DryRun: *dryRun, BatchSize: *batch})
	for _, line := range rep.Lines() {
		fmt.Fprintln(env.Stdout, line)
	}
	switch {
	case err != nil:
		fmt.Fprintln(env.Stderr, "users-backfill: stopped before the last page")
		return cli.ExitFailure
	case rep.SkippedTotal() > 0:
		return cli.ExitFailure
	}
	return cli.ExitOK
}
