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
		Summary: "auth control plane operator verbs: users-backfill, users-reconcile, orgs-backfill",
		Children: []cli.Command{{
			Name:    "users-backfill",
			Kind:    cli.Verb,
			Summary: "copy public.users into the auth schema (idempotent; counts only)",
			Run:     run,
		}, {
			Name:    "users-reconcile",
			Kind:    cli.Verb,
			Summary: "compare public.users with the auth schema in one read-only snapshot (counts only; exit 1 on any gated diff)",
			Run:     runReconcile,
		}, {
			Name:    "orgs-backfill",
			Kind:    cli.Verb,
			Summary: "copy public.organizations and public.memberships into the auth schema (idempotent; counts only; run users-backfill first)",
			Run:     runOrgs,
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
	pool, schema, code := connect(ctx, env, "users-backfill")
	if code != cli.ExitOK {
		return code
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

// connect resolves the schema and opens the auth runtime pool. A non-zero
// code means the reason is already printed.
func connect(ctx context.Context, env cli.Env, verb string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
	schemaName := "auth"
	if v, ok := env.Lookup(schemaEnv); ok && v != "" {
		schemaName = v
	}
	schema, err := authschema.NewValidatedIdentifier(schemaName)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: AUTH_DATABASE_SCHEMA is not a valid schema name\n", verb)
		return nil, schema, cli.ExitFailure
	}
	dsn, configured, err := secrets.ResolveDSNSetting(dsnEnv, env.Lookup)
	if err != nil || !configured {
		fmt.Fprintf(env.Stderr, "%s: AUTH_DATABASE_URI (or AUTH_DATABASE_URI_FILE) is required\n", verb)
		return nil, schema, cli.ExitFailure
	}
	boundary := pgstorage.Boundary(dsn.Reveal())
	pool, err := pgxpool.New(ctx, dsn.Reveal())
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: could not open the database pool: %v\n", verb, boundary.Redact(err))
		return nil, schema, cli.ExitFailure
	}
	return pool, schema, cli.ExitOK
}

func runReconcile(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho auth users-reconcile", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	batch := flags.Int("batch", 500, "rows read per page")
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
	pool, schema, code := connect(ctx, env, "users-reconcile")
	if code != cli.ExitOK {
		return code
	}
	defer pool.Close()
	tx, err := OpenSnapshot(ctx, pool)
	if err != nil {
		fmt.Fprintln(env.Stderr, "users-reconcile: could not open one read-only snapshot of public.users and the auth schema; stopping")
		return cli.ExitFailure
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	rep, err := Reconcile(ctx, TxSource{Tx: tx}, TxTarget{Tx: tx, Schema: schema}, *batch)
	for _, line := range rep.Lines() {
		fmt.Fprintln(env.Stdout, line)
	}
	switch {
	case err != nil:
		fmt.Fprintln(env.Stderr, "users-reconcile: stopped before the last page")
		return cli.ExitFailure
	case rep.GatedTotal() > 0:
		return cli.ExitFailure
	}
	return cli.ExitOK
}

func runOrgs(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho auth orgs-backfill", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	dryRun := flags.Bool("dry-run", false, "read and report; write nothing")
	batch := flags.Int("batch", 500, "rows read per page")
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
		fmt.Fprintln(env.Stderr, "orgs-backfill: AUTH_USERS_OWNED must be true or false")
		return cli.ExitFailure
	} else if owned {
		fmt.Fprintln(env.Stderr, "orgs-backfill: refused: AUTH_USERS_OWNED is true, the auth schema is the authority and this command copies from public tables")
		return cli.ExitRefused
	}
	pool, schema, code := connect(ctx, env, "orgs-backfill")
	if code != cli.ExitOK {
		return code
	}
	defer pool.Close()
	store, err := identity.New(identity.Config{Pool: pool, Schema: schema, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		fmt.Fprintln(env.Stderr, "orgs-backfill: could not build the identity store")
		return cli.ExitFailure
	}
	rep, err := RunOrgs(ctx, PGOrgSource{Pool: pool}, store, PGAuthOnly{Pool: pool, Schema: schema}, Options{DryRun: *dryRun, BatchSize: *batch})
	for _, line := range rep.Lines() {
		fmt.Fprintln(env.Stdout, line)
	}
	if rep.AuthOnlyMemberships > 0 {
		fmt.Fprintln(env.Stderr, "orgs-backfill: auth_only_memberships is above zero: STOP for the cutover until it is explained")
	}
	switch {
	case err != nil:
		fmt.Fprintln(env.Stderr, "orgs-backfill: stopped before the end")
		return cli.ExitFailure
	case rep.SkippedTotal() > 0:
		return cli.ExitFailure
	}
	return cli.ExitOK
}
