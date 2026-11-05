package pushlifecyclecli

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/customerpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile/reconcilejob"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	defaultDSNEnv = "POSTGRES_URI"
	schemaEnv     = "AUTH_DATABASE_SCHEMA"
)

var envNamePattern = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

var (
	connectFn   = connect
	backfillFn  = customerpush.BackfillLifecycle
	reconcileFn = reconcile
)

// Command is the `push-lifecycle` group of dho.
func Command() cli.Command {
	return cli.Command{
		Name: "push-lifecycle", Kind: cli.Group, Summary: "External Push credential lifecycle operator verbs: backfill-created, reconcile",
		Children: []cli.Command{{
			Name: "backfill-created", Kind: cli.Verb,
			Summary: "emit lifecycle events for push tokens that have no aggregate (dry run unless -apply; idempotent; counts only; -dsn-env NAME picks the database URI variable, default POSTGRES_URI)",
			Run:     run,
		}, {
			Name: "reconcile", Kind: cli.Verb,
			Summary: "compare the push credential projection with the token store (read-only; counts only; exit 1 on drift)",
			Run:     runReconcile,
		}},
	}
}

func run(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho push-lifecycle backfill-created", flag.ContinueOnError)
	apply := flags.Bool("apply", false, "write the events (without it the run only counts)")
	batch := flags.Int("batch", 100, "tokens read per page")
	dsnEnv := flags.String("dsn-env", defaultDSNEnv, "name of the environment variable that holds the database URI (NAME or NAME_FILE)")
	if code, ok := parseFlags(flags, env.Stderr, env.Args); !ok {
		return code
	}
	if flags.NArg() != 0 || *batch < 1 || *batch > 10000 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted and -batch must be 1..10000")
		return cli.ExitUsage
	}
	if !envNamePattern.MatchString(*dsnEnv) {
		fmt.Fprintln(env.Stderr, "argument error: -dsn-env must be an environment variable name like POSTGRES_URI")
		return cli.ExitUsage
	}
	schema := "auth"
	if v, ok := env.Lookup(schemaEnv); ok && v != "" {
		schema = v
	}
	dsn, configured, err := secrets.ResolveDSNSetting(*dsnEnv, env.Lookup)
	if err != nil || !configured {
		fmt.Fprintf(env.Stderr, "backfill-created: %s (or %s_FILE) is required\n", *dsnEnv, *dsnEnv)
		return cli.ExitFailure
	}
	pool, err := connectFn(ctx, dsn.Reveal())
	if err != nil {
		fmt.Fprintf(env.Stderr, "backfill-created: could not open the database pool (the DSN did not parse, or the connect failed); check %s\n", *dsnEnv)
		return cli.ExitFailure
	}
	if pool != nil {
		defer pool.Close()
	}
	rep, err := backfillFn(ctx, pool, schema, *apply, *batch)
	mode := "apply"
	if !*apply {
		mode = "dry-run"
	}
	fmt.Fprintf(env.Stdout, "backfill-created mode=%s candidates=%d source_bound=%d organization_wide=%d revoked=%d written=%d skipped=%d failed=%d\n",
		mode, rep.Candidates, rep.SourceBound, rep.OrganizationWide, rep.Revoked, rep.Written, rep.Skipped, rep.Failed)
	if err != nil {
		fmt.Fprintln(env.Stderr, "backfill-created: stopped before the last page")
		return cli.ExitFailure
	}
	if rep.Failed > 0 {
		fmt.Fprintf(env.Stderr, "backfill-created: %d tokens failed; rerun to retry them\n", rep.Failed)
		return cli.ExitFailure
	}
	return cli.ExitOK
}

func connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) { return pgxpool.New(ctx, dsn) }

const maxReconcileBatch = 1000

// reconcile runs the read-only reconcile over pool: two passes delay apart.
func reconcile(ctx context.Context, pool *pgxpool.Pool, schema string, batch int, delay time.Duration) (reconcilejob.Report, error) {
	ident, err := authschema.NewValidatedIdentifier(schema)
	if err != nil {
		return reconcilejob.Report{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, reconcilejob.RunBudget+delay)
	defer cancel()
	return reconcilejob.Run(ctx, reconcilejob.NewPass(pool, ident, batch, nil), delay, reconcilejob.Sleep)
}

// runReconcile is `dho push-lifecycle reconcile`. It only reads. It prints counts; with -list it also prints
// one "class token_id" line per confirmed finding. Exit 0 on zero drift, 1 on drift or a failed read.
func runReconcile(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho push-lifecycle reconcile", flag.ContinueOnError)
	batch := flags.Int("batch", reconcilejob.DefaultBatch, "rows read per page")
	delay := flags.Duration("confirm-delay", reconcilejob.DefaultConfirmDelay, "wait between the two passes (must be positive)")
	list := flags.Bool("list", false, "also print one line per finding: class and token id")
	dsnEnv := flags.String("dsn-env", defaultDSNEnv, "name of the environment variable that holds the database URI (NAME or NAME_FILE)")
	if code, ok := parseFlags(flags, env.Stderr, env.Args); !ok {
		return code
	}
	if flags.NArg() != 0 || *batch < 1 || *batch > maxReconcileBatch || *delay <= 0 {
		fmt.Fprintln(env.Stderr, "argument error: positional arguments are not accepted, -batch must be 1..1000 and -confirm-delay must be positive")
		return cli.ExitUsage
	}
	if !envNamePattern.MatchString(*dsnEnv) {
		fmt.Fprintln(env.Stderr, "argument error: -dsn-env must be an environment variable name like POSTGRES_URI")
		return cli.ExitUsage
	}
	schema := "auth"
	if v, ok := env.Lookup(schemaEnv); ok && v != "" {
		schema = v
	}
	dsn, configured, err := secrets.ResolveDSNSetting(*dsnEnv, env.Lookup)
	if err != nil || !configured {
		fmt.Fprintf(env.Stderr, "reconcile: %s (or %s_FILE) is required\n", *dsnEnv, *dsnEnv)
		return cli.ExitFailure
	}
	pool, err := connectFn(ctx, dsn.Reveal())
	if err != nil {
		fmt.Fprintf(env.Stderr, "reconcile: could not open the database pool (the DSN did not parse, or the connect failed); check %s\n", *dsnEnv)
		return cli.ExitFailure
	}
	if pool != nil {
		defer pool.Close()
	}
	rep, err := reconcileFn(ctx, pool, schema, *batch, *delay)
	if err != nil {
		fmt.Fprintln(env.Stderr, "reconcile: a read failed before the end of the run; no result")
		return cli.ExitFailure
	}
	counts := rep.Counts()
	fmt.Fprintf(env.Stdout, "reconcile mode=read-only passes=%d domain_tokens=%d projected=%d\n", rep.Passes, rep.Tokens, rep.Projected)
	fmt.Fprintf(env.Stdout, "reconcile drift total=%d missing=%d extra=%d status=%d scopes=%d expiry=%d binding=%d unwireable=%d transient=%d\n",
		len(rep.Drift), counts[pushreconcile.ClassMissing], counts[pushreconcile.ClassExtra], counts[pushreconcile.ClassStatus],
		counts[pushreconcile.ClassScopes], counts[pushreconcile.ClassExpiry], counts[pushreconcile.ClassBinding],
		counts[pushreconcile.ClassUnwireable], rep.Transient)
	if *list {
		for _, f := range rep.Drift {
			fmt.Fprintf(env.Stdout, "%s %s\n", f.Class, f.ID)
		}
	}
	if len(rep.Drift) > 0 {
		return cli.ExitFailure
	}
	return cli.ExitOK
}
