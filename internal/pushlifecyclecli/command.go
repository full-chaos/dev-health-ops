package pushlifecyclecli

import (
	"context"
	"errors"
	"flag"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/customerpush"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

const (
	dsnEnv    = "POSTGRES_URI"
	schemaEnv = "AUTH_DATABASE_SCHEMA"
)

var (
	connectFn  = connect
	backfillFn = customerpush.BackfillLifecycle
)

// Command is the `push-lifecycle` group of dho.
func Command() cli.Command {
	return cli.Command{
		Name: "push-lifecycle", Kind: cli.Group, Summary: "External Push credential lifecycle operator verbs: backfill-created",
		Children: []cli.Command{{
			Name: "backfill-created", Kind: cli.Verb,
			Summary: "emit lifecycle events for push tokens that have no aggregate (dry run unless -apply; idempotent; counts only)",
			Run:     run,
		}},
	}
}

func run(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho push-lifecycle backfill-created", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	apply := flags.Bool("apply", false, "write the events (without it the run only counts)")
	batch := flags.Int("batch", 100, "tokens read per page")
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
	schema := "auth"
	if v, ok := env.Lookup(schemaEnv); ok && v != "" {
		schema = v
	}
	dsn, configured, err := secrets.ResolveDSNSetting(dsnEnv, env.Lookup)
	if err != nil || !configured {
		fmt.Fprintf(env.Stderr, "backfill-created: %s (or %s_FILE) is required\n", dsnEnv, dsnEnv)
		return cli.ExitFailure
	}
	pool, err := connectFn(ctx, dsn.Reveal())
	if err != nil {
		fmt.Fprintf(env.Stderr, "backfill-created: could not open the database pool: %v\n", pgstorage.Boundary(dsn.Reveal()).Redact(err))
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
