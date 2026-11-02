package pushlifecyclecli

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/customerpush"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// Command is the `push-lifecycle` group of dho.
func Command() cli.Command {
	return cli.Command{
		Name: "push-lifecycle", Kind: cli.Group, Summary: "External Push credential lifecycle operator verbs",
		Children: []cli.Command{{Name: "backfill-created", Kind: cli.Verb, Summary: "stub", Run: run}},
	}
}

func run(ctx context.Context, env cli.Env) int { return cli.ExitFailure }

var (
	connectFn  = func(ctx context.Context, dsn string) (*pgxpool.Pool, error) { return nil, nil }
	backfillFn = customerpush.BackfillLifecycle
)
