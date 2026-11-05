package usersbackfill

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/auth/repoidentity"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// repoIdentitySelect reads the current row of every repository that has a provider id; the path is the
// canonical key the catalog serves (lowercased, trimmed repo).
const repoIdentitySelect = `SELECT org_id, provider, provider_repo_id, lowerUTF8(trimBoth(repo)) AS path FROM repos FINAL WHERE provider_repo_id != ''`

// chRepoSource is the ClickHouse repos table as a repoidentity.Source.
type chRepoSource struct{ conn driver.Conn }

func (s chRepoSource) Repos(ctx context.Context) ([]repoidentity.Repo, error) {
	rows, err := s.conn.Query(ctx, repoIdentitySelect)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repoidentity.Repo
	for rows.Next() {
		var r repoidentity.Repo
		if err := rows.Scan(&r.OrgID, &r.Provider, &r.ProviderRepoID, &r.Path); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// fixedRepos is a Source over rows already read.
type fixedRepos []repoidentity.Repo

func (f fixedRepos) Repos(context.Context) ([]repoidentity.Repo, error) { return f, nil }

func runRepoIdentities(ctx context.Context, env cli.Env) int {
	flags := flag.NewFlagSet("dho auth repo-identities-sync", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
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
	dsn, configured, err := config.ResolveDSN(env.Lookup, "CLICKHOUSE_URI", config.ClickHouseSpec)
	if err != nil || !configured {
		fmt.Fprintln(env.Stderr, "repo-identities-sync: CLICKHOUSE_URI is required")
		return cli.ExitFailure
	}
	boundary := secrets.NewBoundary(dsn.Reveal())
	chConfig := chstorage.DefaultConfig(dsn.Reveal())
	chConfig.MaxOpenConns, chConfig.MaxIdleConns = 1, 1
	chConfig.ReadTimeout = 10 * time.Minute
	conn, err := chstorage.Open(ctx, chConfig)
	if err != nil {
		fmt.Fprintf(env.Stderr, "repo-identities-sync: could not open ClickHouse: %v\n", boundary.Redact(err))
		return cli.ExitFailure
	}
	defer conn.Close()
	repos, err := chRepoSource{conn: conn}.Repos(ctx)
	if err != nil {
		fmt.Fprintf(env.Stderr, "repo-identities-sync: could not read the repositories: %v\n", boundary.Redact(err))
		return cli.ExitFailure
	}
	pool, schema, code := connectFn(ctx, env, "repo-identities-sync")
	if code != cli.ExitOK {
		return code
	}
	defer pool.Close()
	rep, err := repoidentity.Run(ctx, fixedRepos(repos), repoidentity.PGSink{Pool: pool, Schema: schema})
	for _, line := range rep.Lines() {
		fmt.Fprintln(env.Stdout, line)
	}
	if err != nil {
		fmt.Fprintf(env.Stderr, "repo-identities-sync: %v\n", boundary.Redact(err))
		return cli.ExitFailure
	}
	if rep.OrgsFailed > 0 {
		return cli.ExitFailure
	}
	return cli.ExitOK
}
