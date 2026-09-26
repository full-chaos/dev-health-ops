package synccli

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/localgit"
)

// runLocalRepo is sync_local_target for git and prs (CHAOS-6775) and blame (CHAOS-6776): main()'s
// first-organization lookup when no organization was given (a database that has
// none, or none configured, leaves the store with no org_id, as in Python), then
// the local repository sync through localgit.
func runLocalRepo(ctx context.Context, deps InlineDeps, lookups dbLookups, plan Plan, env cli.Env) error {
	if plan.OrgSource == OrgFromDBFirst {
		dbURL := ""
		if plan.DB != nil {
			dbURL = *plan.DB
		}
		if id, found := lookups.FirstOrg(ctx, dbURL); found {
			plan.Org, plan.OrgSource = &id, OrgFromDB
		}
	}
	conn, err := deps.OpenStore(ctx, plan.SinkURI)
	if err != nil {
		return fmt.Errorf("open ClickHouse: %w", errors.New(redactDSN(err.Error(), plan.SinkURI)))
	}
	if closer, ok := conn.(interface{ Close() error }); ok {
		defer func() { _ = closer.Close() }()
	}
	return deps.Local(ctx, plan, conn, env, deps.Now())
}

// syncLocal runs localgit.Sync for the plan. A path with no .git is Python's
// logged error and a clean exit: nothing is written.
func syncLocal(ctx context.Context, plan Plan, conn driver.Conn, env cli.Env, now time.Time) error {
	repo, err := localgit.Open(plan.RepoPath)
	if err != nil {
		if errors.Is(err, localgit.ErrNoRepository) {
			if env.Stderr != nil {
				_, _ = fmt.Fprintf(env.Stderr, "ERROR: %v\n", err)
			}
			return nil
		}
		return err
	}
	org := ""
	if plan.Org != nil {
		org = *plan.Org
	}
	writer := localgit.Writer{Conn: conn, OrgID: org, Now: func() time.Time { return now }}
	options := localgit.Options{
		Since: plan.Since, SyncGit: plan.SyncGit, SyncPRs: plan.SyncPrs, Lookup: env.Lookup, Now: func() time.Time { return now },
	}
	if plan.Call == CallLocalBlame {
		return localgit.SyncBlame(ctx, repo, writer, options)
	}
	return localgit.Sync(ctx, repo, writer, options)
}
