package localgit

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Options selects the stages of processors/local.py process_local_repo.
type Options struct {
	Since   *time.Time
	SyncGit bool
	SyncPRs bool
	// Lookup reads the environment (REPO_UUID); nil reads none.
	Lookup func(string) (string, bool)
	// Now is the clock of the inferred open-PR fallback; nil is time.Now.
	Now func() time.Time
}

// Sync is process_local_repo(store, repo_path, since, sync_git=..., sync_prs=...,
// sync_blame=False): the repository row, then commits (sync_git), the
// merged and open pull requests inferred from them (sync_prs), then the commit
// stats (sync_git). Python swallows a failing insert into a log line and exits
// 0; here it is returned, so the verb exits 1 (a named divergence).
func Sync(ctx context.Context, repo Repo, writer Writer, opts Options) error {
	id, err := repo.RepoID(ctx, opts.Lookup)
	if err != nil {
		return err
	}
	tags, err := repo.TagsJSON(ctx)
	if err != nil {
		return err
	}
	if err := writer.InsertRepo(ctx, id, repo.Name(), tags); err != nil {
		return err
	}
	commits, err := repo.IterCommitsSince(ctx, opts.Since)
	if err != nil {
		return err
	}
	if opts.SyncGit {
		if err := writer.InsertCommits(ctx, id, commits); err != nil {
			return err
		}
	}
	if opts.SyncPRs {
		now := opts.Now
		if now == nil {
			now = time.Now
		}
		open, err := repo.InferOpenPullRequests(ctx, now)
		if err != nil {
			return err
		}
		merged := InferMergedPullRequests(commits, opts.Since)
		if err := writer.InsertPullRequests(ctx, id, append(open, merged...)); err != nil {
			return err
		}
	}
	if opts.SyncGit {
		stats, err := repo.allCommitStats(ctx, commits)
		if err != nil {
			return err
		}
		if err := writer.InsertCommitStats(ctx, id, stats); err != nil {
			return fmt.Errorf("commit stats: %w", err)
		}
	}
	return nil
}

// statsWorkers is MAX_WORKERS' default (utils.py): the git subprocesses of the
// stats stage run four at a time, as Python's executor threads do; the rows
// keep commit order.
const statsWorkers = 4

func (r Repo) allCommitStats(ctx context.Context, commits []Commit) ([]CommitStat, error) {
	results := make([][]CommitStat, len(commits))
	slots := make(chan struct{}, statsWorkers)
	var wg sync.WaitGroup
	for i := range commits {
		slots <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer func() { <-slots; wg.Done() }()
			results[i] = r.CommitStats(ctx, commits[i])
		}(i)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err // a cancelled run is not "no rows"
	}
	var all []CommitStat
	for _, rows := range results {
		all = append(all, rows...)
	}
	return all, nil
}
