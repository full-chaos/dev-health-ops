package localgit

import (
	"context"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"
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

// SyncBlame is process_local_blame(store, repo_path, since): the repository
// row, then a git_files row for EVERY file of the working tree and the blame
// lines of the files the commits since `since` changed (all files when no
// commit changed one that still exists).
func SyncBlame(ctx context.Context, repo Repo, writer Writer, opts Options) error {
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
	var changed map[string]bool
	if len(commits) > 0 {
		changed = repo.ChangedFiles(ctx, commits)
	}
	all := repo.AllFiles()
	forBlame := changed
	if len(forBlame) == 0 {
		forBlame = make(map[string]bool, len(all))
		for _, path := range all {
			forBlame[path] = true
		}
	}
	files := make([]File, len(all))
	blames := make([][]BlameLine, len(all))
	slots := make(chan struct{}, statsWorkers)
	var wg sync.WaitGroup
	for i, path := range all {
		slots <- struct{}{}
		wg.Add(1)
		go func(i int, path string) {
			defer func() { <-slots; wg.Done() }()
			files[i] = repo.ReadFile(path)
			if forBlame[path] {
				blames[i] = repo.Blame(ctx, files[i].Path)
			}
		}(i, path)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	var lines []BlameLine
	for _, rows := range blames {
		lines = append(lines, rows...)
	}
	for _, file := range files {
		if !utf8.ValidString(file.Path) {
			// Python holds the path as a str with surrogate escapes, and the
			// ClickHouse client cannot encode it: the files insert raises, so no
			// file or blame row is written (for a repository of fewer than 1000
			// files, whose single batch it is; a larger one may have written
			// earlier batches there).
			return fmt.Errorf("a file path is not valid UTF-8 and cannot be written: %q", file.Path)
		}
	}
	if err := writer.InsertFiles(ctx, id, files); err != nil {
		return fmt.Errorf("git files: %w", err)
	}
	if err := writer.InsertBlame(ctx, id, lines); err != nil {
		return fmt.Errorf("git blame: %w", err)
	}
	return nil
}
