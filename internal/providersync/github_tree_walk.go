package providersync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Ceilings of one GitHub tree walk. Each is at least twice the largest measured
// organisation repository (dev-health-ops: 1,040 directories, 12,033 entries;
// dev-health-web: depth 12) so no live repository stops before the resumable
// walk exists; the entry ceiling is GitHub's own recursive-tree entry cap, so a
// repository above it cannot be listed in one call by GitHub itself.
const (
	gitHubTreeWalkMaxDepth   = 64
	gitHubTreeWalkMaxDirs    = 5_000
	gitHubTreeWalkMaxEntries = 100_000
	// The walk stops, before the next directory request, when its own time
	// budget is spent or the job deadline is too close to leave time to use
	// what it found.
	gitHubTreeWalkBudget         = 12 * time.Minute
	gitHubTreeWalkDeadlineMargin = 2 * time.Minute
)

// Limit names of GitHubTreeWalkLimitError.
const (
	GitHubTreeWalkLimitCycle   = "cycle"
	GitHubTreeWalkLimitDepth   = "depth"
	GitHubTreeWalkLimitDirs    = "dirs"
	GitHubTreeWalkLimitEntries = "entries"
	GitHubTreeWalkLimitTime    = "time"
)

// ErrGitHubTreeWalkLimit is matched (errors.Is) by every GitHubTreeWalkLimitError.
var ErrGitHubTreeWalkLimit = errors.New("github tree walk limit reached")

// GitHubTreeWalkLimitError says which ceiling stopped a tree walk. The walk
// then returns no paths: a partial inventory is never banked.
type GitHubTreeWalkLimitError struct {
	Limit string
	Value int
}

func (err *GitHubTreeWalkLimitError) Error() string {
	return fmt.Sprintf("%s: %s", ErrGitHubTreeWalkLimit, err.Limit)
}

func (err *GitHubTreeWalkLimitError) Is(target error) bool { return target == ErrGitHubTreeWalkLimit }

type gitHubTreeWalkLimits struct {
	maxDepth, maxDirs, maxEntries int
	budget, deadlineMargin        time.Duration
	now                           func() time.Time
}

func defaultGitHubTreeWalkLimits() gitHubTreeWalkLimits {
	return gitHubTreeWalkLimits{
		maxDepth: gitHubTreeWalkMaxDepth, maxDirs: gitHubTreeWalkMaxDirs, maxEntries: gitHubTreeWalkMaxEntries,
		budget: gitHubTreeWalkBudget, deadlineMargin: gitHubTreeWalkDeadlineMargin, now: time.Now,
	}
}

// gitHubTreeWalkStats are the counts of one finished or stopped walk.
type gitHubTreeWalkStats struct {
	dirs, entries, depth int
}

type gitHubTreeWalker struct {
	ctx       context.Context
	root      string
	limits    gitHubTreeWalkLimits
	startedAt time.Time
	ancestors map[string]bool
	paths     []string
	sizes     map[string]*int
	stats     gitHubTreeWalkStats
	metrics   *providerfoundation.Metrics
	nearWarn  map[string]bool
}

// gitHubWalkTree recovers a complete blob inventory when the recursive tree
// listing reports truncated. It re-fetches the same tree object without
// recursive=1 and descends into every "tree" entry by its own sha, joining
// each level's path onto prefix. A subtree that itself reports truncated, any
// fetch that fails, or any ceiling aborts the whole walk instead of returning
// the paths already collected -- a partial walk is exactly the incomplete
// inventory this recovery exists to avoid.
func gitHubWalkTree(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	root, sha, prefix string,
) ([]string, map[string]*int, error) {
	paths, sizes, _, err := gitHubWalkTreeLimited(ctx, client, root, sha, prefix, defaultGitHubTreeWalkLimits())
	return paths, sizes, err
}

func gitHubWalkTreeLimited(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	root, sha, prefix string,
	limits gitHubTreeWalkLimits,
) ([]string, map[string]*int, gitHubTreeWalkStats, error) {
	walker := &gitHubTreeWalker{
		ctx: ctx, root: root, limits: limits, startedAt: limits.now(),
		ancestors: map[string]bool{}, sizes: map[string]*int{},
		metrics: client.Metrics, nearWarn: map[string]bool{},
	}
	if err := walker.walk(client, sha, prefix, 0); err != nil {
		return nil, nil, walker.stats, err
	}
	return walker.paths, walker.sizes, walker.stats, nil
}

// noteNearCeiling says, once per walk and ceiling, that a walk reached half of
// that ceiling (inclusive): a WARN and a counter, both labelled by the ceiling's
// closed name only. A repository growing toward a ceiling shows here long
// before a walk is stopped.
func (walker *gitHubTreeWalker) noteNearCeiling(limit string, value, ceiling int) {
	if ceiling <= 0 || value*2 < ceiling || walker.nearWarn[limit] {
		return
	}
	walker.nearWarn[limit] = true
	walker.metrics.RecordTreeWalkNearCeiling(limit)
	slog.Warn(
		"github tree walk passed half of a ceiling",
		"limit", limit, "value", value, "ceiling", ceiling,
		"dirs", walker.stats.dirs, "depth", walker.stats.depth, "entries", walker.stats.entries,
	)
}

func (walker *gitHubTreeWalker) stop(limit string, value int) error {
	return &GitHubTreeWalkLimitError{Limit: limit, Value: value}
}

func (walker *gitHubTreeWalker) walk(client *providerfoundation.HTTPClient, sha, prefix string, depth int) error {
	if depth > walker.limits.maxDepth {
		return walker.stop(GitHubTreeWalkLimitDepth, walker.limits.maxDepth)
	}
	if walker.ancestors[sha] {
		return walker.stop(GitHubTreeWalkLimitCycle, depth)
	}
	if walker.stats.dirs >= walker.limits.maxDirs {
		return walker.stop(GitHubTreeWalkLimitDirs, walker.limits.maxDirs)
	}
	now := walker.limits.now()
	if now.Sub(walker.startedAt) >= walker.limits.budget {
		return walker.stop(GitHubTreeWalkLimitTime, int(walker.limits.budget/time.Second))
	}
	if deadline, ok := walker.ctx.Deadline(); ok && deadline.Sub(now) <= walker.limits.deadlineMargin {
		return walker.stop(GitHubTreeWalkLimitTime, int(walker.limits.deadlineMargin/time.Second))
	}
	walker.stats.dirs++
	walker.stats.depth = max(walker.stats.depth, depth)
	walker.noteNearCeiling(GitHubTreeWalkLimitDepth, depth, walker.limits.maxDepth)
	walker.noteNearCeiling(GitHubTreeWalkLimitDirs, walker.stats.dirs, walker.limits.maxDirs)
	walker.noteNearCeiling(GitHubTreeWalkLimitTime, int(now.Sub(walker.startedAt)/time.Second), int(walker.limits.budget/time.Second))
	var tree gitHubTreePayload
	if err := fetchObjectRoute(walker.ctx, client, githubTreeRoute, walker.root+"/git/trees/"+url.PathEscape(sha), &tree); err != nil {
		return fmt.Errorf("path=%q: %w", prefix, err)
	}
	if tree.Truncated {
		return fmt.Errorf("path=%q: %w", prefix, ErrGitHubFilesSubtreeTruncated)
	}
	walker.stats.entries += len(tree.Tree)
	walker.noteNearCeiling(GitHubTreeWalkLimitEntries, walker.stats.entries, walker.limits.maxEntries)
	if walker.stats.entries > walker.limits.maxEntries {
		return walker.stop(GitHubTreeWalkLimitEntries, walker.limits.maxEntries)
	}
	walker.ancestors[sha] = true
	defer delete(walker.ancestors, sha)
	for _, entry := range tree.Tree {
		if entry.Path == "" {
			continue
		}
		fullPath := entry.Path
		if prefix != "" {
			fullPath = prefix + "/" + entry.Path
		}
		switch entry.Type {
		case "blob":
			walker.paths = append(walker.paths, fullPath)
			walker.sizes[fullPath] = entry.Size
		case "tree":
			if entry.SHA == "" {
				return fmt.Errorf("path=%q: %w", fullPath, providerfoundation.ErrNormalizationInvalid)
			}
			if err := walker.walk(client, entry.SHA, fullPath, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// logGitHubTreeWalkStop writes the one WARN of a walk a ceiling stopped. Closed
// labels and counts only: no repository name, no path.
func logGitHubTreeWalkStop(err error, stats gitHubTreeWalkStats) {
	var limit *GitHubTreeWalkLimitError
	if !errors.As(err, &limit) {
		return
	}
	// Plain values, not the error: a log call that takes the error (even a
	// field of it) counts against the legacy error-text census.
	name, value := limit.Limit, limit.Value
	slog.Warn(
		"github tree walk stopped at ceiling; no inventory was banked",
		"limit", name, "value", value,
		"dirs", stats.dirs, "depth", stats.depth, "entries", stats.entries,
	)
}
