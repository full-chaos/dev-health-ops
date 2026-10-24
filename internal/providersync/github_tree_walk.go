package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"
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
	// recursiveSubtrees asks GitHub for each directory's whole subtree in one
	// request and descends per directory only below an answer that is truncated
	// or over the object cap. Off, the walk costs one request per directory.
	recursiveSubtrees bool
	// rootListed says the caller already asked for the recursive listing of the
	// root and it failed, so the root is listed alone at once.
	rootListed bool
	// slice makes a ceiling stop a result instead of a failure: the walk keeps
	// what it read and reports where it stopped, and a later run walks on. A
	// subtree answer that does not fit the remaining ceilings is listed and
	// descended instead of stopping the walk.
	slice bool
	// after is the lower bound of a slice walk: only paths >= after are kept and
	// every directory wholly before it is skipped. until, when set, is the upper
	// bound: the walk ends before the first path >= until, with no ceiling.
	after, until string
}

func defaultGitHubTreeWalkLimits() gitHubTreeWalkLimits {
	return gitHubTreeWalkLimits{
		maxDepth: gitHubTreeWalkMaxDepth, maxDirs: gitHubTreeWalkMaxDirs, maxEntries: gitHubTreeWalkMaxEntries,
		budget: gitHubTreeWalkBudget, deadlineMargin: gitHubTreeWalkDeadlineMargin, now: time.Now,
		recursiveSubtrees: true,
	}
}

// rootListedWalkLimits are the default limits for a walk that starts after the
// root's recursive listing was truncated or over the cap.
func rootListedWalkLimits() gitHubTreeWalkLimits {
	limits := defaultGitHubTreeWalkLimits()
	limits.rootListed = true
	return limits
}

// gitHubTreeWalkStats are the counts of one finished or stopped walk.
type gitHubTreeWalkStats struct {
	dirs, entries, depth int
}

// gitHubWalkSlice is the result of a walk in slice mode: every blob path in
// [after, Next) in tree order. Next is the lower bound of the part the walk did
// not read; it is empty for a walk that read to its end.
type gitHubWalkSlice struct {
	Paths []string
	Sizes map[string]*int
	Stats gitHubTreeWalkStats
	Next  string
	// Stopped is the ceiling that ended the walk early, or nil for a whole walk.
	Stopped *GitHubTreeWalkLimitError
}

// gitHubWalkFrontierMaxBytes bounds a frontier path, as the cursor encodes it,
// so two of them fit the chunk cursor. A longer one is cut back to the nearest
// ancestor directory: that is a smaller lower bound, so the slice only shrinks.
const gitHubWalkFrontierMaxBytes = 1000

func gitHubWalkFrontierEncodedLen(path string) int {
	encoded, err := json.Marshal(path)
	if err != nil {
		return gitHubWalkFrontierMaxBytes + 1
	}
	return len(encoded)
}

type gitHubTreeWalker struct {
	ctx       context.Context
	root      string
	limits    gitHubTreeWalkLimits
	startedAt time.Time
	// timeCeiling is the time the walk may take: the budget, or less when the
	// job deadline (minus its margin) comes first, so the half-way note covers
	// every way the time can end a walk.
	timeCeiling time.Duration
	ancestors   map[string]bool
	paths       []string
	sizes       map[string]*int
	stats       gitHubTreeWalkStats
	metrics     *providerfoundation.Metrics
	nearWarn    map[string]bool
	// frontier is the directory at which a ceiling stopped the walk; cut is set
	// once the walk reached its until bound.
	frontier    string
	frontierSet bool
	cut         bool
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
	walker := newGitHubTreeWalker(ctx, client, root, limits)
	if err := walker.walk(client, sha, prefix, 0); err != nil {
		return nil, nil, walker.stats, err
	}
	return walker.paths, walker.sizes, walker.stats, nil
}

// gitHubWalkTreeSlice walks like gitHubWalkTreeLimited with limits.slice set:
// a dirs, entries or time ceiling ends the walk with what it read, up to the
// directory it stopped at. A walk that stopped before it passed limits.after,
// any other error, and a frontier too long for the cursor are errors.
func gitHubWalkTreeSlice(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	root, sha string,
	limits gitHubTreeWalkLimits,
) (gitHubWalkSlice, error) {
	limits.slice = true
	walker := newGitHubTreeWalker(ctx, client, root, limits)
	err := walker.walk(client, sha, "", 0)
	if err == nil {
		return gitHubWalkSlice{Paths: walker.paths, Sizes: walker.sizes, Stats: walker.stats}, nil
	}
	var stopped *GitHubTreeWalkLimitError
	if !errors.As(err, &stopped) || !gitHubWalkLimitResumable(stopped.Limit) || !walker.frontierSet {
		return gitHubWalkSlice{Stats: walker.stats}, err
	}
	next := gitHubWalkFrontierKey(walker.frontier)
	for next != "" && gitHubWalkFrontierEncodedLen(next) > gitHubWalkFrontierMaxBytes {
		next = next[:strings.LastIndex(strings.TrimSuffix(next, "/"), "/")+1]
	}
	if next == "" || next <= limits.after {
		return gitHubWalkSlice{Stats: walker.stats}, err
	}
	result := gitHubWalkSlice{Stats: walker.stats, Next: next, Stopped: stopped, Sizes: walker.sizes}
	for _, path := range walker.paths {
		if path < next {
			result.Paths = append(result.Paths, path)
		} else {
			delete(result.Sizes, path)
		}
	}
	return result, nil
}

func gitHubWalkLimitResumable(limit string) bool {
	switch limit {
	case GitHubTreeWalkLimitDirs, GitHubTreeWalkLimitEntries, GitHubTreeWalkLimitTime:
		return true
	}
	return false
}

// gitHubWalkFrontierKey is the lower bound of everything inside directory dir
// ("" is the root, which has none).
func gitHubWalkFrontierKey(dir string) string {
	if dir == "" {
		return ""
	}
	return dir + "/"
}

func newGitHubTreeWalker(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	root string,
	limits gitHubTreeWalkLimits,
) *gitHubTreeWalker {
	walker := &gitHubTreeWalker{
		ctx: ctx, root: root, limits: limits, startedAt: limits.now(), timeCeiling: limits.budget,
		ancestors: map[string]bool{}, sizes: map[string]*int{},
		metrics: client.Metrics, nearWarn: map[string]bool{},
	}
	if deadline, ok := ctx.Deadline(); ok {
		if usable := deadline.Sub(walker.startedAt) - limits.deadlineMargin; usable < walker.timeCeiling {
			walker.timeCeiling = usable
		}
	}
	return walker
}

// noteNearCeiling says, once per walk and ceiling, that a walk reached half of
// that ceiling (inclusive): a WARN and a counter, both labelled by the ceiling's
// closed name only. A repository growing toward a ceiling shows here long
// before a walk is stopped. Counts (directories, entries, depth) are compared as
// integers; time is compared as a duration at full precision (noteNearTime): a
// whole-second compare misses the threshold of a deadline-derived ceiling.
func (walker *gitHubTreeWalker) noteNearCeiling(limit string, value, ceiling int) {
	walker.noteNear(limit, int64(value), int64(ceiling), value, ceiling)
}

func (walker *gitHubTreeWalker) noteNearTime(elapsed, ceiling time.Duration) {
	walker.noteNear(GitHubTreeWalkLimitTime, int64(elapsed), int64(ceiling),
		int(elapsed.Milliseconds()), int(ceiling.Milliseconds()))
}

// noteNear compares value >= ceil(ceiling/2) in int64 (no product that can
// overflow; ceiling <= 0 never fires) and logs the values it was given in
// shown units (counts, or milliseconds for time).
func (walker *gitHubTreeWalker) noteNear(limit string, value, ceiling int64, shownValue, shownCeiling int) {
	if ceiling <= 0 || value < ceiling-ceiling/2 || walker.nearWarn[limit] {
		return
	}
	walker.nearWarn[limit] = true
	walker.metrics.RecordTreeWalkNearCeiling(limit)
	slog.Warn(
		"github tree walk passed half of a ceiling",
		"limit", limit, "value", shownValue, "ceiling", shownCeiling,
		"dirs", walker.stats.dirs, "depth", walker.stats.depth, "entries", walker.stats.entries,
	)
}

func (walker *gitHubTreeWalker) stop(limit string, value int) error {
	return &GitHubTreeWalkLimitError{Limit: limit, Value: value}
}

// noteStop records the directory at which a resumable ceiling stopped the walk:
// the deepest frame sees the error first, so the first call wins.
func (walker *gitHubTreeWalker) noteStop(prefix string, err error) {
	var stopped *GitHubTreeWalkLimitError
	if walker.frontierSet || !errors.As(err, &stopped) || !gitHubWalkLimitResumable(stopped.Limit) {
		return
	}
	walker.frontier, walker.frontierSet = prefix, true
}

// withinBound says bound lies inside directory dir (the root contains every
// bound): such a directory is listed on its own, not read as one subtree.
func withinBound(bound, dir string) bool {
	return bound != "" && (dir == "" || strings.HasPrefix(bound, dir+"/"))
}

// beforeAfter says every path of directory dir is below the slice's lower bound.
func (walker *gitHubTreeWalker) beforeAfter(dir string) bool {
	after := walker.limits.after
	return after != "" && after > dir+"/" && !strings.HasPrefix(after, dir+"/")
}

// reachedUntil says key (a blob path, or a directory path plus "/") is at or
// above the slice's upper bound; the walk then ends.
func (walker *gitHubTreeWalker) reachedUntil(key string) bool {
	if walker.limits.until != "" && key >= walker.limits.until {
		walker.cut = true
	}
	return walker.cut
}

func (walker *gitHubTreeWalker) walk(client *providerfoundation.HTTPClient, sha, prefix string, depth int) (err error) {
	defer func() {
		if err != nil {
			walker.noteStop(prefix, err)
		}
	}()
	now := walker.limits.now()
	// The half-way signal comes BEFORE any stop: a walk that is stopped by the
	// very value it first observes (elapsed past the budget at the next
	// directory, depth past the ceiling) still says it passed half.
	walker.noteNearCeiling(GitHubTreeWalkLimitDepth, depth, walker.limits.maxDepth)
	walker.noteNearTime(now.Sub(walker.startedAt), walker.timeCeiling)
	if depth > walker.limits.maxDepth {
		return walker.stop(GitHubTreeWalkLimitDepth, walker.limits.maxDepth)
	}
	if walker.ancestors[sha] {
		return walker.stop(GitHubTreeWalkLimitCycle, depth)
	}
	if walker.stats.dirs >= walker.limits.maxDirs {
		return walker.stop(GitHubTreeWalkLimitDirs, walker.limits.maxDirs)
	}
	if now.Sub(walker.startedAt) >= walker.limits.budget {
		return walker.stop(GitHubTreeWalkLimitTime, int(walker.limits.budget/time.Second))
	}
	if deadline, ok := walker.ctx.Deadline(); ok && deadline.Sub(now) <= walker.limits.deadlineMargin {
		return walker.stop(GitHubTreeWalkLimitTime, int(walker.limits.deadlineMargin/time.Second))
	}
	if walker.limits.recursiveSubtrees && !(depth == 0 && walker.limits.rootListed) {
		done, err := walker.walkSubtree(client, sha, prefix, depth)
		if err != nil || done {
			return err
		}
	}
	walker.stats.dirs++
	walker.stats.depth = max(walker.stats.depth, depth)
	walker.noteNearCeiling(GitHubTreeWalkLimitDirs, walker.stats.dirs, walker.limits.maxDirs)
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
	entries := tree.Tree
	if walker.limits.slice {
		entries = sortedGitHubTreeEntries(entries)
	}
	for _, entry := range entries {
		if entry.Path == "" {
			continue
		}
		fullPath := joinGitHubWalkPath(prefix, entry.Path)
		switch entry.Type {
		case "blob":
			if walker.reachedUntil(fullPath) {
				return nil
			}
			if fullPath < walker.limits.after {
				continue
			}
			walker.paths = append(walker.paths, fullPath)
			walker.sizes[fullPath] = entry.Size
		case "tree":
			if entry.SHA == "" {
				return fmt.Errorf("path=%q: %w", fullPath, providerfoundation.ErrNormalizationInvalid)
			}
			if walker.reachedUntil(fullPath + "/") {
				return nil
			}
			if walker.beforeAfter(fullPath) {
				continue
			}
			if err := walker.walk(client, entry.SHA, fullPath, depth+1); err != nil {
				return err
			}
			if walker.cut {
				return nil
			}
		}
	}
	return nil
}

// sortedGitHubTreeEntries orders one directory listing as full paths sort: a
// directory sorts as its name plus "/". GitHub lists in that order; a slice
// cut is only valid if the walk reads in it, so the walk does not rely on it.
func sortedGitHubTreeEntries(entries []gitHubTreeEntry) []gitHubTreeEntry {
	key := func(entry gitHubTreeEntry) string {
		if entry.Type == "tree" {
			return entry.Path + "/"
		}
		return entry.Path
	}
	sorted := append([]gitHubTreeEntry(nil), entries...)
	sort.SliceStable(sorted, func(left, right int) bool { return key(sorted[left]) < key(sorted[right]) })
	return sorted
}

func joinGitHubWalkPath(prefix, path string) string {
	if prefix == "" {
		return path
	}
	return prefix + "/" + path
}

// walkSubtree reads the whole subtree of sha in one recursive request. It
// returns false, with nothing recorded, when GitHub marks the answer truncated
// or it is above the object cap: the caller then lists the directory alone and
// descends. The same ceilings apply as to a per-directory walk: every directory
// of the answer counts as one, so a walk stops at the same repository size.
func (walker *gitHubTreeWalker) walkSubtree(client *providerfoundation.HTTPClient, sha, prefix string, depth int) (bool, error) {
	if withinBound(walker.limits.after, prefix) || withinBound(walker.limits.until, prefix) {
		return false, nil
	}
	var tree gitHubTreePayload
	err := fetchObjectRoute(walker.ctx, client, githubTreeRoute, walker.root+"/git/trees/"+url.PathEscape(sha)+"?recursive=true", &tree)
	var tooLarge *providerfoundation.ObjectTooLargeError
	switch {
	case errors.As(err, &tooLarge):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("path=%q: %w", prefix, err)
	case tree.Truncated:
		return false, nil
	}
	dirs, deepest := 1, depth
	for _, entry := range tree.Tree {
		if entry.Type == "tree" && entry.Path != "" {
			dirs++
			deepest = max(deepest, depth+1+strings.Count(entry.Path, "/"))
		}
	}
	walker.noteNearCeiling(GitHubTreeWalkLimitDepth, deepest, walker.limits.maxDepth)
	if deepest > walker.limits.maxDepth {
		return false, walker.stop(GitHubTreeWalkLimitDepth, walker.limits.maxDepth)
	}
	if walker.limits.slice && (walker.stats.dirs+dirs > walker.limits.maxDirs ||
		walker.stats.entries+len(tree.Tree) > walker.limits.maxEntries) {
		return false, nil
	}
	walker.stats.dirs += dirs
	walker.noteNearCeiling(GitHubTreeWalkLimitDirs, walker.stats.dirs, walker.limits.maxDirs)
	if walker.stats.dirs > walker.limits.maxDirs {
		return false, walker.stop(GitHubTreeWalkLimitDirs, walker.limits.maxDirs)
	}
	walker.stats.entries += len(tree.Tree)
	walker.noteNearCeiling(GitHubTreeWalkLimitEntries, walker.stats.entries, walker.limits.maxEntries)
	if walker.stats.entries > walker.limits.maxEntries {
		return false, walker.stop(GitHubTreeWalkLimitEntries, walker.limits.maxEntries)
	}
	walker.stats.depth = max(walker.stats.depth, deepest)
	for _, entry := range tree.Tree {
		if entry.Type != "blob" || entry.Path == "" {
			continue
		}
		fullPath := entry.Path
		if prefix != "" {
			fullPath = prefix + "/" + entry.Path
		}
		walker.paths = append(walker.paths, fullPath)
		walker.sizes[fullPath] = entry.Size
	}
	return true, nil
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
