package synccli

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// `dho sync <target> --search PATTERN` is the batch mode of
// process_github_repos_batch / process_gitlab_projects_batch: list the
// repositories (or projects) the pattern selects, then sync each of them the
// way the single-repository mode does. The listing is
// providersync.ListInProcess (pinned to the Python listing by its own live
// oracle); each repository is the same per-dataset providersync.RunInProcess run
// the single-repository executor makes.
//
// Where it follows Python: repositories run in chunks of --batch-size, at most
// --max-concurrent at a time (both floored at 1, as `max(1, ...)` does there),
// and --max-repos caps the listing.
//
// Where it deliberately differs, each named so a script can rely on it:
//   - a failure is never swallowed. Every dataset of every repository is run on
//     its own; a failed one is reported on stderr with its repository and
//     dataset, the rest of the batch continues, and the command exits 1 when any
//     failed. (Python's batch logs a repository error and carries on.)
//   - --use-async is accepted and ignored: this executor is always concurrent.
//   - --rate-limit-delay is accepted and has no effect. In Python it only seeds
//     the pull-request backoff gate's first delay; the in-process gate here
//     backs off from the provider's own retry-after signals.
//   - --max-concurrent bounds repositories in flight. The worker's per-cost-class
//     budget (4 light / 2 medium / 1 heavy) is per repository run here, so the
//     overall request concurrency can exceed it on a large --max-concurrent.

// clampInt narrows a Python int to a Go int, saturating instead of wrapping.
func clampInt(n *big.Int) int {
	switch {
	case n == nil:
		return 0
	case n.IsInt64() && n.Int64() >= math.MinInt32 && n.Int64() <= math.MaxInt32:
		return int(n.Int64())
	case n.Sign() < 0:
		return math.MinInt32
	}
	return math.MaxInt32
}

// batchListing builds the listing a batch plan asks for.
func batchListing(plan Plan) providersync.InProcessListing {
	var maxRepos *int
	if plan.MaxRepos != nil {
		n := clampInt(plan.MaxRepos)
		maxRepos = &n
	}
	group := ""
	if plan.Group != nil {
		group = *plan.Group
	}
	if plan.Call == CallGitLabBatch {
		return providersync.InProcessListing{GitLab: &providersync.GitLabListing{
			Group: group, Pattern: plan.Search, MaxProjects: maxRepos,
		}}
	}
	listing := &providersync.GitHubListing{Org: group, Pattern: plan.Search, MaxRepos: maxRepos}
	if group == "" {
		// _list_github_repositories_for_batch passes --owner as the user only
		// when no --group names an organization.
		listing.User = plan.Owner
	}
	return providersync.InProcessListing{GitHub: listing}
}

// batchFailure is one dataset of one repository that did not complete.
type batchFailure struct {
	index   int
	repo    string
	dataset string
	err     error
}

func runBatch(ctx context.Context, deps InlineDeps, plan Plan, datasets []string, run providersync.InProcessRun, env cli.Env) error {
	secretValues := batchSecrets(run)
	redact := func(err error) string {
		text := redactDSN(err.Error(), plan.SinkURI)
		for _, secret := range secretValues {
			text = strings.ReplaceAll(text, secret, "<redacted>")
		}
		return text
	}
	repos, err := deps.List(ctx, run, batchListing(plan))
	if err != nil {
		return fmt.Errorf("%s: list repositories: %s", plan.Provider, redact(err))
	}
	if len(repos) == 0 {
		writeLine(env.Stderr, fmt.Sprintf("dho sync %s: no repositories matched %q", plan.Target, plan.Search))
		return nil
	}
	writeLine(env.Stderr, fmt.Sprintf("dho sync %s: %d repositories matched %q", plan.Target, len(repos), plan.Search))

	batchSize := max(1, clampInt(plan.BatchSize))
	concurrent := max(1, clampInt(plan.MaxConcurrent))
	slots := make(chan struct{}, concurrent)
	var (
		mu       sync.Mutex
		failures []batchFailure
	)
	report := func(failure batchFailure) {
		mu.Lock()
		defer mu.Unlock()
		failures = append(failures, failure)
		writeLine(env.Stderr, fmt.Sprintf("dho sync %s: %s: %s: %s", plan.Target, failure.repo, failure.dataset, redact(failure.err)))
	}
	for start := 0; start < len(repos); start += batchSize {
		end := min(start+batchSize, len(repos))
		var wg sync.WaitGroup
		for index := start; index < end; index++ {
			wg.Add(1)
			slots <- struct{}{}
			go func(index int) {
				defer wg.Done()
				defer func() { <-slots }()
				syncListed(ctx, deps, plan, datasets, run, index, repos[index], report)
			}(index)
		}
		wg.Wait()
	}
	if len(failures) == 0 {
		return nil
	}
	failed := map[int]bool{}
	for _, failure := range failures {
		failed[failure.index] = true
	}
	return fmt.Errorf("%d of %d repositories failed (%d dataset runs)", len(failed), len(repos), len(failures))
}

// syncListed runs every dataset of the target for one listed repository. A
// dataset that fails does not stop the ones after it.
func syncListed(ctx context.Context, deps InlineDeps, plan Plan, datasets []string, base providersync.InProcessRun, index int, repo providersync.ListedRepository, report func(batchFailure)) {
	run := base
	switch plan.Call {
	case CallGitLabBatch:
		if repo.ProjectID == 0 {
			report(batchFailure{index, repoLabel(repo), "listing", errors.New("the listed project has no numeric id")})
			return
		}
		run.SourceExternalID = strconv.FormatInt(repo.ProjectID, 10)
		run.SourceName = repoLabel(repo)
	default:
		if repo.FullName == "" {
			report(batchFailure{index, repoLabel(repo), "listing", errors.New("the listed repository has no full name")})
			return
		}
		run.SourceExternalID, run.SourceName = repo.FullName, repo.FullName
	}
	for _, dataset := range datasets {
		run.Dataset = dataset
		if _, err := deps.Run(ctx, run); err != nil {
			report(batchFailure{index, repoLabel(repo), dataset, err})
		}
	}
}

func repoLabel(repo providersync.ListedRepository) string {
	switch {
	case repo.FullName != "":
		return repo.FullName
	case repo.Name != "":
		return repo.Name
	case repo.ProjectID != 0:
		return strconv.FormatInt(repo.ProjectID, 10)
	}
	return "<unnamed>"
}

// batchSecrets are the values an error message must never carry.
func batchSecrets(run providersync.InProcessRun) []string {
	var values []string
	for _, key := range []string{"token", "private_key"} {
		if value := run.Credential[key]; value != "" {
			values = append(values, value)
		}
	}
	// Longest first, so a secret that contains another is replaced whole.
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	return values
}
