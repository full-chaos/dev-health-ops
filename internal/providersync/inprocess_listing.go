package providersync

import (
	"context"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// InProcessListing selects what a batch run lists: exactly one of GitHub and
// GitLab is set, matching run.Provider.
type InProcessListing struct {
	GitHub *GitHubListing
	GitLab *GitLabListing
}

// ListInProcess lists the repositories (GitHub) or projects (GitLab) of a batch
// with the same credential and HTTP settings RunInProcess uses (run.Credential,
// run.Config, run.Doer, run.Retry), so a batch lists and then syncs as one
// identity. It needs no store: nothing is written.
func ListInProcess(ctx context.Context, run InProcessRun, listing InProcessListing) ([]ListedRepository, error) {
	if len(run.Credential) == 0 {
		return nil, ErrInvalidConfiguration
	}
	fields := make(map[string]secrets.Value, len(run.Credential))
	for name, value := range run.Credential {
		fields[name] = secrets.NewValue(value)
	}
	credential := providerfoundation.NewCredential(run.Provider, "cli", run.Config, fields)
	retry := run.Retry
	if retry.MaxAttempts == 0 {
		retry = providerfoundation.DefaultRetryPolicy()
	}
	// The process is the only claimant: a lease never lapses, a cancelled
	// context still stops a request.
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	doer := inProcessHTTPDoer(run.Doer)
	switch {
	case run.Provider == "github" && listing.GitHub != nil:
		client, err := providerfoundation.NewGitHubClient(credential, doer, retry, lease)
		if err != nil {
			return nil, fmt.Errorf("github credential: %w", err)
		}
		return ListGitHubRepositories(ctx, client, *listing.GitHub)
	case run.Provider == "gitlab" && listing.GitLab != nil:
		client, err := providerfoundation.NewGitLabClient(credential, doer, retry, lease)
		if err != nil {
			return nil, fmt.Errorf("gitlab credential: %w", err)
		}
		return ListGitLabProjects(ctx, client, *listing.GitLab)
	}
	return nil, ErrNotAGitFamilyRoute
}
