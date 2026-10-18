package providersync

import (
	"context"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// resolveGitHubRepoIdentity is the one way a GitHub route gets its repository
// identity (CHAOS-9879, the GitHub sibling of CHAOS-3383 option A): the stored
// identity of the repository's numeric id decides it, so a rename or transfer
// continues the same repo_id and ids, and a new repository at a name another
// repository was minted from gets "{name}@{id}". Without a reader (a store
// that holds no repository) it mints from the current name.
func resolveGitHubRepoIdentity(
	ctx context.Context, reader GitLabRepoIdentityReader, claim Claim, repo gitHubRepositoryPayload,
) (GitLabRepoIdentity, error) {
	name := strings.TrimSpace(repo.FullName)
	if reader == nil {
		minted, err := repositoryIdentity(name)
		return GitLabRepoIdentity{RepoID: minted, Path: name}, err
	}
	numericID, err := repo.ID.Int64()
	if err != nil || numericID < 1 {
		return GitLabRepoIdentity{}, providerfoundation.ErrNormalizationInvalid
	}
	return resolveRepoIdentity(ctx, "github", reader, claim, numericID, name)
}

// resolveGitHubRepoID is resolveGitHubRepoIdentity for a route that keys rows
// by repo_id only.
func resolveGitHubRepoID(
	ctx context.Context, reader GitLabRepoIdentityReader, claim Claim, repo gitHubRepositoryPayload,
) (string, error) {
	identity, err := resolveGitHubRepoIdentity(ctx, reader, claim, repo)
	return identity.RepoID, err
}
