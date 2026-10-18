package providersync

import (
	"context"
	"log/slog"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
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

// githubWorkItemRepoOfID is the repository name of a "gh:<name>#<n>" or
// "ghpr:<name>#<n>" work-item id, and the rest ("#<n>").
func githubWorkItemRepoOfID(id string) (prefix, name, rest string, ok bool) {
	for _, candidate := range [2]string{"ghpr:", "gh:"} {
		if body, found := strings.CutPrefix(id, candidate); found {
			index := strings.LastIndex(body, "#")
			if index < 1 || index == len(body)-1 {
				return "", "", "", false
			}
			return candidate, body[:index], body[index:], true
		}
	}
	return "", "", "", false
}

// canonicalGitHubForeignRefs rewrites the ids of rows that name another
// repository by the name a payload states (a closing reference, an
// owner/repo#n in text) to the identity that repository's own items carry
// (CHAOS-9879). The syncing repository's own current name maps to its identity
// even before its repos row is stored. A row that then links an item to itself
// is dropped, and a row the rewrite made equal to an earlier one keeps the
// earlier.
func canonicalGitHubForeignRefs(
	ctx context.Context, reader GitLabRepoIdentityReader, claim Claim, names githubRepoNames,
	rows []githubWorkItemDependencyRow,
) ([]githubWorkItemDependencyRow, error) {
	foreign := make([]string, 0)
	for _, row := range rows {
		if _, name, _, ok := githubWorkItemRepoOfID(row.foreignID); ok {
			foreign = append(foreign, name)
		}
	}
	if len(foreign) == 0 {
		return rows, nil
	}
	identityNames, err := resolveGitLabIdentityPaths(ctx, reader, claim, names.Current,
		GitLabRepoIdentity{Path: names.Identity}, foreign)
	if err != nil {
		slog.Default().WarnContext(ctx, "github_foreign_refs_unresolved",
			"org_id", claim.OrgID, "unit_id", claim.ID, "dataset", claim.Dataset, "refs", len(foreign),
			"error_class", logging.ErrorClass(err), "error_type", logging.ErrorType(err))
		return nil, err
	}
	rewritten, unchanged := 0, 0
	canonical := func(id string) string {
		prefix, name, rest, ok := githubWorkItemRepoOfID(id)
		if !ok {
			return id
		}
		if identity := identityNames[strings.ToLower(name)]; identity != "" {
			if identity != name {
				rewritten++
			} else {
				unchanged++
			}
			return prefix + identity + rest
		}
		unchanged++
		return id
	}
	type key struct{ source, target, relationship, raw string }
	seen := make(map[key]bool, len(rows))
	out := make([]githubWorkItemDependencyRow, 0, len(rows))
	for _, row := range rows {
		if row.foreignID != "" {
			if row.SourceWorkItemID == row.foreignID {
				row.SourceWorkItemID = canonical(row.foreignID)
			}
			if row.TargetWorkItemID == row.foreignID {
				row.TargetWorkItemID = canonical(row.foreignID)
			}
			row.foreignID = ""
		}
		if row.SourceWorkItemID == row.TargetWorkItemID {
			continue
		}
		k := key{row.SourceWorkItemID, row.TargetWorkItemID, row.RelationshipType, row.RelationshipTypeRaw}
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, row)
	}
	slog.Default().InfoContext(ctx, "github_foreign_refs_canonical",
		"org_id", claim.OrgID, "unit_id", claim.ID, "dataset", claim.Dataset,
		"refs", len(foreign), "rewritten", rewritten, "unchanged", unchanged, "rows_in", len(rows), "rows_out", len(out))
	if dropped := len(rows) - len(out); dropped > 0 {
		slog.Default().WarnContext(ctx, "github_foreign_refs_rows_dropped",
			"org_id", claim.OrgID, "unit_id", claim.ID, "dataset", claim.Dataset, "dropped", dropped)
	}
	return out, nil
}
