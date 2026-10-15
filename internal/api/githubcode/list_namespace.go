package githubcode

import (
	"context"
	"math/big"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Owner types a namespace listing reports.
const (
	OwnerTypeOrg  = "org"
	OwnerTypeUser = "user"
)

// NamespaceOptions are the keyword arguments of ListNamespace.
type NamespaceOptions struct {
	// Owner is the namespace: an organization or a personal account.
	Owner string
	// UserRepos says /user/repos is the authority for the credential (a token that belongs to a user). A
	// GitHub App installation token has no such listing and sets it false.
	UserRepos bool
	Pattern   string
	MaxRepos  *big.Int
}

// NamespaceListing is what ListNamespace found.
type NamespaceListing struct {
	Repos []Repo
	// OwnerType is OwnerTypeOrg or OwnerTypeUser when the namespace's own listing answered, empty when it did not.
	OwnerType string
	// NotFound is set when the credential reaches no repository of the namespace and the namespace's own listing
	// answered 404 on both routes.
	NotFound bool
	// NamespaceErr is the failure of the namespace's own listing when the token's repositories were kept without it.
	NamespaceErr error
}

// ListNamespace lists the repositories of one namespace with the decision rule of scheduler discovery
// (internal/scheduler/sync, discoverGitHub, ops #4158). /user/repos (U) is authoritative for the credential and is
// never complete for a namespace (another user, an organization the token is not a member of, public
// repositories), so the repositories of U owned by the namespace are unioned with the namespace's own listing (N),
// organization first, then user account. An error of U is returned as it is: N is a public listing, so N alone would
// be a partial success. U ok and N failed keeps U and reports N's error; U empty and N failed returns N's error,
// except a 404 on both routes, which is NotFound with no repositories.
func (c Client) ListNamespace(ctx context.Context, opts NamespaceOptions) (NamespaceListing, error) {
	var tokenRepos []Repo
	if opts.UserRepos {
		all, err := c.ListRepositories(ctx, ListOptions{Pattern: opts.Pattern})
		if isNotFound(err) {
			// The token's own listing is not an owner route: its 404 is a credential or base URL fault, never "owner not found".
			return NamespaceListing{}, &Error{Class: "APIException", Message: "github token repository listing was not found; check the credential's base URL"}
		}
		if err != nil {
			return NamespaceListing{}, err
		}
		prefix := strings.ToLower(opts.Owner) + "/"
		for _, repo := range all {
			if strings.HasPrefix(strings.ToLower(repo.FullName), prefix) {
				tokenRepos = append(tokenRepos, repo)
			}
		}
	}
	listing := NamespaceListing{OwnerType: OwnerTypeOrg}
	owned, err := c.ListRepositories(ctx, ListOptions{Org: opts.Owner, Pattern: opts.Pattern, MaxRepos: opts.MaxRepos})
	err = RedactOwner(err, opts.Owner)
	if err != nil {
		var userErr error
		owned, userErr = c.ListRepositories(ctx, ListOptions{User: opts.Owner, Pattern: opts.Pattern, MaxRepos: opts.MaxRepos})
		listing.OwnerType = OwnerTypeUser
		if userErr != nil {
			listing.OwnerType = ""
			switch {
			case len(tokenRepos) > 0:
				listing.NamespaceErr = err
			case isNotFound(err) && isNotFound(userErr):
				listing.NotFound = true
				return listing, nil
			case isNotFound(err):
				// The organization route said 404 as it does for every personal account: the failure worth
				// reporting is the user route's.
				return NamespaceListing{}, userErr
			default:
				return NamespaceListing{}, err
			}
		}
	}
	listing.Repos = providerfoundation.UnionByKey(tokenRepos, owned, func(repo Repo) string { return strings.ToLower(repo.FullName) })
	// A cap below one keeps one repository, as ListRepositories does for the same value; a cap past int64 cuts nothing.
	if opts.MaxRepos != nil && opts.MaxRepos.IsInt64() {
		if limit := max(opts.MaxRepos.Int64(), 1); int64(len(listing.Repos)) > limit {
			listing.Repos = listing.Repos[:limit]
		}
	}
	return listing, nil
}

func isNotFound(err error) bool {
	typed, ok := err.(*Error)
	return ok && typed.Class == "NotFoundException"
}
