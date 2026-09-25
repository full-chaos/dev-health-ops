package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// This file is the repository/project LISTING the `--search` batch mode of
// `dho sync <target>` starts from. It mirrors what the Python batch path
// actually does -- GitHubCodeClient.list_repositories and
// GitLabCodeClient.list_projects, reached through
// processors/github.py _list_github_repositories_for_batch and
// processors/gitlab.py _gitlab_effective_group -- and NOT the scheduler's
// source discovery (internal/scheduler/sync/source_discovery.go), which
// mirrors a different Python function (discovery/repos.py) with different
// rules: it matches the pattern on the repository NAME within an owner and
// falls back from org to user, where the batch path matches the pattern on the
// case-folded FULL name and does not fall back. Both are the contract of their
// own producer; the live oracle in repo_listing_live_python_oracle_test.go pins
// this one to the real Python.

// ListedRepository is one repository (GitHub) or project (GitLab) a listing
// found. FullName is `owner/repo` for GitHub and path_with_namespace for
// GitLab; ProjectID is the numeric GitLab project id (0 for GitHub).
type ListedRepository struct {
	Name      string
	FullName  string
	ProjectID int64
}

// GitHubListing selects what to list.
type GitHubListing struct {
	// Org and User are --group and --owner. When neither is set and Pattern
	// starts with a literal owner (`acme/*`), that owner is listed as a user.
	Org, User string
	// Pattern is an fnmatch pattern matched against the lower-cased full name.
	Pattern string
	// MaxRepos stops the listing after this many matches. nil is no cap; a
	// pointer to 0 (or a negative value) is a cap too, as in Python where
	// `max_repos is not None and len(repos) >= max_repos` is true after the
	// first match.
	MaxRepos *int
}

const (
	repoListingPerPage  = 100
	repoListingMaxPages = 100
)

// effectiveGitHubOwner is _list_github_repositories_for_batch's owner
// extraction: with no org or user given, a pattern of the form `owner/...`
// whose owner part has no glob character names the user to list.
func effectiveGitHubOwner(listing GitHubListing) (org, user string) {
	org, user = listing.Org, listing.User
	if org == "" && user == "" && listing.Pattern != "" && strings.Contains(listing.Pattern, "/") {
		ownerPart, _, _ := strings.Cut(listing.Pattern, "/")
		if ownerPart != "" && !strings.ContainsAny(ownerPart, "*?") {
			user = ownerPart
		}
	}
	return org, user
}

// matchesRepoPattern is _matches_repo_pattern: fnmatchcase on the lower-cased
// full name and pattern; no pattern matches everything.
func matchesRepoPattern(fullName, pattern string) bool {
	if pattern == "" {
		return true
	}
	return pythonparity.FnMatch(pythonparity.Lower(fullName), pythonparity.Lower(pattern))
}

// repoObject decodes one listing item the way Python's json.loads does and
// keeps only mappings (`isinstance(item, Mapping)`).
func repoObject(raw json.RawMessage) (*pyjson.Object, bool) {
	value, err := pyjson.Decode(raw)
	if err != nil {
		return nil, false
	}
	object, ok := value.(*pyjson.Object)
	return object, ok
}

// pyStrOrEmpty is `str(item.get(key) or "")`: a missing, null, false, zero or
// empty value is "", anything else is Python's str() of it.
func pyStrOrEmpty(object *pyjson.Object, key string) string {
	value, present := object.Get(key)
	if !present || !pyjson.Truthy(value) {
		return ""
	}
	return pyjson.Str(value)
}

// clientBasePath is the client's base URL path without a trailing slash: a
// GitLab client's base ends in /api/v4, a GitHub Enterprise one in /api/v3.
func clientBasePath(client *providerfoundation.HTTPClient) string {
	if client == nil || client.BaseURL == nil {
		return ""
	}
	return strings.TrimSuffix(client.BaseURL.EscapedPath(), "/")
}

// ListGitHubRepositories lists repositories the way GitHubCodeClient.
// list_repositories does for a batch: /orgs/{org}/repos, else
// /users/{user}/repos, else /user/repos, 100 per page, at most 100 pages (a
// listing that runs out of pages is truncated with a warning, not an error),
// each item kept when its lower-cased full name matches the pattern, stopping
// at the MaxRepos-th match without requesting another page.
func ListGitHubRepositories(ctx context.Context, client *providerfoundation.HTTPClient, listing GitHubListing) ([]ListedRepository, error) {
	org, user := effectiveGitHubOwner(listing)
	path := clientBasePath(client) + "/user/repos"
	switch {
	case org != "":
		path = clientBasePath(client) + "/orgs/" + pythonparity.Quote(org, "") + "/repos"
	case user != "":
		path = clientBasePath(client) + "/users/" + pythonparity.Quote(user, "") + "/repos"
	}
	matched := 0
	keep := func(raw json.RawMessage) bool {
		item, ok := repoObject(raw)
		if !ok {
			return false // Python skips a non-mapping item
		}
		return matchesRepoPattern(pyStrOrEmpty(item, "full_name"), listing.Pattern)
	}
	options := providerfoundation.GitHubPageOptions{
		Path:     path,
		Query:    url.Values{"per_page": {fmt.Sprint(repoListingPerPage)}},
		MaxPages: repoListingMaxPages,
		Keep:     keep,
	}
	if listing.MaxRepos != nil {
		options.StopAfter = func(json.RawMessage) bool {
			matched++
			return matched >= *listing.MaxRepos
		}
	}
	page, err := providerfoundation.CollectGitHubLinkPages(ctx, client, options)
	if err != nil {
		return nil, err
	}
	if page.PageBudgetExhausted {
		slog.Warn("github repository listing hit the page cap", "pages", repoListingMaxPages, "path", path)
	}
	repos := make([]ListedRepository, 0, len(page.Items))
	for _, raw := range page.Items {
		item, ok := repoObject(raw)
		if !ok {
			continue
		}
		repos = append(repos, ListedRepository{Name: pyStrOrEmpty(item, "name"), FullName: pyStrOrEmpty(item, "full_name")})
	}
	return repos, nil
}

// GitLabListing selects what to list.
type GitLabListing struct {
	// Group is --group; when empty, a pattern of the form `group/...` with a
	// literal group part names it (_gitlab_effective_group).
	Group string
	// Pattern is an fnmatch pattern matched against the lower-cased
	// path_with_namespace.
	Pattern string
	// MaxProjects stops the listing after this many matches. nil is no cap; a
	// pointer to 0 (or a negative value) stops after the first match, as in
	// Python (`max_projects is not None and len(repos) >= max_projects`).
	MaxProjects *int
}

func effectiveGitLabGroup(listing GitLabListing) string {
	if listing.Group != "" || listing.Pattern == "" || !strings.Contains(listing.Pattern, "/") {
		return listing.Group
	}
	prefix, _, _ := strings.Cut(listing.Pattern, "/")
	if prefix != "" && !strings.ContainsAny(prefix, "*?") {
		return prefix
	}
	return ""
}

// ListGitLabProjects lists projects the way GitLabCodeClient.list_projects
// does for a batch: /groups/{group}/projects, else /projects, 100 per page and
// page-number pagination, the whole listing read before the pattern is applied
// (only an uncapped, patternless listing bounds the page count by MaxProjects),
// then each project kept when its lower-cased path_with_namespace matches.
func ListGitLabProjects(ctx context.Context, client *providerfoundation.HTTPClient, listing GitLabListing) ([]ListedRepository, error) {
	path := clientBasePath(client) + "/projects"
	if group := effectiveGitLabGroup(listing); group != "" {
		path = clientBasePath(client) + "/groups/" + pythonparity.Quote(group, "") + "/projects"
	}
	// `1_000_000 if pattern and max_projects is not None else max_projects or
	// 1_000_000`: a pattern reads everything, and a max of 0 is falsy here.
	maxItems := 1_000_000
	if listing.MaxProjects != nil && listing.Pattern == "" && *listing.MaxProjects != 0 {
		maxItems = *listing.MaxProjects
	}
	maxPages := (maxItems + repoListingPerPage - 1) / repoListingPerPage
	if maxPages < 1 {
		maxPages = 1
	}
	page, err := providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
		Path: path, PerPage: repoListingPerPage, MaxPages: maxPages,
	})
	if err != nil {
		return nil, err
	}
	raw := page.Items
	// Python slices `[:max_items]`: a negative value drops that many from the end.
	if maxItems < 0 {
		keep := len(raw) + maxItems
		if keep < 0 {
			keep = 0
		}
		raw = raw[:keep]
	} else if len(raw) > maxItems {
		raw = raw[:maxItems]
	}
	var repos []ListedRepository
	for _, item := range raw {
		project, ok := repoObject(item)
		if !ok {
			continue // a non-dict item is dropped
		}
		id := gitLabCoerceInt(project, "id")
		name := pyStrOrEmpty(project, "name")
		if name == "" {
			name = fmt.Sprint(id)
		}
		fullName := pyStrOrEmpty(project, "path_with_namespace")
		if fullName == "" {
			fullName = pyStrOrEmpty(project, "path")
		}
		if fullName == "" {
			fullName = name
		}
		if listing.Pattern != "" && !matchesRepoPattern(fullName, listing.Pattern) {
			continue
		}
		repos = append(repos, ListedRepository{Name: name, FullName: fullName, ProjectID: id})
		if listing.MaxProjects != nil && len(repos) >= *listing.MaxProjects {
			break
		}
	}
	return repos, nil
}

// gitLabCoerceInt is _coerce_int: a string, int, float or bool becomes int(...)
// of it; anything else, or a value int() refuses, is 0.
func gitLabCoerceInt(object *pyjson.Object, key string) int64 {
	value, present := object.Get(key)
	if !present {
		return 0
	}
	switch typed := value.(type) {
	case string:
		n, err := pythonparity.ParseInt(typed)
		if err != nil || !n.IsInt64() {
			return 0
		}
		return n.Int64()
	case pyjson.Int:
		if typed.IsInt64() {
			return typed.Int64()
		}
	case pyjson.Float:
		f := float64(typed)
		if !math.IsNaN(f) && !math.IsInf(f, 0) && math.Abs(f) < 1<<63 {
			return int64(f)
		}
	case bool:
		if typed {
			return 1
		}
	}
	return 0
}
