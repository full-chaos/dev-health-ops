package teamsidentity

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubTeamListEntry is one element of GET /orgs/{org}/teams's response
// (github.com/get_teams -> PyGithub's org.get_teams()). id/slug/name/
// description are the fields every version of GitHub's team-list schema
// carries; member_count/repos_count are NOT read from this list response
// -- see the doc comment on discoverGitHub for why.
type githubTeamListEntry struct {
	Slug        string  `json:"slug"`
	Name        string  `json:"name"`
	Description *string `json:"description"`
}

// githubTeamDetail is GET /orgs/{org}/teams/{slug}'s response -- the
// fields GitHub's "Get a team by name" endpoint documents on its own team
// object, distinct from (and richer than) the list entry above.
type githubTeamDetail struct {
	MembersCount *int64 `json:"members_count"`
}

type githubRepoListEntry struct {
	Name string `json:"name"`
}

// discoverGitHub mirrors TeamDiscoveryService.discover_github
// (team_discovery.py:127-158): org.get_teams() (paginated, every page, no
// cap), then per team org.get_teams()[i].get_repos() (also paginated,
// every page) for repo_patterns, and gh_team.members_count for
// member_count.
//
// SETTLED (testdata/github_discover_fixtures, captured live 2026-09-23 via
// `gh api /orgs/full-chaos/teams` and `gh api /orgs/full-chaos/teams/<slug>`,
// sanitized before commit): GET /orgs/{org}/teams's own list response does
// NOT carry members_count/repos_count -- only GET /orgs/{org}/teams/{slug}
// (the per-team detail endpoint) does, which is what PyGithub's
// gh_team.members_count lazily fetches under the hood. The per-team detail
// call below (githubTeamDetailFor) is therefore REQUIRED, not merely a safe
// extra call -- there is no cheaper way to populate member_count.
//
// per_page=100 on both paginated list calls below matches
// team_discovery.py:135's `Github(auth=auth, per_page=100)` -- PyGithub
// applies that page size to every paginated request the client makes.
// GitHub's REST API default (when per_page is omitted, as this call used
// to do) is 30, not 100 -- caught live by CHAOS-6311's venue oracle
// (venue_oracle_github_test.go), which runs this function and the real
// Python route against the SAME stub server and found their request
// query strings diverging on exactly this parameter.
func discoverGitHub(ctx context.Context, credential providerfoundation.Credential, orgName string) ([]discoveredTeam, error) {
	client, err := providerfoundation.NewGitHubClient(credential, discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{})
	if err != nil {
		return nil, err
	}
	entries, err := githubListAllPages(ctx, client, "org teams", orgName, "/orgs/{org}/teams", "/orgs/"+orgName+"/teams?per_page=100", func(entry githubTeamListEntry) string { return entry.Slug })
	if err != nil {
		return nil, err
	}
	teams := make([]discoveredTeam, 0, len(entries))
	for _, entry := range entries {
		repoEntries, err := githubListAllPages(ctx, client, "team repos", orgName+"/"+entry.Slug, "/orgs/{org}/teams/{slug}/repos", "/orgs/"+orgName+"/teams/"+entry.Slug+"/repos?per_page=100", func(repo githubRepoListEntry) string { return repo.Name })
		if err != nil {
			return nil, err
		}
		repoPatterns := make([]string, len(repoEntries))
		for index, repo := range repoEntries {
			repoPatterns[index] = orgName + "/" + repo.Name
		}
		detail, err := githubTeamDetailFor(ctx, client, orgName, entry.Slug)
		if err != nil {
			return nil, err
		}
		associations := pyjson.NewObject()
		associations.Set("repo_patterns", repoPatterns)
		associations.Set("provider_org", orgName)
		teams = append(teams, discoveredTeam{
			ProviderType:   "github",
			ProviderTeamID: entry.Slug,
			Name:           entry.Name,
			Description:    entry.Description,
			MemberCount:    detail.MembersCount,
			Associations:   associations,
		})
	}
	return teams, nil
}

func githubTeamDetailFor(ctx context.Context, client *providerfoundation.HTTPClient, orgName, slug string) (githubTeamDetail, error) {
	response, err := client.DoRoute(ctx, "GET", "/orgs/{org}/teams/{slug}", "/orgs/"+orgName+"/teams/"+slug, nil)
	if err != nil {
		return githubTeamDetail{}, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return githubTeamDetail{}, err
	}
	var detail githubTeamDetail
	if err := json.Unmarshal(raw, &detail); err != nil {
		return githubTeamDetail{}, fmt.Errorf("decode github team detail response: %w", err)
	}
	return detail, nil
}

// githubListAllPages walks GitHub's Link-header pagination (rel="next")
// for any GET endpoint returning a bare JSON array, matching PyGithub's
// own PaginatedList behavior of walking every page with no upper bound
// for org.get_teams()/team.get_repos().
func githubListAllPages[T any](ctx context.Context, client *providerfoundation.HTTPClient, walkName, owner, route, path string, key func(T) string) ([]T, error) {
	var out []T
	seenItems := map[string]struct{}{}
	walk := newPageWalk(ctx, "github", walkName, owner, path)
	next := path
	for next != "" {
		response, err := client.DoRoute(ctx, "GET", route, next, nil)
		if err != nil {
			return nil, walk.pageError(err)
		}
		raw, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, walk.pageError(readErr)
		}
		var page []T
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, walk.pageError(fmt.Errorf("decode github list page: %w", err))
		}
		out = appendNew(out, seenItems, page, key)
		link, advertised := githubNextLinkPresent(response.Header.Get("Link"))
		var more bool
		if next, more, err = walk.advance(link, advertised, len(page)); err != nil {
			return nil, err
		} else if !more {
			next = ""
		}
	}
	return out, nil
}

// githubNextLink extracts the rel="next" URL from a GitHub Link response
// header (RFC 8288: `<url>; rel="next", <url>; rel="last"`), returning ""
// when there is no next page.
func githubNextLink(header string) string {
	next, _ := githubNextLinkPresent(header)
	return next
}

// isNextRelParam reads one Link parameter as Python's _parse_link_header_next does (trim, lower-case, exact rel="next"), plus
// spaces around "=" and the unquoted rel=next this walk already accepted. A rel list and single quotes stay "not next", as in Python.
func isNextRelParam(param string) bool {
	key, value, found := strings.Cut(strings.ToLower(strings.TrimSpace(param)), "=")
	if !found {
		return false
	}
	value = strings.TrimSpace(value)
	return strings.TrimSpace(key) == "rel" && (value == `"next"` || value == "next")
}

// githubNextLinkPresent is githubNextLink plus whether a rel="next" entry exists at all: `<>; rel="next"` advertises a next page
// with no URL, which a walk must fail on instead of reading as the end.
func githubNextLinkPresent(header string) (string, bool) {
	for _, part := range strings.Split(header, ",") {
		segments := strings.Split(part, ";")
		for index, param := range segments {
			if !isNextRelParam(param) {
				continue
			}
			// a bare rel="next" (no <url> segment before it) still advertises a next page, with no URL
			if index == 0 {
				return "", true
			}
			url := strings.TrimSpace(segments[0])
			url = strings.TrimPrefix(url, "<")
			url = strings.TrimSuffix(url, ">")
			return url, true
		}
	}
	return "", false
}
