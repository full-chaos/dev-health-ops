package apiservice

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// retiredBrowseNames are the owner requests whose Python answer the ruling retires: the 35 the frozen reference cannot
// express (the union adds the token's repositories, a token listing that 404s is a credential fault, the owner's login
// is redacted from the failure text, and the failing call named is /user/repos). Every other request, search with an
// owner and every non-owner request included, is still compared byte for byte.
var retiredBrowseNames = map[string]bool{
	"github repos: org max_repos=1":                           true,
	"github repos: org max_repos=2":                           true,
	"github repos: org max_repos=3":                           true,
	"github repos: org max_repos=4":                           true,
	"github repos: org max_repos=0":                           true,
	"github repos: org max_repos=-1":                          true,
	"github repos: org max_repos=-3":                          true,
	"github repos: org max_repos=100000":                      true,
	"github repos: org max_repos=99999999999999999999999999":  true,
	"github repos: org max_repos=-99999999999999999999999999": true,
	"github repos: credential orgcfg with an owner":           true,
	"github repos: credential orgdec with an owner":           true,
	"github repos: credential orgboth with an owner":          true,
	"github repos: credential orgnonstr with an owner":        true,
	"github repos: credential orgemptycfg with an owner":      true,
	"github repos: credential orgnonstrboth with an owner":    true,
	"github repos: credential ghe with an owner":              true,
	"github repos: credential gheorg with an owner":           true,
	"github repos: credential cfgbase with an owner":          true,
	"github repos: credential emptybase with an owner":        true,
	"github repos: credential nullbase with an owner":         true,
	"github repos: credential aliasbase with an owner":        true,
	"github repos: credential bothbase with an owner":         true,
	"github repos: credential aliasandcfg with an owner":      true,
	"github repos: credential basequery with an owner":        true,
	"github repos: credential basefrag with an owner":         true,
	"github repos: credential basequeryfrag with an owner":    true,
	"github repos: provider answer denied for an org":         true,
	"github repos: provider answer forbidden for an org":      true,
	"github repos: provider answer limited for an org":        true,
	"github repos: provider answer toomany for an org":        true,
	"github repos: provider answer notfound for an org":       true,
	"github repos: provider answer teapot for an org":         true,
	"github repos: provider answer dict for an org":           true,
	"github repos: app appdenied with an owner":               true,
}

func retiredBrowseRequest(request venueoracle.Request) bool { return retiredBrowseNames[request.Name] }

const browseBaseURLText = "github token repository listing was not found; check the credential's base URL"

var browseCallPath = regexp.MustCompile(`^GET (/[^?\s]*)`)

var browseRouteMarkers = []string{"/app/installations/", "/installation/repositories", "/search/repositories", "/orgs/", "/users/", "/user/repos"}

// callRoute is the route of a recorded call, past a GitHub Enterprise base prefix; routeless is set when the call shows
// no route at all (a base URL holding a query or a fragment folds the route into the query).
func callRoute(call string) (route string, routeless bool) {
	match := browseCallPath.FindStringSubmatch(call)
	if match == nil {
		return call, true
	}
	best := -1
	for _, marker := range browseRouteMarkers {
		if index := strings.Index(match[1], marker); index >= 0 && (best < 0 || index < best) {
			best = index
		}
	}
	if best < 0 {
		return match[1], true
	}
	return match[1][best:], false
}

func callPath(call string) string {
	route, _ := callRoute(call)
	return route
}

// userReposCall reports a call to the token's own listing: the route /user/repos, or a call that shows no route at all.
func userReposCall(call string) bool {
	if !strings.HasPrefix(call, "GET ") {
		return false
	}
	route, routeless := callRoute(call)
	return routeless || route == "/user/repos"
}

// browseRequestNamesOwner reports whether the request's query carries a non-empty owner.
func browseRequestNamesOwner(request venueoracle.Request) bool {
	_, rawQuery, _ := strings.Cut(request.Path, "?")
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return false
	}
	for _, owner := range query["owner"] {
		if owner != "" {
			return true
		}
	}
	return false
}

// alignBrowseProviderCalls holds the provider traffic of the credential repos oracle's Go answers to Python's flat
// sequence of calls, request by request. Go's calls for one request are tied to that request: set aside the two kinds
// of call the ruling adds (every /user/repos page before the owner's /orgs listing, and the /users/{owner}/repos
// fallback after it) and what remains is the number of calls Python made for it, in order. The calls of a request that
// is not retired must equal Python's text for text; a retired request, and a request that failed on the token's own
// listing before the owner's (whose Python call went to the owner), has its Python calls counted but not compared. A
// request can therefore never absorb a neighbour's call: the counts must add up to Python's whole sequence.
func alignBrowseProviderCalls(requests []venueoracle.Request, goCalls map[string][]string, goStatus map[string]int, python []string) (bool, string) {
	cursor := 0
	for _, request := range requests {
		calls := goCalls[request.Name]
		firstOrg := -1
		for position, call := range calls {
			if strings.HasPrefix(callPath(call), "/orgs/") {
				firstOrg = position
				break
			}
		}
		var expected []string
		onlyTokenListing := len(calls) > 0 && goStatus[request.Name] >= 400 && browseRequestNamesOwner(request)
		for position, call := range calls {
			path := callPath(call)
			if path != "/user/repos" {
				onlyTokenListing = false
			}
			if path == "/user/repos" && firstOrg > position {
				continue
			}
			if strings.HasPrefix(path, "/users/") && strings.HasSuffix(path, "/repos") && firstOrg >= 0 && firstOrg < position {
				continue
			}
			expected = append(expected, call)
		}
		if cursor+len(expected) > len(python) {
			return false, request.Name + " expects " + strings.Join(expected, " | ") + " but Python's sequence has only " + strconv.Itoa(len(python)-cursor) + " calls left"
		}
		got := python[cursor : cursor+len(expected)]
		if !retiredBrowseRequest(request) && !onlyTokenListing && strings.Join(got, "\n") != strings.Join(expected, "\n") {
			return false, request.Name + " expects " + strings.Join(expected, " | ") + " but Python's calls at that position are " + strings.Join(got, " | ")
		}
		cursor += len(expected)
	}
	if cursor != len(python) {
		return false, "Python's sequence has " + strconv.Itoa(len(python)-cursor) + " calls no request accounts for"
	}
	return true, ""
}
