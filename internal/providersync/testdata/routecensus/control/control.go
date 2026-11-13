// Package routecensuscontrol is the route census control fixture
// (CHAOS-10268). Every route value below is static under the 9801 ruling: a
// string literal, a constant named *Route, or a concatenation of those with
// static suffix variables. The census must accept all of them.
package routecensuscontrol

import (
	"context"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const fixtureRepoRoute = "/repos/{owner}/{repo}"

type endpoint struct {
	route string
	path  string
}

func closeBody(response *http.Response, err error) error {
	if err != nil {
		return err
	}
	return response.Body.Close()
}

// FetchLiteral sends a string literal route.
func FetchLiteral(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, "/users/{login}", path, nil))
}

// FetchConst sends a constant route and a page option with a constant route.
func FetchConst(ctx context.Context, client *providerfoundation.HTTPClient, path string) (providerfoundation.GitHubPageOptions, error) {
	return providerfoundation.GitHubPageOptions{Path: path, Route: fixtureRepoRoute + "/branches"},
		closeBody(client.DoRoute(ctx, http.MethodGet, fixtureRepoRoute, path, nil))
}

// FetchSuffix sends a constant route with a static suffix variable.
func FetchSuffix(ctx context.Context, client *providerfoundation.HTTPClient, path string, comments bool) error {
	suffix := "/events"
	if comments {
		suffix = "/comments"
	}
	return closeBody(client.DoRoute(ctx, http.MethodGet, fixtureRepoRoute+"/issues/{number}"+suffix, path, nil))
}

// FetchRoutes sends routes from a local slice of static routes.
func FetchRoutes(ctx context.Context, client *providerfoundation.HTTPClient, paths [2]string) error {
	routes := []string{fixtureRepoRoute + "/notes", fixtureRepoRoute + "/events"}
	for index := range routes {
		if err := closeBody(client.DoRoute(ctx, http.MethodGet, routes[index], paths[index], nil)); err != nil {
			return err
		}
	}
	return nil
}

// fetchRoute passes its route parameter through to DoRoute.
func fetchRoute(ctx context.Context, client *providerfoundation.HTTPClient, route, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, route, path, nil))
}

// FetchParameter gives the pass-through helper a constant route.
func FetchParameter(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	return fetchRoute(ctx, client, fixtureRepoRoute+"/pulls", path)
}

// FetchField sends a struct field set from a constant route.
func FetchField(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	target := endpoint{route: fixtureRepoRoute + "/tags", path: path}
	return closeBody(client.DoRoute(ctx, http.MethodGet, target.route, target.path, nil))
}
