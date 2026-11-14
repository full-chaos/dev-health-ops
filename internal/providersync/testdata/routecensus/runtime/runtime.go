// Package routecensusruntime is a route census fixture (CHAOS-10268). Each
// route value below comes from the request at run time. The names look like
// route names, so a census that reads names only accepts them all.
package routecensusruntime

import (
	"context"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type endpoint struct {
	route string
	path  string
}

// FetchLocal sends a local named route that the request set, and builds a
// page option with it.
func FetchLocal(ctx context.Context, client *providerfoundation.HTTPClient, request *http.Request) (providerfoundation.GitHubPageOptions, error) {
	route := request.URL.Query().Get("route")
	options := providerfoundation.GitHubPageOptions{Path: request.URL.Path, Route: route}
	response, err := client.DoRoute(ctx, http.MethodGet, route, request.URL.Path, nil)
	if err != nil {
		return options, err
	}
	return options, response.Body.Close()
}

// fetchRoute passes its route parameter through to DoRoute.
func fetchRoute(ctx context.Context, client *providerfoundation.HTTPClient, route, path string) error {
	response, err := client.DoRoute(ctx, http.MethodGet, route, path, nil)
	if err != nil {
		return err
	}
	return response.Body.Close()
}

// FetchParameter gives the pass-through helper a route from the request.
func FetchParameter(ctx context.Context, client *providerfoundation.HTTPClient, request *http.Request) error {
	return fetchRoute(ctx, client, request.URL.Path, request.URL.Path)
}

// FetchField sends a struct field named route that the request set.
func FetchField(ctx context.Context, client *providerfoundation.HTTPClient, request *http.Request) error {
	target := endpoint{route: request.Header.Get("X-Route"), path: request.URL.Path}
	response, err := client.DoRoute(ctx, http.MethodGet, target.route, target.path, nil)
	if err != nil {
		return err
	}
	return response.Body.Close()
}
