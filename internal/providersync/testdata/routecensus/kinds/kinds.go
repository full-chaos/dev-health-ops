// Package routecensuskinds is a route census fixture (CHAOS-10268): one
// census site per source kind the census cannot prove static. Each site
// line carries a want comment with a word of the expected finding.
package routecensuskinds

import (
	"context"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const kindsRoute = "/repos/{owner}/{repo}"

var mutableRoute = "/orgs/{org}"

func closeBody(response *http.Response, err error) error {
	if err != nil {
		return err
	}
	return response.Body.Close()
}

func routeFor(path string) string { return kindsRoute + path }

// FetchCallResult sends a route that a call returns.
func FetchCallResult(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, routeFor("/x"), path, nil)) // want "a call result"
}

func fetchByValue(ctx context.Context, client *providerfoundation.HTTPClient, route, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, route, path, nil)) // want "a call through a func value"
}

// FetchFuncValue calls a pass-through helper through a func value.
func FetchFuncValue(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	fetch := fetchByValue
	return fetch(ctx, client, kindsRoute, path)
}

type router interface{ Route() string }

// FetchInterfaceResult sends a route that an interface method returns.
func FetchInterfaceResult(ctx context.Context, client *providerfoundation.HTTPClient, r router, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, r.Route(), path, nil)) // want "a call result"
}

type methodFetcher struct {
	client *providerfoundation.HTTPClient
}

func (f methodFetcher) fetch(ctx context.Context, route, path string) error {
	return closeBody(f.client.DoRoute(ctx, http.MethodGet, route, path, nil)) // want "a call through an interface cannot be ruled out"
}

// FetchMethod calls a pass-through method.
func FetchMethod(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	return methodFetcher{client: client}.fetch(ctx, kindsRoute, path)
}

// FetchMapRead sends a route read from a map.
func FetchMapRead(ctx context.Context, client *providerfoundation.HTTPClient, key, path string) error {
	routes := map[string]string{"repo": kindsRoute}
	return closeBody(client.DoRoute(ctx, http.MethodGet, routes[key], path, nil)) // want "a map read"
}

// FetchSliceParameter sends a route read from a slice parameter.
func FetchSliceParameter(ctx context.Context, client *providerfoundation.HTTPClient, routes []string, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, routes[0], path, nil)) // want "slice parameter"
}

// FetchAppendedSlice sends a route from a local slice that grows by append.
func FetchAppendedSlice(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	routes := []string{kindsRoute}
	routes = append(routes, path)
	return closeBody(client.DoRoute(ctx, http.MethodGet, routes[1], path, nil)) // want "outside an index"
}

// FetchChannel sends a route received from a channel.
func FetchChannel(ctx context.Context, client *providerfoundation.HTTPClient, routes chan string, path string) error {
	route := <-routes
	return closeBody(client.DoRoute(ctx, http.MethodGet, route, path, nil)) // want "a channel receive"
}

type setEndpoint struct{ route string }

// FetchFieldSet sends a field that code sets outside a composite literal.
func FetchFieldSet(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	target := setEndpoint{route: kindsRoute}
	target.route = path
	return closeBody(client.DoRoute(ctx, http.MethodGet, target.route, path, nil)) // want "field set outside a composite literal"
}

// ExportedEndpoint has an exported route field: a decoder can set it.
type ExportedEndpoint struct{ Route string }

// FetchExportedField sends an exported field.
func FetchExportedField(ctx context.Context, client *providerfoundation.HTTPClient, target ExportedEndpoint, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, target.Route, path, nil)) // want "exported field"
}

// FetchClosure sends the parameter of a func literal.
func FetchClosure(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	return func(route string) error {
		return closeBody(client.DoRoute(ctx, http.MethodGet, route, path, nil)) // want "func literal"
	}(kindsRoute)
}

func setRoute(route *string, path string) { *route = path }

// FetchAddressTaken sends a local whose address another function writes.
func FetchAddressTaken(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	route := kindsRoute
	setRoute(&route, path)
	return closeBody(client.DoRoute(ctx, http.MethodGet, route, path, nil)) // want "address taken"
}

// SetMutableRoute writes a package var.
func SetMutableRoute(path string) { mutableRoute = path }

// FetchPackageVar sends a package var that code assigns.
func FetchPackageVar(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, mutableRoute, path, nil)) // want "package var assigned"
}

// FetchUncalled passes its route parameter on; no loaded package calls it.
func FetchUncalled(ctx context.Context, client *providerfoundation.HTTPClient, route, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, route, path, nil)) // want "no call site"
}

func lookupRoute(path string) (string, bool) { return kindsRoute + path, path != "" }

// FetchMultiValue sends a local set from a multi-value call.
func FetchMultiValue(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	route, ok := lookupRoute(path)
	if !ok {
		return nil
	}
	return closeBody(client.DoRoute(ctx, http.MethodGet, route, path, nil)) // want "multi-value assignment"
}

// FetchForeignField sends a field of a type from another package.
func FetchForeignField(ctx context.Context, client *providerfoundation.HTTPClient, request *http.Request) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, request.URL.Path, request.URL.Path, nil)) // want "another package"
}

// OptionsSetOutside sets an option Route from a request value outside the
// literal.
func OptionsSetOutside(path string) providerfoundation.GitHubPageOptions {
	options := providerfoundation.GitHubPageOptions{Path: path, Route: kindsRoute}
	options.Route = path // want "Route set: cannot prove static"
	return options
}

// OptionsWithoutRoute builds an option without a Route.
func OptionsWithoutRoute(path string) providerfoundation.GitHubPageOptions {
	return providerfoundation.GitHubPageOptions{Path: path} // want "without a Route"
}

// FetchUntemplated sends requests with no route template.
func FetchUntemplated(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	if err := closeBody(client.Do(ctx, http.MethodGet, path, nil)); err != nil { // want "un-templated client request"
		return err
	}
	return closeBody(client.DoUnauthenticated(ctx, http.MethodGet, path)) // want "un-templated redirect request"
}

// FailureWithPath names a request path as the route of a failure.
func FailureWithPath(path string) error {
	return &providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, Route: path} // want "ProviderError Route: cannot prove static"
}
