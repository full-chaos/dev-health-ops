// Package routecensuskinds is a route census fixture (CHAOS-10268): one
// census site per source kind the census cannot prove static. Each site
// line carries a want comment with a word of the expected finding.
package routecensuskinds

import (
	"context"
	"io"
	"net/http"
	"strings"

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

// anyClient has a DoRoute whose route is any. The census finds DoRoute by
// name, so this reaches the reject branches a string route cannot reach.
type anyClient struct{}

func (anyClient) DoRoute(ctx context.Context, method string, route any, path string, body io.Reader) {
}

// shortClient has a DoRoute with too few arguments for a route.
type shortClient struct{}

func (shortClient) DoRoute(ctx context.Context) {}

// AnyKinds sends values that are not string routes.
func AnyKinds(ctx context.Context, client *providerfoundation.HTTPClient, size int, path string) {
	anyClient{}.DoRoute(ctx, http.MethodGet, client.DoRoute, path, nil)                      // want "a method value"
	anyClient{}.DoRoute(ctx, http.MethodGet, 7, path, nil)                                   // want "a constant that is not a string"
	anyClient{}.DoRoute(ctx, http.MethodGet, size*2, path, nil)                              // want "operator *"
	anyClient{}.DoRoute(ctx, http.MethodGet, path[0], path, nil)                             // want "an index read"
	anyClient{}.DoRoute(ctx, http.MethodGet, routeFor, path, nil)                            // want "not a value"
	anyClient{}.DoRoute(ctx, http.MethodGet, func() string { return kindsRoute }, path, nil) // want "an unknown expression kind *ast.FuncLit"
	anyClient{}.DoRoute(ctx, http.MethodGet, http.DefaultClient, path, nil)                  // want "without a declaration in the loaded packages"
	shortClient{}.DoRoute(ctx)                                                               // want "DoRoute with 1 arguments"
}

func fetchVariadic(ctx context.Context, route ...string) {
	anyClient{}.DoRoute(ctx, http.MethodGet, route, "", nil) // want "variadic parameter"
}

// FetchVariadic calls a helper whose route is variadic.
func FetchVariadic(ctx context.Context) { fetchVariadic(ctx, kindsRoute) }

// FetchPointer sends a route read through a pointer.
func FetchPointer(ctx context.Context, client *providerfoundation.HTTPClient, route *string, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, *route, path, nil)) // want "a pointer read"
}

// FetchTypeAssertion sends a route from a type assertion.
func FetchTypeAssertion(ctx context.Context, client *providerfoundation.HTTPClient, value any, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, value.(string), path, nil)) // want "a type assertion"
}

// FetchSliceExpression sends a slice of a string.
func FetchSliceExpression(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, path[1:], path, nil)) // want "an unknown expression kind *ast.SliceExpr"
}

// FetchTypeSwitch sends a type switch value: the census reads no assignment.
func FetchTypeSwitch(ctx context.Context, client *providerfoundation.HTTPClient, value any, path string) error {
	switch v := value.(type) {
	case string:
		return closeBody(client.DoRoute(ctx, http.MethodGet, v, path, nil)) // want "v has no assignment the census can read"
	case []string:
		return closeBody(client.DoRoute(ctx, http.MethodGet, v[0], path, nil)) // want "slice v has no assignment the census can read"
	}
	return nil
}

// FetchMapRange sends a value of a map range.
func FetchMapRange(ctx context.Context, client *providerfoundation.HTTPClient, routes map[string]string, path string) error {
	for _, route := range routes {
		return closeBody(client.DoRoute(ctx, http.MethodGet, route, path, nil)) // want "a range over a value that is not a slice of routes"
	}
	return nil
}

// FetchRangeKey sends a key of a map range.
func FetchRangeKey(ctx context.Context, client *providerfoundation.HTTPClient, routes map[string]bool, path string) error {
	for route := range routes {
		return closeBody(client.DoRoute(ctx, http.MethodGet, route, path, nil)) // want "range key"
	}
	return nil
}

// FetchMultiValueDeclaration sends a var declared from a multi-value call.
func FetchMultiValueDeclaration(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	var route, ok = lookupRoute(path)
	if !ok {
		return nil
	}
	return closeBody(client.DoRoute(ctx, http.MethodGet, route, path, nil)) // want "multi-value declaration"
}

// FetchElementAssigned sends an element of a local slice that code writes.
func FetchElementAssigned(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	routes := []string{kindsRoute}
	routes[0] = path
	return closeBody(client.DoRoute(ctx, http.MethodGet, routes[0], path, nil)) // want "element assigned"
}

type addressEndpoint struct{ route string }

// FetchFieldAddress sends a field whose address another function writes.
func FetchFieldAddress(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	target := addressEndpoint{route: kindsRoute}
	setRoute(&target.route, path)
	return closeBody(client.DoRoute(ctx, http.MethodGet, target.route, path, nil)) // want "field address taken"
}

// FetchSplitSlice sends an element of a slice a call returns.
func FetchSplitSlice(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	routes := strings.Split(path, ",")
	return closeBody(client.DoRoute(ctx, http.MethodGet, routes[0], path, nil)) // want "is set from a value that is not a literal"
}

func splitRoutes(path string) ([]string, bool) { return strings.Split(path, ","), path != "" }

// FetchMultiValueSlice sends an element of a slice from a multi-value call.
func FetchMultiValueSlice(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	routes, ok := splitRoutes(path)
	if !ok {
		return nil
	}
	return closeBody(client.DoRoute(ctx, http.MethodGet, routes[0], path, nil)) // want "slice routes: multi-value assignment"
}

var kindsRoutes = []string{kindsRoute}

// FetchPackageSlice sends an element of a package slice.
func FetchPackageSlice(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, kindsRoutes[0], path, nil)) // want "a read of a package slice"
}

type sliceEndpoint struct{ routes []string }

// FetchFieldSlice sends an element of a field slice.
func FetchFieldSlice(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	target := sliceEndpoint{routes: []string{kindsRoute}}
	return closeBody(client.DoRoute(ctx, http.MethodGet, target.routes[0], path, nil)) // want "a slice read of an expression"
}

func fetchSpread(ctx context.Context, client *providerfoundation.HTTPClient, route string, extra ...string) error {
	return closeBody(client.DoRoute(ctx, http.MethodGet, route, strings.Join(extra, "/"), nil)) // want "a call with a spread argument"
}

// FetchSpread calls a pass-through helper with a spread argument.
func FetchSpread(ctx context.Context, client *providerfoundation.HTTPClient) error {
	extras := []string{"a"}
	return fetchSpread(ctx, client, kindsRoute, extras...)
}

func fetchReassigned(ctx context.Context, client *providerfoundation.HTTPClient, route, path string) error {
	route = routeFor(path)
	return closeBody(client.DoRoute(ctx, http.MethodGet, route, path, nil)) // want "parameter route assigned in its function"
}

// FetchReassigned gives a helper a constant route that the helper replaces.
func FetchReassigned(ctx context.Context, client *providerfoundation.HTTPClient, path string) error {
	return fetchReassigned(ctx, client, kindsRoute, path)
}

// OptionsAppend appends to an option Route.
func OptionsAppend(path string) providerfoundation.GitHubPageOptions {
	options := providerfoundation.GitHubPageOptions{Path: path, Route: kindsRoute}
	options.Route += "/x" // want "Route set by +="
	return options
}

func fetchObject(path string) error { return nil }

// FetchObject calls the un-templated object fetch.
func FetchObject(path string) error {
	return fetchObject(path) // want "un-templated object fetch"
}
