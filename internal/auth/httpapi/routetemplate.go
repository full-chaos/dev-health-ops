package httpapi

import (
	"fmt"
	"net/http"
	"strings"
)

// RouteTemplates finds, before routing, the "METHOD pattern" key the server's
// mux registers a route under (NewServer: route.Method+" "+route.Pattern), so
// a middleware outside the mux knows the route template a request will reach.
// It matches with net/http's own mux, never by building a string from the
// path.
type RouteTemplates struct {
	mux *http.ServeMux
}

// NewRouteTemplates registers every route's key; a route the server's mux
// would refuse is an error here too.
func NewRouteTemplates(routes []Route) (templates *RouteTemplates, err error) {
	mux := http.NewServeMux()
	defer func() {
		if recovered := recover(); recovered != nil {
			templates, err = nil, fmt.Errorf("route templates: %v", recovered)
		}
	}()
	for _, route := range routes {
		if route.Method == "" || route.Method != strings.ToUpper(route.Method) || !strings.HasPrefix(route.Pattern, "/") {
			return nil, fmt.Errorf("route templates: route %q %q is not a mounted route", route.Method, route.Pattern)
		}
		mux.Handle(route.Method+" "+route.Pattern, routeMatch{})
	}
	return &RouteTemplates{mux: mux}, nil
}

// Match returns the route template r reaches, and false when no route
// matches (a not-found, a 405, or net/http's own redirect).
func (t *RouteTemplates) Match(r *http.Request) (string, bool) {
	handler, pattern := t.mux.Handler(r)
	if _, ok := handler.(routeMatch); !ok {
		return "", false
	}
	return pattern, true
}
