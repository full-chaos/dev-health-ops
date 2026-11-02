package server

// routeActions maps every protected query-api route to the catalog action a
// decision request for it names. The key is the profile row's "METHOD route
// @listeners", as endpoint-profiles.query-api.json spells its id.
var routeActions = map[string]string{}

// RouteActionTable returns a copy of the route action table.
func RouteActionTable() map[string]string {
	out := make(map[string]string, len(routeActions))
	for key, action := range routeActions {
		out[key] = action
	}
	return out
}
