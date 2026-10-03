package server

import "net/http"

// GraphQLEdgeRoutes is the (method, pattern) set the /graphql product path serves
// (mountGraphQLRoute, graphql_edge_route.go): GET and POST. Any other method is the
// edge's 405 (Allow: GET); PUT and PATCH pass through the request-size limit first, so
// an oversize body is a 413, but a body within the limit still ends in the 405. The set
// is declared here for the route-vs-profile gate (CHAOS-8305) and pinned against the
// edge chain itself by TestGraphQLEdgeServesExactlyTheDeclaredMethods.
func GraphQLEdgeRoutes() []RESTRoute {
	return []RESTRoute{
		{Method: http.MethodGet, Pattern: graphQLEdgePath, Builder: "mountGraphQLRoute", File: "graphql_edge_route.go"},
		{Method: http.MethodPost, Pattern: graphQLEdgePath, Builder: "mountGraphQLRoute", File: "graphql_edge_route.go"},
	}
}
