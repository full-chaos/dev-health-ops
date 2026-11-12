package providerfoundation

import "net/http"

const ReasonGraphQLErrors = "graphql_errors"

// ResponseGraphQL is the failure for a GraphQL `errors` array in a 2xx body.
// It names the route of response and keeps sentinel as the cause, so errors.Is
// still matches ErrGraphQLResponse, ErrGraphQLComplexity or the caller's own.
func ResponseGraphQL(response *http.Response, sentinel error) error {
	return ResponseFailure(response, ReasonGraphQLErrors, sentinel)
}

// GraphQLShapeFailure is the same for a caller that no longer holds the
// response: route is the static route template of the GraphQL request.
func GraphQLShapeFailure(route string, sentinel error) error {
	return &ProviderError{Class: ErrorPermanent, Route: route, Reason: ReasonBodyShape, cause: sentinel}
}
