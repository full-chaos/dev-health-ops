package providerfoundation

import "net/http"

const ReasonGraphQLErrors = "graphql_errors"

func ResponseGraphQL(response *http.Response, sentinel error) error {
	return sentinel
}

func GraphQLShapeFailure(route string, sentinel error) error {
	return sentinel
}
