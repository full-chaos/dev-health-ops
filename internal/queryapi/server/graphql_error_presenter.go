package server

import (
	"context"
	"errors"
	"log"
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const graphQLInternalMessage = "Internal Server Error"

// generatedIntrospectionRefusal is the one plain error the generated executor
// raises for a client-built document; it carries no internal text.
const generatedIntrospectionRefusal = "introspection disabled"

// graphQLRequestID is the correlation id of the request that raised an error:
// the one the request-id middleware bound to the context, else the inbound
// header gqlgen kept on the operation context.
func graphQLRequestID(ctx context.Context) string {
	if id := httpapi.RequestIDFrom(ctx); id != "" {
		return id
	}
	if graphql.HasOperationContext(ctx) {
		if headers := graphql.GetOperationContext(ctx).Headers; headers != nil {
			return headers.Get(httpapi.RequestIDHeader)
		}
	}
	return ""
}

// safeGraphQLErrorPresenter decides which error text a GraphQL client sees.
// Only an error authored for the client travels: a cause-free *gqlerror.Error
// (the resolvers' and guards' own refusals, gqlgen's validation) or a
// *policy.ClientError. Any other resolver error (a store or provider failure
// wrapped with fmt.Errorf, a driver error, a panic) is presented as a fixed
// message with the request id, and its whole cause chain goes to the log.
func safeGraphQLErrorPresenter(label string, observe func(context.Context)) graphql.ErrorPresenterFunc {
	return func(ctx context.Context, err error) *gqlerror.Error {
		if observe != nil {
			observe(ctx)
		}
		chain := []string{err.Error()}
		for unwrapped := errors.Unwrap(err); unwrapped != nil; unwrapped = errors.Unwrap(unwrapped) {
			chain = append(chain, unwrapped.Error())
		}
		requestID := graphQLRequestID(ctx)
		presented := graphql.DefaultErrorPresenter(ctx, err)
		var client *policy.ClientError
		switch {
		case authoredGraphQLError(err):
		case errors.As(err, &client) && client.Message != "":
			presented.Message = client.Message
		default:
			presented.Message = graphQLInternalMessage
			presented.Extensions = withRequestID(presented.Extensions, requestID)
			log.Printf("query-api: %s resolver error masked from the client: request_id=%s unwrap chain: %s",
				label, requestID, truncateForLog(strings.Join(chain, " <- "), maxUnwrapChainLogBytes))
			return withMutationLocation(ctx, presented)
		}
		log.Printf("query-api: %s resolver error unwrap chain: request_id=%s %s",
			label, requestID, truncateForLog(strings.Join(chain, " <- "), maxUnwrapChainLogBytes))
		return withMutationLocation(ctx, presented)
	}
}

// authoredGraphQLError reports whether err holds a *gqlerror.Error that a
// resolver or guard built itself. gqlgen wraps every resolver error in a
// *gqlerror.Error whose Err is that resolver error, so a gqlerror with no
// cause is the only mark of an authored one.
func authoredGraphQLError(err error) bool {
	for link := err; link != nil; link = errors.Unwrap(link) {
		if typed, ok := link.(*gqlerror.Error); ok && typed.Err == nil {
			return true
		}
		if link.Error() == generatedIntrospectionRefusal && errors.Unwrap(link) == nil {
			return true
		}
	}
	return false
}

// withRequestID adds the request id beside the extensions the error already
// carries (code, reason): acr keys on those, so they are never replaced.
func withRequestID(existing map[string]any, requestID string) map[string]any {
	merged := make(map[string]any, len(existing)+1)
	for key, value := range existing {
		merged[key] = value
	}
	merged["request_id"] = requestID
	return merged
}
