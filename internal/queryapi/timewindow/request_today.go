package timewindow

import (
	"context"
	"time"

	"github.com/99designs/gqlgen/graphql"
)

// RequestToday is the one clock reading of a GraphQL request: the instant gqlgen began the operation, so every field of
// one request clips its window against the same day, even when the request crosses UTC midnight. Outside an operation
// (a REST route, a test) it is fallback().
func RequestToday(ctx context.Context, fallback func() time.Time) time.Time {
	if graphql.HasOperationContext(ctx) {
		if oc := graphql.GetOperationContext(ctx); oc != nil && !oc.Stats.OperationStart.IsZero() {
			return oc.Stats.OperationStart.UTC()
		}
	}
	return fallback()
}
