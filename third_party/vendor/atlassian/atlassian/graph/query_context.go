package graph

import "context"

// QueryContextHeader is the header the Teamwork Graph team reads must carry:
// without it the gateway answers "Query context must not be null and should be
// a valid platform site or workspace ARI. Please send the required
// X-Query-Context header." (a live run, CHAOS-7132; local modification, patch 0003). Execute sends it when the
// context carries a value (WithQueryContext), so only the calls a caller marks send it.
const QueryContextHeader = "X-Query-Context"

type queryContextKey struct{}

// WithQueryContext returns ctx carrying the value the team reads send as
// X-Query-Context. An empty value sends no header (the client's behaviour
// before this addition).
func WithQueryContext(ctx context.Context, value string) context.Context {
	return context.WithValue(ctx, queryContextKey{}, value)
}

// queryContextHeaders is the extra headers of a request made under ctx: the
// X-Query-Context header when ctx carries a value, else none.
func queryContextHeaders(ctx context.Context) map[string]string {
	if value, _ := ctx.Value(queryContextKey{}).(string); value != "" {
		return map[string]string{QueryContextHeader: value}
	}
	return nil
}
