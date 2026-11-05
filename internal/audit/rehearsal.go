package audit

import "context"

type rehearsalKey struct{}

// WithRehearsal marks ctx so that Commit runs every statement of a mutation, its outbox events and its audit rows
// included, and then rolls the transaction back instead of committing it. Commit returns nil and nothing is
// stored. A caller uses it for a decoy that must cost what the real write costs.
func WithRehearsal(ctx context.Context) context.Context {
	return context.WithValue(ctx, rehearsalKey{}, true)
}

// Rehearsing reports whether ctx came from WithRehearsal.
func Rehearsing(ctx context.Context) bool {
	rehearsal, _ := ctx.Value(rehearsalKey{}).(bool)
	return rehearsal
}
