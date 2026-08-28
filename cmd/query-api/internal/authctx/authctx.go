// Package authctx carries a verified effective-principal envelope
// (cmd/query-api/internal/principal) through a request's context.Context,
// from the HTTP auth middleware (main.go) down to a GraphQL field
// resolver -- the same "auth stays put initially, query-api trusts a
// signed envelope" contract plan §3 describes, applied at the Go
// plumbing layer.
package authctx

import "context"

type contextKey struct{}

var claimsKey = contextKey{}

// Claims is the narrow view of principal.Claims a resolver needs to
// authorize a request -- declared here rather than importing
// principal.Claims directly so this package has no dependency on the
// verifier's JWT/jwx machinery, only on the fields a resolver actually
// checks.
type Claims struct {
	OrgID string
}

// WithClaims returns a context carrying claims for downstream resolvers.
func WithClaims(ctx context.Context, claims Claims) context.Context {
	return context.WithValue(ctx, claimsKey, claims)
}

// FromContext returns the claims WithClaims attached, and whether any
// were present. A resolver reached without an auth middleware ever having
// run (e.g. a unit test that calls the resolver directly) gets ok=false --
// the safe default is "no principal", never a zero-value OrgID treated as
// a real, empty-string org.
func FromContext(ctx context.Context) (Claims, bool) {
	claims, ok := ctx.Value(claimsKey).(Claims)
	return claims, ok
}
