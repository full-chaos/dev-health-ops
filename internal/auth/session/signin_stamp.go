package session

import "context"

type signInKey struct{}

// WithSignIn marks ctx as a sign-in's: CreateAudited then stamps the user's
// last login (see Store.stampSignIn). Other session creators never stamp.
func WithSignIn(ctx context.Context) context.Context {
	return context.WithValue(ctx, signInKey{}, true)
}

// IsSignIn reports whether ctx came through WithSignIn.
func IsSignIn(ctx context.Context) bool {
	on, _ := ctx.Value(signInKey{}).(bool)
	return on
}
