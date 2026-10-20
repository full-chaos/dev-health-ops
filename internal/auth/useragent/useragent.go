// Package useragent is the user agent of one auth-service request: the first
// User-Agent value, each byte decoded as one latin-1 code point, as go-api's
// refresh-token rows record it (internal/api/session/tokens.go userAgent). It
// is kept whole: net/http's header cap (MaxHeaderBytes, 1 MiB by default) is
// the only bound, as in go-api.
package useragent

import (
	"context"
	"net/http"
	"strings"
)

type key struct{}

// Middleware binds the request's user agent, when it sends one.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("User-Agent")
		if len(values) == 0 {
			next.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r.WithContext(With(r.Context(), latin1(values[0]))))
	})
}

func latin1(raw string) string {
	var builder strings.Builder
	builder.Grow(len(raw))
	for index := 0; index < len(raw); index++ {
		builder.WriteRune(rune(raw[index]))
	}
	return builder.String()
}

// With binds agent to ctx.
func With(ctx context.Context, agent string) context.Context {
	return context.WithValue(ctx, key{}, agent)
}

// Text is the bound user agent, or nil.
func Text(ctx context.Context) *string {
	agent, ok := ctx.Value(key{}).(string)
	if !ok {
		return nil
	}
	return &agent
}
