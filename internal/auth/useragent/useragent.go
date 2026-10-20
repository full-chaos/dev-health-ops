// Package useragent is the user agent of one auth-service request.
package useragent

import (
	"context"
	"net/http"
)

// Middleware is not built yet.
func Middleware(next http.Handler) http.Handler { return next }

// Text is not built yet.
func Text(context.Context) *string { return nil }

// With is not built yet.
func With(ctx context.Context, _ string) context.Context { return ctx }
