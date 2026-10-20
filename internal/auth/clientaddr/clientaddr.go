// Package clientaddr is the client address of one auth-service request.
package clientaddr

import (
	"context"
	"net/http"
	"net/netip"
)

// Env names the trusted-proxy list.
const Env = "AUTH_SERVICE_TRUSTED_PROXIES"

// Trust is the set of peers whose X-Forwarded-For is believed.
type Trust struct{}

// ParseTrust is not built yet.
func ParseTrust(string) (Trust, error) { return Trust{}, nil }

// Len is not built yet.
func (Trust) Len() int { return 0 }

func (Trust) trusts(netip.Addr) bool { return false }

// Resolve is not built yet.
func (Trust) Resolve(*http.Request) netip.Addr { return netip.Addr{} }

// Middleware is not built yet.
func Middleware(Trust) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler { return next }
}

// With is not built yet.
func With(ctx context.Context, _ netip.Addr) context.Context { return ctx }

// From is not built yet.
func From(context.Context) (netip.Addr, bool) { return netip.Addr{}, false }

// Text is not built yet.
func Text(context.Context) *string { return nil }
