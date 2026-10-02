// Package method is a fixture of the site walker: a method outside the module that returns a client, called and as a value.
package method

import (
	"context"
	nethttp "net/http"

	"golang.org/x/oauth2"
)

func Call(ctx context.Context, config *oauth2.Config, token *oauth2.Token) {
	_ = config.Client(ctx, token)
}

func Value(config *oauth2.Config) { _ = config.Client }

var build func(context.Context, oauth2.TokenSource) *nethttp.Client

type maker struct{ make func() *nethttp.Client }

// Variable calls a client constructor through a variable and through a field: the callee cannot be read.
func Variable(ctx context.Context, src oauth2.TokenSource, m maker) {
	_ = build(ctx, src)
	_ = m.make()
}
