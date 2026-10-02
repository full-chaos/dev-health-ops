// Package method is a fixture of the site walker: a method outside the module that returns a client, called and as a value.
package method

import (
	"context"

	"golang.org/x/oauth2"
)

func Call(ctx context.Context, config *oauth2.Config, token *oauth2.Token) {
	_ = config.Client(ctx, token)
}

func Value(config *oauth2.Config) { _ = config.Client }
