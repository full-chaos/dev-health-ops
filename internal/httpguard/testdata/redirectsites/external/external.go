// Package external is a fixture of the site walker: a call outside the module whose result is a client.
package external

import (
	"context"

	"golang.org/x/oauth2"
)

func Make(ctx context.Context, src oauth2.TokenSource) { _ = oauth2.NewClient(ctx, src) }
