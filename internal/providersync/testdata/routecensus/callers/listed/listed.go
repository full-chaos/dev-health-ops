// Package listed is a route caller scan fixture (CHAOS-10268): a package in
// the list that calls DoRoute.
package listed

import (
	"context"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Fetch calls DoRoute with a literal route.
func Fetch(ctx context.Context, client *providerfoundation.HTTPClient) (*http.Response, error) {
	return client.DoRoute(ctx, http.MethodGet, "/users/{login}", "/users/x", nil)
}
