// Package unlisted is a route caller scan fixture (CHAOS-10268): a package
// outside the list that keeps DoRoute as a method value and never calls it
// by name.
package unlisted

import "github.com/full-chaos/dev-health-ops/internal/providerfoundation"

// Fetcher returns DoRoute as a method value.
func Fetcher(client *providerfoundation.HTTPClient) any {
	return client.DoRoute
}
