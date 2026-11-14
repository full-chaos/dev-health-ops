// Package unlistedredirect is a route caller scan fixture (CHAOS-10268): a
// package outside the list that keeps DoUnauthenticatedRoute as a method
// value.
package unlistedredirect

import "github.com/full-chaos/dev-health-ops/internal/providerfoundation"

// Redirector returns DoUnauthenticatedRoute as a method value.
func Redirector(client *providerfoundation.HTTPClient) any {
	fetch := client.DoUnauthenticatedRoute
	return fetch
}
