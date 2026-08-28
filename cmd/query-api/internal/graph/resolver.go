package graph

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

import "github.com/full-chaos/dev-health-ops/cmd/query-api/internal/featureflags"

// Resolver holds every dependency a field resolver needs. ClickHouse is
// the shared dev-health-go query client (main.go builds the real one;
// featureflags.QueryClient is the narrow interface a test can fake) --
// CHAOS-4367 Wave 1's featureFlags resolver is the first field to use it.
type Resolver struct {
	ClickHouse featureflags.QueryClient
}
