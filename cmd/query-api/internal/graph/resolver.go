package graph

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

import "github.com/full-chaos/dev-health-ops/cmd/query-api/internal/featureflags"

// Resolver holds every dependency a field resolver needs. ClickHouse is
// the shared dev-health-go query client (main.go builds the real one;
// featureflags.QueryClient is the narrow interface a test can fake) --
// CHAOS-4367 Wave 1's featureFlags resolver is the first field to use it.
// reviewedges.QueryClient (CHAOS-4368 Wave 2) has an identical single-
// method shape, so ClickHouse satisfies it too without a second field or
// a wrapper -- Go's interface-to-interface assignability only requires a
// matching method set, not a shared declared type.
type Resolver struct {
	ClickHouse featureflags.QueryClient
}
