package graph

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

import (
	"github.com/full-chaos/dev-health-ops/internal/queryapi/datahealth"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/featureflags"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/reports"
)

// Resolver holds every dependency a field resolver needs. ClickHouse is
// the shared dev-health-go query client (main.go builds the real one;
// featureflags.QueryClient is the narrow interface a test can fake) --
// CHAOS-4367 Wave 1's featureFlags resolver is the first field to use it.
// reviewedges.QueryClient (CHAOS-4368 Wave 2), cognitiveload.QueryClient,
// complexitytimeseries.QueryClient, and hotspots.QueryClient (CHAOS-4369
// Wave 3), and operatingreview.QueryClient (CHAOS-4352 Wave 4 Lane B,
// CHAOS-4505) each have an identical single-method shape, so ClickHouse
// satisfies them too without a second field or a wrapper -- Go's
// interface-to-interface assignability only requires a matching method
// set, not a shared declared type.
type Resolver struct {
	ClickHouse featureflags.QueryClient
	// Postgres is the read-only Postgres surface the data-health connector
	// read and the saved-report reads use; without it the connector list is
	// empty and a saved-report read is an error.
	Postgres datahealth.PGQuerier
	// ReportWriter runs the saved-report mutations (createSavedReport,
	// updateSavedReport, deleteSavedReport, cloneSavedReport, triggerReport).
	// It is the only write path this service has; without it a mutation is an
	// error, never a silent no-op.
	ReportWriter *reports.Writer
}
