//go:build integration

package providersync

import "time"

// linearCollectorForScopeTest is the Linear collector of the two-integration
// tests: the real collector with the census of the test. now is the clock a
// project row is stamped with, so a run in the test is one point in time.
func linearCollectorForScopeTest(sink LinearReferenceCatalogClickHouseEffects, census OwnershipScopeCensus, now func() time.Time) LinearTeamCatalogCollector {
	return LinearTeamCatalogCollector{
		Handler:     LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10, Now: now},
		Sink:        sink,
		ScopeCensus: census,
	}
}

// jiraCollectorForScopeTest is the Jira catalog collector of the
// two-integration tests: the real collector with the census of the test.
func jiraCollectorForScopeTest(sink JiraTeamCatalogClickHouseEffects, census OwnershipScopeCensus) JiraTeamCatalogCollector {
	return JiraTeamCatalogCollector{Handler: JiraTeamCatalogRouteHandler{}, Sink: sink, ScopeCensus: census}
}
