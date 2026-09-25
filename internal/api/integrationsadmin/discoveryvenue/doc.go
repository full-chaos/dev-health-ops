// Package discoveryvenue holds the venue differential oracle for the
// integration discover route (TestIntegrationDiscoverVenueOracle): the real
// Python api and the Go api discovering Jira projects from one recorded
// project list served by one fake Jira over two copies of one seeded
// database, compared as raw response text, and the resulting
// integration_sources rows compared as raw column text. It is its own package
// so the venue run has its own time budget, apart from the other api venues.
package discoveryvenue
