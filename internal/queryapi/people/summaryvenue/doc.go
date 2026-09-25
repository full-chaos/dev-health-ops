// Package summaryvenue holds the venue differential oracle for the people
// summary response's naive timestamps (TestPeopleSummaryVenueOracle and its
// server-zone twin): the real Python build_person_summary_response and the Go
// people.BuildSummaryResponse, run over two ClickHouse databases seeded with
// the same rows and compared as raw response text. It is its own package so
// the venue run has its own time budget, apart from the other venues.
package summaryvenue
