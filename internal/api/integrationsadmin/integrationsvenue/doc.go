// Package integrationsvenue holds the venue differential oracle for the
// generic integration admin routes (TestIntegrationsAdminVenueOracle): the
// real Python api and the Go api answering the same requests over two copies
// of one seeded Postgres, compared as raw response text, and the resulting
// table contents compared as raw column text. It is its own package so the
// venue run has its own time budget, apart from the other api venues.
package integrationsvenue
