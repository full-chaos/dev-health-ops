// Package metricsvenue holds the dho api's Prometheus counter parity
// oracle (TestCounterParityVenueOracle): the real Python api and the dho
// api answer the same requests, and the counters each exposes afterwards
// must have moved by the same amounts. It is its own package so the venue
// run has its own time budget, apart from the other api venues.
package metricsvenue
