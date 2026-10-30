// Package lockorder holds the live tests of the auth lock order (the doc
// comment of membership.Store's readPairs): every auth membership, invite and
// organization verb and every go-api writer of the same rows, run against each
// other on one Postgres. The tests are integration tests only.
package lockorder
