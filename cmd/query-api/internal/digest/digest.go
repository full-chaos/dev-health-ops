// Package digest is cmd/query-api's door to the ONE place the Go-API
// digests are computed.
//
// CHAOS-4696 PR1 pulled document-digesting out of query_route.go's private
// digestHex helper so cmd/query-api/tools/registrydump could compute the
// EXACT SAME digest a running query-api process would, by importing it
// instead of re-typing the algorithm in a different binary and hoping the
// two copies never drift. PR2 added Schema for the same reason one level
// up: a running process (its PostgresSwitch routing key,
// query_route.go's buildQueryRoute) and registrydump's schema-digest
// producer must compute the canonical value from the same function.
//
// CHAOS-5425 added a THIRD reader -- cmd/go-api-prove, an operator command
// outside cmd/query-api, which Go's internal-package rule cannot let
// import anything under cmd/query-api/internal. The implementation
// therefore moved to internal/goapidigest, reachable from every binary in
// the module, and this package now forwards to it.
//
// Why forward rather than update the two call sites and delete this
// package: the import path is referenced by query_route.go's and
// registrydump's own doc comments as the named single source of truth,
// and a forwarding var is a rename-free move -- there is still exactly ONE
// sha256 implementation, and this file cannot drift from it because it
// contains no algorithm to drift.
package digest

import "github.com/full-chaos/dev-health-ops/internal/goapidigest"

// Document returns the canonical hex-encoded sha256 digest of a GraphQL
// document's text: sha256(strings.TrimSpace(text)). See
// goapidigest.Document.
var Document = goapidigest.Document

// Schema returns the canonical schema-digest value for a GraphQL SDL
// file's RAW bytes. See goapidigest.Schema.
var Schema = goapidigest.Schema
