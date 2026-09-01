// Package digest is the ONE place cmd/query-api computes a digest --
// CHAOS-4696 PR1 pulled document-digesting out of query_route.go's
// private digestHex helper so cmd/query-api/tools/registrydump can
// compute the EXACT SAME digest a running query-api process would, by
// importing this package, instead of re-typing the algorithm a second
// time in a different binary and hoping the two copies never drift.
// Package main cannot be imported, so this had to move somewhere both
// binaries can reach -- here. CHAOS-4696 PR2 adds Schema for the same
// reason, one level up: query-api's startup verification and
// registrydump's schema-digest producer must both compute the canonical
// GO_API_SCHEMA_DIGEST value from the exact same function.
//
// This is deliberately NOT the class of cross-language "printer" CHAOS-4696
// warns against (see query_route.go's registered*Document doc comments):
// both functions hash bytes they are handed, neither parses or reprints
// GraphQL/SDL. The two-printer trap is about two different printers
// producing different bytes for the same input; a sha256 is a pure
// function of the bytes it is given, so importing the same package from
// two binaries is exact reuse, not two implementations that could
// disagree.
package digest

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Document returns the canonical hex-encoded sha256 digest of a GraphQL
// document's text: sha256(strings.TrimSpace(text)). This is what
// query_route.go's operationForDocument computes over an incoming
// request's raw query text, and what a registered*Document const's
// digest must equal for that operation to be reachable.
func Document(text string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(text)))
	return hex.EncodeToString(sum[:])
}

// Schema returns the canonical GO_API_SCHEMA_DIGEST value for a GraphQL
// SDL file's RAW bytes: "sha256:" + hex-encoded sha256 of the bytes
// UNMODIFIED -- no trim, no normalisation, no re-printing. This is
// deliberately NOT Document's algorithm: schema.graphql is a checked-in
// contract file (contracts/graphql/v1/schema.graphql, embedded verbatim
// by contracts/graphql/v1's schemav1.SDL), not caller-supplied request
// text, so there is no whitespace-variance to tolerate and every byte
// (including a trailing newline) is part of what's being pinned. Ops
// CI/harnesses and a running query-api process both call this over the
// SAME embedded bytes (see cmd/query-api/tools/registrydump's
// schema-digest subcommand and query_route.go's buildQueryRoute), so a
// value computed here can never disagree with itself across planes --
// only a genuine content change (a different schema.graphql) changes it.
func Schema(sdl []byte) string {
	sum := sha256.Sum256(sdl)
	return "sha256:" + hex.EncodeToString(sum[:])
}
