// Package digest is the ONE place cmd/query-api computes a document
// digest -- CHAOS-4696 pulls this out of query_route.go's private
// digestHex helper so cmd/query-api/tools/registrydump can compute the
// EXACT SAME digest a running query-api process would, by importing this
// package, instead of re-typing sha256(strings.TrimSpace(text)) a second
// time in a different binary and hoping the two copies never drift.
// Package main cannot be imported, so this two-line algorithm had to
// move somewhere both binaries can reach -- here.
//
// This is deliberately NOT the class of cross-language "printer" CHAOS-4696
// warns against (see query_route.go's registered*Document doc comments):
// it hashes bytes it is handed, it never parses or reprints GraphQL. The
// two-printer trap is about two different GraphQL printers producing
// different bytes for the same document; a sha256+trim is a pure function
// of the bytes it is given, so importing the same package from two
// binaries is exact reuse, not two implementations that could disagree.
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
