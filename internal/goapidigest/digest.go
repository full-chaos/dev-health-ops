// Package goapidigest holds the ONE implementation of the Go-API
// document and schema digests.
//
// It exists for the same reason cmd/query-api/internal/digest existed
// before it, one directory level up. That package's own doc comment
// records the rule: "Package main cannot be imported, so this had to move
// somewhere both binaries can reach". CHAOS-5425 adds a THIRD reader --
// cmd/go-api-prove, an operator command outside cmd/query-api -- and Go's
// internal-package rule puts anything under cmd/query-api/internal out of
// its reach. The choice was to move the algorithm here or to re-type it
// in the new command, and re-typing it is exactly the two-copies drift
// CHAOS-4696 closed for the other half of this contract.
//
// cmd/query-api/internal/digest now DELEGATES here rather than being
// deleted, so query_route.go and registrydump keep their import paths and
// this is a move of the implementation, not a second one. There is still
// exactly one sha256 in play, reached by import from every binary.
//
// This is deliberately NOT the class of cross-language "printer"
// CHAOS-4696 warns against: both functions hash bytes they are handed;
// neither parses nor reprints GraphQL/SDL. A sha256 is a pure function of
// its input bytes, so importing the same package from several binaries is
// exact reuse.
package goapidigest

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Document returns the canonical hex-encoded sha256 digest of a GraphQL
// document's text: sha256(strings.TrimSpace(text)). This is what
// query_route.go's operationForDocument computes over an incoming
// request's raw query text, and what a registered*Document const's digest
// must equal for that operation to be reachable.
func Document(text string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(text)))
	return hex.EncodeToString(sum[:])
}

// Schema returns the canonical schema-digest value for a GraphQL SDL
// file's RAW bytes: "sha256:" + hex-encoded sha256 of the bytes
// UNMODIFIED -- no trim, no normalisation, no re-printing.
//
// Deliberately NOT Document's algorithm: schema.graphql is a checked-in
// contract file (contracts/graphql/v1/schema.graphql, embedded verbatim
// by schemav1.SDL), not caller-supplied request text, so there is no
// whitespace variance to tolerate and every byte -- including a trailing
// newline -- is part of what is being pinned.
func Schema(sdl []byte) string {
	sum := sha256.Sum256(sdl)
	return "sha256:" + hex.EncodeToString(sum[:])
}
