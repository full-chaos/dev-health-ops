// Package schemav1 embeds this directory's canonical GraphQL SDL
// (schema.graphql) directly into any Go binary that imports it.
//
// CHAOS-4696 PR2: query-api's startup schema-digest verification needs
// the exact SDL bytes it was built against, and the ops CI/harness
// digest producer (cmd/query-api/tools/registrydump's schema-digest
// subcommand) needs the SAME bytes -- both compute
// cmd/query-api/internal/digest.Schema() over whatever []byte this
// package hands them. go:embed is what makes "the bytes a running
// process verifies against" and "the bytes checked into this directory"
// the SAME bytes by construction: there is no copy step, no build-time
// injection, nothing that could drift the embedded content from
// schema.graphql itself. Go's compiler embeds the file's on-disk
// contents verbatim at build time; a change to schema.graphql with no
// rebuild cannot silently desync a running binary from this repo's
// checked-in contract.
//
// go:embed cannot reach outside the directory containing the .go file
// that declares it, so this trivial package exists here -- alongside
// schema.graphql, not inside cmd/query-api -- purely to make the file
// embeddable; see README.md in this directory for what schema.graphql
// itself is and who else consumes it (Python export pin, web codegen,
// gqlgen schema-first input).
package schemav1

import _ "embed"

// SDL is schema.graphql's exact byte content, embedded at compile time.
//
//go:embed schema.graphql
var SDL []byte
