//go:build tools

// Package tools pins the build-time tools this module invokes but does not
// import from production code, so their own dependencies enter the module
// graph and `go.sum`.
//
// CHAOS-5489: without this, `go run github.com/99designs/gqlgen` -- the
// command cmd/query-api/README.md documents for regenerating the GraphQL
// layer -- cannot start:
//
//	missing go.sum entry for module providing package github.com/urfave/cli/v2
//	(imported by github.com/99designs/gqlgen)
//
// gqlgen is already a direct require, but only its RUNTIME library is
// imported (cmd/query-api/internal/graph uses github.com/99designs/gqlgen/graphql).
// Nothing imports the CLI's own main package, so `go mod tidy` has no reason
// to record its transitive deps -- and tidy alone does NOT fix this, measured:
// it produces an empty diff. The blank import below is what puts the CLI in
// the graph.
package tools

import (
	_ "github.com/99designs/gqlgen"
)
