package graph

import (
	"bytes"
	"testing"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
)

// The pin (contracts/graphql/v1/schema.graphql) is the Go plane's schema:
// gqlgen generates this package's executable schema from it. The schema
// baked into generated.go must be byte-identical to the pin, so a pin edit
// without `go run ./cmd/gqlgen-guard generate` (or a hand edit of generated.go) fails here rather
// than serving a schema the checked-in contract does not describe.
func TestGeneratedSchemaSourceIsTheCheckedInPin(t *testing.T) {
	if len(sources) != 1 {
		t.Fatalf("generated schema has %d sources, want exactly the pin", len(sources))
	}
	if !bytes.Equal([]byte(sources[0].Input), schemav1.SDL) {
		t.Fatalf("generated.go's embedded schema differs from contracts/graphql/v1/schema.graphql; run `go run ./cmd/gqlgen-guard generate` after editing the pin")
	}
}
