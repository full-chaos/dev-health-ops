//go:build integration

package server

// CHAOS-7078: "a complexity limit and a depth limit ... sized from the
// registered documents with a test that every registered document still
// executes under the limits." Tagged integration because it shells out to
// `go build ./cmd/registrydump` and runs it -- the SAME real producer
// docker/go-api-tools.Dockerfile uses to bake documents.json into the
// go-api-tools image, never a hand-copied list of document names that
// could drift from what is actually registered.

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/vektah/gqlparser/v2"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

func TestEveryRegisteredDocumentExecutesUnderTheConfiguredLimits(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	dumpBinary := filepath.Join(t.TempDir(), "registrydump")
	build := exec.Command("go", "build", "-o", dumpBinary, "./cmd/registrydump")
	build.Dir = root
	if out, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("building registrydump: %v\n%s", buildErr, out)
	}
	dump := exec.Command(dumpBinary, "-file", "internal/queryapi/server/query_route.go")
	dump.Dir = root
	documentsJSON, dumpErr := dump.Output()
	if dumpErr != nil {
		t.Fatalf("running registrydump: %v", dumpErr)
	}
	documentsPath := filepath.Join(t.TempDir(), "documents.json")
	if writeErr := os.WriteFile(documentsPath, documentsJSON, 0o644); writeErr != nil {
		t.Fatal(writeErr)
	}
	documents, loadErr := goapiproof.LoadDocuments(documentsPath)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if len(documents) == 0 {
		t.Fatal("registrydump produced zero registered documents -- this test can prove nothing")
	}

	schema := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})
	for operation, text := range documents {
		t.Run(operation, func(t *testing.T) {
			query, parseErr := gqlparser.LoadQuery(schema.Schema(), text)
			if parseErr != nil {
				t.Fatalf("parsing the real registered document: %v", parseErr)
			}
			op := query.Operations[0]
			c := documentComplexity(schema, op)
			if c > graphQLComplexityLimit {
				t.Fatalf("complexity %d exceeds graphQLComplexityLimit=%d -- raise the limit deliberately (graphql_server_limits.go) with a comment naming which document needed it, never quietly", c, graphQLComplexityLimit)
			}
			d := selectionSetDepth(op.SelectionSet, 1)
			if d > graphQLDepthLimit {
				t.Fatalf("depth %d exceeds graphQLDepthLimit=%d -- raise the limit deliberately (graphql_server_limits.go) with a comment naming which document needed it, never quietly", d, graphQLDepthLimit)
			}
		})
	}
}
