package externalingest

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestSchemaBundleMatchesFrozenPython compares the Go bundle with the frozen
// answer of the real production functions (schema_registry.get_bundle,
// schema_registry.compute_etag over schemas.py's actual Pydantic models),
// recorded once by execution and replayed here (no Python runs). The live
// run was deleted: Go is the implementation of record.
//
// The golden asset itself STAYS the Go runtime's source of truth (bundle.go
// embeds it; no request path calls Python) -- this test only proves the
// golden has not drifted from what Python would generate today, and that
// this package's Go-side merge (schemaDocument) + ETag computation
// (computeETag) agree with Python's on the full document, not just a hash.
func TestSchemaBundleMatchesFrozenPython(t *testing.T) {
	frozen := venueoracle.OpenGolden(t, programGolden("schema-bundle", t.Name(), "e8a475f2d99ec02ebd7b102e4636464e3fe64309ba9bd7f52b0e9565e67370a8"))
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	root = frozen.PythonRoot(t, root)
	program, err := os.ReadFile("testdata/python_schema_bundle_oracle.py")
	if err != nil {
		t.Fatal(err)
	}
	request := venueoracle.ProgramRequest("schema bundle producer", string(program), nil, producerEnv)
	answers := frozen.Produce(t, root, []venueoracle.Request{request}, func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
		producer.RequireDeployed()
		command, err := producer.Command(context.Background(), producerEnv, nil, "testdata/python_schema_bundle_oracle.py")
		if err != nil {
			t.Fatal(err)
		}
		command.Dir = filepath.Join(root, "internal", "api", "externalingest")
		var stdout, stderr bytes.Buffer
		command.Stdout = &stdout
		command.Stderr = &stderr
		if err := command.Run(); err != nil {
			t.Fatalf("execute production Python schema-bundle oracle: %v\nstdout:\n%s",
				pyoracle.RunError(command.Path, err, stderr.Bytes()), stdout.Bytes())
		}
		return []venueoracle.Response{{Status: 0, Body: withoutLogLines(stdout.Bytes())}}
	})
	frozen.Consumed(t, answers...)
	stdout := bytes.NewBufferString(answers[0].Body)

	var oracle struct {
		Body string `json:"body"`
		ETag string `json:"etag"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &oracle); err != nil {
		t.Fatalf("decode production Python oracle output %q: %v", stdout.String(), err)
	}

	goDocument, err := schemaDocument(Limits{MaxRecords: DefaultLimits.MaxRecords, MaxBodyBytes: DefaultLimits.MaxBodyBytes})
	if err != nil {
		t.Fatalf("schemaDocument: %v", err)
	}
	goETag, err := computeETag(goDocument)
	if err != nil {
		t.Fatalf("computeETag: %v", err)
	}
	if goETag != oracle.ETag {
		t.Errorf("etag mismatch: go=%s python=%s", goETag, oracle.ETag)
	}
	// The served body as raw text: key order, separators, escapes and the
	// absence of a trailing newline all count.
	goBody, err := pyjson.Marshal(goDocument)
	if err != nil {
		t.Fatal(err)
	}
	if string(goBody) != oracle.Body {
		t.Errorf("schema document differs from the live Python producer's served body (go %d bytes, python %d bytes); first difference at byte %d",
			len(goBody), len(oracle.Body), firstDifference(string(goBody), oracle.Body))
	}

	frozen.SkipDiff(t)
	frozen.Finish(t)
}

func firstDifference(a, b string) int {
	for index := 0; index < len(a) && index < len(b); index++ {
		if a[index] != b[index] {
			return index
		}
	}
	return min(len(a), len(b))
}
