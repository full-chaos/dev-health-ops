package externalingest

import (
	"bytes"
	"encoding/json"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// TestSchemaBundleMatchesLivePython is the live-Python oracle team-lead's
// review required in place of a hardcoded ETag string: it EXECUTES the real
// production functions (schema_registry.get_bundle, schema_registry.
// compute_etag over schemas.py's actual Pydantic models), not a digest of a
// value computed once and typed into the test. It runs only under
// `ci/check_go.sh live-python-oracles` (DEV_HEALTH_LIVE_PYTHON_ORACLES=1,
// -count=1 -- see that verb's own doc comment on why a cached `go test` run
// would silently skip re-executing Python), never under the plain `test`
// verb, and never folds into it: that verb's cache defeats exactly what
// this test exists to catch (a schemas.py/schema_registry.py change the
// checked-in golden asset, testdata/schema_bundle.v1.json, was not
// regenerated to match).
//
// The golden asset itself STAYS the Go runtime's source of truth (bundle.go
// embeds it; no request path calls Python) -- this test only proves the
// golden has not drifted from what Python would generate today, and that
// this package's Go-side merge (schemaDocument) + ETag computation
// (computeETag) agree with Python's on the full document, not just a hash.
func TestSchemaBundleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, "testdata/python_schema_bundle_oracle.py")
	command.Dir = filepath.Join(root, "internal", "api", "externalingest")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("execute production Python schema-bundle oracle: %v\nstdout:\n%s",
			pyoracle.RunError(python, err, stderr.Bytes()), stdout.Bytes())
	}

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

	if err := os.WriteFile(filepath.Join(proofDirectory, "externalingest-schema-bundle"), []byte("executed\n"), 0o600); err != nil {
		t.Fatalf("write externalingest schema-bundle live Python oracle proof: %v", err)
	}
}

func firstDifference(a, b string) int {
	for index := 0; index < len(a) && index < len(b); index++ {
		if a[index] != b[index] {
			return index
		}
	}
	return min(len(a), len(b))
}
