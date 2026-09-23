package customerpush

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// The producer admin_get_schema serves, minus the live limits.
const pythonAdminSchemaProgram = `
import json
from dev_health_ops.api.external_ingest.schemas import BatchEnvelope, RECORD_KIND_MODELS
print(json.dumps({
    "envelope": BatchEnvelope.model_json_schema(by_alias=True),
    "recordKinds": {kind: model.model_json_schema(by_alias=True) for kind, model in RECORD_KIND_MODELS.items()},
}))
`

// TestAdminSchemaMatchesLivePython executes the real producer and compares
// it, key order included, with the embedded golden the route serves.
func TestAdminSchemaMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, "-c", pythonAdminSchemaProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	output, err := command.Output()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := bytes.Split(bytes.TrimSpace(output), []byte("\n"))
	live, err := pyjson.Decode(lines[len(lines)-1])
	if err != nil {
		t.Fatal(err)
	}
	golden, err := pyjson.Decode(adminSchemaGolden)
	if err != nil {
		t.Fatal(err)
	}
	liveText, _ := pyjson.Marshal(live)
	goldenText, _ := pyjson.Marshal(golden)
	if !bytes.Equal(liveText, goldenText) {
		t.Fatalf("testdata/admin_schema.v1.json is stale: regenerate it from the producer in pythonAdminSchemaProgram")
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "api-customerpush-schema"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}
