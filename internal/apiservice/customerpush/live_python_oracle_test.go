package customerpush

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
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
func TestAdminSchemaMatchesFrozenPython(t *testing.T) {
	frozen := venueoracle.OpenGolden(t, programGolden("admin-schema", t.Name(), "ed4fc8a511f464994c7d8681823eac426c348485f627967cbf020abce8b3380a"))
	_, file, _, _ := moduleroot.Caller(0)
	root := frozen.PythonRoot(t, filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..")))
	request := venueoracle.ProgramRequest("admin schema producer", pythonAdminSchemaProgram, nil, producerEnv)
	answers := frozen.Produce(t, root, []venueoracle.Request{request}, func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
		producer.RequireDeployed()
		command, err := producer.Command(context.Background(), producerEnv, nil, "-c", pythonAdminSchemaProgram)
		if err != nil {
			t.Fatal(err)
		}
		output, err := command.Output()
		if err != nil {
			t.Fatalf("live python: %v", pyoracle.RunError(command.Path, err, output))
		}
		return []venueoracle.Response{{Status: 0, Body: withoutLogLines(output)}}
	})
	frozen.Consumed(t, answers...)
	output := []byte(answers[0].Body)
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
	frozen.SkipDiff(t)
	frozen.Finish(t)
}
