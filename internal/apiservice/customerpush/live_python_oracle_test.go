package customerpush

import (
	"bytes"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestAdminSchemaMatchesFrozenPython compares the frozen producer answer, key order included, with the embedded golden the route serves.
func TestAdminSchemaMatchesFrozenPython(t *testing.T) {
	frozen := venueoracle.OpenGolden(t, programGolden("admin-schema", t.Name(), "ed4fc8a511f464994c7d8681823eac426c348485f627967cbf020abce8b3380a"))
	request := venueoracle.ProgramRequest("admin schema producer", nil)
	answers := frozen.Produce(t, []venueoracle.Request{request})
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
		t.Fatalf("testdata/admin_schema.v1.json is stale: regenerate it from the frozen producer answer")
	}
	frozen.SkipDiff(t)
	frozen.Finish(t)
}
