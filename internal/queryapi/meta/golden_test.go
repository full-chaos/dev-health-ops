package meta

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

// loadGolden decodes testdata/meta.json. The golden is hand-written: the
// ClickHouse `version` key of the original Python capture was removed on
// purpose (the route no longer names the server build).
// DisallowUnknownFields makes a field-name mismatch (Python emitted a key
// this Go type does not declare) a hard test failure rather than a silent
// drop.
func loadGolden(t *testing.T, name string) Response {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var resp Response
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return resp
}

// TestGoldenMeta requires field equality between BuildResponse and the
// golden; DisallowUnknownFields in loadGolden rejects a golden key the
// Response type does not declare.
func TestGoldenMeta(t *testing.T) {
	want := loadGolden(t, "meta.json")

	got := BuildResponse()

	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("Go BuildResponse diverges from golden:\n got:  %s\n want: %s", gotJSON, wantJSON)
	}
}
