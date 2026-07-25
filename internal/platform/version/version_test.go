package version

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCurrentReportsStableServiceAndGoMetadata(t *testing.T) {
	t.Parallel()

	info := Current("dev-health-worker")
	if info.Service != "dev-health-worker" || info.Version == "" || info.GoVersion == "" {
		t.Fatalf("incomplete version info: %#v", info)
	}
	var output bytes.Buffer
	if err := info.WriteJSON(&output); err != nil {
		t.Fatal(err)
	}
	var decoded Info
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Service != info.Service || decoded.GoVersion != info.GoVersion {
		t.Fatalf("version report changed during encoding: %#v", decoded)
	}
}
