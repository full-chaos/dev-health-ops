package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestRunProducesSanitizedParityOutput(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	fixture := filepath.Join("..", "..", "internal", "providerfoundation", "testdata", "provider_parity.json")
	if err := run([]string{"-fixture", fixture}, &output); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		SchemaVersion string `json:"schema_version"`
		Cases         []struct {
			ID             string `json:"id"`
			Classification string `json:"classification"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.SchemaVersion != "v1" || len(decoded.Cases) != 8 || decoded.Cases[0].Classification != "rate_limited" {
		t.Fatalf("unexpected output: %s", output.String())
	}
}
