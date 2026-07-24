package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRunProducesSanitizedParityOutput(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	fixturePath := filepath.Join("..", "..", "internal", "providerfoundation", "testdata", "provider_parity.json")
	if err := run([]string{"-fixture", fixturePath}, &output); err != nil {
		t.Fatal(err)
	}
	var decoded result
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var input fixture
	if err := json.Unmarshal(content, &input); err != nil {
		t.Fatal(err)
	}
	if decoded.SchemaVersion != input.SchemaVersion || len(decoded.Cases) != len(input.Cases) {
		t.Fatalf("unexpected output: %s", output.String())
	}
	for index, item := range input.Cases {
		if decoded.Cases[index].ID != item.ID || decoded.Cases[index].Classification != item.Classification {
			t.Fatalf("case %d mismatch: got %+v want %+v", index, decoded.Cases[index], item)
		}
	}
}

func TestRunRejectsClassificationMismatch(t *testing.T) {
	t.Parallel()
	fixturePath := filepath.Join("..", "..", "internal", "providerfoundation", "testdata", "provider_parity.json")
	content, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var input fixture
	if err := json.Unmarshal(content, &input); err != nil {
		t.Fatal(err)
	}
	input.Cases[0].Classification = "authentication"
	content, err = json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	mismatchPath := filepath.Join(t.TempDir(), "mismatch.json")
	if err := os.WriteFile(mismatchPath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"-fixture", mismatchPath}, &output); err == nil {
		t.Fatal("classification mismatch was accepted")
	}
	if output.Len() != 0 {
		t.Fatalf("partial output was emitted: %s", output.String())
	}
}

func TestResultJSONShapeRemainsStable(t *testing.T) {
	t.Parallel()
	var decoded struct {
		SchemaVersion string `json:"schema_version"`
		Cases         []struct {
			ID             string `json:"id"`
			Classification string `json:"classification"`
		} `json:"cases"`
	}
	if err := json.Unmarshal([]byte(`{"schema_version":"v1","cases":[{"id":"case","classification":"transient"}]}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.SchemaVersion != "v1" || len(decoded.Cases) != 1 || decoded.Cases[0].Classification != "transient" {
		t.Fatalf("unexpected decoded shape: %+v", decoded)
	}
}
