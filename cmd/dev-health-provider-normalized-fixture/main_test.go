package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRunValidatesFullNormalizedCorpus(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	path := filepath.Join("..", "..", "internal", "providerfoundation", "testdata", "normalized_envelope_parity.json")
	if err := run([]string{"-fixture", path}, &output); err != nil {
		t.Fatal(err)
	}
	var decoded result
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.SchemaVersion != "v1" || len(decoded.Cases) != 7 {
		t.Fatalf("unexpected normalized corpus output: %s", output.String())
	}
	providers := map[string]bool{}
	for _, item := range decoded.Cases {
		if err := item.Envelope.Validate(); err != nil {
			t.Fatalf("%s: %v", item.ID, err)
		}
		providers[item.Envelope.Provider] = true
	}
	for _, provider := range []string{"github", "gitlab", "jira", "linear", "launchdarkly", "pagerduty"} {
		if !providers[provider] {
			t.Fatalf("provider %s missing from normalized corpus", provider)
		}
	}
}

func TestRunRejectsModelEnvelopeDrift(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "internal", "providerfoundation", "testdata", "normalized_envelope_parity.json")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var input fixture
	if err := json.Unmarshal(content, &input); err != nil {
		t.Fatal(err)
	}
	var model map[string]any
	if err := json.Unmarshal(input.Cases[0].Model, &model); err != nil {
		t.Fatal(err)
	}
	model["title"] = "model changed without updating the expected envelope"
	input.Cases[0].Model, err = json.Marshal(model)
	if err != nil {
		t.Fatal(err)
	}
	mutated, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	mutatedPath := filepath.Join(t.TempDir(), "normalized-drift.json")
	if err := os.WriteFile(mutatedPath, mutated, 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"-fixture", mutatedPath}, &output); err == nil {
		t.Fatal("model/envelope drift was accepted")
	}
	if output.Len() != 0 {
		t.Fatalf("partial output emitted for drifted fixture: %s", output.String())
	}
}
