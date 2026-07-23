package main

import (
	"bytes"
	"encoding/json"
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
