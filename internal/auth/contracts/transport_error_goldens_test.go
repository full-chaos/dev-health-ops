package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const transportErrorSurface = "transport-error.v1"

type transportErrorManifest struct {
	Accept []struct {
		File string `json:"file"`
	} `json:"accept"`
	Reject []struct {
		File                   string `json:"file"`
		ExpectInstanceLocation string `json:"expect_instance_location"`
		ExpectKeyword          string `json:"expect_keyword"`
	} `json:"reject"`
}

var transportErrorKeywordTokens = map[string]string{
	"additionalProperties": "unexpected additional properties",
	"enum":                 "enum:",
	"pattern":              "pattern:",
	"required":             "required:",
}

func loadTransportErrorCorpus(t *testing.T) (string, transportErrorManifest) {
	t.Helper()
	dir := filepath.Join(testRoot(t), "contracts/auth/v1/examples/transport-error")
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m transportErrorManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return dir, m
}

func TestTransportErrorCorpusIsFullyClaimedAndAgreesWithTheSchema(t *testing.T) {
	root := testRoot(t)
	dir, m := loadTransportErrorCorpus(t)
	if len(m.Accept) == 0 || len(m.Reject) == 0 {
		t.Fatalf("empty group: accept=%d reject=%d", len(m.Accept), len(m.Reject))
	}
	claimed := map[string]bool{"manifest.json": true}
	decode := func(file string) any {
		raw, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			t.Fatal(err)
		}
		var doc any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		return doc
	}
	for _, e := range m.Accept {
		claimed[e.File] = true
		if err := Validate(root, transportErrorSurface, decode(e.File)); err != nil {
			t.Errorf("accept %s refused: %v", e.File, err)
		}
	}
	for _, e := range m.Reject {
		claimed[e.File] = true
		token, ok := transportErrorKeywordTokens[e.ExpectKeyword]
		if !ok {
			t.Fatalf("no Go message token for keyword %q", e.ExpectKeyword)
		}
		err := Validate(root, transportErrorSurface, decode(e.File))
		if err == nil || !strings.Contains(err.Error(), token) {
			t.Errorf("reject %s: err %v, want the %s keyword", e.File, err, e.ExpectKeyword)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !claimed[entry.Name()] {
			t.Errorf("unclaimed fixture %s", entry.Name())
		}
	}
}
