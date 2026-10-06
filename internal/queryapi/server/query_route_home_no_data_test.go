package server

import (
	"os"
	"strings"
	"testing"
)

func TestHomeCurrentAndV1DifferByTheDataSelections(t *testing.T) {
	current := registeredHomeDocument
	without := current
	for _, line := range []string{"      hasData\n", "      hasPriorData\n"} {
		if strings.Count(current, line) != 1 {
			t.Fatalf("current Home document does not contain %q exactly once", line)
		}
		if strings.Contains(registeredHomeV1Document, strings.TrimSpace(line)) {
			t.Fatalf("legacy Home document contains %q", strings.TrimSpace(line))
		}
		without = strings.Replace(without, line, "", 1)
	}
	if without != registeredHomeV1Document {
		t.Fatalf("current Home document without data selections is not the V1 document")
	}
	if digestHex(current) == digestHex(registeredHomeV1Document) {
		t.Fatal("current and legacy Home documents have the same digest")
	}
}

func TestHomeCurrentAndV1TextsResolveToHome(t *testing.T) {
	legacy := legacyDigestsByOperation["home"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredHomeV1Document) {
		t.Fatalf("legacyDigestsByOperation[home] = %v, want the V1 document digest", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{"home": digestHex(registeredHomeDocument)},
		map[string][]string{"home": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"home_captured.graphql", "home_v1_captured.graphql"} {
		text, err := os.ReadFile("testdata/wire_capture/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		operation, ok := operationForDocument(string(text), byDigest)
		if !ok || operation != "home" {
			t.Errorf("%s resolves to %q, %t; want home, true", name, operation, ok)
		}
	}
}
