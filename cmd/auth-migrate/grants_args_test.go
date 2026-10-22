package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// -check with grants is refused as an argument error before any configuration or database is read.
func TestGrantsRefusesCheckBeforeTouchingTheDatabase(t *testing.T) {
	var stdout, stderr bytes.Buffer
	lookup := func(string) (string, bool) { return "", false }
	if code := execute(context.Background(), []string{"grants", "-check"}, lookup, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "-check does not apply to grants") {
		t.Errorf("stderr = %q", stderr.String())
	}
}
