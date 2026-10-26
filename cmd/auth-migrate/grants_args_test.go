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

// -defer-narrowing is a flag of both forms: it parses, and the run goes on to
// read its configuration (exit 1 here, not the argument error 2).
func TestDeferNarrowingIsAcceptedByBothForms(t *testing.T) {
	lookup := func(string) (string, bool) { return "", false }
	for _, args := range [][]string{
		{"-defer-narrowing", "-ack-grant-narrowing=df0950bc72ed8889"},
		{"grants", "-defer-narrowing", "-ack-grant-narrowing=df0950bc72ed8889"},
	} {
		var stdout, stderr bytes.Buffer
		if code := execute(context.Background(), args, lookup, &stdout, &stderr); code != 1 {
			t.Errorf("%v: exit = %d, want 1 (configuration); stderr: %s", args, code, stderr.String())
		}
		if strings.Contains(stderr.String(), "flag provided but not defined") {
			t.Errorf("%v: flag refused: %s", args, stderr.String())
		}
	}
}
