package main

import (
	"io"
	"strings"
	"testing"
)

const bootstrapTarget = "11111111-1111-4111-8111-111111111111"

func TestParsePlatformRoleBootstrapArgs(t *testing.T) {
	got, err := parsePlatformRoleBootstrapArgs([]string{"-principal", bootstrapTarget, "-confirm"}, io.Discard)
	if err != nil || got.String() != bootstrapTarget {
		t.Fatalf("a valid command line: %v %v", got, err)
	}
	for name, args := range map[string][]string{
		"no confirm":          {"-principal", bootstrapTarget},
		"no principal":        {"-confirm"},
		"bad principal":       {"-principal", "x", "-confirm"},
		"nil principal":       {"-principal", "00000000-0000-0000-0000-000000000000", "-confirm"},
		"positional argument": {"-principal", bootstrapTarget, "-confirm", "extra"},
		"unknown flag":        {"-principal", bootstrapTarget, "-confirm", "-apply"},
	} {
		if _, err := parsePlatformRoleBootstrapArgs(args, io.Discard); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestExecutePlatformRoleBootstrapRefusesBeforeReadingTheEnvironment(t *testing.T) {
	var stderr strings.Builder
	lookup := func(string) (string, bool) {
		t.Fatal("the environment was read for a bad command line")
		return "", false
	}
	if code := execute(t.Context(), []string{"platform-role-bootstrap", "-principal", bootstrapTarget}, lookup, io.Discard, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2 (no -confirm)", code)
	}
}

func TestExecutePlatformRoleBootstrapNeedsTheMigrationEndpoint(t *testing.T) {
	var stderr strings.Builder
	lookup := func(string) (string, bool) { return "", false }
	code := execute(t.Context(), []string{"platform-role-bootstrap", "-principal", bootstrapTarget, "-confirm"}, lookup, io.Discard, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "is required") {
		t.Fatalf("exit/stderr = %d / %q", code, stderr.String())
	}
}
