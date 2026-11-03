package main

import (
	"io"
	"strings"
	"testing"
)

func TestParsePlatformOwnerArgs(t *testing.T) {
	for _, verb := range []string{"platform-owner-grant", "platform-owner-revoke"} {
		got, err := parsePlatformOwnerArgs(verb, []string{"-principal", bootstrapTarget, "-confirm"}, io.Discard)
		if err != nil || got.String() != bootstrapTarget {
			t.Fatalf("%s: a valid command line: %v %v", verb, got, err)
		}
		for name, args := range map[string][]string{
			"no confirm":          {"-principal", bootstrapTarget},
			"no principal":        {"-confirm"},
			"bad principal":       {"-principal", "x", "-confirm"},
			"nil principal":       {"-principal", "00000000-0000-0000-0000-000000000000", "-confirm"},
			"positional argument": {"-principal", bootstrapTarget, "-confirm", "extra"},
			"unknown flag":        {"-principal", bootstrapTarget, "-confirm", "-role", "platform_admin"},
		} {
			if _, err := parsePlatformOwnerArgs(verb, args, io.Discard); err == nil {
				t.Errorf("%s: %s accepted", verb, name)
			}
		}
	}
}

func TestPlatformOwnerVerbsRefuseBeforeReadingTheEnvironment(t *testing.T) {
	for _, verb := range []string{"platform-owner-grant", "platform-owner-revoke"} {
		lookup := func(string) (string, bool) {
			t.Fatalf("%s read the environment for a bad command line", verb)
			return "", false
		}
		var stderr strings.Builder
		if code := execute(t.Context(), []string{verb, "-principal", bootstrapTarget}, lookup, io.Discard, &stderr); code != 2 {
			t.Errorf("%s: exit = %d, want 2 (no -confirm)", verb, code)
		}
		if !strings.Contains(stderr.String(), "usage: auth-migrate "+verb+" -principal <uuid> -confirm") {
			t.Errorf("%s: stderr = %q lacks the usage line", verb, stderr.String())
		}
	}
}

func TestPlatformOwnerVerbsNeedTheMigrationEndpoint(t *testing.T) {
	for _, verb := range []string{"platform-owner-grant", "platform-owner-revoke"} {
		var stderr strings.Builder
		lookup := func(string) (string, bool) { return "", false }
		code := execute(t.Context(), []string{verb, "-principal", bootstrapTarget, "-confirm"}, lookup, io.Discard, &stderr)
		if code != 1 || !strings.Contains(stderr.String(), "is required") {
			t.Errorf("%s: exit/stderr = %d / %q", verb, code, stderr.String())
		}
	}
}
