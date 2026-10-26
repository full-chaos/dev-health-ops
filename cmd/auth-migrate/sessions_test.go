package main

import (
	"io"
	"strings"
	"testing"
)

func TestParseSessionsArgs(t *testing.T) {
	for name, args := range map[string][]string{
		"count":        {"count-unpinned"},
		"revoke dry":   {"revoke-unpinned"},
		"revoke apply": {"revoke-unpinned", "-apply"},
	} {
		if _, err := parseSessionsArgs(args, io.Discard); err != nil {
			t.Errorf("%s refused: %v", name, err)
		}
	}
	for name, args := range map[string][]string{
		"no verb":             {},
		"unknown verb":        {"drop"},
		"count with apply":    {"count-unpinned", "-apply"},
		"positional argument": {"revoke-unpinned", "extra"},
	} {
		if _, err := parseSessionsArgs(args, io.Discard); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestExecuteSessionsRefusesBeforeReadingTheEnvironment(t *testing.T) {
	var stderr strings.Builder
	lookup := func(string) (string, bool) {
		t.Fatal("the environment was read for a bad command line")
		return "", false
	}
	if code := executeSessions(t.Context(), []string{"drop"}, lookup, io.Discard, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
}
