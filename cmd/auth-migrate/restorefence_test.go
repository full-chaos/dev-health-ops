package main

import (
	"io"
	"testing"
)

func TestRestoreFenceArgsAcceptTheDocumentedVerbs(t *testing.T) {
	cases := map[string][]string{
		"export":                {"export", "-out", "m.json"},
		"apply with a manifest": {"apply", "-manifest", "m.json"},
		"apply with a margin":   {"apply", "-blind-margin", "1000000"},
		"apply for real":        {"apply", "-manifest", "m.json", "-apply"},
		"apply with both":       {"apply", "-manifest", "m.json", "-blind-margin", "5", "-apply"},
		"status":                {"status"},
	}
	for name, args := range cases {
		if _, err := parseRestoreFenceArgs(args, io.Discard); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestRestoreFenceArgsRefuseWhatTheVerbsDoNotTake(t *testing.T) {
	cases := map[string][]string{
		"no verb":                  {},
		"an unknown verb":          {"rewind"},
		"export without a file":    {"export"},
		"apply without an input":   {"apply"},
		"apply with a zero margin": {"apply", "-blind-margin", "0"},
		"manifest with a margin":   {"apply", "-manifest", "m.json", "-blind-margin", "5"},
		"a positional argument":    {"status", "extra"},
		"status with -apply":       {"status", "-apply"},
		"export with a manifest":   {"export", "-out", "m.json", "-manifest", "x"},
	}
	for name, args := range cases {
		if _, err := parseRestoreFenceArgs(args, io.Discard); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestRestoreFenceIsADryRunUnlessApplyIsGiven(t *testing.T) {
	got, err := parseRestoreFenceArgs([]string{"apply", "-manifest", "m.json"}, io.Discard)
	if err != nil || got.apply {
		t.Fatalf("parsed = %+v, %v; want a dry run", got, err)
	}
}
