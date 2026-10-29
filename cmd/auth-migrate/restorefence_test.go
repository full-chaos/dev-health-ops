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

func TestRestoreFenceInitIsAVerbWithOneOptionalFlag(t *testing.T) {
	for name, args := range map[string][]string{"init": {"init"}, "init if absent": {"init", "-if-absent"}, "init naming the timeline": {"init", "-timeline", "2"}} {
		if _, err := parseRestoreFenceArgs(args, io.Discard); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, args := range map[string][]string{
		"init with a manifest":      {"init", "-manifest", "m.json"},
		"init with -apply":          {"init", "-apply"},
		"init with a margin":        {"init", "-blind-margin", "5"},
		"init positional":           {"init", "extra"},
		"-if-absent on apply":       {"apply", "-manifest", "m.json", "-if-absent"},
		"-if-absent with -timeline": {"init", "-if-absent", "-timeline", "2"},
		"-timeline on apply":        {"apply", "-manifest", "m.json", "-timeline", "2"},
		"a zero timeline":           {"init", "-timeline", "0"},
	} {
		if _, err := parseRestoreFenceArgs(args, io.Discard); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
