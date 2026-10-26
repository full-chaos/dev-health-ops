package main

import (
	"io"
	"strings"
	"testing"
)

func TestParseWorkloadsArgs(t *testing.T) {
	bind := []string{"bind", "-workload", "api", "-environment", "prod", "-trust-domain", "cluster.local",
		"-issuer", "https://kubernetes.default.svc", "-namespace", "dev-health", "-service-account", "api"}
	ok := map[string][]string{
		"seed dry run":   {"seed"},
		"seed apply":     {"seed", "-apply"},
		"bind":           bind,
		"bind apply uid": append(append([]string{}, bind...), "-uid", "6f1c3c2e-0b8e-4d2a-9d7e-2f4a5b6c7d8e", "-apply"),
	}
	for name, args := range ok {
		if _, err := parseWorkloadsArgs(args, io.Discard); err != nil {
			t.Errorf("%s refused: %v", name, err)
		}
	}
	bad := map[string][]string{
		"no verb":             {},
		"unknown verb":        {"drop"},
		"seed with a binding": {"seed", "-workload", "api"},
		"bind without a name": {"bind", "-environment", "prod"},
		"bind with a bad uid": append(append([]string{}, bind...), "-uid", "x"),
		"bind missing issuer": {"bind", "-workload", "api", "-environment", "prod", "-trust-domain", "t", "-namespace", "n", "-service-account", "s"},
		"positional argument": {"seed", "extra"},
	}
	for name, args := range bad {
		if _, err := parseWorkloadsArgs(args, io.Discard); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestExecuteWorkloadsRefusesBeforeReadingTheEnvironment(t *testing.T) {
	var stderr strings.Builder
	lookup := func(string) (string, bool) {
		t.Fatal("the environment was read for a bad command line")
		return "", false
	}
	if code := executeWorkloads(t.Context(), []string{"bind"}, lookup, io.Discard, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
}

func TestExecuteWorkloadsNeedsTheMigrationEndpoint(t *testing.T) {
	var stderr strings.Builder
	lookup := func(string) (string, bool) { return "", false }
	if code := executeWorkloads(t.Context(), []string{"seed"}, lookup, io.Discard, &stderr); code != 1 || !strings.Contains(stderr.String(), "is required") {
		t.Fatalf("exit/stderr = %d / %q", code, stderr.String())
	}
}
