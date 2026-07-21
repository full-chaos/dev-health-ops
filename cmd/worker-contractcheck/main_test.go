package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateAndCapabilitiesCommands(t *testing.T) {
	t.Parallel()
	root := filepath.Join("..", "..", "contracts", "jobs", "v1")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := run([]string{"validate", "--root", root}, &stdout, &stderr); code != 0 {
		t.Fatalf("validate code = %d, stderr = %s", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"capabilities", "--root", root, "--profile", "ops"}, &stdout, &stderr); code != 0 {
		t.Fatalf("capabilities code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"system.heartbeat"`) || strings.Contains(stdout.String(), "payload") {
		t.Fatalf("unsafe or incomplete capability output: %s", stdout.String())
	}
}

func TestRolloutCommand(t *testing.T) {
	t.Parallel()
	root := filepath.Join("..", "..", "contracts", "jobs", "v1")
	report := filepath.Join(t.TempDir(), "capability.json")
	var capability bytes.Buffer
	var capabilityErrors bytes.Buffer
	if code := run([]string{"capabilities", "--root", root, "--profile", "ops"}, &capability, &capabilityErrors); code != 0 {
		t.Fatalf("capabilities code = %d, stderr = %s", code, capabilityErrors.String())
	}
	if err := os.WriteFile(report, capability.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := run([]string{"rollout", "--root", root, "--report", report}, &stdout, &stderr); code != 0 {
		t.Fatalf("rollout code = %d, stderr = %s", code, stderr.String())
	}
}

func TestCommandErrorsAreBounded(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := run([]string{"unknown"}, &stdout, &stderr); code != 2 {
		t.Fatalf("unknown command code = %d", code)
	}
	if strings.Contains(stderr.String(), "encoded_args") {
		t.Fatalf("error unexpectedly contains arguments: %s", stderr.String())
	}
}
