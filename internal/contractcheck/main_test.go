package contractcheck

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func moduleRootPath(elems ...string) string {
	return filepath.Join(append([]string{"..", ".."}, elems...)...)
}

func TestCapabilitiesUsesExplicitQueuesAndRejectsProfiles(t *testing.T) {
	t.Parallel()
	root := moduleRootPath("contracts", "jobs", "v1")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := runCapabilities([]string{
		"--root", root,
		"--queues", "heartbeat,webhooks", "--queues", "retention",
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("capabilities code = %d, stderr = %s", code, stderr.String())
	}
	var report struct {
		Queues []string `json:"queues"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode capability report: %v; output=%s", err, stdout.String())
	}
	if want := []string{"heartbeat", "retention", "webhooks"}; !slices.Equal(report.Queues, want) {
		t.Fatalf("capability queues = %v, want %v", report.Queues, want)
	}

	stdout.Reset()
	stderr.Reset()
	if code := runCapabilities([]string{"--root", root, "--profile", "ops"}, &stdout, &stderr); code != 2 {
		t.Fatalf("profile compatibility code = %d, want 2; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := runCapabilities([]string{"--root", root}, &stdout, &stderr); code != 2 {
		t.Fatalf("missing queues code = %d, want 2; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := runRollout([]string{"--root", root, "--report", "missing.json"}, &stdout, &stderr); code != 2 {
		t.Fatalf("rollout without queues code = %d, want 2; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
}

func TestValidateAndCapabilitiesCommands(t *testing.T) {
	t.Parallel()
	root := moduleRootPath("contracts", "jobs", "v1")
	deployment := moduleRootPath("deploy", "go-workers", "deployment.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := runValidate([]string{"--root", root, "--deployment", deployment}, &stdout, &stderr); code != 0 {
		t.Fatalf("validate code = %d, stderr = %s", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := runCapabilities([]string{"--root", root, "--queues", "coverage,heartbeat,retention,webhooks"}, &stdout, &stderr); code != 0 {
		t.Fatalf("capabilities code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"system.heartbeat"`) || strings.Contains(stdout.String(), "payload") {
		t.Fatalf("unsafe or incomplete capability output: %s", stdout.String())
	}
}

func TestRolloutCommand(t *testing.T) {
	t.Parallel()
	root := moduleRootPath("contracts", "jobs", "v1")
	report := filepath.Join(t.TempDir(), "capability.json")
	heavyReport := filepath.Join(t.TempDir(), "heavy-capability.json")
	syncReport := filepath.Join(t.TempDir(), "sync-capability.json")
	var capability bytes.Buffer
	var capabilityErrors bytes.Buffer
	if code := runCapabilities([]string{"--root", root, "--queues", "coverage,heartbeat,retention,webhooks"}, &capability, &capabilityErrors); code != 0 {
		t.Fatalf("capabilities code = %d, stderr = %s", code, capabilityErrors.String())
	}
	if err := os.WriteFile(report, capability.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	capability.Reset()
	capabilityErrors.Reset()
	if code := runCapabilities([]string{"--root", root, "--queues", "investment,metrics,reports,workgraph"}, &capability, &capabilityErrors); code != 0 {
		t.Fatalf("heavy capabilities code = %d, stderr = %s", code, capabilityErrors.String())
	}
	if err := os.WriteFile(heavyReport, capability.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	capability.Reset()
	capabilityErrors.Reset()
	if code := runCapabilities([]string{"--root", root, "--queues", "sync,sync_provider"}, &capability, &capabilityErrors); code != 0 {
		t.Fatalf("sync capabilities code = %d, stderr = %s", code, capabilityErrors.String())
	}
	if err := os.WriteFile(syncReport, capability.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := runRollout([]string{
		"--root", root,
		"--queues", "coverage,heartbeat,retention,webhooks",
		"--queues", "investment,metrics,reports,workgraph",
		"--queues", "sync,sync_provider",
		"--report", report, "--report", heavyReport, "--report", syncReport,
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("rollout code = %d, stderr = %s", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := runRollout([]string{
		"--root", root,
		"--queues", "coverage,heartbeat,retention,webhooks",
		"--report", report,
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("queue-scoped rollout code = %d, stderr = %s", code, stderr.String())
	}
}

func TestCommandErrorsAreBounded(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := runValidate([]string{"--unknown-flag"}, &stdout, &stderr); code != 2 {
		t.Fatalf("unknown flag code = %d", code)
	}
	if strings.Contains(stderr.String(), "encoded_args") {
		t.Fatalf("error unexpectedly contains arguments: %s", stderr.String())
	}
}
