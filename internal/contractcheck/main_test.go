package contractcheck

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func moduleRootPath(elems ...string) string {
	return filepath.Join(append([]string{"..", ".."}, elems...)...)
}

func TestCapabilitiesUsesExplicitQueuesAndRejectsProfiles(t *testing.T) {
	t.Parallel()
	root := moduleRootPath("contracts", "jobs", "v1")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runCapabilities([]string{
		"--root", root,
		"--queues", "heartbeat,webhooks", "--queues", "retention",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("capabilities: %v, stderr = %s", err, stderr.String())
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
	if err := runCapabilities([]string{"--root", root, "--profile", "ops"}, &stdout, &stderr); cli.ExitForVerbError(err) != cli.ExitUsage {
		t.Fatalf("profile compatibility err = %v (exit %d), want ExitUsage", err, cli.ExitForVerbError(err))
	}
	stdout.Reset()
	stderr.Reset()
	if err := runCapabilities([]string{"--root", root}, &stdout, &stderr); cli.ExitForVerbError(err) != cli.ExitUsage {
		t.Fatalf("missing queues err = %v (exit %d), want ExitUsage", err, cli.ExitForVerbError(err))
	}
	stdout.Reset()
	stderr.Reset()
	if err := runRollout([]string{"--root", root, "--report", "missing.json"}, &stdout, &stderr); cli.ExitForVerbError(err) != cli.ExitUsage {
		t.Fatalf("rollout without queues err = %v (exit %d), want ExitUsage", err, cli.ExitForVerbError(err))
	}
}

func TestValidateAndCapabilitiesCommands(t *testing.T) {
	t.Parallel()
	root := moduleRootPath("contracts", "jobs", "v1")
	deployment := moduleRootPath("deploy", "go-workers", "deployment.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runValidate([]string{"--root", root, "--deployment", deployment}, &stdout, &stderr); err != nil {
		t.Fatalf("validate: %v, stderr = %s", err, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if err := runCapabilities([]string{"--root", root, "--queues", "coverage,heartbeat,retention,webhooks"}, &stdout, &stderr); err != nil {
		t.Fatalf("capabilities: %v, stderr = %s", err, stderr.String())
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
	if err := runCapabilities([]string{"--root", root, "--queues", "coverage,heartbeat,retention,webhooks"}, &capability, &capabilityErrors); err != nil {
		t.Fatalf("capabilities: %v, stderr = %s", err, capabilityErrors.String())
	}
	if err := os.WriteFile(report, capability.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	capability.Reset()
	capabilityErrors.Reset()
	if err := runCapabilities([]string{"--root", root, "--queues", "investment,metrics,reports,workgraph"}, &capability, &capabilityErrors); err != nil {
		t.Fatalf("heavy capabilities: %v, stderr = %s", err, capabilityErrors.String())
	}
	if err := os.WriteFile(heavyReport, capability.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	capability.Reset()
	capabilityErrors.Reset()
	if err := runCapabilities([]string{"--root", root, "--queues", "sync,sync_provider"}, &capability, &capabilityErrors); err != nil {
		t.Fatalf("sync capabilities: %v, stderr = %s", err, capabilityErrors.String())
	}
	if err := os.WriteFile(syncReport, capability.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runRollout([]string{
		"--root", root,
		"--queues", "coverage,heartbeat,retention,webhooks",
		"--queues", "investment,metrics,reports,workgraph",
		"--queues", "sync,sync_provider",
		"--report", report, "--report", heavyReport, "--report", syncReport,
	}, &stdout, &stderr); err != nil {
		t.Fatalf("rollout: %v, stderr = %s", err, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if err := runRollout([]string{
		"--root", root,
		"--queues", "coverage,heartbeat,retention,webhooks",
		"--report", report,
	}, &stdout, &stderr); err != nil {
		t.Fatalf("queue-scoped rollout: %v, stderr = %s", err, stderr.String())
	}
}

func TestCommandErrorsAreBounded(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runValidate([]string{"--unknown-flag"}, &stdout, &stderr)
	if cli.ExitForVerbError(err) != cli.ExitUsage {
		t.Fatalf("unknown flag err = %v (exit %d), want ExitUsage", err, cli.ExitForVerbError(err))
	}
	if strings.Contains(stderr.String(), "encoded_args") {
		t.Fatalf("error unexpectedly contains arguments: %s", stderr.String())
	}
}

// TestExitCodeContract pins the binary-wide mapping every vertical's verbs
// share (internal/cli.ExitForVerbError): a malformed flag or a disallowed
// positional argument exits 2 (cli.ExitUsage), -h/--help exits 0
// (cli.ExitOK), and every other failure exits 1 (cli.ExitFailure). "Move
// only" (this fold's own parser classification) does not exempt these
// verbs from the contract every OTHER vertical in this binary honors.
func TestExitCodeContract(t *testing.T) {
	root := moduleRootPath("contracts", "jobs", "v1")
	for _, cell := range []struct {
		name string
		args []string
		want int
	}{
		{"validate: bad flag", []string{"--not-a-real-flag"}, cli.ExitUsage},
		{"validate: -h", []string{"-h"}, cli.ExitOK},
		{"validate: --help", []string{"--help"}, cli.ExitOK},
		{"validate: unwanted positional", []string{"unexpected"}, cli.ExitUsage},
		{"validate: real failure (missing contract tree)", []string{"--root", filepath.Join(t.TempDir(), "does-not-exist")}, cli.ExitFailure},
		{"capabilities: -h", []string{"-h"}, cli.ExitOK},
		{"capabilities: missing --queues", []string{"--root", root}, cli.ExitUsage},
		{"rollout: -h", []string{"-h"}, cli.ExitOK},
		{"compare: -h", []string{"-h"}, cli.ExitOK},
		{"compare: missing --base", []string{}, cli.ExitUsage},
	} {
		t.Run(cell.name, func(t *testing.T) {
			var run func(args []string, stdout, stderr *bytes.Buffer) error
			switch {
			case strings.HasPrefix(cell.name, "validate"):
				run = func(args []string, stdout, stderr *bytes.Buffer) error { return runValidate(args, stdout, stderr) }
			case strings.HasPrefix(cell.name, "capabilities"):
				run = func(args []string, stdout, stderr *bytes.Buffer) error { return runCapabilities(args, stdout, stderr) }
			case strings.HasPrefix(cell.name, "rollout"):
				run = func(args []string, stdout, stderr *bytes.Buffer) error { return runRollout(args, stdout, stderr) }
			case strings.HasPrefix(cell.name, "compare"):
				run = func(args []string, stdout, stderr *bytes.Buffer) error { return runCompare(args, stdout, stderr) }
			}
			var stdout, stderr bytes.Buffer
			err := run(cell.args, &stdout, &stderr)
			if got := cli.ExitForVerbError(err); got != cell.want {
				t.Fatalf("exit = %d, want %d (err=%v)", got, cell.want, err)
			}
		})
	}
}

// TestCommandRunPrintsErrorOnceExceptOnHelp proves the Command() wrapper
// (not each run* function) owns printing the verb's own error, and never
// prints anything for a help request -- the same discipline
// internal/goapicli's own verbs follow.
func TestCommandRunPrintsErrorOnceExceptOnHelp(t *testing.T) {
	validate := Command().Children[0]
	if validate.Name != "validate" {
		t.Fatalf("Children[0] = %q, want validate", validate.Name)
	}
	var stdout, stderr bytes.Buffer
	code := validate.Run(context.Background(), cli.Env{Args: []string{"--not-a-real-flag"}, Stdout: &stdout, Stderr: &stderr})
	if code != cli.ExitUsage {
		t.Fatalf("code = %d, want ExitUsage", code)
	}
	if !strings.Contains(stderr.String(), "contracts validate:") {
		t.Fatalf("stderr = %q, want it to name the verb", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = validate.Run(context.Background(), cli.Env{Args: []string{"-h"}, Stdout: &stdout, Stderr: &stderr})
	if code != cli.ExitOK {
		t.Fatalf("-h code = %d, want ExitOK", code)
	}
	if strings.Contains(stderr.String(), "contracts validate:") {
		t.Fatalf("stderr = %q, want no error line for a help request", stderr.String())
	}
}

// TestUsageErrorIsAFlagUsageError proves the hand-rolled usage() helper
// produces the SAME error type cli.WrapFlagParseError does, so
// cli.ExitForVerbError classifies both identically. A negative control:
// wrapping a plain error (not through usageError) must NOT be classified
// as ExitUsage, proving this cell can fail.
func TestUsageErrorIsAFlagUsageError(t *testing.T) {
	wrapped := usageError(errors.New("bad invocation"))
	var target *cli.FlagUsageError
	if !errors.As(wrapped, &target) {
		t.Fatalf("usageError(...) = %v, want a *cli.FlagUsageError", wrapped)
	}
	if cli.ExitForVerbError(wrapped) != cli.ExitUsage {
		t.Fatalf("exit = %d, want ExitUsage", cli.ExitForVerbError(wrapped))
	}
	plain := errors.New("an ordinary failure")
	if cli.ExitForVerbError(plain) != cli.ExitFailure {
		t.Fatalf("a plain error must classify as ExitFailure, got %d", cli.ExitForVerbError(plain))
	}
}
