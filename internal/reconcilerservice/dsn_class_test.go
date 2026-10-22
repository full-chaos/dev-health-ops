package reconcilerservice

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

func noEnvironment(string) (string, bool) { return "", false }

func TestStartRefusesDatabaseDSNsOutsideItsClass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var stderr bytes.Buffer
	code := shell.Execute(ctx, reconcilerSpec, nil, noEnvironment, shell.IO{
		Stderr:  &stderr,
		Environ: []string{"POSTGRES_URI=secret-domain", "COORDINATOR_DATABASE_URI=secret-coordinator", "API_DATABASE_URI=secret-api", "EMPTY_DATABASE_URI="},
	})
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr %q", code, stderr.String())
	}
	for _, want := range []string{"start refused", "process class control", "API_DATABASE_URI"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr %q lacks %q", stderr.String(), want)
		}
	}
	if strings.Contains(stderr.String(), "secret-") || strings.Contains(stderr.String(), "EMPTY_DATABASE_URI") {
		t.Errorf("stderr carries a value or an empty variable: %q", stderr.String())
	}
}

func TestClassAllowedDSNsPassTheStartGuard(t *testing.T) {
	if err := shell.CheckDSNClass(reconcilerSpec, []string{"POSTGRES_URI=secret-domain", "COORDINATOR_DATABASE_URI=secret-coordinator"}); err != nil {
		t.Fatalf("a class-allowed environment was refused: %v", err)
	}
}

// The production entry point reads the process environment itself: nothing is
// injected, so a guard that only worked on an injected list would pass here.
func TestCommandRunRefusesADSNFromTheProcessEnvironment(t *testing.T) {
	t.Setenv("API_DATABASE_URI", "secret-process-env")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	code := Command().Run(ctx, cli.Env{Lookup: noEnvironment, Stdout: &stdout, Stderr: &stderr})
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr %q", code, stderr.String())
	}
	for _, want := range []string{"start refused", "process class control", "API_DATABASE_URI"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr %q lacks %q", stderr.String(), want)
		}
	}
	if strings.Contains(stderr.String()+stdout.String(), "secret-process-env") {
		t.Error("the refusal carries the value")
	}
}
