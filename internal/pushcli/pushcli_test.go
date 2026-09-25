package pushcli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func run(t *testing.T, stdin string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
		Args: append([]string{"push"}, args...), Stdin: strings.NewReader(stdin), Stdout: &stdout, Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

// TestExamplesAreThePythonFiles holds the embedded examples to the Python package's
// while those files exist.
func TestExamplesAreThePythonFiles(t *testing.T) {
	source := filepath.Join("..", "..", "src", "dev_health_ops", "api", "external_ingest", "examples")
	if _, err := os.Stat(source); err != nil {
		t.Skip("the Python examples are gone: the embedded copies are the source")
	}
	entries, err := examples.ReadDir("examples")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		want, err := os.ReadFile(filepath.Join(source, entry.Name()))
		if err != nil {
			t.Errorf("%s: %v", entry.Name(), err)
			continue
		}
		got, _ := examples.ReadFile("examples/" + entry.Name())
		if !bytes.Equal(got, want) {
			t.Errorf("%s differs from the Python example", entry.Name())
		}
	}
	pythonFiles, _ := filepath.Glob(filepath.Join(source, "*.json"))
	if len(pythonFiles) != len(entries) {
		t.Errorf("%d embedded examples, %d Python examples", len(entries), len(pythonFiles))
	}
}

// Every record kind has an example and a correlation-id rule, and its sample
// validates as the API would accept it.
func TestEveryKindHasASampleThatValidates(t *testing.T) {
	for _, kind := range recordKinds() {
		record, err := sampleRecord(kind)
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		text, err := dumps(wrapBatchEnvelope([]pyjson.Value{record}, "k"), true, false)
		if err != nil {
			t.Fatal(err)
		}
		code, stdout, _ := run(t, text, "validate", "-")
		if code != 0 || !strings.HasPrefix(stdout, "valid: 1 record(s) accepted") {
			t.Errorf("%s: sample does not validate: exit %d, %q", kind, code, stdout)
		}
	}
}

func TestUsageErrorsExitTwoBeforeReadingAnything(t *testing.T) {
	for _, args := range [][]string{
		{"validate"}, {"validate", "a", "b"}, {"validate", "--schema", "v9", "-"}, {"validate", "--nope", "-"},
		{"sample"}, {"sample", "--kind", "nope"}, {"sample", "--kind", "commit", "--all"}, {"sample", "--all", "x"},
		{"export"}, {"export", "a", "b"},
	} {
		if code, stdout, _ := run(t, "", args...); code != 2 || stdout != "" {
			t.Errorf("%v: exit %d, stdout %q, want a usage error", args, code, stdout)
		}
	}
}

func TestExportIsNotImplementedForAnyProvider(t *testing.T) {
	code, stdout, stderr := run(t, "", "export", "github", "--repo", "a/b")
	if code != 1 || stdout != "" || !strings.Contains(stderr, "`push export github` is not implemented in v1") {
		t.Fatalf("exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

func TestFlagsMayFollowThePositional(t *testing.T) {
	if code, stdout, _ := run(t, "{}", "validate", "-", "--json"); code != 1 || !strings.Contains(stdout, `"valid": false`) {
		t.Fatalf("exit %d, stdout %q", code, stdout)
	}
}
