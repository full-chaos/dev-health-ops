package venueoracle

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// checkoutWithInterpreter is a directory that looks like a checkout with its
// own virtual environment, whose python3 prints the named variables it sees.
func checkoutWithInterpreter(t *testing.T, names ...string) string {
	t.Helper()
	root := t.TempDir()
	script := "#!/bin/sh\nfor n in " + strings.Join(names, " ") + "; do eval \"v=\\${$n-<unset>}\"; echo \"$n=$v\"; done\n"
	for _, name := range []string{"python", "python3"} {
		file := filepath.Join(root, ".venv", "bin", name)
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func seenBy(t *testing.T, command *exec.Cmd) map[string]string {
	t.Helper()
	output, err := command.Output()
	if err != nil {
		t.Fatalf("the stand-in interpreter failed: %v", err)
	}
	seen := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		name, value, _ := strings.Cut(line, "=")
		seen[name] = value
	}
	return seen
}

// The command a producer gets: the checkout's interpreter in the one closed
// environment, the request's declared entries and the call's own entries
// after it, and nothing of the process.
func TestTheProducersCommandRunsInTheClosedEnvironment(t *testing.T) {
	names := []string{"VENUE_AMBIENT", "PYTHONHASHSEED", "TZ", "LANG", "PYTHONPATH", "OTEL_ENABLED", "HOME", "DECLARED_A", "DECLARED_B", "DATABASE_URI", producerPoisonName}
	root := checkoutWithInterpreter(t, names...)
	t.Setenv("VENUE_AMBIENT", "ambient")
	t.Setenv("PYTHONHASHSEED", "123")
	t.Setenv("TZ", "Pacific/Auckland")
	producer := &Producer{Root: root, t: t}
	declared := map[string]string{"DECLARED_B": "2", "DECLARED_A": "1"}
	// As Produce does while the producer runs.
	defer poisonInheritedEnvironment()()
	command, err := producer.Command(context.Background(), declared, []string{"DATABASE_URI=postgresql://run"}, "-c", "pass")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(root, ".venv", "bin", "python3"); command.Path != want || command.Args[0] != want {
		t.Fatalf("the command runs %s under the name %s, want the checkout's interpreter %s under its full path (a bare name is looked up in the child's PATH, which is the host's)", command.Path, command.Args[0], want)
	}
	if got, want := strings.Join(command.Env, "\n"), strings.Join(pyoracle.ClosedEnv(root, "DECLARED_A=1", "DECLARED_B=2", "DATABASE_URI=postgresql://run"), "\n"); got != want {
		t.Fatalf("the command's environment:\n%s\nwant the one closed form with the declared entries in name order and the call's entries last:\n%s", got, want)
	}
	seen := seenBy(t, command)
	for name, want := range map[string]string{
		"VENUE_AMBIENT":    "<unset>", // nothing of the process
		"HOME":             "<unset>",
		producerPoisonName: "<unset>", // the guard's poison is the process's, not the child's
		"PYTHONHASHSEED":   "0",       // the closed form's value, not the process's
		"TZ":               "UTC",
		"LANG":             "C.UTF-8",
		"OTEL_ENABLED":     "false",
		"PYTHONPATH":       filepath.Join(root, "src"),
		"DECLARED_A":       "1",
		"DECLARED_B":       "2",
		"DATABASE_URI":     "postgresql://run",
	} {
		if seen[name] != want {
			t.Errorf("the producer's child sees %s=%q, want %q", name, seen[name], want)
		}
	}

	// A child started the old way, with the process environment, holds the
	// poison: a real interpreter cannot start with it.
	inherited := exec.Command(filepath.Join(root, ".venv", "bin", "python3"))
	inherited.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	if got := seenBy(t, inherited)[producerPoisonName]; got != producerPoison {
		t.Errorf("a child that inherits the process environment sees %s=%q, want the poison", producerPoisonName, got)
	}

	// Another python3 first on PATH after the producer fixed its interpreter: refused.
	other := checkoutWithInterpreter(t, names...)
	t.Setenv("PATH", filepath.Join(other, ".venv", "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	if _, err := producer.Command(context.Background(), declared, nil, "-c", "pass"); err == nil || !strings.Contains(err.Error(), "another interpreter would answer under the same key") {
		t.Errorf("another python3 first on PATH: err = %v, want a refusal", err)
	}
}

// The poison is in the process only while a producer runs, and what was
// there before comes back.
func TestThePoisonIsThereOnlyWhileAProducerRuns(t *testing.T) {
	os.Unsetenv(producerPoisonName)
	restore := poisonInheritedEnvironment()
	if got := os.Getenv(producerPoisonName); got != producerPoison {
		t.Fatalf("%s = %q, want the poison", producerPoisonName, got)
	}
	if _, err := os.Stat(producerPoison); err == nil {
		t.Fatalf("the poison directory %s exists: an interpreter could start with it", producerPoison)
	}
	restore()
	if got, held := os.LookupEnv(producerPoisonName); held {
		t.Fatalf("after the producer %s = %q, want it unset", producerPoisonName, got)
	}
	t.Setenv(producerPoisonName, "/the/tests/own")
	poisonInheritedEnvironment()()
	if got := os.Getenv(producerPoisonName); got != "/the/tests/own" {
		t.Fatalf("after the producer %s = %q, want the value it had", producerPoisonName, got)
	}
	// The poison is the harness's, never the test's: no venue child gets it
	// and no key holds it.
	defer poisonInheritedEnvironment()()
	if set := strings.Join(testSetEnv(), " "); strings.Contains(set, producerPoisonName) {
		t.Errorf("the test-set variables hold the poison: %q", set)
	}
}
