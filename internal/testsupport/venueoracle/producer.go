package venueoracle

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// A Produce oracle's Python child gets one closed environment, and the
// harness, not the test, starts it.
//
// The closed environment is pyoracle.ClosedEnv, the one form of this
// repository: a fixed, named set (search path, locale, time zone, hash seed,
// the checkout's src), then the request's declared entries (the ones its key
// holds, ProgramRequest), then the entries a call passes by name (the address
// of the run's own database). Nothing of the test process's environment
// reaches the child.
//
// A producer that starts Python itself with the process environment is
// stopped by a guard on behaviour, not on text: while a producer runs in a
// recording, the test process holds producerPoisonName set to a directory
// that does not exist, so a Python child that inherits the process
// environment cannot start. The child Producer.Command builds does not carry
// it.
//
// NOT covered: a producer that copies the process environment and removes
// producerPoisonName by name. That is a deliberate edit, visible in a review.

// producerPoisonName is the variable the guard sets in the test process while
// a producer runs: the interpreter reads it before anything else, and with a
// directory that does not exist it cannot start.
const producerPoisonName = "PYTHONHOME"

// producerPoison is the directory that does not exist. Its name is what the
// interpreter prints when it fails, so the failure says what happened.
const producerPoison = "/a-python-producer-inherited-the-test-process-environment--start-it-with-the-command-Produce-gives"

// poisonInheritedEnvironment puts the poison in the process and returns what
// takes it out again.
func poisonInheritedEnvironment() (restore func()) {
	before, held := os.LookupEnv(producerPoisonName)
	os.Setenv(producerPoisonName, producerPoison)
	return func() {
		if held {
			os.Setenv(producerPoisonName, before)
			return
		}
		os.Unsetenv(producerPoisonName)
	}
}

// Producer starts the Python side of a Produce oracle while it is recorded.
type Producer struct {
	// Root is the checkout the producer runs from: the one golden.PythonRoot
	// verified.
	Root   string
	t      *testing.T
	python string
}

// activate resolves Root's interpreter, once, and activates it as Start does:
// its directory first on PATH, the program run as "python3". It runs at the
// first Command, so a producer that starts no Python needs none.
func (p *Producer) activate() error {
	if p.python != "" {
		return nil
	}
	bin, err := interpreterDir(pyoracle.Resolve(p.t, p.Root))
	if err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	p.t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if found, err := exec.LookPath("python3"); err != nil || filepath.Dir(found) != bin {
		return fmt.Errorf("produce: python3 on PATH is %q (%v), want the one in %s", found, err, bin)
	}
	p.python = filepath.Join(bin, "python3")
	return nil
}

// Command is the producer's interpreter with args, in the closed environment:
// pyoracle.ClosedEnv for Root, then declared (the entries the request's key
// holds; pass the map the request was built with), then extra (NAME=VALUE
// entries made for this run, by name: the address of the run's database). The
// launch is refused unless python3 through PATH is still the interpreter the
// producer fixed.
func (p *Producer) Command(ctx context.Context, declared map[string]string, extra []string, args ...string) (*exec.Cmd, error) {
	if err := p.activate(); err != nil {
		return nil, err
	}
	command := exec.CommandContext(ctx, "python3", args...)
	switch {
	case command.Err != nil:
		return nil, fmt.Errorf("produce: python3 is not found through PATH any more (%v); the producer's interpreter is %s: PATH changed after the producer was built", command.Err, p.python)
	case command.Path != p.python:
		return nil, fmt.Errorf("produce: python3 through PATH is now %s, the producer's interpreter is %s: PATH changed after the producer was built, and another interpreter would answer under the same key", command.Path, p.python)
	}
	command.Env = p.Env(declared, extra...)
	return command, nil
}

// Env is the closed environment Command gives the child.
func (p *Producer) Env(declared map[string]string, extra ...string) []string {
	names := make([]string, 0, len(declared))
	for name := range declared {
		names = append(names, name)
	}
	sort.Strings(names)
	entries := make([]string, 0, len(names)+len(extra))
	for _, name := range names {
		entries = append(entries, name+"="+declared[name])
	}
	return pyoracle.ClosedEnv(p.Root, append(entries, extra...)...)
}
