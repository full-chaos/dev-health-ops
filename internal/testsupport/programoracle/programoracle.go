// Package programoracle freezes the answers of Python programs for the
// oracles that compare a Go port with what Python computes over a corpus: a
// `python -c` program, a corpus on stdin, an answer on stdout.
//
// It adds no rule of its own. It is the sequence every such test would
// otherwise spell out against venueoracle's golden: open the golden, hand each
// program to Golden.Produce as a ProgramRequest, take the answers, finish. In a
// frozen run (every run but a recording) no Python runs and the answers come
// from the golden file; the golden's guards apply (digest pin, the request key
// of each program, every answer used once, none left over). In a recording
// (the goldenrecord verb) each program is executed by the interpreter of the
// pinned checkout, with an environment built here and nothing inherited.
//
// The answers are the producer's exact stdout. The comparison is the
// caller's: compare by value, and decode no bare JSON number through float64
// (decode with json.Decoder.UseNumber and compare the literal text, or decode
// into exact integer fields).
package programoracle

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// Program is one run of an inline Python program.
type Program struct {
	// Name identifies the run in the golden. Names are unique within one call.
	Name string
	// Text is the program, run as `python -c Text`. Its digest is part of the
	// request, so a changed program is another request.
	Text string
	// Stdin is the program's input. Its digest is part of the request.
	Stdin []byte
	// Env holds the environment entries that shape this program's answer, by
	// exact name. They are part of the request and are the only entries the
	// program gets besides the defaults (DefaultEnv) and the interpreter's own
	// (PATH, HOME, the pinned sources on PYTHONPATH, no bytecode).
	Env map[string]string
}

// Answer is what one program gave: its exit code and its stdout. A program
// that fails is an answer with a non-zero exit code, never an empty answer.
type Answer struct {
	ExitCode int
	Stdout   string
}

// DefaultEnv is in every program's environment and request: a fixed hash seed
// and UTF-8 mode, so an answer does not depend on either.
var DefaultEnv = map[string]string{"PYTHONHASHSEED": "0", "PYTHONUTF8": "1"}

// packAbove is the stdout size above which a golden stores the answer packed
// (venueoracle.PackBody): the same bytes, compressed.
const packAbove = 64 << 10

const packedPrefix = "gzip+base64:"

// IdentityName is the name of the producer identity program (Identity).
const IdentityName = "producer identity"

// Identity is a program that prints the identity of the producer when the
// producer is the interpreter or an installed package and not the repository's
// own sources (the golden header identifies only those): the Python version,
// the Unicode data version, and the version of each named distribution, one
// "name version" line each. A test puts it first in its list and compares the
// answer with a constant (RequireIdentity), so a golden recorded by another
// interpreter or another package version is refused by name.
func Identity(distributions ...string) Program {
	sorted := append([]string(nil), distributions...)
	sort.Strings(sorted)
	var text strings.Builder
	text.WriteString("import sys, unicodedata\n")
	text.WriteString("from importlib import metadata\n")
	text.WriteString("print('python %d.%d.%d' % sys.version_info[:3])\n")
	text.WriteString("print('unicodedata ' + unicodedata.unidata_version)\n")
	for _, distribution := range sorted {
		fmt.Fprintf(&text, "print(%q + ' ' + metadata.version(%q))\n", distribution, distribution)
	}
	return Program{Name: IdentityName, Text: text.String()}
}

// RequireIdentity fails the test unless answer is the identity want names. The
// failure names both.
func RequireIdentity(t *testing.T, answer Answer, want string) {
	t.Helper()
	if err := identityErr(answer, want); err != nil {
		t.Fatal(err)
	}
}

func identityErr(answer Answer, want string) error {
	found := strings.TrimSpace(answer.Stdout)
	if answer.ExitCode != 0 {
		return fmt.Errorf("the producer identity program exited %d (stdout %q): the answers were not produced by a known interpreter", answer.ExitCode, found)
	}
	if found != strings.TrimSpace(want) {
		return fmt.Errorf("the answers were produced by another producer than the test pins.\nexpected identity:\n%s\nfound identity:\n%s\n"+
			"Record with the interpreter and packages of the pinned build (its .venv), or, when the producer moved on purpose, re-read the port against it and pin the new identity",
			strings.TrimSpace(want), found)
	}
	return nil
}

// Run returns the answer of each program, in order. root is the repository
// root of the running test; a recording replaces it by the pinned checkout
// (venueoracle.Golden.PythonRoot).
func Run(t *testing.T, spec venueoracle.GoldenSpec, root string, programs []Program) []Answer {
	t.Helper()
	if err := programsErr(programs); err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, spec)
	pythonRoot := golden.PythonRoot(t, root)
	requests := make([]venueoracle.Request, len(programs))
	for index, program := range programs {
		requests[index] = venueoracle.ProgramRequest(program.Name, program.Text, program.Stdin, keyedEnv(program))
	}
	responses := golden.Produce(t, pythonRoot, requests, func(pinnedRoot string, _ []venueoracle.Request) []venueoracle.Response {
		recorded := make([]venueoracle.Response, len(programs))
		for index, program := range programs {
			exitCode, stdout := execute(t, pinnedRoot, program)
			body := string(stdout)
			if len(stdout) > packAbove {
				body = venueoracle.PackBody(stdout)
			}
			recorded[index] = venueoracle.Response{Status: exitCode, Body: body}
		}
		return recorded
	})
	golden.Consumed(t, responses...)
	answers := make([]Answer, len(responses))
	for index, response := range responses {
		stdout := response.Body
		if strings.HasPrefix(stdout, packedPrefix) {
			stdout = venueoracle.UnpackBody(t, stdout)
		}
		answers[index] = Answer{ExitCode: response.Status, Stdout: stdout}
	}
	golden.SkipDiff(t)
	golden.Finish(t)
	return answers
}

// programsErr is an error for a list a golden cannot hold: no program, a
// program without a name or a text, or two programs with one name.
func programsErr(programs []Program) error {
	if len(programs) == 0 {
		return errors.New("programoracle: no program: a golden with no answer compares nothing")
	}
	seen := map[string]bool{}
	for index, program := range programs {
		if program.Name == "" || program.Text == "" {
			return fmt.Errorf("programoracle: program %d needs a name and a text", index)
		}
		if seen[program.Name] {
			return fmt.Errorf("programoracle: two programs are named %q: an answer is found by its program's name", program.Name)
		}
		seen[program.Name] = true
	}
	return nil
}

// keyedEnv is the environment that identifies a program's request: the
// defaults, then the program's own entries.
func keyedEnv(program Program) map[string]string {
	keyed := make(map[string]string, len(DefaultEnv)+len(program.Env))
	for name, value := range DefaultEnv {
		keyed[name] = value
	}
	for name, value := range program.Env {
		keyed[name] = value
	}
	return keyed
}

// interpreterEnv is the whole environment a recording gives the interpreter:
// PATH and HOME of the recording process, the pinned sources first on the
// module path, no bytecode written, and the keyed entries. Nothing else of
// the recording process's environment reaches the program.
func interpreterEnv(pinnedRoot string, program Program) []string {
	environment := []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
		"PYTHONPATH=" + filepath.Join(pinnedRoot, "src"), "PYTHONDONTWRITEBYTECODE=1",
	}
	keyed := keyedEnv(program)
	names := make([]string, 0, len(keyed))
	for name := range keyed {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		environment = append(environment, name+"="+keyed[name])
	}
	return environment
}

// execute runs program with the interpreter of the pinned checkout and returns
// its exit code and stdout. An interpreter that cannot be started, or that is
// older than the deployed release, fails the test.
func execute(t *testing.T, pinnedRoot string, program Program) (int, []byte) {
	t.Helper()
	python := pyoracle.Resolve(t, pinnedRoot)
	probe, probeErr := exec.Command(python, pyoracle.VersionProbeArgs...).Output()
	pyoracle.RequireDeployed(t, python, probe, probeErr)
	command := exec.Command(python, "-c", program.Text)
	command.Stdin = bytes.NewReader(program.Stdin)
	command.Env = interpreterEnv(pinnedRoot, program)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdout, err := command.Output()
	if err == nil {
		return 0, stdout
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		t.Logf("programoracle: program %q exited %d; stderr: %s", program.Name, exit.ExitCode(), stderr.String())
		return exit.ExitCode(), stdout
	}
	t.Fatalf("programoracle: program %q: %v", program.Name, pyoracle.RunError(python, err, stderr.Bytes()))
	return 0, nil
}
