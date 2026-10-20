// Package programoracle replays the frozen answers of the Python programs that
// oracles compared a Go port with: a program, a corpus on stdin, an answer on
// stdout. The answers were executed once, on the last Python-bearing build, and
// are frozen in golden files; no Python runs and none can run again.
//
// It adds no rule of its own. It is the sequence every such test would
// otherwise spell out against venueoracle's golden: open the golden, ask for
// each program's answer by its name and its stdin (venueoracle.ProgramRequest),
// take the answers, finish. The golden's guards apply: digest pin, the name and
// stdin of each program in order, every answer used once, none left over.
//
// The answers are the producer's exact stdout. The comparison is the
// caller's: compare by value, and decode no bare JSON number through float64
// (decode with json.Decoder.UseNumber and compare the literal text, or decode
// into exact integer fields).
package programoracle

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// Program is one frozen run of a program.
type Program struct {
	// Name identifies the run in the golden. Names are unique within one call.
	Name string
	// Stdin is the program's input. Its digest is part of the request.
	Stdin []byte
}

// Answer is what one program gave: its exit code and its stdout. A program
// that failed is an answer with a non-zero exit code, never an empty answer.
type Answer struct {
	ExitCode int
	Stdout   string
}

const packedPrefix = "gzip+base64:"

// IdentityName is the name of the producer identity program (Identity).
const IdentityName = "producer identity"

// Identity is the producer identity program: the Python version, the Unicode
// data version and the version of each distribution the programs imported,
// as they were recorded. A test puts it first in its list and compares the
// answer with a constant (RequireIdentity), so a golden of another producer is
// refused by name.
func Identity() Program { return Program{Name: IdentityName} }

// RequireIdentity fails the test unless answer is the identity want names. The
// failure names both.
func RequireIdentity(t *testing.T, answer Answer, want string) {
	t.Helper()
	if err := IdentityErr(answer, want); err != nil {
		t.Fatal(err)
	}
}

// IdentityErr is nil when answer is the identity want names, and otherwise an
// error that names both.
func IdentityErr(answer Answer, want string) error {
	found := strings.TrimSpace(answer.Stdout)
	if answer.ExitCode != 0 {
		return fmt.Errorf("the producer identity program exited %d (stdout %q): the answers were not produced by a known interpreter", answer.ExitCode, found)
	}
	if found != strings.TrimSpace(want) {
		return fmt.Errorf("the frozen answers were produced by another producer than the test pins.\nexpected identity:\n%s\nfound identity:\n%s",
			strings.TrimSpace(want), found)
	}
	return nil
}

// Run returns the frozen answer of each program, in order.
func Run(t *testing.T, spec venueoracle.GoldenSpec, programs []Program) []Answer {
	t.Helper()
	golden := venueoracle.OpenGolden(t, spec)
	answers := Produce(t, golden, programs)
	golden.SkipDiff(t)
	golden.Finish(t)
	return answers
}

// Produce returns the frozen answer of each program, in order, from a golden
// the caller opened and will finish: a test whose golden also belongs to a
// venue (venueoracle.Options.Golden) asks for its programs through it. Every
// answer is marked as consumed: the caller compares it.
func Produce(t *testing.T, golden *venueoracle.Golden, programs []Program) []Answer {
	t.Helper()
	if err := programsErr(programs); err != nil {
		t.Fatal(err)
	}
	requests := make([]venueoracle.Request, len(programs))
	for index, program := range programs {
		requests[index] = venueoracle.ProgramRequest(program.Name, program.Stdin)
	}
	responses := golden.Produce(t, requests)
	golden.Consumed(t, responses...)
	answers := make([]Answer, len(responses))
	for index, response := range responses {
		stdout := response.Body
		if strings.HasPrefix(stdout, packedPrefix) {
			stdout = venueoracle.UnpackBody(t, stdout)
		}
		answers[index] = Answer{ExitCode: response.Status, Stdout: stdout}
	}
	return answers
}

// programsErr is an error for a list a golden cannot hold: no program, a
// program without a name, or two programs with one name.
func programsErr(programs []Program) error {
	if len(programs) == 0 {
		return errors.New("programoracle: no program: a golden with no answer compares nothing")
	}
	seen := map[string]bool{}
	for index, program := range programs {
		if program.Name == "" {
			return fmt.Errorf("programoracle: program %d needs a name", index)
		}
		if seen[program.Name] {
			return fmt.Errorf("programoracle: two programs are named %q: an answer is found by its program's name", program.Name)
		}
		seen[program.Name] = true
	}
	return nil
}

// Set is the goldens of one package's program oracles: the build and the
// producer they were recorded from, and their pins.
type Set struct {
	// Build is the 40-hex commit whose interpreter answered.
	Build string
	// Identity is the pinned producer identity (see Identity).
	Identity string
	// Pins maps each golden file name under testdata/golden to its SHA-256.
	Pins map[string]string
}

// Outputs returns the stdout of each program, in order, from the golden named
// golden under testdata/golden of the running test's package. It fails the
// test when the golden is not pinned in the set, when the producer identity is
// not the pinned one, or when a program exited non-zero when it was recorded.
// The identity program is asked for first, as its own request in the same golden.
func (set Set) Outputs(t *testing.T, golden string, programs ...Program) []string {
	t.Helper()
	pin, pinned := set.Pins[golden]
	if !pinned {
		t.Fatalf("test %s has no frozen golden: the pins of the set name no %s. A frozen oracle never runs Python and never skips", t.Name(), golden)
	}
	spec := venueoracle.GoldenSpec{
		Path:        filepath.Join("testdata", "golden", golden),
		PythonBuild: set.Build,
		SHA256:      pin,
	}
	all := append([]Program{Identity()}, programs...)
	outputs, err := successfulOutputs(set.Identity, programs, Run(t, spec, all))
	if err != nil {
		t.Fatal(err)
	}
	return outputs
}

// successfulOutputs is the stdout of each program, given the answers of the
// identity program and of programs. It is an error when the identity is not
// the pinned one, or when a program exited non-zero when it was recorded: a
// failed producer is not an answer.
func successfulOutputs(identity string, programs []Program, answers []Answer) ([]string, error) {
	if len(answers) != len(programs)+1 {
		return nil, fmt.Errorf("%d answers for the identity program and %d programs", len(answers), len(programs))
	}
	if err := IdentityErr(answers[0], identity); err != nil {
		return nil, err
	}
	outputs := make([]string, 0, len(programs))
	for index, answer := range answers[1:] {
		name := programs[index].Name
		if answer.ExitCode != 0 {
			return nil, fmt.Errorf("program %q exited %d when it was recorded (stdout %q): a failed producer is not an answer", name, answer.ExitCode, answer.Stdout)
		}
		outputs = append(outputs, answer.Stdout)
	}
	return outputs, nil
}
