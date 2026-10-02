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
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path"
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
	// Text is the program, run as `python3 -c Text` with the pinned checkout
	// as its working directory. Its digest is part of the request, so a
	// changed program is another request.
	Text string
	// Stdin is the program's input. Its digest is part of the request.
	Stdin []byte
	// Env holds the environment entries that shape this program's answer, by
	// exact name. They are part of the request and are the only entries the
	// program gets besides the defaults (DefaultEnv) and the interpreter's own
	// (PATH, HOME, the pinned sources on PYTHONPATH, no bytecode).
	Env map[string]string
	// PerRun gives the entries of one run only, such as the address of a
	// database that exists for this run. It is called at a recording, the
	// entries are set for the program by name, and they are not part of the
	// request: a value that differs from run to run cannot be in a key. A
	// recording fails when the program's answer holds such a value or a part
	// of an address in it (perRunParts), and what the program wrote to
	// stderr is logged with those parts replaced.
	PerRun func() map[string]string
	// PerRunNames are the names of the entries PerRun gives: the same names, a
	// run's PerRun must give exactly these. The names (not the values) are
	// part of the request, so a program recorded with another set of per-run
	// entries is another request and the golden must be recorded again; each
	// name must be on the closed list perRunProgramNames. Required with
	// PerRun, refused without it.
	PerRunNames []string
}

// perRunProgramNames is the closed list of the names a program may be given
// for one run, with why: the value of each is made for the run (a database that
// exists for it, a gateway the test starts), so it cannot be in a key by value.
// A name is added here with the test that needs it, in the same change.
var perRunProgramNames = map[string]string{
	"ORACLE_DATABASE_URI":      "the address of the run's own database (the scheduler venue oracles)",
	"ATLASSIAN_ORACLE_GATEWAY": "the address of the Atlassian gateway the test starts (the teams-client oracle)",
}

// perRunNamesKey is the keyed entry that holds the sorted per-run names in a
// request. It is never given to the program.
const perRunNamesKey = "PROGRAMORACLE_PER_RUN_NAMES"

// Answer is what one program gave: its exit code and its stdout. A program
// that fails is an answer with a non-zero exit code, never an empty answer.
type Answer struct {
	ExitCode int
	Stdout   string
}

// Script is the program that runs a script of this repository as its file
// would run. path is the script's place under the repository root and text is
// its source. The script sees __name__ "__main__" and, as __file__, its place
// in the checkout the interpreter runs in: a script that finds the repository
// from its own location finds the pinned checkout, and the source that runs is
// the test's own, which is part of the request.
func Script(name, path, text string, stdin []byte) Program {
	return Program{Name: name, Stdin: stdin, Text: "import os\n" +
		"path = os.path.join(os.getcwd(), " + pythonString(path) + ")\n" +
		"exec(compile(" + pythonString(text) + ", path, \"exec\"), {\"__name__\": \"__main__\", \"__file__\": path})\n"}
}

// pythonString is value as a Python string literal. A JSON string is one:
// every escape the encoder writes means the same in Python.
func pythonString(value string) string {
	var literal bytes.Buffer
	encoder := json.NewEncoder(&literal)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		panic(err)
	}
	return strings.TrimSuffix(literal.String(), "\n")
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
	golden := venueoracle.OpenGolden(t, spec)
	answers := Produce(t, golden, golden.PythonRoot(t, root), programs)
	golden.SkipDiff(t)
	golden.Finish(t)
	return answers
}

// Produce returns the answer of each program, in order, from a golden the
// caller opened and will finish: a test whose golden also belongs to a venue
// (venueoracle.Options.Golden) runs its programs through it. pythonRoot is
// what golden.PythonRoot returned. Every answer is marked as consumed: the
// caller compares it.
func Produce(t *testing.T, golden *venueoracle.Golden, pythonRoot string, programs []Program) []Answer {
	t.Helper()
	if err := programsErr(programs); err != nil {
		t.Fatal(err)
	}
	requests := make([]venueoracle.Request, len(programs))
	for index, program := range programs {
		requests[index] = venueoracle.ProgramRequest(program.Name, program.Text, program.Stdin, keyedEnv(program))
	}
	responses := golden.Produce(t, pythonRoot, requests, func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
		pinnedRoot := producer.Root
		activateInterpreter(t, pinnedRoot)
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
		if err := perRunNamesErr(program); err != nil {
			return err
		}
	}
	return nil
}

// perRunNamesErr is an error when a program's declared per-run names are not
// what PerRun needs: a name that is not on the closed list, a repeated name,
// names with no PerRun, or a PerRun with no names.
func perRunNamesErr(program Program) error {
	if program.PerRun == nil && len(program.PerRunNames) > 0 {
		return fmt.Errorf("programoracle: program %q declares per-run names %v and has no PerRun", program.Name, program.PerRunNames)
	}
	if program.PerRun != nil && len(program.PerRunNames) == 0 {
		return fmt.Errorf("programoracle: program %q has a PerRun and declares no PerRunNames: the names of its per-run entries are part of its request", program.Name)
	}
	if _, held := program.Env[perRunNamesKey]; held {
		return fmt.Errorf("programoracle: program %q sets %s in Env: it is the request's own entry", program.Name, perRunNamesKey)
	}
	seen := map[string]bool{}
	for _, name := range program.PerRunNames {
		if _, keyed := program.Env[name]; keyed {
			return fmt.Errorf("programoracle: program %q gives %s both in Env (keyed by value) and as a per-run name: one name, one source", program.Name, name)
		}
		if _, listed := perRunProgramNames[name]; !listed {
			return fmt.Errorf("programoracle: program %q: %s is not on the closed list of per-run names (perRunProgramNames); add it there with the reason", program.Name, name)
		}
		if seen[name] {
			return fmt.Errorf("programoracle: program %q declares the per-run name %s twice", program.Name, name)
		}
		seen[name] = true
	}
	return nil
}

// perRunGivenErr is an error when the entries PerRun gave are not exactly the
// declared names.
func perRunGivenErr(program Program, given map[string]string) error {
	want := append([]string(nil), program.PerRunNames...)
	sort.Strings(want)
	got := sortedNames(given)
	if strings.Join(want, ",") != strings.Join(got, ",") {
		return fmt.Errorf("programoracle: program %q gave the per-run entries %v, its PerRunNames are %v: the names are part of its request", program.Name, got, want)
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
	if len(program.PerRunNames) > 0 {
		names := append([]string(nil), program.PerRunNames...)
		sort.Strings(names)
		keyed[perRunNamesKey] = strings.Join(names, ",")
	}
	return keyed
}

// interpreterEnv is the whole environment a recording gives the interpreter:
// the closed environment every recording of the repository uses
// (pyoracle.ClosedEnv), then the keyed entries, then the program's entries of
// this run. Nothing of the recording process's environment reaches the
// program.
func interpreterEnv(pinnedRoot string, program Program, perRun map[string]string) []string {
	keyed := keyedEnv(program)
	extra := make([]string, 0, len(keyed)+len(perRun))
	for _, name := range sortedNames(keyed) {
		if name == perRunNamesKey {
			continue // the request's, never the program's
		}
		extra = append(extra, name+"="+keyed[name])
	}
	for _, name := range sortedNames(perRun) {
		extra = append(extra, name+"="+perRun[name])
	}
	return pyoracle.ClosedEnv(pinnedRoot, extra...)
}

func sortedNames(entries map[string]string) []string {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// perRunPart is one text of a per-run entry that a recording must keep out of
// a golden and out of a log.
type perRunPart struct{ name, what, text string }

// perRunMinimum is the shortest part that is searched for: a shorter text
// (a one-letter user name) occurs in an answer by chance.
const perRunMinimum = 6

// perRunParts is the value of each per-run entry and, when the value is an
// address, its host with the port, its password and its last path element (a
// database name): an answer or an error message usually holds a part of an
// address, not the whole of it.
func perRunParts(perRun map[string]string) []perRunPart {
	var parts []perRunPart
	add := func(name, what, text string) {
		if len(text) >= perRunMinimum {
			parts = append(parts, perRunPart{name: name, what: what, text: text})
		}
	}
	for _, name := range sortedNames(perRun) {
		value := perRun[name]
		add(name, "value", value)
		parsed, err := url.Parse(value)
		if err != nil || parsed.Host == "" {
			continue
		}
		add(name, "host and port", parsed.Host)
		if password, ok := parsed.User.Password(); ok {
			add(name, "password", password)
		}
		add(name, "last path element", path.Base(parsed.Path))
	}
	return parts
}

// perRunErr is an error when text holds a part of a per-run entry. The message
// names the entry and the part and does not print it.
func perRunErr(program, text string, perRun map[string]string) error {
	for _, part := range perRunParts(perRun) {
		if strings.Contains(text, part.text) {
			return fmt.Errorf("programoracle: the answer of program %q holds the %s of the per-run entry %s: a golden cannot hold a value of one run, and such a value is not to be stored; print in the program only what is the same in every run",
				program, part.what, part.name)
		}
	}
	return nil
}

// withoutPerRun is text with every part of a per-run entry replaced by its
// name, for a log line. A whole value is replaced before its parts.
func withoutPerRun(text string, perRun map[string]string) string {
	for _, part := range perRunParts(perRun) {
		text = strings.ReplaceAll(text, part.text, "<"+part.name+" "+part.what+">")
	}
	return text
}

// activateInterpreter puts the directory of the pinned checkout's interpreter
// first on PATH for the rest of the test, as `source bin/activate` does, and
// checks that the python3 a command would start is the one in that directory.
// A recording starts the program as "python3", never by a path.
func activateInterpreter(t *testing.T, pinnedRoot string) {
	t.Helper()
	bin, err := venueoracle.ActivateInterpreter(t, pinnedRoot)
	if err != nil {
		t.Fatalf("programoracle: %v", err)
	}
	// The probe starts the interpreter by its path, in the closed environment:
	// nothing of the test process's environment reaches it.
	pyoracle.RequireDeployed(t, filepath.Join(bin, "python3"), pinnedRoot)
}

// interpreterDir returns, as an absolute path, the directory of the
// interpreter at path. It must also hold an executable python3: every
// virtualenv and every Python 3 install directory does.
func interpreterDir(path string) (string, error) {
	if !filepath.IsAbs(path) {
		found, err := exec.LookPath(path)
		if err != nil {
			return "", fmt.Errorf("interpreter %q: %w", path, err)
		}
		path = found
	}
	dir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return "", err
	}
	python3 := filepath.Join(dir, "python3")
	info, err := os.Stat(python3)
	if err != nil {
		return "", fmt.Errorf("interpreter %q: no python3 beside it: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", fmt.Errorf("interpreter %q: %s is not an executable file", path, python3)
	}
	return dir, nil
}

// interpreterCommand is the command a recording runs for program: python3 (the
// activated interpreter) with the program as its -c text, in the pinned
// checkout, with the program's input and the interpreter environment. The
// interpreter gets its own full path as its name: it looks for its
// environment (the installed distributions) beside the file it was started
// as, and with only the name it would look the name up on the closed PATH of
// its environment and find another interpreter.
func interpreterCommand(pinnedRoot string, program Program, perRun map[string]string) *exec.Cmd {
	command := exec.Command("python3", "-c", program.Text)
	if command.Err == nil {
		command.Args[0] = command.Path
	}
	command.Dir = pinnedRoot
	command.Stdin = bytes.NewReader(program.Stdin)
	command.Env = interpreterEnv(pinnedRoot, program, perRun)
	return command
}

// executed is what one run of a program gave. stderr holds no part of a
// per-run entry.
type executed struct {
	exitCode int
	stdout   []byte
	stderr   string
}

// executeErr runs program with the activated interpreter. It is an error when
// the interpreter cannot be started and when the answer holds a part of a
// per-run entry.
func executeErr(pinnedRoot string, program Program) (executed, error) {
	var perRun map[string]string
	if program.PerRun != nil {
		perRun = program.PerRun()
		if err := perRunGivenErr(program, perRun); err != nil {
			return executed{}, err
		}
	}
	command := interpreterCommand(pinnedRoot, program, perRun)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdout, err := command.Output()
	result := executed{stdout: stdout, stderr: withoutPerRun(stderr.String(), perRun)}
	if leak := perRunErr(program.Name, string(stdout), perRun); leak != nil {
		return executed{}, leak
	}
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		result.exitCode = exit.ExitCode()
	default:
		return executed{}, fmt.Errorf("programoracle: program %q: %w", program.Name, pyoracle.RunError("python3", err, []byte(result.stderr)))
	}
	return result, nil
}

// execute is executeErr for a recording: an error fails the test, and the
// stderr of a program that exited non-zero is logged.
func execute(t *testing.T, pinnedRoot string, program Program) (int, []byte) {
	t.Helper()
	result, err := executeErr(pinnedRoot, program)
	if err != nil {
		t.Fatal(err)
	}
	if result.exitCode != 0 {
		t.Logf("programoracle: program %q exited %d; stderr: %s", program.Name, result.exitCode, result.stderr)
	}
	return result.exitCode, result.stdout
}

// Set is the goldens of one package's program oracles: where they live, the
// build and the producer they were recorded from, and their pins.
type Set struct {
	// Package is the package pattern the goldenrecord verb takes, for the
	// recipe a failure prints (for example "./internal/pythonparity/").
	Package string
	// Build is the 40-hex commit whose interpreter answered.
	Build string
	// Identity is the pinned producer identity (see Identity).
	Identity string
	// Distributions are the installed distributions the programs import; their
	// versions are part of the identity.
	Distributions []string
	// Pins maps each golden file name under testdata/golden to its SHA-256.
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins map[string]string
}

// Outputs returns the stdout of each program, in order, from the golden named
// golden under testdata/golden of the running test's package. It fails the
// test when the golden is not pinned in the set, when the producer identity is
// not the pinned one, or when a program exited non-zero when it was recorded.
// The identity program is run first, as its own request in the same golden.
func (set Set) Outputs(t *testing.T, root, golden string, programs ...Program) []string {
	t.Helper()
	recipe := fmt.Sprintf("git worktree add --detach $DIR %s (with its .venv: uv sync --frozen --no-install-project); "+
		"then from the repository root: go run ./internal/testsupport/venueoracle/goldenrecord -pkg %s "+
		"-test '^%s$' -python-root $DIR", set.Build, set.Package, strings.SplitN(t.Name(), "/", 2)[0])
	pin, pinned := set.Pins[golden]
	if !pinned {
		t.Fatalf("test %s has no frozen golden: the pins of %s name no %s. A frozen oracle never runs Python and never skips. "+
			"Add the entry with the value %q, then record: %s", t.Name(), set.Package, golden, "PIN:"+strings.TrimSuffix(golden, ".json"), recipe)
	}
	spec := venueoracle.GoldenSpec{
		Path:        filepath.Join("testdata", "golden", golden),
		PythonBuild: set.Build,
		SHA256:      pin,
		Recipe:      recipe,
	}
	all := append([]Program{Identity(set.Distributions...)}, programs...)
	outputs, err := successfulOutputs(set.Identity, programs, Run(t, spec, root, all))
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
