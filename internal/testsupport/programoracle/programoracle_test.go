package programoracle

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// runGoldenPythonBuild is the build whose interpreter answered run.golden.json.
const runGoldenPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// TestRunAnswersFromTheGolden runs the whole path frozen: three programs, one
// golden, the answers in order. The second program exited 3 when it was
// recorded: its exit code and its stdout are both kept, so a failed producer
// is never an empty success. No Python runs here.
func TestRunAnswersFromTheGolden(t *testing.T) {
	_, file, _, _ := moduleroot.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	spec := venueoracle.GoldenSpec{
		Path:        "testdata/golden/run.golden.json",
		PythonBuild: runGoldenPythonBuild,
		SHA256:      "6c3807fe456312d0464a8eca1e752311b8cbbbdd7277792d4d038b73e66b9c45",
		Recipe: "git worktree add --detach $DIR " + runGoldenPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/testsupport/programoracle/ -test '^TestRunAnswersFromTheGolden$' -python-root $DIR",
	}
	answers := Run(t, spec, root, []Program{
		{Name: "upper", Text: "import sys; sys.stdout.write(sys.stdin.read().upper())", Stdin: []byte("corpus")},
		{Name: "fails", Text: "import sys; sys.stdout.write('partial'); sys.exit(3)"},
		{Name: "environment", Text: "import os; print(os.environ.get('ORACLE_CASE'), os.environ.get('PYTHONHASHSEED'), os.environ.get('PYTHONUTF8'))",
			Env: map[string]string{"ORACLE_CASE": "keyed"}},
	})
	want := []Answer{{0, "CORPUS"}, {3, "partial"}, {0, "keyed 0 1\n"}}
	if len(answers) != len(want) {
		t.Fatalf("%d answers for %d programs", len(answers), len(want))
	}
	for index := range want {
		if answers[index] != want[index] {
			t.Errorf("answer %d = %+v, want %+v", index, answers[index], want[index])
		}
	}
}

// TestAScriptRunsAsItsFileInThePinnedCheckout runs Script frozen: the script
// starts with a __future__ import, which only the head of a file may hold, and
// prints where it believes it is, its module name and its input.
func TestAScriptRunsAsItsFileInThePinnedCheckout(t *testing.T) {
	_, file, _, _ := moduleroot.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	spec := venueoracle.GoldenSpec{
		Path:        "testdata/golden/script.golden.json",
		PythonBuild: runGoldenPythonBuild,
		SHA256:      "d0884e9313816f8f0771546274d5d950cde05fc31606301cb9af9f70de142c88",
		Recipe: "git worktree add --detach $DIR " + runGoldenPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/testsupport/programoracle/ -test '^TestAScriptRunsAsItsFileInThePinnedCheckout$' -python-root $DIR",
	}
	const script = "from __future__ import annotations\n" +
		"import os, sys\n" +
		"from pathlib import Path\n" +
		"root = Path(__file__).resolve().parents[2]\n" +
		"print(os.path.relpath(__file__, root), root == Path.cwd().resolve(), (root / \"go.mod\").is_file(), __name__, sys.stdin.read())\n"
	answers := Run(t, spec, root, []Program{Script("script", "some/place/oracle.py", script, []byte("input"))})
	if want := (Answer{0, "some/place/oracle.py True True __main__ input\n"}); answers[0] != want {
		t.Fatalf("answer = %+v, want %+v", answers[0], want)
	}
}

// TestAPythonStringLiteralKeepsEveryCharacter pins pythonString on the
// characters a script holds: quotes, backslashes, line breaks, HTML
// characters and text outside ASCII.
func TestAPythonStringLiteralKeepsEveryCharacter(t *testing.T) {
	got := pythonString("a\"b'c\\d\n<&>\u00e9\t")
	if want := `"a\"b'c\\d\n<&>` + "\u00e9" + `\t"`; got != want {
		t.Fatalf("pythonString = %s, want %s", got, want)
	}
}

// fakeInterpreter puts a python3 that is a shell script first on PATH and
// returns its path. The script gets the program text as its second argument:
// "leak" prints the per-run database entry, "fail" writes it to stderr, prints
// a part of an answer and exits 3, any other text prints "ok".
func fakeInterpreter(t *testing.T) string {
	t.Helper()
	bin := t.TempDir()
	script := "#!/bin/sh\ncase \"$2\" in\n" +
		"leak) printf '%s' \"$ORACLE_DATABASE_URI\" ;;\n" +
		"fail) printf 'cannot reach %s\n' \"$ORACLE_DATABASE_URI\" >&2; printf 'part'; exit 3 ;;\n" +
		"*) printf 'ok' ;;\nesac\n"
	python3 := filepath.Join(bin, "python3")
	if err := os.WriteFile(python3, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return python3
}

// TestTheInterpreterRunsInThePinnedCheckout pins the command of a recording:
// the python3 that is first on PATH, started under its full path, with the
// program as -c text, the pinned checkout as working directory, the program's
// input, and the interpreter environment.
func TestTheInterpreterRunsInThePinnedCheckout(t *testing.T) {
	python3 := fakeInterpreter(t)
	command := interpreterCommand("/pinned", Program{Text: "print(1)", Stdin: []byte("in")}, nil)
	if command.Dir != "/pinned" {
		t.Errorf("working directory = %q", command.Dir)
	}
	if command.Path != python3 {
		t.Errorf("the command starts %q, want the python3 first on PATH (%s)", command.Path, python3)
	}
	if want := []string{python3, "-c", "print(1)"}; !slices.Equal(command.Args, want) {
		t.Errorf("arguments = %q, want %q: the interpreter must get its full path as its name", command.Args, want)
	}
	if input, err := io.ReadAll(command.Stdin); err != nil || string(input) != "in" {
		t.Errorf("input = %q, %v", input, err)
	}
	if !slices.Contains(command.Env, "PYTHONPATH="+filepath.Join("/pinned", "src")) {
		t.Errorf("environment = %q", command.Env)
	}
}

// TestARunRefusesAnAnswerWithAPerRunValueAndLogsNone pins what a run does with
// the entries of one run: an answer that holds one is an error, and the
// stderr it returns for the log holds none.
func TestARunRefusesAnAnswerWithAPerRunValueAndLogsNone(t *testing.T) {
	fakeInterpreter(t)
	address, host, _, _ := perRunAddress()
	perRun := func() map[string]string { return map[string]string{"ORACLE_DATABASE_URI": address} }
	root := t.TempDir()
	if result, err := executeErr(root, Program{Name: "clean", Text: "clean", PerRun: perRun, PerRunNames: []string{"ORACLE_DATABASE_URI"}}); err != nil || result.exitCode != 0 || string(result.stdout) != "ok" {
		t.Fatalf("a clean answer = %+v, %v", result, err)
	}
	result, err := executeErr(root, Program{Name: "leaks", Text: "leak", PerRun: perRun, PerRunNames: []string{"ORACLE_DATABASE_URI"}})
	if err == nil || !strings.Contains(err.Error(), "value of the per-run entry ORACLE_DATABASE_URI") {
		t.Fatalf("an answer that holds the address = %+v, %v: want it refused by the name of the entry", result, err)
	}
	if strings.Contains(err.Error(), host) || len(result.stdout) != 0 {
		t.Fatalf("the refusal hands the address on: %v, stdout %q", err, result.stdout)
	}
	result, err = executeErr(root, Program{Name: "fails", Text: "fail", PerRun: perRun, PerRunNames: []string{"ORACLE_DATABASE_URI"}})
	if err != nil || result.exitCode != 3 || string(result.stdout) != "part" {
		t.Fatalf("a program that exits 3 = %+v, %v", result, err)
	}
	if want := "cannot reach <ORACLE_DATABASE_URI value>\n"; result.stderr != want {
		t.Fatalf("stderr for the log = %q, want %q", result.stderr, want)
	}
	if result, err := executeErr(root, Program{Name: "no entries", Text: "leak"}); err != nil || len(result.stdout) != 0 {
		t.Fatalf("a program with no per-run entry = %+v, %v", result, err)
	}
}

func TestAProgramListMustBeNamedAndUnique(t *testing.T) {
	refused := map[string][]Program{
		"no program":     nil,
		"no name":        {{Text: "print(1)"}},
		"no text":        {{Name: "a"}},
		"duplicate name": {{Name: "a", Text: "print(1)"}, {Name: "a", Text: "print(2)"}},
	}
	for name, programs := range refused {
		if err := programsErr(programs); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if err := programsErr([]Program{{Name: "a", Text: "print(1)"}, {Name: "b", Text: "print(1)"}}); err != nil {
		t.Errorf("two named programs were refused: %v", err)
	}
}

func TestTheKeyedEnvironmentHoldsTheDefaultsAndTheProgramsOwn(t *testing.T) {
	keyed := keyedEnv(Program{Env: map[string]string{"TZ": "UTC", "PYTHONHASHSEED": "7"}})
	want := map[string]string{"PYTHONHASHSEED": "7", "PYTHONUTF8": "1", "TZ": "UTC"}
	if len(keyed) != len(want) {
		t.Fatalf("keyed environment = %v, want %v", keyed, want)
	}
	for name, value := range want {
		if keyed[name] != value {
			t.Errorf("%s = %q, want %q", name, keyed[name], value)
		}
	}
	if DefaultEnv["PYTHONHASHSEED"] != "0" {
		t.Errorf("a program's own entry changed the defaults: %v", DefaultEnv)
	}
}

// TestTheInterpreterGetsNothingOfTheProcessEnvironment pins the recording
// environment: the repository's closed environment for the pinned sources and
// then the keyed entries. No variable of the process reaches the program, its
// PATH and HOME included.
func TestTheInterpreterGetsNothingOfTheProcessEnvironment(t *testing.T) {
	ambient := []string{"must not reach the program", "/ambient/path", "/bin-of-the-test", "/home-of-the-test"}
	t.Setenv("PROGRAMORACLE_AMBIENT", ambient[0])
	t.Setenv("PYTHONPATH", ambient[1])
	t.Setenv("PATH", ambient[2])
	t.Setenv("HOME", ambient[3])
	environment := interpreterEnv("/pinned", Program{Env: map[string]string{"TZ": "UTC"}}, nil)
	want := append(pyoracle.ClosedEnv("/pinned"), "PYTHONHASHSEED=0", "PYTHONUTF8=1", "TZ=UTC")
	if strings.Join(environment, "\n") != strings.Join(want, "\n") {
		t.Fatalf("interpreter environment =\n%s\nwant\n%s", strings.Join(environment, "\n"), strings.Join(want, "\n"))
	}
	text := strings.Join(environment, "\n")
	for _, value := range ambient {
		if strings.Contains(text, value) {
			t.Errorf("the interpreter environment holds %q of the process environment", value)
		}
	}
	if !slices.Contains(environment, "PYTHONPATH="+filepath.Join("/pinned", "src")) {
		t.Errorf("the pinned sources are not on the module path: %v", environment)
	}
}

// TestAPerRunEntryReachesTheProgramAndIsNotInTheRequest pins PerRun: its
// entries are in the interpreter environment, after the keyed ones, and the
// keyed environment (what the request holds) does not have them.
func TestAPerRunEntryReachesTheProgramAndIsNotInTheRequest(t *testing.T) {
	calls := 0
	program := Program{Env: map[string]string{"TZ": "UTC"}, PerRunNames: []string{"ORACLE_DATABASE_URI", "ATLASSIAN_ORACLE_GATEWAY"}, PerRun: func() map[string]string {
		calls++
		return map[string]string{"ORACLE_DATABASE_URI": "address-of-this-run", "ATLASSIAN_ORACLE_GATEWAY": "x"}
	}}
	// The request holds the per-run NAMES (one entry), never a value.
	if keyed := keyedEnv(program); len(keyed) != 4 || keyed[perRunNamesKey] != "ATLASSIAN_ORACLE_GATEWAY,ORACLE_DATABASE_URI" || keyed["ORACLE_DATABASE_URI"] != "" {
		t.Fatalf("the keyed environment = %v: a per-run value is in the request or its names are not", keyed)
	}
	if calls != 0 {
		t.Fatalf("PerRun was called %d times to build the request: it is for a recording only", calls)
	}
	environment := interpreterEnv("/pinned", program, program.PerRun())
	tail := environment[len(environment)-5:]
	if want := []string{"PYTHONHASHSEED=0", "PYTHONUTF8=1", "TZ=UTC", "ATLASSIAN_ORACLE_GATEWAY=x", "ORACLE_DATABASE_URI=address-of-this-run"}; strings.Join(tail, "\n") != strings.Join(want, "\n") {
		t.Fatalf("interpreter environment ends with %q, want %q", tail, want)
	}
	if without := interpreterEnv("/pinned", Program{Env: map[string]string{"TZ": "UTC"}}, nil); len(without) != len(environment)-2 {
		t.Fatalf("a program with no per-run entry gets %d entries, want %d", len(without), len(environment)-2)
	}
}

// TestAnAnswerHookReplacesStdoutAndIsHeldToTheSameRules pins Program.Answer:
// what it returns is the answer, it runs only after an exit with status 0, its
// error stops the recording, and an answer it makes with a per-run value is
// refused like a printed one.
func TestAnAnswerHookReplacesStdoutAndIsHeldToTheSameRules(t *testing.T) {
	fakeInterpreter(t)
	address, _, _, _ := perRunAddress()
	perRun := func() map[string]string { return map[string]string{"ORACLE_DATABASE_URI": address} }
	root := t.TempDir()
	calls := 0
	seen := func(stdout []byte) ([]byte, error) {
		calls++
		return []byte("seen by the server after " + string(stdout)), nil
	}
	if result, err := executeErr(root, Program{Name: "clean", Text: "clean", PerRun: perRun, PerRunNames: []string{"ORACLE_DATABASE_URI"}, Answer: seen}); err != nil || string(result.stdout) != "seen by the server after ok" || calls != 1 {
		t.Fatalf("the answer of the hook = %q, %v after %d calls", result.stdout, err, calls)
	}
	if result, err := executeErr(root, Program{Name: "fails", Text: "fail", PerRun: perRun, PerRunNames: []string{"ORACLE_DATABASE_URI"}, Answer: seen}); err != nil || result.exitCode != 3 || string(result.stdout) != "part" || calls != 1 {
		t.Fatalf("a program that exits 3 = %+v, %v after %d calls: the hook runs only after status 0", result, err, calls)
	}
	refused := func([]byte) ([]byte, error) { return nil, errors.New("the server received nothing") }
	if _, err := executeErr(root, Program{Name: "nothing", Text: "clean", Answer: refused}); err == nil || !strings.Contains(err.Error(), "the server received nothing") {
		t.Fatalf("an error of the hook = %v, want it to stop the recording", err)
	}
	leaking := func([]byte) ([]byte, error) { return []byte("the server saw " + address), nil }
	if _, err := executeErr(root, Program{Name: "leaks", Text: "clean", PerRun: perRun, PerRunNames: []string{"ORACLE_DATABASE_URI"}, Answer: leaking}); err == nil || !strings.Contains(err.Error(), "value of the per-run entry ORACLE_DATABASE_URI") {
		t.Fatalf("an answer of the hook with the address = %v, want it refused by the name of the entry", err)
	}
}

// perRunAddress is an address of the shape a test hands a program for one
// run. It is built from parts so that no line of this file holds an address.
func perRunAddress() (address, host, password, database string) {
	host, password, database = "127.0.0.1:"+"54329", "pw-of-this-run", "venue_ab12cd"
	return "postgresql+psycopg2://admin:" + password + "@" + host + "/" + database, host, password, database
}

// TestAnAnswerThatHoldsAPartOfAPerRunEntryIsRefused pins the guard of a
// recording: the whole value, the host with its port, the password or the
// database name in the answer is an error that names the entry and the part
// and prints none of them. A part shorter than perRunMinimum is not searched
// for: it is in an answer by chance.
func TestAnAnswerThatHoldsAPartOfAPerRunEntryIsRefused(t *testing.T) {
	address, host, password, database := perRunAddress()
	perRun := map[string]string{"ORACLE_DATABASE_URI": address, "PLAIN": "value-of-this-run", "SHORT": "abc"}
	for _, row := range []struct{ name, answer, want string }{
		{"clean", `[["0", "", ""], "admin"]`, ""},
		{"a short value is not searched for", `["abc", "abcdef"]`, ""},
		{"whole value", `{"uri": "` + address + `"}`, "value of the per-run entry ORACLE_DATABASE_URI"},
		{"host and port", "connected to " + host, "host and port of the per-run entry ORACLE_DATABASE_URI"},
		{"password", "auth " + password, "password of the per-run entry ORACLE_DATABASE_URI"},
		{"database name", `relation in "` + database + `"`, "last path element of the per-run entry ORACLE_DATABASE_URI"},
		{"value that is no address", "x value-of-this-run y", "value of the per-run entry PLAIN"},
	} {
		err := perRunErr("p", row.answer, perRun)
		switch {
		case row.want == "" && err != nil:
			t.Errorf("%s: refused: %v", row.name, err)
		case row.want != "" && (err == nil || !strings.Contains(err.Error(), row.want)):
			t.Errorf("%s: error = %v, want one that holds %q", row.name, err, row.want)
		case err != nil && (strings.Contains(err.Error(), host) || strings.Contains(err.Error(), password) || strings.Contains(err.Error(), database) || strings.Contains(err.Error(), "value-of-this-run")):
			t.Errorf("%s: the error prints the per-run text: %v", row.name, err)
		}
	}
	if err := perRunErr("p", "anything", nil); err != nil {
		t.Errorf("a program with no per-run entry is refused: %v", err)
	}
}

// TestALoggedErrorTextHoldsNoPartOfAPerRunEntry pins what a failed program's
// stderr becomes in the log: each part replaced by the name of its entry, the
// rest unchanged.
func TestALoggedErrorTextHoldsNoPartOfAPerRunEntry(t *testing.T) {
	address, host, password, database := perRunAddress()
	perRun := map[string]string{"ORACLE_DATABASE_URI": address}
	stderr := "OperationalError: connection to " + host + " failed for database " + database + " (" + address + ") password " + password + "; line 3"
	got := withoutPerRun(stderr, perRun)
	want := "OperationalError: connection to <ORACLE_DATABASE_URI host and port> failed for database <ORACLE_DATABASE_URI last path element> " +
		"(<ORACLE_DATABASE_URI value>) password <ORACLE_DATABASE_URI password>; line 3"
	if got != want {
		t.Fatalf("logged text =\n%s\nwant\n%s", got, want)
	}
	if unchanged := withoutPerRun(stderr, nil); unchanged != stderr {
		t.Fatalf("a program with no per-run entry has its text changed: %s", unchanged)
	}
}

func TestTheIdentityProgramNamesItsDistributionsInOrder(t *testing.T) {
	program := Identity("pydantic-core", "idna")
	if program.Name != IdentityName {
		t.Errorf("name = %q", program.Name)
	}
	idna, pydantic := strings.Index(program.Text, `metadata.version("idna")`), strings.Index(program.Text, `metadata.version("pydantic-core")`)
	if idna < 0 || pydantic < 0 || idna > pydantic {
		t.Errorf("distributions are not listed in sorted order:\n%s", program.Text)
	}
	for _, line := range []string{"sys.version_info[:3]", "unicodedata.unidata_version"} {
		if !strings.Contains(program.Text, line) {
			t.Errorf("the identity program does not print %s", line)
		}
	}
}

// TestAnotherIdentityIsRefusedByName pins the identity check: the pinned
// identity is accepted, another one or a failed identity program is refused,
// and the refusal names what was expected and what was found.
func TestAnotherIdentityIsRefusedByName(t *testing.T) {
	pinned := "python 3.14.7\nunicodedata 16.0.0"
	if err := IdentityErr(Answer{Stdout: pinned + "\n"}, pinned); err != nil {
		t.Errorf("the pinned identity was refused: %v", err)
	}
	err := IdentityErr(Answer{Stdout: "python 3.15.0\nunicodedata 17.0.0\n"}, pinned)
	if err == nil {
		t.Fatal("another identity was accepted")
	}
	for _, part := range []string{"expected identity", "python 3.14.7", "found identity", "python 3.15.0", "unicodedata 17.0.0"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("the refusal does not name %q:\n%v", part, err)
		}
	}
	if err := IdentityErr(Answer{ExitCode: 1, Stdout: pinned}, pinned); err == nil {
		t.Error("an identity program that failed was accepted")
	}
}

// TestOutputsNeedThePinnedProducerAndSuccessfulPrograms pins successfulOutputs:
// the pinned identity and exit code 0 give the outputs; another identity, a
// failed program, or a missing answer is an error.
func TestOutputsNeedThePinnedProducerAndSuccessfulPrograms(t *testing.T) {
	const pinned = "python 3.14.7\nunicodedata 16.0.0"
	programs := []Program{{Name: "first", Text: "print(1)"}, {Name: "second", Text: "print(2)"}}
	identity := Answer{Stdout: pinned + "\n"}
	outputs, err := successfulOutputs(pinned, programs, []Answer{identity, {Stdout: "1\n"}, {Stdout: "2\n"}})
	if err != nil || len(outputs) != 2 || outputs[0] != "1\n" || outputs[1] != "2\n" {
		t.Fatalf("outputs = %q, %v", outputs, err)
	}
	refused := map[string][]Answer{
		"another identity": {{Stdout: "python 3.15.0\nunicodedata 17.0.0\n"}, {Stdout: "1\n"}, {Stdout: "2\n"}},
		"a failed program": {identity, {Stdout: "1\n"}, {ExitCode: 2, Stdout: "partial"}},
		"a missing answer": {identity, {Stdout: "1\n"}},
	}
	for name, answers := range refused {
		if _, err := successfulOutputs(pinned, programs, answers); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestTheInterpreterDirectoryMustHoldPython3 pins interpreterDir: the directory
// of the chosen interpreter, which is activated on PATH, must hold an
// executable python3.
func TestTheInterpreterDirectoryMustHoldPython3(t *testing.T) {
	bin := t.TempDir()
	python := filepath.Join(bin, "python")
	for name, mode := range map[string]os.FileMode{"python": 0o755, "python3": 0o755} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"), mode); err != nil {
			t.Fatal(err)
		}
	}
	if dir, err := interpreterDir(python); err != nil || dir != bin {
		t.Fatalf("interpreterDir = %q, %v, want %q", dir, err, bin)
	}
	if err := os.Chmod(filepath.Join(bin, "python3"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := interpreterDir(python); err == nil {
		t.Error("a python3 that is not executable was accepted")
	}
	if err := os.Remove(filepath.Join(bin, "python3")); err != nil {
		t.Fatal(err)
	}
	if _, err := interpreterDir(python); err == nil {
		t.Error("a directory with no python3 was accepted")
	}
	if err := os.Mkdir(filepath.Join(bin, "python3"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := interpreterDir(python); err == nil {
		t.Error("a python3 that is a directory was accepted")
	}
	// A name that is not on PATH is refused even when the working directory
	// holds a python3: a name is looked up, never taken as a relative path.
	here := t.TempDir()
	if err := os.WriteFile(filepath.Join(here, "python3"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(here)
	if _, err := interpreterDir("no-such-interpreter-on-path"); err == nil {
		t.Error("an interpreter name that is not on PATH was accepted")
	}
}

// A recording test holds PYTHONHOME set to a directory that does not exist, so
// that a Python child that inherits the test process's environment cannot
// start (venueoracle's producer guard). The helper's own Python starts, the
// version probe and the program, do not inherit it: under that guard both run.
func TestTheRecordingGuardDoesNotStopTheHelpersOwnPython(t *testing.T) {
	root := t.TempDir()
	bin := filepath.Join(root, ".venv", "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	// The stand-in behaves as the real interpreter does under the guard: with
	// PYTHONHOME set it cannot start. Otherwise "-c <probe>" prints a version
	// and a program prints "ran".
	script := "#!/bin/sh\nif [ -n \"${PYTHONHOME+set}\" ]; then echo \"Fatal Python error: PYTHONHOME = '$PYTHONHOME'\" >&2; exit 1; fi\n" +
		"case \"$2\" in *version_info*) : > '" + filepath.Join(root, "probed") + "'; echo 3.14 ;; *) printf 'ran' ;; esac\n"
	for _, name := range []string{"python", "python3"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("DEV_HEALTH_PYTHON", "")
	t.Setenv("PYTHON", "")
	t.Setenv("PYTHONHOME", "/a-directory-that-does-not-exist")

	activateInterpreter(t, root)
	if _, err := os.Stat(filepath.Join(root, "probed")); err != nil {
		t.Fatalf("the checkout's interpreter was not probed: %v", err)
	}
	exitCode, stdout := execute(t, root, Program{Name: "p", Text: "print('x')"})
	if exitCode != 0 || string(stdout) != "ran" {
		t.Fatalf("the program under the guard: exit %d, stdout %q", exitCode, stdout)
	}
}

// The names of a program's per-run entries are part of its request: a closed
// list, declared with PerRun, exactly what a recording gives.
func TestThePerRunNamesAreInTheRequestFromAClosedList(t *testing.T) {
	give := func(names ...string) func() map[string]string {
		return func() map[string]string {
			given := map[string]string{}
			for _, name := range names {
				given[name] = "v"
			}
			return given
		}
	}
	base := Program{Name: "p", Text: "t"}
	with := func(names []string, perRun func() map[string]string) Program {
		program := base
		program.PerRunNames, program.PerRun = names, perRun
		return program
	}
	// One more name, one fewer, another name: another request.
	one := keyedEnv(with([]string{"ORACLE_DATABASE_URI"}, give("ORACLE_DATABASE_URI")))
	two := keyedEnv(with([]string{"ORACLE_DATABASE_URI", "ATLASSIAN_ORACLE_GATEWAY"}, give("ORACLE_DATABASE_URI", "ATLASSIAN_ORACLE_GATEWAY")))
	other := keyedEnv(with([]string{"ATLASSIAN_ORACLE_GATEWAY"}, give("ATLASSIAN_ORACLE_GATEWAY")))
	none := keyedEnv(base)
	if one[perRunNamesKey] == two[perRunNamesKey] || one[perRunNamesKey] == other[perRunNamesKey] || none[perRunNamesKey] != "" {
		t.Fatalf("per-run names in the key: one %q two %q other %q none %q", one[perRunNamesKey], two[perRunNamesKey], other[perRunNamesKey], none[perRunNamesKey])
	}
	// The order of the declaration is not the key's.
	reordered := keyedEnv(with([]string{"ORACLE_DATABASE_URI", "ATLASSIAN_ORACLE_GATEWAY"}, give()))
	reversed := keyedEnv(with([]string{"ATLASSIAN_ORACLE_GATEWAY", "ORACLE_DATABASE_URI"}, give()))
	if reordered[perRunNamesKey] != reversed[perRunNamesKey] {
		t.Fatal("the order of the declared names changes the key")
	}
	// The closed list and the declaration rules.
	for name, program := range map[string]Program{
		"a name off the list":        with([]string{"ANYTHING"}, give("ANYTHING")),
		"a repeated name":            with([]string{"ORACLE_DATABASE_URI", "ORACLE_DATABASE_URI"}, give("ORACLE_DATABASE_URI")),
		"PerRun with no names":       with(nil, give("ORACLE_DATABASE_URI")),
		"names with no PerRun":       with([]string{"ORACLE_DATABASE_URI"}, nil),
		"a name that is also in Env": {Name: "p", Text: "t", Env: map[string]string{"ORACLE_DATABASE_URI": "x"}, PerRunNames: []string{"ORACLE_DATABASE_URI"}, PerRun: give("ORACLE_DATABASE_URI")},
		"the request's own entry":    {Name: "p", Text: "t", Env: map[string]string{perRunNamesKey: "x"}},
	} {
		if err := programsErr([]Program{program}); err == nil {
			t.Errorf("%s: the program was accepted", name)
		}
	}
	if err := programsErr([]Program{with([]string{"ORACLE_DATABASE_URI"}, give("ORACLE_DATABASE_URI"))}); err != nil {
		t.Errorf("a declared, listed name: %v", err)
	}
	// A recording that gives other names than declared fails.
	fakeInterpreter(t)
	root := t.TempDir()
	for name, program := range map[string]Program{
		"an added name":  with([]string{"ORACLE_DATABASE_URI"}, give("ORACLE_DATABASE_URI", "ATLASSIAN_ORACLE_GATEWAY")),
		"a removed name": with([]string{"ORACLE_DATABASE_URI", "ATLASSIAN_ORACLE_GATEWAY"}, give("ORACLE_DATABASE_URI")),
		"another name":   with([]string{"ORACLE_DATABASE_URI"}, give("ATLASSIAN_ORACLE_GATEWAY")),
	} {
		program.Name, program.Text = "clean", "clean"
		if _, err := executeErr(root, program); err == nil || !strings.Contains(err.Error(), "its PerRunNames are") {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	good := with([]string{"ORACLE_DATABASE_URI"}, give("ORACLE_DATABASE_URI"))
	good.Name, good.Text = "clean", "clean"
	if _, err := executeErr(root, good); err != nil {
		t.Errorf("the declared names: %v", err)
	}
	// The pseudo-entry never reaches the program.
	environment := interpreterEnv("/pinned", good, good.PerRun())
	for _, entry := range environment {
		if strings.HasPrefix(entry, perRunNamesKey) {
			t.Errorf("the request's per-run names reach the program: %q", entry)
		}
	}
}
