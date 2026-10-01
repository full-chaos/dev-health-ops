package programoracle

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// runGoldenPythonBuild is the build whose interpreter answered run.golden.json.
const runGoldenPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// TestRunAnswersFromTheGolden runs the whole path frozen: three programs, one
// golden, the answers in order. The second program exited 3 when it was
// recorded: its exit code and its stdout are both kept, so a failed producer
// is never an empty success. No Python runs here.
func TestRunAnswersFromTheGolden(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
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
	_, file, _, _ := runtime.Caller(0)
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

// TestTheInterpreterRunsInThePinnedCheckout pins the command of a recording:
// python3 by name with the program as -c text, the pinned checkout as working
// directory, the program's input, and the interpreter environment.
func TestTheInterpreterRunsInThePinnedCheckout(t *testing.T) {
	command := interpreterCommand("/pinned", Program{Text: "print(1)", Stdin: []byte("in")})
	if command.Dir != "/pinned" {
		t.Errorf("working directory = %q", command.Dir)
	}
	if strings.Join(command.Args, " ") != "python3 -c print(1)" {
		t.Errorf("arguments = %q", command.Args)
	}
	if input, err := io.ReadAll(command.Stdin); err != nil || string(input) != "in" {
		t.Errorf("input = %q, %v", input, err)
	}
	if !slices.Contains(command.Env, "PYTHONPATH="+filepath.Join("/pinned", "src")) {
		t.Errorf("environment = %q", command.Env)
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
// environment: only PATH and HOME come from the process; the pinned sources,
// the bytecode switch and the keyed entries are set; an ambient variable does
// not reach the program.
func TestTheInterpreterGetsNothingOfTheProcessEnvironment(t *testing.T) {
	t.Setenv("PROGRAMORACLE_AMBIENT", "must not reach the program")
	t.Setenv("PYTHONPATH", "/ambient/path")
	t.Setenv("PATH", "/bin-of-the-test")
	t.Setenv("HOME", "/home-of-the-test")
	environment := interpreterEnv("/pinned", Program{Env: map[string]string{"TZ": "UTC"}})
	want := []string{
		"PATH=/bin-of-the-test", "HOME=/home-of-the-test",
		"PYTHONPATH=" + filepath.Join("/pinned", "src"), "PYTHONDONTWRITEBYTECODE=1",
		"PYTHONHASHSEED=0", "PYTHONUTF8=1", "TZ=UTC",
	}
	if strings.Join(environment, "\n") != strings.Join(want, "\n") {
		t.Fatalf("interpreter environment =\n%s\nwant\n%s", strings.Join(environment, "\n"), strings.Join(want, "\n"))
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
	if _, err := interpreterDir("no-such-interpreter-on-path"); err == nil {
		t.Error("an interpreter name that is not on PATH was accepted")
	}
}
