package programoracle

import (
	"path/filepath"
	"runtime"
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
		SHA256:      "f4777075bb6f8df8d57eedb9661bcf2e1e6089f4550cc2932181121d0b97b9a3",
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
