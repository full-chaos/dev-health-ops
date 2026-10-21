package programoracle

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// runGoldenPythonBuild is the build whose interpreter answered run.golden.json.
const runGoldenPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// TestRunAnswersFromTheGolden runs the whole path frozen: three programs, one
// golden, the answers in order. The second program exited 3 when it was
// recorded: its exit code and its stdout are both kept, so a failed producer
// is never an empty success.
func TestRunAnswersFromTheGolden(t *testing.T) {
	spec := venueoracle.GoldenSpec{
		Path:        "testdata/golden/run.golden.json",
		PythonBuild: runGoldenPythonBuild,
		SHA256:      "6c3807fe456312d0464a8eca1e752311b8cbbbdd7277792d4d038b73e66b9c45",
	}
	answers := Run(t, spec, []Program{
		{Name: "upper", Stdin: []byte("corpus")},
		{Name: "fails"},
		{Name: "environment"},
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

// TestAScriptRunsAsItsFileInThePinnedCheckout replays a frozen script answer:
// the script printed where it believed it was, its module name and its input.
func TestAScriptRunsAsItsFileInThePinnedCheckout(t *testing.T) {
	spec := venueoracle.GoldenSpec{
		Path:        "testdata/golden/script.golden.json",
		PythonBuild: runGoldenPythonBuild,
		SHA256:      "d0884e9313816f8f0771546274d5d950cde05fc31606301cb9af9f70de142c88",
	}
	answers := Run(t, spec, []Program{{Name: "script", Stdin: []byte("input")}})
	if want := (Answer{0, "some/place/oracle.py True True __main__ input\n"}); answers[0] != want {
		t.Fatalf("answer = %+v, want %+v", answers[0], want)
	}
}

func TestAProgramListMustBeNamedAndUnique(t *testing.T) {
	refused := map[string][]Program{
		"no program":     nil,
		"no name":        {{Stdin: []byte("1")}},
		"duplicate name": {{Name: "a"}, {Name: "a", Stdin: []byte("2")}},
	}
	for name, programs := range refused {
		if err := programsErr(programs); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if err := programsErr([]Program{{Name: "a"}, {Name: "b"}}); err != nil {
		t.Errorf("two named programs were refused: %v", err)
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
	programs := []Program{{Name: "first"}, {Name: "second"}}
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
