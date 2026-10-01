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
	// The checkout's own interpreter is the one under test: an override the
	// process holds (a developer's shell, the Python-free job's tripwire, which
	// points both names at its shim) would replace it.
	t.Setenv("DEV_HEALTH_PYTHON", "")
	t.Setenv("PYTHON", "")
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
	// As OpenGolden does in a recording.
	poisonInheritedEnvironment(t)
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

// A recording test holds the poison from OpenGolden to its end; a test that
// is not recording never holds it; and the poison is the harness's, never the
// test's: no venue child gets it and no key holds it.
func TestARecordingTestHoldsThePoisonAndOnlyARecordingTest(t *testing.T) {
	if _, err := os.Stat(producerPoison); err == nil {
		t.Fatalf("the poison directory %s exists: an interpreter could start with it", producerPoison)
	}
	spec := func(t *testing.T) GoldenSpec {
		return GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, Recipe: "record it"}
	}
	t.Run("recording", func(t *testing.T) {
		t.Setenv(goldenUpdateEnv, "1")
		t.Setenv(goldenCandidateEnv, "")
		OpenGolden(t, spec(t))
		if got := os.Getenv(producerPoisonName); got != producerPoison {
			t.Fatalf("in a recording test %s = %q, want the poison", producerPoisonName, got)
		}
		if set := strings.Join(testSetEnv(), " "); strings.Contains(set, producerPoisonName) {
			t.Errorf("the test-set variables hold the poison: %q", set)
		}
	})
	if got, held := os.LookupEnv(producerPoisonName); held {
		t.Fatalf("after the recording test ended %s is still %q", producerPoisonName, got)
	}
	t.Run("frozen", func(t *testing.T) {
		t.Setenv(goldenUpdateEnv, "")
		t.Setenv(goldenCandidateEnv, "")
		requests := []Request{ProgramRequest("corpus", sampleProgram, []byte("abc"), nil)}
		path, digest := programGolden(t, requests, "out")
		OpenGolden(t, GoldenSpec{Path: path, SHA256: digest, PythonBuild: goldenBuild, Recipe: "record it"})
		if got, held := os.LookupEnv(producerPoisonName); held {
			t.Fatalf("a test that is not recording holds %s = %q", producerPoisonName, got)
		}
	})
}

// The interpreter options that make Python ignore its PYTHON* variables are
// refused by the producer's command: with one of them the closed environment
// and the recording guard would say nothing.
func TestTheProducersCommandRefusesTheOptionsThatIgnoreTheEnvironment(t *testing.T) {
	root := checkoutWithInterpreter(t, "TZ")
	producer := &Producer{Root: root, t: t}
	for _, row := range []struct {
		args    []string
		refused bool
	}{
		{[]string{"-c", "pass"}, false},
		{[]string{"-m", "dev_health_ops.cli", "-I"}, false}, // the program's own argument
		{[]string{"-c", "pass", "-E"}, false},
		{[]string{"-u", "-B", "-c", "pass"}, false},
		{[]string{"-X", "importtime", "-c", "pass"}, false},
		{[]string{"-Werror", "-c", "pass"}, false},
		{[]string{"script.py", "-E"}, false},
		{[]string{"-E", "-c", "pass"}, true},
		{[]string{"-I", "-c", "pass"}, true},
		{[]string{"-sE", "-c", "pass"}, true},
		{[]string{"-u", "-BI", "-m", "x"}, true},
		// An option with a value: the value is not a cluster of flags, and
		// what comes after it is still an option.
		{[]string{"-Werror::ImportWarning", "-c", "pass"}, false},
		{[]string{"-XfrozEn_modules", "-c", "pass"}, false},
		{[]string{"-X", "utf8", "-c", "pass"}, false},
		{[]string{"-uc", "pass", "-I"}, false},
		{[]string{"-cIMPORTANT = 1"}, false}, // the program's text, joined to -c
		{[]string{"-umIPython"}, false},
		{[]string{"-", "-E"}, false},  // the program comes from standard input
		{[]string{"--", "-E"}, false}, // "--" ends the options: -E is the script's name
		{[]string{"-X", "utf8", "-E", "-c", "pass"}, true},
		{[]string{"-W", "error", "-I", "-c", "pass"}, true},
		{[]string{"-BW", "error", "-I", "-c", "pass"}, true},
		{[]string{"-Werror", "-E", "-c", "pass"}, true},
		{[]string{"--check-hash-based-pycs", "default", "-E", "-c", "pass"}, true},
		{[]string{"--version", "-I"}, true},
	} {
		_, err := producer.Command(context.Background(), nil, nil, row.args...)
		if refused := err != nil && strings.Contains(err.Error(), "makes Python ignore its environment variables"); refused != row.refused {
			t.Errorf("python3 %v: refused = %v (%v), want %v", row.args, refused, err, row.refused)
		}
	}
}

// underTheVerb makes the test's recordings ones the record verb started: the
// verb always sets goldenPassedEnv for its runs, with no names when it passed
// none.
func underTheVerb(t *testing.T) {
	t.Helper()
	t.Setenv(goldenPassedEnv, "")
}

// A recording holds the record verb's stamp when the verb started it, and a
// run a person started by hand writes no candidate at all.
func TestOnlyARecordingTheVerbStartedWritesACandidateAndItHoldsTheStamp(t *testing.T) {
	record := func(t *testing.T) (*Golden, string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "g.json")
		golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
		if err != nil {
			t.Fatal(err)
		}
		golden.recorded.Header.ProducerDigest = strings.Repeat("a", 64)
		body := "{}"
		golden.recorded.Requests = append(golden.recorded.Requests, goldenRequest{Name: "one", Method: "GET", Path: "/x", Status: 200, Body: body})
		return golden, path
	}
	t.Run("started by hand", func(t *testing.T) {
		os.Unsetenv(goldenPassedEnv)
		golden, path := record(t)
		if _, err := golden.writeCandidate(false); err == nil || !strings.Contains(err.Error(), "was not started by the record verb") {
			t.Fatalf("a hand-started recording: err = %v, want a refusal", err)
		}
		if _, err := os.Stat(path + GoldenCandidateSuffix); err == nil {
			t.Fatal("a hand-started recording wrote a candidate")
		}
	})
	t.Run("started by the verb", func(t *testing.T) {
		underTheVerb(t)
		golden, path := record(t)
		if _, err := golden.writeCandidate(false); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(path + GoldenCandidateSuffix)
		if err != nil {
			t.Fatal(err)
		}
		// The stamp is the verb's (goldenrecord writes it into the candidate):
		// a test process holding the verb's variable writes none.
		if strings.Contains(string(raw), "recorded_by") {
			t.Fatalf("the test process wrote the stamp of the verb into the candidate:\n%s", raw)
		}
	})
}

// The replay of a candidate refuses one without the verb's stamp.
func TestTheReplayOfACandidateRefusesOneWithoutTheStamp(t *testing.T) {
	spec := GoldenSpec{Path: "g.json.recording", Recipe: "record it"}
	if err := candidateStampErr(spec, goldenHeader{}); err == nil {
		t.Fatal("a candidate with no stamp was accepted")
	}
	if err := candidateStampErr(spec, goldenHeader{RecordedBy: "someone"}); err == nil {
		t.Fatal("a candidate with another stamp was accepted")
	}
	if err := candidateStampErr(spec, goldenHeader{RecordedBy: recordVerbName}); err != nil {
		t.Fatal(err)
	}
}

// The producer's version probe runs in the closed environment too: in a
// recording test, whose process holds the guard's variable, it passes, and it
// judges the interpreter by what it says.
func TestTheProducersVersionProbeIsNotStoppedByTheGuard(t *testing.T) {
	root := t.TempDir()
	probed := filepath.Join(root, "probed")
	script := "#!/bin/sh\nif [ -n \"${" + producerPoisonName + "+set}\" ]; then echo 'Fatal Python error: cannot start' >&2; exit 1; fi\n: > '" + probed + "'\necho 3.14\n"
	for _, name := range []string{"python", "python3"} {
		file := filepath.Join(root, ".venv", "bin", name)
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("DEV_HEALTH_PYTHON", "")
	t.Setenv("PYTHON", "")
	poisonInheritedEnvironment(t)
	producer := &Producer{Root: root, t: t}
	producer.RequireDeployed()
	if got := filepath.Join(root, ".venv", "bin", "python3"); producer.python != got {
		t.Fatalf("the producer's interpreter is %q, want the checkout's %s", producer.python, got)
	}
	if _, err := os.Stat(probed); err != nil {
		t.Fatalf("the checkout's interpreter was not the one probed: %v", err)
	}
}

// An extra entry is a value made for one run: only the closed list of names
// may be passed so. A name that shapes the answer (HOME, PATH, PYTHONPATH, a
// setting) goes in declared, where the request's key holds it by value.
func TestAnExtraEntryOutsideTheClosedListIsRefused(t *testing.T) {
	root := checkoutWithInterpreter(t)
	producer := &Producer{Root: root, t: t}
	for name, row := range map[string]struct {
		declared map[string]string
		extra    []string
		allowed  bool
	}{
		"the run's database":      {nil, []string{"POSTGRES_URI=postgresql://run", "DATABASE_URI=postgresql://run", "CLICKHOUSE_URI=http://run", "REDIS_URL=redis://run"}, true},
		"a fake server's address": {nil, []string{"SMTP_HOST=127.0.0.1", "SMTP_PORT=2525"}, true},
		"HOME":                    {nil, []string{"HOME=/elsewhere"}, false},
		"PATH":                    {nil, []string{"PATH=/elsewhere"}, false},
		"PYTHONPATH":              {nil, []string{"PYTHONPATH=/elsewhere"}, false},
		"TZ":                      {nil, []string{"TZ=Pacific/Auckland"}, false},
		"a name of the harness":   {nil, []string{"TMPDIR=/elsewhere"}, false},
		"an unknown name":         {nil, []string{"ANYTHING=1"}, false},
		"no equals sign":          {nil, []string{"POSTGRES_URI"}, false},
		"the same name twice":     {nil, []string{"POSTGRES_URI=a", "POSTGRES_URI=b"}, false},
		"a name that is declared": {map[string]string{"POSTGRES_URI": "x"}, []string{"POSTGRES_URI=a"}, false},
	} {
		_, err := producer.Command(context.Background(), row.declared, row.extra, "-c", "pass")
		if (err == nil) != row.allowed {
			t.Errorf("%s: err = %v, allowed = %v", name, err, row.allowed)
		}
	}
}

// pyoracle cannot import this package, so it holds the recording variable by
// name: the two must be the same.
func TestPyoracleNamesTheRecordingVariableOfThisPackage(t *testing.T) {
	if pyoracle.RecordingEnv != goldenUpdateEnv {
		t.Fatalf("pyoracle names %q as the recording variable, venueoracle %q", pyoracle.RecordingEnv, goldenUpdateEnv)
	}
}

// The launcher gets the real interpreter in a recording: it is the one place
// that starts Python there.
func TestTheLauncherIsGivenTheRealInterpreterInARecording(t *testing.T) {
	root := checkoutWithInterpreter(t)
	t.Setenv(pyoracle.RecordingEnv, "1")
	producer := &Producer{Root: root, t: t}
	dir, err := producer.PythonDir()
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(root, ".venv", "bin"); dir != want {
		t.Fatalf("the launcher's interpreter is in %s, want %s", dir, want)
	}
}
