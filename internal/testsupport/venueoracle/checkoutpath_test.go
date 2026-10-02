package venueoracle

import (
	"path/filepath"
	"strings"
	"testing"
)

func recordingGolden(t *testing.T) *Golden {
	t.Helper()
	golden, err := openGolden(GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, Recipe: "record it"}, t.Name(), true)
	if err != nil {
		t.Fatal(err)
	}
	return golden
}

// A keyed value that holds the absolute path of a checkout is refused at record time, by the entry's
// NAME and never its value: the golden would replay in that checkout only (CHAOS-7849).
func TestRecordingRefusesAKeyedValueThatHoldsTheCheckoutPath(t *testing.T) {
	root := t.TempDir()
	secretLooking := filepath.Join(root, "stub") + ":" + filepath.Join(root, "src")
	options := Options{Root: root, JWTKey: "k", PythonEnv: []string{"PYTHONPATH=" + secretLooking}}
	err := recordingGolden(t).bindPythonEnv(options)
	if err == nil || !strings.Contains(err.Error(), "PYTHONPATH") || !strings.Contains(err.Error(), "absolute path of a checkout") {
		t.Fatalf("a test-supplied absolute PYTHONPATH is not refused at record time: %v", err)
	}
	if strings.Contains(err.Error(), root) {
		t.Fatalf("the refusal shows the path: %v", err)
	}
	// The same options in a FROZEN run are not this guard's business: the golden's key already differs per
	// checkout and the replay compares it. A frozen golden is opened for them and must bind without this
	// refusal (a guard that also ran frozen would fail here).
	frozen, _ := frozenWithEnv(t, currentKey(t, options), pythonEnvKeyVersion)
	if err := frozen.bindPythonEnv(options); err != nil {
		t.Fatalf("a frozen run is refused as a recording is: %v", err)
	}
}

// An entry keyed by name only (the harness's own PYTHONPATH, the inherited PATH and HOME) may hold a
// checkout path: its value is not in the key. A value that does not hold the path whole (a sibling name) is
// not a hit either.
func TestRecordingAllowsPerRunNamesAndSiblingPaths(t *testing.T) {
	root := t.TempDir()
	if err := recordingGolden(t).bindPythonEnv(Options{Root: root, JWTKey: "k"}); err != nil {
		t.Fatalf("the harness's own entries are refused: %v", err)
	}
	sibling := root + "2/src"
	if err := recordingGolden(t).bindPythonEnv(Options{Root: root, JWTKey: "k", PythonEnv: []string{"SOME_DIR=" + sibling, "OTHER=" + filepath.Join("/elsewhere", filepath.Base(root))}}); err != nil {
		t.Fatalf("a sibling path or a same-named directory elsewhere is refused: %v", err)
	}
	if err := recordingGolden(t).bindPythonEnv(Options{Root: root, JWTKey: "k", PythonEnv: []string{"REQUESTS_CA_BUNDLE=" + filepath.Join(root, "ca.pem")}}); err != nil {
		t.Fatalf("a name keyed by name only (test-supplied per-run name) is refused: %v", err)
	}
}

// The path may sit anywhere in the value: after an '=' or a PATH-list colon, or alone.
func TestHoldsPathFindsTheRootWhole(t *testing.T) {
	root := "/work/tree/ops"
	for value, want := range map[string]bool{
		root:                        true,
		root + "/src":               true,
		"/a:" + root + ":/b":        true,
		"x=" + root + "/src":        true,
		"/work/tree/ops2/src":       false,
		"/other/work/tree/ops/src":  false,
		"/work/tree/opsx":           false,
		"":                          false,
		"relative/path":             false,
		"--dir '" + root + "/data'": true,
		// The first occurrence is a sibling name, the root comes after it: the search goes on.
		"/work/tree/ops2/src:" + root + "/src": true,
		"/work/tree/opsx/a:" + root:            true,
	} {
		if got := holdsPath(value, root); got != want {
			t.Errorf("holdsPath(%q) = %v, want %v", value, got, want)
		}
	}
}

// A call's extra entries and the variables a test set in the process after the venue was built are keyed
// by name and value too: they are refused at record time the same way.
func TestRecordingRefusesACallEntryThatHoldsTheCheckoutPath(t *testing.T) {
	root := t.TempDir()
	golden := recordingGolden(t)
	golden.verifiedRoot = root
	if _, err := golden.callEnvKey([]string{"EXTRA=" + filepath.Join(root, "x")}); err == nil || !strings.Contains(err.Error(), "EXTRA") {
		t.Fatalf("a call's extra entry holding the checkout path is not refused: %v", err)
	}
	if _, err := golden.callEnvKey([]string{"EXTRA=fine"}); err != nil {
		t.Fatalf("an entry without a path is refused: %v", err)
	}
}

// The entries a producer call declares are keyed by name and value: one that holds the checkout path is
// refused before any interpreter is looked for.
func TestProducerCommandRefusesADeclaredValueThatHoldsTheCheckoutPath(t *testing.T) {
	root := t.TempDir()
	producer := &Producer{Root: root, t: t}
	_, err := producer.Command(t.Context(), map[string]string{"PYTHONPATH": filepath.Join(root, "src")}, nil, "-c", "pass")
	if err == nil || !strings.Contains(err.Error(), "PYTHONPATH") || !strings.Contains(err.Error(), "absolute path of a checkout") || strings.Contains(err.Error(), root) {
		t.Fatalf("a declared value holding the checkout path is not refused by name: %v", err)
	}
}

// A variable a test set in the process (not through Options.PythonEnv) reaches the child and is keyed by
// name and value: it is read too.
func TestRecordingRefusesAProcessVariableThatHoldsTheCheckoutPath(t *testing.T) {
	root := t.TempDir()
	t.Setenv("SOME_TEST_DIR_FOR_CHECKOUT_GUARD", filepath.Join(root, "stub"))
	err := recordingGolden(t).bindPythonEnv(Options{Root: root, JWTKey: "k"})
	if err == nil || !strings.Contains(err.Error(), "SOME_TEST_DIR_FOR_CHECKOUT_GUARD") {
		t.Fatalf("a process variable holding the checkout path is not refused: %v", err)
	}
}

// A variable a test changes AFTER the venue was built is the call's own environment and is read the same way.
func TestRecordingRefusesAVariableChangedAfterTheVenueThatHoldsTheCheckoutPath(t *testing.T) {
	root := t.TempDir()
	golden := recordingGolden(t)
	golden.verifiedRoot = root
	if err := golden.bindPythonEnv(Options{Root: root, JWTKey: "k"}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LATER_TEST_DIR_FOR_CHECKOUT_GUARD", filepath.Join(root, "later"))
	if _, err := golden.callEnvKey(nil); err == nil || !strings.Contains(err.Error(), "LATER_TEST_DIR_FOR_CHECKOUT_GUARD") {
		t.Fatalf("a variable changed after the venue, holding the checkout path, is not refused: %v", err)
	}
}

// The guard also does not run for a frozen golden's calls.
func TestAFrozenCallEntryThatHoldsAPathIsNotRefused(t *testing.T) {
	root := t.TempDir()
	options := Options{Root: root, JWTKey: "k"}
	frozen, _ := frozenWithEnv(t, currentKey(t, options), pythonEnvKeyVersion)
	frozen.verifiedRoot = root
	if _, err := frozen.callEnvKey([]string{"EXTRA=" + filepath.Join(root, "x")}); err != nil && strings.Contains(err.Error(), "absolute path of a checkout") {
		t.Fatalf("a frozen call is refused as a recording is: %v", err)
	}
}

// The module root of the repository the test runs in is a root too, whatever Options.Root says.
func TestRecordingRefusesAValueThatHoldsTheModuleRoot(t *testing.T) {
	roots := checkoutRoots(Options{})
	if len(roots) == 0 {
		t.Fatal("no module root found from the test's directory")
	}
	moduleRoot := roots[0]
	options := Options{Root: t.TempDir(), JWTKey: "k", PythonEnv: []string{"DATA_DIR=" + filepath.Join(moduleRoot, "internal")}}
	err := recordingGolden(t).bindPythonEnv(options)
	if err == nil || !strings.Contains(err.Error(), "DATA_DIR") {
		t.Fatalf("a value holding the module root is not refused: %v", err)
	}
}
