package abstime_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/abstime"
)

func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func check(t *testing.T, root string, rows []abstime.Row) []string {
	t.Helper()
	got, err := abstime.Check(root, rows)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	return got
}

func wantClean(t *testing.T, got []string) {
	t.Helper()
	if len(got) != 0 {
		t.Fatalf("want no violation, got %q", got)
	}
}

func wantOne(t *testing.T, got []string, substrs ...string) {
	t.Helper()
	if len(got) != 1 {
		t.Fatalf("want exactly one violation, got %q", got)
	}
	for _, s := range substrs {
		if !strings.Contains(got[0], s) {
			t.Fatalf("violation %q lacks %q", got[0], s)
		}
	}
}

const head = "package p\n\nimport (\n\t\"context\"\n\t\"testing\"\n\t\"time\"\n)\n\nvar _ = context.Background\n\n"

func TestPlantedSinceAgainstAConstantIsRefused(t *testing.T) {
	root := writeTree(t, map[string]string{"p/x_test.go": head + "func TestSlow(t *testing.T) {\n\tstart := time.Now()\n\tif time.Since(start) > time.Second {\n\t\tt.Fatal(\"slow\")\n\t}\n}\n"})
	wantOne(t, check(t, root, nil), "p/x_test.go:13", "TestSlow", "D6418", "//abstime:allow")
}

func TestPlantedElapsedVariableAgainstAConstantIsRefused(t *testing.T) {
	for name, cond := range map[string]string{
		"gt":       "elapsed > 5*time.Second",
		"ge":       "elapsed >= 200*time.Millisecond",
		"lt":       "elapsed < 30*time.Millisecond",
		"constant": "elapsed > budget",
		"reversed": "time.Second < elapsed",
	} {
		root := writeTree(t, map[string]string{"p/x_test.go": head + "const budget = time.Second\n\nfunc TestV(t *testing.T) {\n\tstart := time.Now()\n\telapsed := time.Since(start)\n\tif " + cond + " {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"})
		got := check(t, root, nil)
		if len(got) != 1 {
			t.Fatalf("%s: want one violation, got %q", name, got)
		}
	}
}

func TestPlantedTakenFromSubIsRefused(t *testing.T) {
	root := writeTree(t, map[string]string{"p/x_test.go": head + "func TestSub(t *testing.T) {\n\ta, b := time.Now(), time.Now()\n\ttook := b.Sub(a)\n\tif took > time.Second {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"})
	wantOne(t, check(t, root, nil), "TestSub")
}

func TestPlantedSliceOfTimingsIsRefused(t *testing.T) {
	root := writeTree(t, map[string]string{"p/x_test.go": head + "func TestSlice(t *testing.T) {\n\tvar took [3]time.Duration\n\tfor i := range took {\n\t\tstart := time.Now()\n\t\ttook[i] = time.Since(start)\n\t}\n\tfor _, d := range took {\n\t\tif d > 250*time.Millisecond {\n\t\t\tt.Fatal(\"x\")\n\t\t}\n\t}\n}\n"})
	wantOne(t, check(t, root, nil), "TestSlice")
}

func TestPlantedClosureInsideATestIsAttributedToTheTopLevelFunc(t *testing.T) {
	root := writeTree(t, map[string]string{"p/x_test.go": head + "func TestOuter(t *testing.T) {\n\tt.Run(\"a\", func(t *testing.T) {\n\t\tstart := time.Now()\n\t\tif time.Since(start) > time.Second {\n\t\t\tt.Fatal(\"x\")\n\t\t}\n\t})\n}\n"})
	wantOne(t, check(t, root, nil), "TestOuter")
}

func TestRatioAgainstABaselineInTheSameProcessPasses(t *testing.T) {
	root := writeTree(t, map[string]string{"p/x_test.go": head + "func TestRatio(t *testing.T) {\n\ts := time.Now()\n\tbase := time.Since(s)\n\ts = time.Now()\n\tbig := time.Since(s)\n\tif big > 2*base {\n\t\tt.Fatal(\"x\")\n\t}\n\tif time.Since(s) > 3*base {\n\t\tt.Fatal(\"y\")\n\t}\n}\n"})
	wantClean(t, check(t, root, nil))
}

func TestHangCeilingsAndPlainComparisonsAreNotAssertions(t *testing.T) {
	root := writeTree(t, map[string]string{"p/x_test.go": head + "func TestCeiling(t *testing.T) {\n\tctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)\n\tdefer cancel()\n\tselect {\n\tcase <-ctx.Done():\n\tcase <-time.After(10 * time.Second):\n\t\tt.Fatal(\"hang\")\n\t}\n\tn := 3\n\tif n > 2 {\n\t\tt.Log(n)\n\t}\n\tvar limit time.Duration = time.Second\n\tif limit > time.Minute {\n\t\tt.Fatal(limit)\n\t}\n}\n"})
	wantClean(t, check(t, root, nil))
}

func TestOnlyTestFilesAreScanned(t *testing.T) {
	root := writeTree(t, map[string]string{"p/x.go": "package p\n\nimport \"time\"\n\nfunc Slow() bool { return time.Since(time.Now()) > time.Second }\n"})
	wantClean(t, check(t, root, nil))
}

func TestMarkerAboveOrBesideAllowsAnAssertionWithItsReason(t *testing.T) {
	above := "func TestA(t *testing.T) {\n\tstart := time.Now()\n\t//abstime:allow hang ceiling, never a budget\n\tif time.Since(start) > time.Minute {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	beside := "func TestB(t *testing.T) {\n\tstart := time.Now()\n\tif time.Since(start) > time.Minute { //abstime:allow hang ceiling\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + above + "\n" + beside}), nil))
}

func TestMarkerWithoutAReasonIsRefused(t *testing.T) {
	src := "func TestA(t *testing.T) {\n\tstart := time.Now()\n\t//abstime:allow\n\tif time.Since(start) > time.Minute {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	got := check(t, writeTree(t, map[string]string{"p/x_test.go": head + src}), nil)
	if len(got) == 0 || !strings.Contains(strings.Join(got, "\n"), "reason") {
		t.Fatalf("want a refusal naming the missing reason, got %q", got)
	}
}

func TestMarkerTwoLinesAboveDoesNotAllow(t *testing.T) {
	src := "func TestA(t *testing.T) {\n\tstart := time.Now()\n\t//abstime:allow far away\n\tt.Log(start)\n\tif time.Since(start) > time.Minute {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + src}), nil), "TestA")
}

const planted = "func TestPlanted(t *testing.T) {\n\tstart := time.Now()\n\tif time.Since(start) > time.Second {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"

func TestAllowlistRowAllowsTheNamedFuncOnly(t *testing.T) {
	root := writeTree(t, map[string]string{"p/x_test.go": head + planted + "\n" + strings.Replace(planted, "TestPlanted", "TestOther", 1)})
	rows := []abstime.Row{{File: "p/x_test.go", Func: "TestPlanted", Reason: "pending", Line: 2}}
	wantOne(t, check(t, root, rows), "TestOther")
	wantClean(t, check(t, root, append(rows, abstime.Row{File: "p/x_test.go", Func: "TestOther", Reason: "pending", Line: 3})))
}

func TestStaleAllowlistRowIsRefused(t *testing.T) {
	root := writeTree(t, map[string]string{"p/x_test.go": head + "func TestFine(t *testing.T) {}\n"})
	rows := []abstime.Row{
		{File: "p/x_test.go", Func: "TestFine", Reason: "pending", Line: 4},
		{File: "p/gone_test.go", Func: "TestGone", Reason: "pending", Line: 5},
	}
	got := check(t, root, rows)
	if len(got) != 2 {
		t.Fatalf("want two stale-row refusals, got %q", got)
	}
	for _, v := range got {
		if !strings.Contains(v, "stale") || !strings.Contains(v, "allowlist.tsv:") {
			t.Fatalf("violation %q should say stale and name the allowlist line", v)
		}
	}
}

func TestParseAllowlistAcceptsRowsAndSkipsCommentsAndBlankLines(t *testing.T) {
	rows, err := abstime.ParseAllowlist(strings.NewReader("# header\n\np/x_test.go\tTestA\tpending CHAOS-11684 conversion\np/y_test.go\tTestB\tlower-bound wait\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0] != (abstime.Row{File: "p/x_test.go", Func: "TestA", Reason: "pending CHAOS-11684 conversion", Line: 3}) || rows[1].Line != 4 {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestMalformedAllowlistRowsAreRefused(t *testing.T) {
	for name, body := range map[string]string{
		"two fields":      "p/x_test.go\tTestA\n",
		"four fields":     "p/x_test.go\tTestA\treason\textra\n",
		"empty reason":    "p/x_test.go\tTestA\t \n",
		"empty func":      "p/x_test.go\t\treason\n",
		"not a test file": "p/x.go\tTestA\treason\n",
		"absolute path":   "/p/x_test.go\tTestA\treason\n",
		"parent path":     "../p/x_test.go\tTestA\treason\n",
		"backslash path":  "p\\x_test.go\tTestA\treason\n",
		"duplicate key":   "p/x_test.go\tTestA\treason\np/x_test.go\tTestA\tagain\n",
		"spaces not tabs": "p/x_test.go TestA reason\n",
	} {
		rows, err := abstime.ParseAllowlist(strings.NewReader(body))
		if err == nil {
			t.Fatalf("%s: want an error, got rows %+v", name, rows)
		}
		if !strings.Contains(err.Error(), "allowlist.tsv:") {
			t.Fatalf("%s: error %q should name the allowlist line", name, err)
		}
	}
}

func TestUnparsableTestFileIsAnError(t *testing.T) {
	root := writeTree(t, map[string]string{"p/x_test.go": "package p\n\nfunc {\n"})
	if _, err := abstime.Check(root, nil); err == nil {
		t.Fatal("want an error for a test file that does not parse: a gate that skips it scans nothing")
	}
}

func TestHiddenAndTestdataDirectoriesAreSkipped(t *testing.T) {
	body := head + planted
	root := writeTree(t, map[string]string{"p/testdata/x_test.go": body, ".hidden/x_test.go": body, "node_modules/m/x_test.go": body})
	wantClean(t, check(t, root, nil))
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// The real tree: every absolute time assertion is allowlisted with a reason, every allowlist row still matches one.
func TestRepositoryTestsHaveNoUnlistedAbsoluteTimeAssertion(t *testing.T) {
	f, err := os.Open("allowlist.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := abstime.ParseAllowlist(f)
	if err != nil {
		t.Fatal(err)
	}
	got, err := abstime.Check(repoRoot(t), rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("%d absolute time assertion problem(s):\n%s", len(got), strings.Join(got, "\n"))
	}
}
