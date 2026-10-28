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

func TestPlantedTakenFromUntilIsRefused(t *testing.T) {
	root := writeTree(t, map[string]string{"p/x_test.go": head + "func TestUntil(t *testing.T) {\n\tleft := time.Until(time.Now().Add(time.Hour))\n\tif left < time.Minute {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"})
	wantOne(t, check(t, root, nil), "TestUntil")
}

func TestTimestampArithmeticWithSubIsNotAMeasurement(t *testing.T) {
	root := writeTree(t, map[string]string{"p/x_test.go": head + "func TestStamps(t *testing.T) {\n\ta, b := time.Now(), time.Now()\n\tif b.Sub(a) > time.Hour {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"})
	wantClean(t, check(t, root, nil))
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

// A duration measured in a child process and carried to the parent as KEY=<number> is still a measurement: the parent
// compares it with a constant the same way a same-process time.Since is compared (CHAOS-11757).
const carriedHelper = "func helper() {\n\ts := time.Now()\n\tos.Stderr.WriteString(\"STOP_MS=\" + strconv.FormatInt(time.Since(s).Milliseconds(), 10) + \" CODE=\" + strconv.Itoa(0) + \"\\n\")\n}\n\n"

func TestPlantedChildEncodedMillisecondsAgainstAConstantIsRefused(t *testing.T) {
	body := carriedHelper + "func TestStop(t *testing.T) {\n\tvar ms int64\n\tvar code int\n\tfmt.Sscanf(text, \"STOP_MS=%d CODE=%d\", &ms, &code)\n\tif stop := time.Duration(ms) * time.Millisecond; stop >= 4*time.Second {\n\t\tt.Fatal(\"slow\")\n\t}\n}\n"
	wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil), "p/x_test.go", "TestStop", "D6418", "child process")
}

func TestPlantedChildMillisecondsAgainstALimitVariableIsRefused(t *testing.T) {
	body := carriedHelper + "func TestOtlp(t *testing.T) {\n\tvar millis int64\n\tvar code int\n\tfmt.Sscanf(text, \"STOP_MS=%d CODE=%d\", &millis, &code)\n\tif limit := (time.Second + 1500*time.Millisecond).Milliseconds(); millis > limit {\n\t\tt.Fatal(\"slow\")\n\t}\n}\n"
	wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil), "TestOtlp")
}

func TestPlantedFprintfEmittedMillisecondsAreRefused(t *testing.T) {
	helper := "func helper() {\n\ts := time.Now()\n\tfmt.Fprintf(os.Stderr, \"SHUTDOWN_MS=%d CODE=%d\\n\", time.Since(s).Milliseconds(), 0)\n}\n\n"
	body := helper + "func TestFprintf(t *testing.T) {\n\tvar millis int64\n\tvar code int\n\tfmt.Sscanf(text, \"SHUTDOWN_MS=%d CODE=%d\", &millis, &code)\n\tif millis > 2500 {\n\t\tt.Fatal(\"slow\")\n\t}\n}\n"
	wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil), "TestFprintf")
}

func TestPlantedShellSecondsParsedByAHelperAreRefused(t *testing.T) {
	body := "const script = \"start=$SECONDS\\nstop_service x\\necho \\\"A_ELAPSED=$((SECONDS - start))\\\"\\n\"\n\nfunc elapsedOf(stdout, key string) (int, bool) {\n\tn, err := strconv.Atoi(stdout)\n\treturn n, err == nil\n}\n\nfunc TestTeardown(t *testing.T) {\n\tprompt, ok := elapsedOf(out, \"A_ELAPSED\")\n\tif !ok {\n\t\treturn\n\t}\n\tif prompt <= 3 {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil), "TestTeardown")
}

func TestCarriedMeasurementTakesTheSameMarkerAndAllowlistRow(t *testing.T) {
	test := "func TestStop(t *testing.T) {\n\tvar ms int64\n\tfmt.Sscanf(text, \"STOP_MS=%d\", &ms)\n\t%s\n\tif time.Duration(ms)*time.Millisecond > time.Minute {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	src := carriedHelper + strings.Replace(test, "%s", "//abstime:allow hang ceiling, never a budget", 1)
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + src}), nil))
	src = carriedHelper + strings.Replace(test, "%s", "", 1)
	rows := []abstime.Row{{File: "p/x_test.go", Func: "TestStop", Reason: "pending", Line: 2}}
	wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + src}), nil), "TestStop")
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + src}), rows))
	stale := writeTree(t, map[string]string{"p/x_test.go": head + carriedHelper + "func TestStop(t *testing.T) {}\n"})
	wantOne(t, check(t, stale, rows), "stale allowlist row")
}

// Limits of the carried-measurement rule: each is stated in the findAssertions doc comment and pinned here, so a
// change of the rule shows in a test.

func TestCarriedHangCeilingAndExitCodeAreNotAssertions(t *testing.T) {
	body := carriedHelper + "func TestStop(t *testing.T) {\n\tvar ms int64\n\tvar code int\n\tfmt.Sscanf(text, \"STOP_MS=%d CODE=%d\", &ms, &code)\n\tselect {\n\tcase <-done:\n\tcase <-time.After(30 * time.Second):\n\t\tt.Fatal(\"hang\")\n\t}\n\tif code > 1 || code < 0 {\n\t\tt.Fatal(code)\n\t}\n\tt.Log(ms)\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}

func TestRatioOfTwoCarriedValuesIsNotAnAbsoluteBudget(t *testing.T) {
	helper := "func helper() {\n\ts := time.Now()\n\tos.Stderr.WriteString(\"FAST_MS=\" + strconv.FormatInt(time.Since(s).Milliseconds(), 10) + \"\\n\")\n\tos.Stderr.WriteString(\"SLOW_MS=\" + strconv.FormatInt(time.Since(s).Milliseconds(), 10) + \"\\n\")\n}\n\n"
	body := helper + "func TestRatio(t *testing.T) {\n\tvar fast, slow int64\n\tfmt.Sscanf(text, \"FAST_MS=%d\", &fast)\n\tfmt.Sscanf(text, \"SLOW_MS=%d\", &slow)\n\tif 2*fast >= slow {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}

func TestAKeyThatCarriesNoMeasurementIsNotTracked(t *testing.T) {
	helper := "func helper(port int) {\n\tos.Stderr.WriteString(\"PORT=\" + strconv.Itoa(port) + \"\\n\")\n}\n\n"
	body := helper + "func TestPort(t *testing.T) {\n\tvar port int\n\tfmt.Sscanf(text, \"PORT=%d\", &port)\n\tif port > 1024 {\n\t\tt.Log(port)\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}

func TestStringsIndexOfAKeyIsNotACarriedValue(t *testing.T) {
	body := carriedHelper + "func TestIndex(t *testing.T) {\n\tindex := strings.Index(text, \"STOP_MS=\")\n\tif index < 3 {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}

func TestEmitterInAnotherFileIsOutOfScope(t *testing.T) {
	root := writeTree(t, map[string]string{
		"p/helper_test.go": head + carriedHelper,
		"p/x_test.go":      head + "func TestStop(t *testing.T) {\n\tvar ms int64\n\tfmt.Sscanf(text, \"STOP_MS=%d\", &ms)\n\tif ms > 4000 {\n\t\tt.Fatal(\"x\")\n\t}\n}\n",
	})
	wantClean(t, check(t, root, nil))
}

func TestDurationCarriedAsJSONIsOutOfScope(t *testing.T) {
	body := "func helper() {\n\ts := time.Now()\n\tjson.NewEncoder(os.Stderr).Encode(map[string]int64{\"ms\": time.Since(s).Milliseconds()})\n}\n\nfunc TestJSON(t *testing.T) {\n\tvar got map[string]int64\n\tjson.Unmarshal(raw, &got)\n\tif got[\"ms\"] > 4000 {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}

// Reviewer probes of the carried-measurement rule (CHAOS-11757): each is a planted shape the rule must refuse, or a
// legal shape it must leave alone.

func TestPlantedIndexedPrintfVerbsAreFollowedToTheirArgument(t *testing.T) {
	helper := "func helper() {\n\tstart := time.Now()\n\ts := fmt.Sprintf(\"DURATION_MS=%[2]d CODE=%[1]d\", 0, time.Since(start).Milliseconds())\n\tos.Stderr.WriteString(s)\n}\n\n"
	body := helper + "func TestIndexed(t *testing.T) {\n\tvar ms int64\n\tvar code int\n\tfmt.Sscanf(text, \"DURATION_MS=%d CODE=%d\", &ms, &code)\n\tif ms > 5000 {\n\t\tt.Fatal(\"slow\")\n\t}\n}\n"
	wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil), "TestIndexed")
}

func TestIndexedVerbOnTheOtherArgumentIsNotAnEmitter(t *testing.T) {
	helper := "func helper() {\n\tstart := time.Now()\n\ts := fmt.Sprintf(\"CODE=%[1]d DURATION_MS=%[1]d\", 0, time.Since(start).Milliseconds())\n\tos.Stderr.WriteString(s)\n}\n\n"
	body := helper + "func TestIndexed(t *testing.T) {\n\tvar ms int64\n\tfmt.Sscanf(text, \"DURATION_MS=%d\", &ms)\n\tif ms > 5000 {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}

func TestPlantedPrintFamilyKeyAsItsOwnArgumentIsRefused(t *testing.T) {
	for name, emit := range map[string]string{
		"Fprintln": "fmt.Fprintln(os.Stderr, \"DURATION_MS=\", time.Since(s).Milliseconds())",
		"Println":  "fmt.Println(\"DURATION_MS=\", time.Since(s).Milliseconds())",
		"Fprint":   "fmt.Fprint(os.Stderr, \"DURATION_MS=\", time.Since(s).Milliseconds())",
		"Sprint":   "_ = fmt.Sprint(\"DURATION_MS=\", time.Since(s).Milliseconds())",
		"Print":    "fmt.Print(\"DURATION_MS=\", time.Since(s).Milliseconds())",
		"Sprintln": "_ = fmt.Sprintln(\"DURATION_MS=\", time.Since(s).Milliseconds())",
	} {
		body := "func helper() {\n\ts := time.Now()\n\t" + emit + "\n}\n\nfunc TestPrint(t *testing.T) {\n\tvar ms int64\n\tfmt.Sscanf(text, \"DURATION_MS=%d\", &ms)\n\tif ms > 5000 {\n\t\tt.Fatal(\"slow\")\n\t}\n}\n"
		got := check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil)
		if len(got) != 1 || !strings.Contains(got[0], "TestPrint") {
			t.Fatalf("%s: want one violation naming TestPrint, got %q", name, got)
		}
	}
}

func TestPrintFamilyKeyBeforeAnUnmeasuredArgumentIsNotAnEmitter(t *testing.T) {
	body := "func helper(port int) {\n\tfmt.Fprintln(os.Stderr, \"PORT=\", port)\n}\n\nfunc TestPort(t *testing.T) {\n\tvar port int\n\tfmt.Sscanf(text, \"PORT=%d\", &port)\n\tif port > 1024 {\n\t\tt.Log(port)\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}

func TestQuotientOfTwoMeasuredValuesIsARatioEvenThroughAVariable(t *testing.T) {
	direct := "func TestRatioVar(t *testing.T) {\n\ta := time.Now()\n\tfast := time.Since(a)\n\tb := time.Now()\n\tslow := time.Since(b)\n\tratio := float64(slow) / float64(fast)\n\tif ratio > 2 {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + direct}), nil))
	helper := "func helper() {\n\ts := time.Now()\n\tos.Stderr.WriteString(\"A_MS=\" + strconv.FormatInt(time.Since(s).Milliseconds(), 10) + \"\\n\")\n\tos.Stderr.WriteString(\"B_MS=\" + strconv.FormatInt(time.Since(s).Milliseconds(), 10) + \"\\n\")\n}\n\n"
	carried := helper + "func TestCarriedRatioVar(t *testing.T) {\n\tvar a, b int64\n\tfmt.Sscanf(text, \"A_MS=%d\", &a)\n\tfmt.Sscanf(text, \"B_MS=%d\", &b)\n\tratio := float64(b) / float64(a)\n\tif ratio > 2 {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + carried}), nil))
}

func TestQuotientOfAMeasuredValueByAConstantIsStillMeasured(t *testing.T) {
	body := "func TestPerOp(t *testing.T) {\n\ta := time.Now()\n\tper := time.Since(a) / 100\n\tif per > time.Millisecond {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil), "TestPerOp")
}

func TestALoggingHelperIsNotAReaderOfACarriedKey(t *testing.T) {
	body := carriedHelper + "func logKey(key string) int {\n\tfmt.Println(\"key\", key)\n\treturn 2\n}\n\nfunc TestLogged(t *testing.T) {\n\tcode := logKey(\"STOP_MS\")\n\tif code > 1 {\n\t\tt.Fatal(code)\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}

func TestHelpersThatParseAreReadersWhateverTheParser(t *testing.T) {
	for name, parse := range map[string]string{
		"Atoi":          "n, err := strconv.Atoi(out)\n\treturn n, err == nil",
		"ParseInt":      "n, err := strconv.ParseInt(out, 10, 64)\n\treturn int(n), err == nil",
		"ParseFloat":    "f, err := strconv.ParseFloat(out, 64)\n\treturn int(f), err == nil",
		"Sscanf":        "var n int\n\t_, err := fmt.Sscanf(out, \"%d\", &n)\n\treturn n, err == nil",
		"ParseDuration": "d, err := time.ParseDuration(out)\n\treturn int(d), err == nil",
		"Unmarshal":     "var n int\n\terr := json.Unmarshal([]byte(out), &n)\n\treturn n, err == nil",
	} {
		body := carriedHelper + "func readKey(out, key string) (int, bool) {\n\t" + parse + "\n}\n\nfunc TestHelper(t *testing.T) {\n\tms, ok := readKey(text, \"STOP_MS\")\n\tif ok && ms > 4000 {\n\t\tt.Fatal(ms)\n\t}\n}\n"
		got := check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil)
		if len(got) != 1 || !strings.Contains(got[0], "TestHelper") {
			t.Fatalf("%s: want one violation naming TestHelper, got %q", name, got)
		}
	}
}

func TestAHelperDeclaredInAnotherFileIsNotAReader(t *testing.T) {
	root := writeTree(t, map[string]string{
		"p/helper_test.go": head + "func readKey(out, key string) (int, bool) {\n\tn, err := strconv.Atoi(out)\n\treturn n, err == nil\n}\n",
		"p/x_test.go":      head + carriedHelper + "func TestHelper(t *testing.T) {\n\tms, _ := readKey(text, \"STOP_MS\")\n\tif ms > 4000 {\n\t\tt.Fatal(ms)\n\t}\n}\n",
	})
	wantClean(t, check(t, root, nil))
}

func TestAComputedKeyIsOutOfScope(t *testing.T) {
	body := "func helper() {\n\ts := time.Now()\n\tkey := \"STOP\" + \"_MS\"\n\tos.Stderr.WriteString(key + \"=\" + strconv.FormatInt(time.Since(s).Milliseconds(), 10))\n}\n\nfunc TestComputed(t *testing.T) {\n\tvar ms int64\n\tfmt.Sscanf(text, \"STOP_MS=%d\", &ms)\n\tif ms > 4000 {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}

func TestALowercaseKeyIsOutOfScope(t *testing.T) {
	body := "func helper() {\n\ts := time.Now()\n\tos.Stderr.WriteString(\"stop_ms=\" + strconv.FormatInt(time.Since(s).Milliseconds(), 10))\n}\n\nfunc TestLower(t *testing.T) {\n\tvar ms int64\n\tfmt.Sscanf(text, \"stop_ms=%d\", &ms)\n\tif ms > 4000 {\n\t\tt.Fatal(\"x\")\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}

// A reader helper may wrap one parsing helper: one level of call depth is followed, two are not (stated limit).
const nestedParser = "func parseMillis(out string) int {\n\tn, _ := strconv.Atoi(out)\n\treturn n\n}\n\n"

func TestAHelperThatCallsAParsingHelperIsAReader(t *testing.T) {
	body := carriedHelper + nestedParser + "func readKey(out, key string) int {\n\treturn parseMillis(out)\n}\n\nfunc TestNested(t *testing.T) {\n\tms := readKey(text, \"STOP_MS\")\n\tif ms > 4000 {\n\t\tt.Fatal(ms)\n\t}\n}\n"
	wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil), "TestNested")
}

func TestTwoLevelsOfHelperCallsAreOutOfScope(t *testing.T) {
	body := carriedHelper + nestedParser + "func readKey(out, key string) int {\n\treturn parseMillis(out)\n}\n\nfunc readKeyTwice(out, key string) int {\n\treturn readKey(out, key)\n}\n\nfunc TestDeep(t *testing.T) {\n\tms := readKeyTwice(text, \"STOP_MS\")\n\tif ms > 4000 {\n\t\tt.Fatal(ms)\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}

// Round-2 reviewer probes (CHAOS-11757, ACP-RULING-r2-fixscope): shell reports through a variable or printf, and fixed
// literal inputs that must not count as a child's report.

const shellElapsedOf = `
func elapsedOf(stdout, key string) (int, bool) {
	n, err := strconv.Atoi(stdout)
	return n, err == nil
}

func TestShell(t *testing.T) {
	prompt, ok := elapsedOf(out, "A_ELAPSED")
	if ok && prompt <= 3 {
		t.Fatal("x")
	}
}
`

func TestPlantedShellAssignedThenEchoedKeyIsRefused(t *testing.T) {
	for name, script := range map[string]string{
		"dollar": `"start=$SECONDS\nelapsed=$((SECONDS - start))\necho \"A_ELAPSED=$elapsed\"\n"`,
		"braces": `"elapsed=${SECONDS}\necho \"A_ELAPSED=${elapsed}\"\n"`,
		"epoch":  `"t=$EPOCHREALTIME\necho A_ELAPSED=$t\n"`,
		"upper":  `"ELAPSED=$((SECONDS - start))\necho \"A_ELAPSED=$ELAPSED\"\n"`,
		"date":   `"d=$(date +%s)\necho \"A_ELAPSED=$d\"\n"`,
		"quoted": `"elapsed=$((SECONDS - start))\necho 'A_ELAPSED='\"$elapsed\"\n"`,
	} {
		body := "const script = " + script + "\n" + shellElapsedOf
		got := check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil)
		if len(got) != 1 || !strings.Contains(got[0], "TestShell") {
			t.Fatalf("%s: want one violation naming TestShell, got %q", name, got)
		}
	}
}

func TestPlantedShellPrintfKeyIsRefused(t *testing.T) {
	for name, script := range map[string]string{
		"arith":    `"printf 'A_ELAPSED=%s\\n' \"$((SECONDS - start))\"\n"`,
		"seconds":  `"printf 'A_ELAPSED=%d\\n' $SECONDS\n"`,
		"variable": `"elapsed=$((SECONDS - start))\nprintf 'A_ELAPSED=%s\\n' \"$elapsed\"\n"`,
		"second":   `"printf 'CODE=%s A_ELAPSED=%s\\n' 0 \"$((SECONDS - start))\"\n"`,
	} {
		body := "const script = " + script + "\n" + shellElapsedOf
		got := check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil)
		if len(got) != 1 || !strings.Contains(got[0], "TestShell") {
			t.Fatalf("%s: want one violation naming TestShell, got %q", name, got)
		}
	}
}

func TestShellEchoOfAnUnmeasuredVariableIsNotAnEmitter(t *testing.T) {
	for name, script := range map[string]string{
		"echo":   `"name=x\necho \"A_ELAPSED=$name\"\n"`,
		"printf": `"printf 'A_ELAPSED=%s\\n' \"$port\"\n"`,
		"first":  `"printf 'CODE=%s A_ELAPSED=%s\\n' \"$((SECONDS - start))\" 0\n"`,
	} {
		body := "const script = " + script + "\n" + shellElapsedOf
		wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
		_ = name
	}
}

const logKeyDiagnostic = `
func diag() {
	s := time.Now()
	log.Printf("STOP_MS=%d", time.Since(s).Milliseconds())
}

`

func TestFixedLiteralInputIsNotAChildReport(t *testing.T) {
	for name, test := range map[string]string{
		"literal":    "var ms int\n\tfmt.Sscanf(\"STOP_MS=2\", \"STOP_MS=%d\", &ms)",
		"identifier": "var ms int\n\tin := \"STOP_MS=2\"\n\tfmt.Sscanf(in, \"STOP_MS=%d\", &ms)",
		"helper":     "ms, _ := readKey(\"STOP_MS=2\", \"STOP_MS\")",
	} {
		body := logKeyDiagnostic + "func readKey(out, key string) (int, bool) {\n\tn, err := strconv.Atoi(out)\n\treturn n, err == nil\n}\n\nfunc TestFixed(t *testing.T) {\n\t" + test + "\n\tif ms > 1 {\n\t\tt.Fatal(ms)\n\t}\n}\n"
		got := check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil)
		if len(got) != 0 {
			t.Fatalf("%s: a fixed literal input is not a child's report, got %q", name, got)
		}
	}
}

func TestAReassignedIdentifierIsNotALiteralSource(t *testing.T) {
	body := logKeyDiagnostic + "func TestReassigned(t *testing.T) {\n\tvar ms int\n\tin := \"STOP_MS=2\"\n\tin = readOutput()\n\tfmt.Sscanf(in, \"STOP_MS=%d\", &ms)\n\tif ms > 1 {\n\t\tt.Fatal(ms)\n\t}\n}\n"
	wantOne(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil), "TestReassigned")
}

func TestStringsContainsAndCutOfAKeyCarryNothing(t *testing.T) {
	body := carriedHelper + "func TestContains(t *testing.T) {\n\tn := strings.Count(text, \"STOP_MS=\")\n\thas := strings.Contains(text, \"STOP_MS=\")\n\tbefore, _, _ := strings.Cut(text, \"STOP_MS=\")\n\tif n > 1 || len(before) > 4 || !has {\n\t\tt.Fatal(n)\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}

func TestAFileChannelIsOutOfScope(t *testing.T) {
	body := "func helper() {\n\ts := time.Now()\n\tos.WriteFile(path, []byte(strconv.FormatInt(time.Since(s).Milliseconds(), 10)), 0o644)\n}\n\nfunc TestFile(t *testing.T) {\n\traw, _ := os.ReadFile(path)\n\tms, _ := strconv.Atoi(string(raw))\n\tif ms > 4000 {\n\t\tt.Fatal(ms)\n\t}\n}\n"
	wantClean(t, check(t, writeTree(t, map[string]string{"p/x_test.go": head + body}), nil))
}
