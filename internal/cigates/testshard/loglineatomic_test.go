package testshard_test

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-12386: go test -json splits a test line longer than 1024 bytes into several output events, and packages run in
// parallel, so another package's event (its "ok" summary) can land between two chunks. The plain log of a leg is rebuilt from the
// events; it must keep every line of one package whole, or the unweighted guard (column-1 match) misses the summary line.

const jqFilterMarker = "GO_TEST_LEG_JQ='"

func scriptText(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// converterBlock is the GO_TEST_LEG_JQ assignment of ci/check_go.sh, text as written, and the filter inside it.
func converterBlock(t *testing.T) (block, filter string) {
	t.Helper()
	script := scriptText(t)
	i := strings.Index(script, jqFilterMarker)
	if i < 0 {
		t.Fatal("ci/check_go.sh has no GO_TEST_LEG_JQ")
	}
	rest := script[i+len(jqFilterMarker):]
	j := strings.Index(rest, "'\n")
	if j < 0 {
		t.Fatal("GO_TEST_LEG_JQ is not closed")
	}
	return script[i : i+len(jqFilterMarker)+j+2], rest[:j]
}

func needJQ(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed")
	}
}

func convert(t *testing.T, events string) string {
	t.Helper()
	needJQ(t)
	_, filter := converterBlock(t)
	cmd := exec.Command("jq", "-nR", "--unbuffered", "-rj", filter)
	cmd.Stdin = strings.NewReader(events)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("the converter failed: %v", err)
	}
	return string(out)
}

func pkgEnd(t *testing.T, pkg, action string) string {
	t.Helper()
	return fmt.Sprintf("{\"Action\":%q,\"Package\":%q,\"Elapsed\":0.01}\n", action, pkg)
}

func chunks(s string, n int) []string {
	var out []string
	for len(s) > n {
		out = append(out, s[:n])
		s = s[n:]
	}
	return append(out, s)
}

func TestTheLogConverterKeepsALongLineWholeWhenAnotherPackagesLineLandsInIt(t *testing.T) {
	t.Parallel()
	needJQ(t)
	long := "partial run: " + strings.Repeat("REST:GET:/api/v1/x ", 1500)
	ch := chunks(long+"\n", 1024)
	if len(ch) < 3 {
		t.Fatalf("the long line has %d chunks, want 3 or more", len(ch))
	}
	ok := "ok  \tgithub.com/x/b\t0.040s\n"
	var ev strings.Builder
	for i, c := range ch {
		ev.WriteString(outputEvent(t, "github.com/x/a", "TestA", c))
		if i == 0 || i == 1 {
			ev.WriteString(outputEvent(t, "github.com/x/b", "", ok))
			ev.WriteString(pkgEnd(t, "github.com/x/b", "pass"))
		}
	}
	got := convert(t, ev.String())
	if want := ok + ok + long + "\n"; got != want {
		t.Fatalf("a package's summary line landed inside another package's long line:\n%q", got)
	}
}

func TestTheLogConverterKeepsTwoInterleavedLongLinesWhole(t *testing.T) {
	t.Parallel()
	a, b := strings.Repeat("a", 3000), strings.Repeat("b", 3000)
	ca, cb := chunks(a+"\n", 1024), chunks(b+"\n", 1024)
	var ev strings.Builder
	for i := 0; i < len(ca) || i < len(cb); i++ {
		if i < len(ca) {
			ev.WriteString(outputEvent(t, "p/a", "", ca[i]))
		}
		if i < len(cb) {
			ev.WriteString(outputEvent(t, "p/b", "", cb[i]))
		}
	}
	if got, want := convert(t, ev.String()), a+"\n"+b+"\n"; got != want {
		t.Fatalf("two long lines of two packages were mixed:\n%.80q ...", got)
	}
}

func TestTheLogConverterFlushesAPackagesUnfinishedLineAtTheEnd(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct{ events, want string }{
		"end of stream": {outputEvent(t, "p/a", "", "no newline at the end"), "no newline at the end"},
		"package end": {
			outputEvent(t, "p/a", "", "unfinished") + outputEvent(t, "p/b", "", "line\n") + pkgEnd(t, "p/a", "fail") + outputEvent(t, "p/b", "", "more\n"),
			"line\nunfinishedmore\n",
		},
		"a test end does not flush": {
			outputEvent(t, "p/a", "TestA", "half ") + "{\"Action\":\"pass\",\"Package\":\"p/a\",\"Test\":\"TestA\",\"Elapsed\":0}\n" + outputEvent(t, "p/b", "", "other\n") + outputEvent(t, "p/a", "TestB", "line\n"),
			"other\nhalf line\n",
		},
	} {
		if got := convert(t, c.events); got != c.want {
			t.Errorf("%s: got %q, want %q", name, got, c.want)
		}
	}
}

func TestTheLogConverterKeepsANonJSONLineBetweenTheLinesAroundIt(t *testing.T) {
	t.Parallel()
	ev := outputEvent(t, "p/a", "", "1\n") + "# p/a [build failed]\n" + outputEvent(t, "p/a", "", "2\n") + "not an event\n" + outputEvent(t, "p/b", "", "3\n")
	if got, want := convert(t, ev), "1\n# p/a [build failed]\n2\nnot an event\n3\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// Lines of one package stay whole, in their order, and none is lost or added, whatever the chunking and the interleave.
func TestTheLogConverterKeepsEveryLineOfEveryPackageWholeAndInOrder(t *testing.T) {
	t.Parallel()
	for seed := int64(1); seed <= 25; seed++ {
		rng := rand.New(rand.NewSource(seed))
		pkgs := []string{"p/a", "p/b", "p/c"}
		lines := map[string][]string{}
		queue := map[string][]string{}
		for _, p := range pkgs {
			for i := 0; i < 12; i++ {
				var l string
				switch i % 4 {
				case 0:
					l = fmt.Sprintf("=== RUN   %s/Test%d\n", p, i)
				case 1:
					l = fmt.Sprintf("--- FAIL: %s/Test%d (0.00s)\n", p, i)
				case 2:
					l = fmt.Sprintf("%s long %d %s\n", p, i, strings.Repeat("z", rng.Intn(5000)))
				default:
					l = fmt.Sprintf("ok  \t%s\t0.0%ds\n", p, i)
				}
				lines[p] = append(lines[p], l)
			}
			queue[p] = chunkRandom(rng, strings.Join(lines[p], ""))
		}
		var ev strings.Builder
		for {
			var open []string
			for _, p := range pkgs {
				if len(queue[p]) > 0 {
					open = append(open, p)
				}
			}
			if len(open) == 0 {
				break
			}
			p := open[rng.Intn(len(open))]
			ev.WriteString(outputEvent(t, p, fmt.Sprintf("Test%d", rng.Intn(3)), queue[p][0]))
			queue[p] = queue[p][1:]
		}
		for _, p := range pkgs {
			ev.WriteString(pkgEnd(t, p, "fail"))
		}
		got := convert(t, ev.String())
		pos := map[string]int{}
		for _, p := range pkgs {
			for _, l := range lines[p] {
				pos[l] = -1
			}
		}
		rest := got
		next := map[string]int{}
		for rest != "" {
			i := strings.Index(rest, "\n")
			if i < 0 {
				t.Fatalf("seed %d: the log ends inside a line: %q", seed, rest)
			}
			l := rest[:i+1]
			rest = rest[i+1:]
			p := strings.Fields(l)[0]
			if strings.HasPrefix(l, "ok  \t") {
				p = strings.Fields(l)[1]
			}
			if strings.HasPrefix(l, "=== RUN") {
				p = strings.Split(strings.Fields(l)[2], "/")[0] + "/" + strings.Split(strings.Fields(l)[2], "/")[1]
			}
			if strings.HasPrefix(l, "--- FAIL") {
				f := strings.Fields(l)[2]
				p = strings.Join(strings.Split(f, "/")[:2], "/")
			}
			if next[p] >= len(lines[p]) || lines[p][next[p]] != l {
				t.Fatalf("seed %d: a line of %s is cut, moved or foreign: %.100q", seed, p, l)
			}
			next[p]++
		}
		for _, p := range pkgs {
			if next[p] != len(lines[p]) {
				t.Fatalf("seed %d: %s has %d of %d lines in the log", seed, p, next[p], len(lines[p]))
			}
		}
	}
}

func chunkRandom(rng *rand.Rand, s string) []string {
	var out []string
	for len(s) > 0 {
		n := 1 + rng.Intn(1024)
		if n > len(s) {
			n = len(s)
		}
		out = append(out, s[:n])
		s = s[n:]
	}
	return out
}

// Through the real test leg: the interleave of the CI run (a long line of one package cut by another package's summary) must not
// fail the unweighted guard, and the plain log keeps the long line whole.
func TestTheTestLegSurvivesAnotherPackagesSummaryInsideALongLine(t *testing.T) {
	t.Parallel()
	plan := fullPlan(t, 350, 350)
	if len(plan) == 0 {
		t.Fatal("test shard 350/350 plans no root-module package")
	}
	long := "partial run: " + strings.Repeat("REST:GET:/api/v1/y ", 1500)
	var ev strings.Builder
	ch := chunks(long+"\n", 1024)
	for i, c := range ch {
		ev.WriteString(outputEvent(t, "github.com/x/restlike", "TestLong", c))
		if i == 1 {
			for _, p := range plan {
				ev.WriteString(outputEvent(t, p, "", fmt.Sprintf("ok  \t%s\t0.010s\n", p)))
				ev.WriteString(pkgEnd(t, p, "pass"))
			}
		}
	}
	events := writeLog(t, ev.String())
	for _, env := range [][]string{freshEnv(false), freshEnv(true)} {
		r := runCheckGo(t, append(env, "FAKE_GO_TEST_STDOUT="+events), false, "ci-leg", "test", "350", "350")
		if r.Err != nil {
			t.Errorf("the leg failed on a summary line inside a long line: %v\n%s", r.Err, r.Stderr)
		}
		if strings.Contains(r.Stderr, "no summary line") {
			t.Errorf("the guard lost a summary line: %s", r.Stderr)
		}
		if !strings.Contains(r.Stdout, long+"\n") {
			t.Errorf("the plain log does not hold the long line whole")
		}
		for _, p := range plan {
			if !strings.Contains(r.Stdout, "\nok  \t"+p+"\t0.010s\n") && !strings.HasPrefix(r.Stdout, "ok  \t"+p+"\t0.010s\n") {
				t.Errorf("the plain log has no column-1 summary line of %s", p)
			}
		}
	}
}

// go_test_json_run_t (the race-excluded rows call) rebuilds the same log: run it as it is written in ci/check_go.sh.
func TestTheRowsCallsLogIsLineWholeToo(t *testing.T) {
	t.Parallel()
	needJQ(t)
	block, _ := converterBlock(t)
	script := scriptText(t)
	fn := functionBody(t, script, "go_test_json_run_t") + "\n}\n"
	dir := t.TempDir()
	long := "partial run: " + strings.Repeat("REST:GET:/api/v1/w ", 1500)
	ok := "ok  \tgithub.com/x/b\t0.040s\n"
	var ev strings.Builder
	for i, c := range chunks(long+"\n", 1024) {
		ev.WriteString(outputEvent(t, "github.com/x/a", "TestA", c))
		if i == 0 {
			ev.WriteString(outputEvent(t, "github.com/x/b", "", ok))
		}
	}
	events := filepath.Join(dir, "events.json")
	if err := os.WriteFile(events, []byte(ev.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte("#!/usr/bin/env bash\ncat \"${EVENTS}\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	driver := "set -euo pipefail\ndie() { echo \"$*\" >&2; exit 2; }\nGO_ENV_OFF=(env)\n" + block + fn +
		"\ngo_test_json_run_t 10m \"${DIR}/rows.json\" \"${DIR}/rows.log\" ./x\n"
	cmd := exec.Command("bash", "-c", driver)
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "EVENTS="+events, "DIR="+dir)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go_test_json_run_t: %v", err)
	}
	logged, err := os.ReadFile(filepath.Join(dir, "rows.log"))
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"stdout": string(out), "log": string(logged)} {
		if want := ok + long + "\n"; text != want {
			t.Errorf("%s of go_test_json_run_t: a summary line landed inside a long line:\n%.120q", name, text)
		}
	}
}
