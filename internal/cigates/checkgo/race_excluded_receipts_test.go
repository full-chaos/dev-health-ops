//go:build !race

package checkgo_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// These tests run ci/check_go.sh in child processes; -race adds nothing to them. ci/race_excluded_tests.tsv runs each one without -race.

const receiptsModulePath = "github.com/full-chaos/dev-health-ops"

type goTestEvent struct {
	Action  string `json:"Action"`
	Package string `json:"Package"`
	Test    string `json:"Test,omitempty"`
	Output  string `json:"Output,omitempty"`
}

func eventLines(t *testing.T, events []goTestEvent) string {
	t.Helper()
	var b strings.Builder
	for _, e := range events {
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(raw)
		b.WriteByte('\n')
	}
	return b.String()
}

// parseCensusRows reads census text with the rule of the loader in ci/check_go.sh: an empty line and a # line are skipped, every
// other line is <package><TAB><test> with both columns non-empty after trimming exactly space, tab and carriage return.
func parseCensusRows(text string) ([][2]string, error) {
	var rows [][2]string
	for i, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 2 || strings.Trim(f[0], " \t\r") == "" || strings.Trim(f[1], " \t\r") == "" {
			return nil, fmt.Errorf("census line %d is not <package><TAB><test>: %q", i+1, line)
		}
		rows = append(rows, [2]string{strings.Trim(f[0], " \t\r"), strings.Trim(f[1], " \t\r")})
	}
	return rows, nil
}

func censusRows(t *testing.T) [][2]string {
	t.Helper()
	rows, err := parseCensusRows(readText(t, scriptPath(t, "ci/race_excluded_tests.tsv")))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("the census holds no row")
	}
	return rows
}

func receiptsVerb(t *testing.T, census, jsonEvents string) cmdResult {
	t.Helper()
	script := scriptPath(t, "ci/check_go.sh")
	dir := t.TempDir()
	list := filepath.Join(dir, "census.tsv")
	if err := os.WriteFile(list, []byte(census), 0o644); err != nil {
		t.Fatal(err)
	}
	events := filepath.Join(dir, "events.json")
	if err := os.WriteFile(events, []byte(jsonEvents), 0o644); err != nil {
		t.Fatal(err)
	}
	return cmdSpec{dir: filepath.Dir(filepath.Dir(script)), env: inheritedEnv([]string{"GO_RACE_EXCLUDED_TESTS", "GO_QUALITY_SELECTION_FILE"}, "GO_RACE_EXCLUDED_TESTS="+list)}.
		run("bash", script, "race-excluded-receipts", events)
}

const (
	pkgA = "internal/goapiproof"
	pkgB = "internal/httpguard"
)

var fixtureCensus = pkgA + "\tTestOne\n" + pkgA + "\tTestTwo\n" + pkgB + "\tTestThree\n"

func fullPkg(rel string) string { return receiptsModulePath + "/" + rel }

func pass(rel, name string) goTestEvent {
	return goTestEvent{Action: "pass", Package: fullPkg(rel), Test: name}
}

func cleanEvents() []goTestEvent {
	return []goTestEvent{
		{Action: "output", Package: fullPkg(pkgA), Output: "ok  \t" + fullPkg(pkgA) + "\t0.5s\n"},
		pass(pkgA, "TestOne"), pass(pkgA, "TestTwo"),
		{Action: "pass", Package: fullPkg(pkgA)},
		{Action: "output", Package: fullPkg(pkgB), Output: "ok  \t" + fullPkg(pkgB) + "\t0.5s\n"},
		pass(pkgB, "TestThree"),
		{Action: "pass", Package: fullPkg(pkgB)},
	}
}

func without(events []goTestEvent, test string) []goTestEvent {
	var out []goTestEvent
	for _, e := range events {
		if e.Test != test {
			out = append(out, e)
		}
	}
	return out
}

func TestRaceExcludedReceiptsReadTheTerminalEventsOfOneFreshRun(t *testing.T) {
	t.Parallel()
	res := receiptsVerb(t, fixtureCensus, eventLines(t, cleanEvents()))
	if res.code != 0 {
		t.Fatalf("a fresh run that passed every row exited %d: %s%s", res.code, res.stdout, res.stderr)
	}
	for _, want := range []string{
		"race-excluded-ran: " + pkgA + " TestOne PASS",
		"race-excluded-ran: " + pkgA + " TestTwo PASS",
		"race-excluded-ran: " + pkgB + " TestThree PASS",
		"race-excluded-ran: 3 test(s) ran and passed without -race",
	} {
		if !strings.Contains(res.stdout, want+"\n") {
			t.Errorf("stdout lacks the receipt line %q:\n%s", want, res.stdout)
		}
	}
	if strings.Contains(res.stdout, "ran and passed without -race") && strings.Count(res.stdout, "race-excluded-ran:") != 4 {
		t.Errorf("want 3 receipt lines and 1 count line:\n%s", res.stdout)
	}
}

func TestRaceExcludedReceiptsFailOnEachPlantedFault(t *testing.T) {
	t.Parallel()
	clean := cleanEvents()
	faults := []struct {
		name   string
		events []goTestEvent
		want   []string
	}{
		{"missing", without(clean, "TestTwo"), []string{pkgA, "TestTwo"}},
		{"skipped", append(without(clean, "TestTwo"), goTestEvent{Action: "skip", Package: fullPkg(pkgA), Test: "TestTwo"}), []string{pkgA, "TestTwo"}},
		{"failed", append(without(clean, "TestTwo"), goTestEvent{Action: "fail", Package: fullPkg(pkgA), Test: "TestTwo"}), []string{pkgA, "TestTwo"}},
		{"passed twice", append(append([]goTestEvent{}, clean...), pass(pkgA, "TestTwo")), []string{pkgA, "TestTwo"}},
		{"cached", append(append([]goTestEvent{}, clean...), goTestEvent{Action: "output", Package: fullPkg(pkgA), Output: "ok  \t" + fullPkg(pkgA) + "\t(cached)\n"}), []string{pkgA, "cached"}},
		{"passed in another package", append(without(clean, "TestThree"), pass(pkgA, "TestThree")), []string{pkgB, "TestThree"}},
		{"only a subtest passed", append(without(clean, "TestTwo"), pass(pkgA, "TestTwo/sub")), []string{pkgA, "TestTwo"}},
		{"no events at all", nil, []string{pkgA, "TestOne"}},
	}
	for _, f := range faults {
		f := f
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			res := receiptsVerb(t, fixtureCensus, eventLines(t, f.events))
			if res.code == 0 {
				t.Fatalf("the planted fault %q passed: %s", f.name, res.stdout)
			}
			for _, w := range f.want {
				if !strings.Contains(res.stderr, w) {
					t.Errorf("the failure does not name %q: %s", w, res.stderr)
				}
			}
		})
	}
	t.Run("a census row that go list omits", censusRowThatGoListOmitsSubtests)
	t.Run("a duplicated census row", duplicateCensusSubtests)
	t.Run("a malformed census line", malformedCensusSubtests)
	t.Run("every line case of the shared table", lineCasesSubtests)
}

const shardFakeGo = `#!/usr/bin/env bash
if [ "$1" != "test" ]; then exec "${FAKE_REAL_GO}" "$@"; fi
printf '%s\n' "$*" >> "${FAKE_GO_LOG}"
if [ -n "${DEV_HEALTH_MULTI_REPLICA_PROOF-}" ]; then printf 3 > "${DEV_HEALTH_MULTI_REPLICA_PROOF}"; exit 0; fi
mod="github.com/full-chaos/dev-health-ops"
json=0
pkgs=()
for a in "$@"; do
  case "$a" in
    -json) json=1 ;;
    "${mod}"*) pkgs+=("$a") ;;
    ./*) pkgs+=("${mod}/${a#./}") ;;
  esac
done
for p in "${pkgs[@]}"; do
  rel="${p#"${mod}/"}"
  secs="0.010s"
  if [ -n "${FAKE_SLOW_PACKAGE-}" ] && [ "${rel}" = "${FAKE_SLOW_PACKAGE}" ]; then secs="45.000s"; fi
  if [ "${json}" = 1 ]; then
    printf '{"Action":"output","Package":"%s","Output":"ok  \\t%s\\t%s\\n"}\n' "${p}" "${p}" "${secs}"
    while IFS=$'\t' read -r rp name; do
      [ "${rp}" = "${rel}" ] && printf '{"Action":"pass","Package":"%s","Test":"%s"}\n' "${p}" "${name}"
    done < <(grep -v '^#' "${FAKE_CENSUS}" | grep -v '^$')
    printf '{"Action":"pass","Package":"%s"}\n' "${p}"
  else
    printf 'ok  \t%s\t%s\n' "${p}" "${secs}"
  fi
done
exit 0
`

func runShardWithFakeGo(t *testing.T, shard int, slowPackage string, extraEnv ...string) (cmdResult, []string) {
	t.Helper()
	script := scriptPath(t, "ci/check_go.sh")
	repo := filepath.Dir(filepath.Dir(script))
	work := t.TempDir()
	bin := filepath.Join(work, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(shardFakeGo), 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(work, "go_test.log")
	if err := os.WriteFile(logPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	env := inheritedEnv(append([]string{"PATH", "TMPDIR", "GO_RACE_EXCLUDED_TESTS", "GO_QUALITY_SELECTION_FILE", "FAKE_SLOW_PACKAGE", "DEV_HEALTH_GO_UNWEIGHTED_REPORT_ONLY"}, deadlineEnvNames...),
		"DEV_HEALTH_GO_UNWEIGHTED_REPORT_ONLY=",
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "TMPDIR="+work, "FAKE_REAL_GO="+realGo, "FAKE_GO_LOG="+logPath,
		"FAKE_CENSUS="+filepath.Join(repo, "ci", "race_excluded_tests.tsv"), "FAKE_SLOW_PACKAGE="+slowPackage)
	env = append(env, extraEnv...)
	res := cmdSpec{dir: repo, env: env}.run("bash", script, "ci-leg", "test", fmt.Sprint(shard), "3")
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, l := range strings.Split(string(raw), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return res, lines
}

func TestAShardRunsEachCensusPackageOnceFreshAndKeepsItsOkLines(t *testing.T) {
	t.Parallel()
	rows := censusRows(t)
	rowPackages := map[string]bool{}
	var want []string
	for _, r := range rows {
		rowPackages[r[0]] = true
		want = append(want, "race-excluded-ran: "+r[0]+" "+r[1]+" PASS")
	}
	var got []string
	total := 0
	jsonRuns := map[string]int{}
	for shard := 1; shard <= 3; shard++ {
		res, lines := runShardWithFakeGo(t, shard, "")
		if res.code != 0 {
			t.Fatalf("test shard %d/3 exited %d: %s", shard, res.code, tail(res.stderr+res.stdout, 2000))
		}
		for _, l := range strings.Split(res.stdout, "\n") {
			var n int
			if strings.HasPrefix(l, "race-excluded-ran: ") && strings.HasSuffix(l, " PASS") {
				got = append(got, l)
			} else if c, _ := fmt.Sscanf(l, "race-excluded-ran: %d test(s) ran and passed without -race", &n); c == 1 {
				total += n
			}
		}
		for _, l := range lines {
			if strings.Contains(l, "-tags=integration") {
				continue
			}
			if strings.Contains(l, " -run ") {
				t.Errorf("shard %d made a per-test go test process (the second execution is back): %s", shard, l)
			}
			for p := range rowPackages {
				if !hasField(l, fullPkg(p)) && !hasField(l, "./"+p) {
					continue
				}
				if strings.Contains(l, "-json") && strings.Contains(l, "-count=1") && strings.Contains(l, "-timeout") {
					jsonRuns[p]++
				} else {
					t.Errorf("shard %d runs the census package %s without -json -count=1 and an explicit -timeout: %s", shard, p, l)
				}
			}
		}
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the three shards print %d receipt lines, the census lists %d (a row is lost or printed twice)", len(got), len(want))
	}
	if total != len(want) {
		t.Errorf("the count lines add up to %d, want %d", total, len(want))
	}
	for p := range rowPackages {
		if jsonRuns[p] != 1 {
			t.Errorf("census package %s ran %d time(s) in the three shards, want exactly once", p, jsonRuns[p])
		}
	}

	// The shard log must still carry the `ok` lines that the unweighted guard reads: a 45 s package with no weights row fails its shard.
	const slow = "internal/auth/httpapi"
	named := 0
	for shard := 1; shard <= 3; shard++ {
		res, _ := runShardWithFakeGo(t, shard, slow)
		if res.code != 0 && strings.Contains(res.stderr, "unweighted-slow-package: "+fullPkg(slow)) {
			named++
		}
	}
	if named != 1 {
		t.Errorf("%d shard(s) failed naming the slow unweighted census package %s, want 1: its ok line no longer reaches the shard log", named, slow)
	}
}

func hasField(line, want string) bool {
	for _, f := range strings.Fields(line) {
		if f == want {
			return true
		}
	}
	return false
}

const plantedUnlistedPackage = "internal/api/billing/testdata"

func plantedCensus(t *testing.T) string {
	t.Helper()
	repo := filepath.Dir(filepath.Dir(scriptPath(t, "ci/check_go.sh")))
	real := readText(t, filepath.Join(repo, "ci", "race_excluded_tests.tsv"))
	path := filepath.Join(t.TempDir(), "planted-census.tsv")
	if err := os.WriteFile(path, []byte(real+plantedUnlistedPackage+"\tTestPlanted\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// A census row whose package `go list ./...` omits (a stale row, an existing testdata directory) is in no shard plan: it must
// fail every shard and every selection mode with the package named, never read as zero tests checked.
func censusRowThatGoListOmitsSubtests(t *testing.T) {
	script := scriptPath(t, "ci/check_go.sh")
	repo := filepath.Dir(filepath.Dir(script))
	planted := plantedCensus(t)
	selection := filepath.Join(t.TempDir(), "selection.txt")
	dirs, err := os.ReadDir(filepath.Join(repo, "internal", "cigates"))
	if err != nil {
		t.Fatal(err)
	}
	var selected []string
	for _, d := range dirs {
		if d.IsDir() {
			selected = append(selected, "internal/cigates/"+d.Name())
		}
	}
	if err := os.WriteFile(selection, []byte(fmt.Sprintf("AFFECTED %d\n%s\n", len(selected), strings.Join(selected, "\n"))), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Run("shard plan", func(t *testing.T) {
		res := cmdSpec{dir: repo, env: inheritedEnv([]string{"GO_RACE_EXCLUDED_TESTS", "GO_QUALITY_SELECTION_FILE"}, "GO_RACE_EXCLUDED_TESTS="+planted)}.
			run("bash", script, "race-excluded-shard-plan", "1", "3")
		if res.code == 0 || !strings.Contains(res.stderr, plantedUnlistedPackage) {
			t.Fatalf("exit %d, stderr %q: want a failure naming %s", res.code, tail(res.stderr, 500), plantedUnlistedPackage)
		}
	})
	t.Run("full leg", func(t *testing.T) {
		res, _ := runShardWithFakeGo(t, 2, "", "GO_RACE_EXCLUDED_TESTS="+planted)
		if res.code == 0 || !strings.Contains(res.stderr, plantedUnlistedPackage) {
			t.Fatalf("exit %d, stderr %q: want a failed leg naming %s", res.code, tail(res.stderr, 500), plantedUnlistedPackage)
		}
	})
	t.Run("affected selection that excludes the package", func(t *testing.T) {
		res, _ := runShardWithFakeGo(t, 2, "", "GO_RACE_EXCLUDED_TESTS="+planted, "GO_QUALITY_SELECTION_FILE="+selection)
		if res.code == 0 || !strings.Contains(res.stderr, plantedUnlistedPackage) {
			t.Fatalf("exit %d, stderr %q: want a failed leg naming %s", res.code, tail(res.stderr, 500), plantedUnlistedPackage)
		}
	})
	t.Run("clean control", func(t *testing.T) {
		res, _ := runShardWithFakeGo(t, 2, "", "GO_QUALITY_SELECTION_FILE="+selection)
		if res.code != 0 {
			t.Fatalf("the real census under the same selection exited %d: %s", res.code, tail(res.stderr+res.stdout, 1500))
		}
	})
}

// A duplicated (package, test) row would print one receipt twice from a single pass event. The loader every verb shares refuses
// it, after the same trim and tab split the receipt match uses, naming the key and both line numbers.
func duplicateCensusSubtests(t *testing.T) {
	script := scriptPath(t, "ci/check_go.sh")
	repo := filepath.Dir(filepath.Dir(script))
	real := readText(t, filepath.Join(repo, "ci", "race_excluded_tests.tsv"))
	first := strings.Split(strings.TrimSpace(strings.Split(strings.TrimPrefix(fixtureCensus, "\n"), "\n")[0]), "\t")
	dup := first[0] + "\t" + first[1]
	plants := []struct{ name, census, verb string }{
		{"exact duplicate, receipts verb", fixtureCensus + dup + "\n", "receipts"},
		{"exact duplicate in the real census, shard plan", real + dup + "\n" + dup + "\n", "plan"},
		{"same key with a trailing space", fixtureCensus + dup + " \n", "receipts"},
	}
	for _, p := range plants {
		p := p
		t.Run(p.name, func(t *testing.T) {
			var res cmdResult
			if p.verb == "receipts" {
				res = receiptsVerb(t, p.census, eventLines(t, cleanEvents()))
			} else {
				list := filepath.Join(t.TempDir(), "census.tsv")
				if err := os.WriteFile(list, []byte(p.census), 0o644); err != nil {
					t.Fatal(err)
				}
				res = cmdSpec{dir: repo, env: inheritedEnv([]string{"GO_RACE_EXCLUDED_TESTS", "GO_QUALITY_SELECTION_FILE"}, "GO_RACE_EXCLUDED_TESTS="+list)}.
					run("bash", script, "race-excluded-shard-plan", "1", "3")
			}
			if res.code == 0 {
				t.Fatalf("a census with a duplicated row was accepted: %s", res.stdout)
			}
			for _, w := range []string{first[0], first[1], "duplicate", "lines"} {
				if !strings.Contains(res.stderr, w) {
					t.Errorf("the failure does not say %q: %s", w, tail(res.stderr, 500))
				}
			}
		})
	}
}

// A census line must be exactly <package><TAB><test>. A whitespace-only line, a 1-column line, a line with more than 2 columns and
// a line with an empty column are refused by the loader every verb shares, naming the line number; a CRLF line ending is trimmed
// like any other surrounding whitespace, so the row still counts once.
func malformedCensusSubtests(t *testing.T) {
	script := scriptPath(t, "ci/check_go.sh")
	repo := filepath.Dir(filepath.Dir(script))
	real := readText(t, filepath.Join(repo, "ci", "race_excluded_tests.tsv"))
	plants := []struct{ name, line string }{
		{"whitespace-only", "   "},
		{"tab-only", "\t"},
		{"carriage-return-only", "\r"},
		{"1 column", pkgA},
		{"3 columns", pkgA + "\tTestFour\textra"},
		{"empty test column", pkgA + "\t "},
		{"empty package column", " \tTestFour"},
	}
	for _, p := range plants {
		p := p
		t.Run(p.name+", receipts verb", func(t *testing.T) {
			res := receiptsVerb(t, fixtureCensus+p.line+"\n", eventLines(t, cleanEvents()))
			if res.code == 0 {
				t.Fatalf("a census with the line %q was accepted: %s", p.line, res.stdout)
			}
			if !strings.Contains(res.stderr, "census line 4 ") {
				t.Errorf("the failure does not name line 4: %s", tail(res.stderr, 500))
			}
		})
		t.Run(p.name+", shard plan verb", func(t *testing.T) {
			list := filepath.Join(t.TempDir(), "census.tsv")
			if err := os.WriteFile(list, []byte(real+p.line+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			res := cmdSpec{dir: repo, env: inheritedEnv([]string{"GO_RACE_EXCLUDED_TESTS", "GO_QUALITY_SELECTION_FILE"}, "GO_RACE_EXCLUDED_TESTS="+list)}.
				run("bash", script, "race-excluded-shard-plan", "1", "3")
			if res.code == 0 {
				t.Fatalf("a census with the line %q was accepted: %s", p.line, res.stdout)
			}
			want := fmt.Sprintf("census line %d ", strings.Count(real, "\n")+1)
			if !strings.Contains(res.stderr, want) {
				t.Errorf("the failure does not say %q: %s", want, tail(res.stderr, 500))
			}
		})
	}
	t.Run("a CRLF row is trimmed and counted once", func(t *testing.T) {
		census := pkgA + "\tTestOne\r\n" + pkgA + "\tTestTwo\r\n" + pkgB + "\tTestThree\r\n"
		res := receiptsVerb(t, census, eventLines(t, cleanEvents()))
		if res.code != 0 {
			t.Fatalf("a CRLF census was refused: %s", tail(res.stderr, 500))
		}
		if got := strings.Count(res.stdout, "race-excluded-ran:"); got != 4 {
			t.Errorf("want 3 receipt lines and 1 count line, got %d:\n%s", got, res.stdout)
		}
	})
}

// ci/race_excluded_line_cases.tsv is one row set for the three readers of the census: the loader of ci/check_go.sh and the two
// Go parsers (parseCensusRows here, parseCensusExcluded in internal/cigates/testshard, which reads the same file). Each case is put
// at line 2 of a census whose line 1 is a valid row; the accept or refuse result and the line number must be the same everywhere.
func lineCasesSubtests(t *testing.T) {
	cases := readText(t, scriptPath(t, "ci/race_excluded_line_cases.tsv"))
	seen := 0
	for i, raw := range strings.Split(cases, "\n") {
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		f := strings.Split(raw, "\t")
		if len(f) != 4 {
			t.Fatalf("ci/race_excluded_line_cases.tsv line %d has %d columns, want 4", i+1, len(f))
		}
		name, want := f[0], f[2]
		line, err1 := strconv.Unquote(f[1])
		test, err2 := strconv.Unquote(f[3])
		if err1 != nil || err2 != nil || (want != "accept" && want != "refuse" && want != "skip") {
			t.Fatalf("ci/race_excluded_line_cases.tsv line %d is malformed: %q", i+1, raw)
		}
		seen++
		t.Run(name, func(t *testing.T) {
			census := pkgB + "\tTestZero\n" + line + "\n"
			rows, perr := parseCensusRows(census)
			events := []goTestEvent{pass(pkgB, "TestZero")}
			if want == "accept" {
				events = append(events, pass(pkgA, test))
			}
			res := receiptsVerb(t, census, eventLines(t, events))
			switch want {
			case "refuse":
				if perr == nil || !strings.Contains(perr.Error(), "census line 2 ") {
					t.Errorf("the Go parser: want an error naming census line 2, got %v", perr)
				}
				if res.code == 0 || !strings.Contains(res.stderr, "census line 2 ") {
					t.Errorf("the loader: want a refusal naming census line 2, got code %d: %s", res.code, tail(res.stderr, 500))
				}
			default:
				wantRows := [][2]string{{pkgB, "TestZero"}}
				if want == "accept" {
					wantRows = append(wantRows, [2]string{pkgA, test})
				}
				if perr != nil || !reflect.DeepEqual(rows, wantRows) {
					t.Errorf("the Go parser: want %q, got %q, %v", wantRows, rows, perr)
				}
				if res.code != 0 {
					t.Fatalf("the loader refused the line: %s", tail(res.stderr, 500))
				}
				if got := strings.Count(res.stdout, "race-excluded-ran:"); got != len(wantRows)+1 {
					t.Errorf("the loader: want %d receipt lines and 1 count line, got %d:\n%s", len(wantRows), got, res.stdout)
				}
				if want == "accept" && !strings.Contains(res.stdout, "race-excluded-ran: "+pkgA+" "+test+" PASS") {
					t.Errorf("the loader did not trim to the test name %q:\n%s", test, res.stdout)
				}
			}
		})
	}
	if seen == 0 {
		t.Fatal("ci/race_excluded_line_cases.tsv holds no case")
	}
}
