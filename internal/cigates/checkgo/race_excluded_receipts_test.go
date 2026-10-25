//go:build !race

package checkgo_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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

func censusRows(t *testing.T) [][2]string {
	t.Helper()
	var rows [][2]string
	for _, line := range strings.Split(readText(t, scriptPath(t, "ci/race_excluded_tests.tsv")), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 2 {
			t.Fatalf("bad census row %q", line)
		}
		rows = append(rows, [2]string{f[0], f[1]})
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

func runShardWithFakeGo(t *testing.T, shard int, slowPackage string) (cmdResult, []string) {
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
	env := inheritedEnv([]string{"PATH", "TMPDIR", "GO_RACE_EXCLUDED_TESTS", "GO_QUALITY_SELECTION_FILE", "FAKE_SLOW_PACKAGE"},
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "TMPDIR="+work, "FAKE_REAL_GO="+realGo, "FAKE_GO_LOG="+logPath,
		"FAKE_CENSUS="+filepath.Join(repo, "ci", "race_excluded_tests.tsv"), "FAKE_SLOW_PACKAGE="+slowPackage)
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
