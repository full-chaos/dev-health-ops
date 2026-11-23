package checkgo_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-14526: a split part runs its test names through one anchored -run "^(a|b|...)$". Linux refuses one argument over
// 128 KiB (MAX_ARG_STRLEN), so a part whose names pass that limit made `env ... go test` fail with "Argument list too long"
// (rc 126). The names now go in -run chunks of at most 60000 bytes, one go test per chunk, every chunk runs even after one
// fails, and a part that fits keeps its one call, byte for byte. These cells run the real shell functions with stand-in runners.

const chunkRunBudget = 60000

const chunkCellPkg = "example.com/m/internal/fake"

// chunkCellStubs replace the two go test runners: each call records its argv (NUL-separated, one file per call, in call order),
// writes one line to the log and one event to the -json file it was given, and fails when its call number is in FAIL_CALLS.
const chunkCellStubs = `
rec_call() {
  local n
  n=$(( $(cat "${REC}/n" 2>/dev/null || printf 0) + 1 ))
  printf '%s' "${n}" > "${REC}/n"
  printf '%s\0' "$@" > "${REC}/call-$(printf '%04d' "${n}")"
  CALL_N="${n}"
}
call_rc() { case " ${FAIL_CALLS:-} " in *" ${CALL_N} "*) return 1 ;; esac; return 0; }
go_test_leg() {
  rec_call go_test_leg "$@"
  printf 'LOG-OF-CALL-%s\n' "${CALL_N}" >> "$1"
  printf '{"Action":"pass","Package":"p","Test":"TestCall%s","Elapsed":0.01}\n' "${CALL_N}" >> "$2"
  call_rc
}
go_test_json_run_t() {
  rec_call go_test_json_run_t "$@"
  printf '{"Action":"pass","Package":"p","Test":"TestCall%s","Elapsed":0.01}\n' "${CALL_N}" >> "$2"
  printf 'LOG-OF-CALL-%s\n' "${CALL_N}" >> "$3"
  call_rc
}
`

// chunkCellFunctions is the text of the named functions of ci/check_go.sh; a name in optional that the script lacks is left out.
func chunkCellFunctions(t *testing.T, required, optional []string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for i, name := range append(append([]string{}, required...), optional...) {
		m := regexp.MustCompile(`(?ms)^` + regexp.QuoteMeta(name) + `\(\) \{\n.*?^\}\n`).Find(body)
		if m == nil {
			if i < len(required) {
				t.Fatalf("%s is not defined in ci/check_go.sh", name)
			}
			continue
		}
		b.Write(m)
	}
	return b.String()
}

var chunkCellOptional = []string{"go_run_name_chunks", "go_test_run_chunks"}

func syntheticNames(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("Test%sSyntheticChunkName%05dPaddedToASteadyLength", prefix, i)
	}
	return out
}

func joinedRegex(names []string) string { return "^(" + strings.Join(names, "|") + ")$" }

type chunkCall struct{ argv []string }

func (c chunkCall) runArg(t *testing.T) (int, string) {
	t.Helper()
	for i, a := range c.argv {
		if a == "-run" && i+1 < len(c.argv) {
			return i + 1, c.argv[i+1]
		}
	}
	t.Fatalf("a call with no -run: %q", c.argv)
	return 0, ""
}

func readChunkCalls(t *testing.T, rec string) []chunkCall {
	t.Helper()
	entries, err := os.ReadDir(rec)
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "call-") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	var calls []chunkCall
	for _, f := range files {
		raw, err := os.ReadFile(filepath.Join(rec, f))
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, chunkCall{argv: strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00")})
	}
	return calls
}

type chunkRun struct {
	calls  []chunkCall
	stdout string
	work   string
}

func runChunkCell(t *testing.T, script string, env ...string) chunkRun {
	t.Helper()
	work := t.TempDir()
	rec := filepath.Join(work, "rec")
	tmp := filepath.Join(work, "tmp")
	for _, d := range []string{rec, tmp} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append(os.Environ(), append([]string{"REC=" + rec, "W=" + work, "TMPDIR=" + tmp, "CELL_ROOT=" + repoRoot(t), "CELL_PKG=" + chunkCellPkg}, env...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		race, _ := os.ReadFile(filepath.Join(work, "race.out"))
		t.Fatalf("the cell script failed: %v\n%s\n%s", err, out, race)
	}
	return chunkRun{calls: readChunkCalls(t, rec), stdout: string(out), work: work}
}

// runSplitJob runs test_split_job_start and test_split_jobs_finish on the names of one part of a fake package; census runs it as
// a census package (go_test_json_run_t), else as a plain part (go_test_leg).
func runSplitJob(t *testing.T, names []string, census bool, failCalls string) chunkRun {
	t.Helper()
	var lines strings.Builder
	for _, n := range names {
		fmt.Fprintf(&lines, "%s\tinternal/fake\t%s\n", chunkCellPkg, n)
	}
	linesFile := filepath.Join(t.TempDir(), "lines")
	if err := os.WriteFile(linesFile, []byte(lines.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	censusKeys := ""
	if census {
		censusKeys = "internal/fake"
	}
	script := "set -euo pipefail\n" +
		chunkCellFunctions(t, []string{"die", "go_run_name_ok", "byte_len", "cigates_go_test_timeout", "test_split_job_start", "test_split_jobs_finish"}, chunkCellOptional) +
		chunkCellStubs + `
ROOT="${CELL_ROOT}"
MERGE_GROUP_FRESH=""
log="${W}/log"; json="${W}/json"; legjson="${W}/legjson"
: > "${log}"; : > "${json}"; : > "${legjson}"
SPLIT_JOB_PIDS=(); SPLIT_JOB_LABELS=(); SPLIT_JOB_FILES=()
test_split_job_start . "${CELL_PKG}" "$(cat "${CELL_LINES}")" "${CELL_CENSUS_KEYS}" > "${W}/start.out"
rc=0
test_split_jobs_finish 1 3 > "${W}/finish.out" || rc=$?
printf 'JOB-RC %s\n' "${rc}"
`
	return runChunkCell(t, script, "CELL_LINES="+linesFile, "CELL_CENSUS_KEYS="+censusKeys, "FAIL_CALLS="+failCalls)
}

// runRaceSplitPart runs check_race_shard with one race split part of a fake package whose part names are names.
func runRaceSplitPart(t *testing.T, names []string, failCalls string) chunkRun {
	t.Helper()
	dir := t.TempDir()
	namesFile := filepath.Join(dir, "names")
	if err := os.WriteFile(namesFile, []byte(strings.Join(names, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	weights := filepath.Join(dir, "race-split-weights.tsv")
	if err := os.WriteFile(weights, []byte("internal/fake\t"+names[0]+"\t1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	fakeGo := "#!/usr/bin/env bash\ncase \"$1 $2\" in \"list -m\") printf 'example.com/m\\n' ;; \"list -mod=readonly\") printf '" + chunkCellPkg + "\\n' ;; esac\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fakeGo), 0o755); err != nil {
		t.Fatal(err)
	}
	script := "set -euo pipefail\n" +
		chunkCellFunctions(t, []string{"die", "go_run_name_ok", "byte_len", "check_race_shard"}, chunkCellOptional) +
		chunkCellStubs + `
check_race_shard_args() { :; }
load_quality_selection() { :; }
leg_json_file() { printf '%s\n' "${W}/race-legjson"; }
quality_selected() { return 1; }
security_fresh_active() { return 1; }
race_weights_rows() { printf 'internal/fake\t1\n'; }
race_shard_packages() { if [ -z "${5:-}" ]; then printf '%s\t1\t2\n' "${CELL_PKG}"; fi; }
race_split_part_names() { cat "${CELL_NAMES}"; }
check_security_fresh_receipt() { :; }
ROOT="${CELL_ROOT}"
MERGE_GROUP_FRESH=""
MODULE_DIRS=(.)
GO_ENV_OFF=(env)
GO_RACE_SPLIT_WEIGHTS="${CELL_WEIGHTS}"
: > "${W}/race-legjson"
rc=0
check_race_shard 1 1 > "${W}/race.out" 2>&1 || rc=$?
printf 'RACE-RC %s\n' "${rc}"
`
	return runChunkCell(t, script, "CELL_NAMES="+namesFile, "CELL_WEIGHTS="+weights, "FAIL_CALLS="+failCalls,
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// chunkDefects names every call whose -run is over the budget, and every name of want that runs in no call, in two, or that want lacks.
func chunkDefects(t *testing.T, calls []chunkCall, want []string) []string {
	t.Helper()
	var out []string
	if len(calls) == 0 {
		return []string{"no go test call ran"}
	}
	seen := map[string]int{}
	for i, c := range calls {
		_, r := c.runArg(t)
		if len(r) > chunkRunBudget {
			out = append(out, fmt.Sprintf("call %d: its -run is %d bytes, want <= %d", i+1, len(r), chunkRunBudget))
		}
		if !strings.HasPrefix(r, "^(") || !strings.HasSuffix(r, ")$") {
			out = append(out, fmt.Sprintf("call %d: -run is not anchored ^(...)$", i+1))
			continue
		}
		for _, n := range strings.Split(r[2:len(r)-2], "|") {
			seen[n]++
		}
	}
	wantSet := map[string]bool{}
	for _, n := range want {
		wantSet[n] = true
		switch seen[n] {
		case 0:
			out = append(out, "runs in no call: "+n)
		case 1:
		default:
			out = append(out, fmt.Sprintf("runs in %d calls: %s", seen[n], n))
		}
	}
	for n := range seen {
		if !wantSet[n] {
			out = append(out, "runs but is not a name of the part: "+n)
		}
	}
	sort.Strings(out)
	return out
}

// sameArgvButRun reports the calls whose argv differs from the first call's anywhere but the -run value (the same runner, files,
// flags and package: every chunk appends to the one job's files).
func sameArgvButRun(t *testing.T, calls []chunkCall) []string {
	t.Helper()
	var out []string
	if len(calls) == 0 {
		return out
	}
	i0, _ := calls[0].runArg(t)
	for k, c := range calls[1:] {
		i, _ := c.runArg(t)
		a, b := append([]string{}, calls[0].argv...), append([]string{}, c.argv...)
		a[i0], b[i] = "", ""
		if strings.Join(a, "\x00") != strings.Join(b, "\x00") {
			out = append(out, fmt.Sprintf("call %d argv %q differs from call 1 %q beyond -run", k+2, c.argv, calls[0].argv))
		}
	}
	return out
}

func readCellFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// RED at base: one part whose joined -run is over 131072 bytes (the Linux per-argument limit) must reach go test in chunks.
func TestSplitPartOverTheArgLimitRunsInRunChunksOfTheBudget(t *testing.T) {
	t.Parallel()
	names := syntheticNames("Big", 3000)
	if l := len(joinedRegex(names)); l <= 131072 {
		t.Fatalf("the fixture's -run is %d bytes: it must pass the 131072-byte limit", l)
	}
	for _, c := range []struct {
		name   string
		census bool
		runner string
		events string
	}{{"go_test_leg part", false, "go_test_leg", "legjson"}, {"census part", true, "go_test_json_run_t", "json"}} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := runSplitJob(t, names, c.census, "")
			if !strings.Contains(r.stdout, "JOB-RC 0\n") {
				t.Errorf("the job of a green part must end rc 0: %s", r.stdout)
			}
			for _, d := range chunkDefects(t, r.calls, names) {
				t.Error(d)
			}
			for _, d := range sameArgvButRun(t, r.calls) {
				t.Error(d)
			}
			for _, call := range r.calls {
				if call.argv[0] != c.runner || call.argv[len(call.argv)-1] != chunkCellPkg {
					t.Errorf("want %s ... %s, got %q", c.runner, chunkCellPkg, call.argv)
				}
			}
			log, events := readCellFile(t, filepath.Join(r.work, "log")), readCellFile(t, filepath.Join(r.work, c.events))
			for i := range r.calls {
				if strings.Count(log, fmt.Sprintf("LOG-OF-CALL-%d\n", i+1)) != 1 || strings.Count(events, fmt.Sprintf(`"Test":"TestCall%d"`, i+1)) != 1 {
					t.Errorf("call %d: its log line and its event must reach the leg's log and %s exactly once", i+1, c.events)
				}
			}
		})
	}
}

// SET-EQUAL (holds at base and after): over every call each name of the part runs exactly once, whatever the part size; two
// parts of one package on one leg keep their names in input order.
func TestSplitJobRunsEveryNameOfItsPartsExactlyOnce(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name  string
		names []string
	}{
		{"one small part", syntheticNames("Small", 40)},
		{"two parts on one leg, under the budget", append(syntheticNames("PartB", 300), syntheticNames("PartA", 300)...)},
		{"two parts on one leg, over the limit", append(syntheticNames("PartB", 1500), syntheticNames("PartA", 1500)...)},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := runSplitJob(t, c.names, false, "")
			var defects []string
			for _, d := range chunkDefects(t, r.calls, c.names) {
				if !strings.Contains(d, "bytes, want <=") {
					defects = append(defects, d)
				}
			}
			for _, d := range defects {
				t.Error(d)
			}
		})
	}
}

// SMALL PART: a part whose -run fits the budget keeps exactly one go test call whose argv is the base form, byte for byte.
func TestSmallSplitPartKeepsItsOneGoTestCall(t *testing.T) {
	t.Parallel()
	unique := append(syntheticNames("Zeta", 5), syntheticNames("Alpha", 5)...)
	r := runSplitJob(t, unique, false, "")
	if len(r.calls) != 1 {
		t.Fatalf("want exactly one go test call, got %d", len(r.calls))
	}
	got := r.calls[0].argv
	want := []string{"go_test_leg", got[1], got[2], "-timeout", "10m", "-run", joinedRegex(unique), chunkCellPkg}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Errorf("argv\n%q\nwant the base form\n%q", got, want)
	}
	r = runSplitJob(t, unique, true, "")
	if len(r.calls) != 1 {
		t.Fatalf("census part: want exactly one go test call, got %d", len(r.calls))
	}
	got = r.calls[0].argv
	want = []string{"go_test_json_run_t", "10m", got[2], got[3], "-run", joinedRegex(unique), chunkCellPkg}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Errorf("census argv\n%q\nwant the base form\n%q", got, want)
	}
	rr := runRaceSplitPart(t, unique, "")
	if len(rr.calls) != 1 {
		t.Fatalf("race part: want exactly one go test call, got %d\n%s", len(rr.calls), readCellFile(t, filepath.Join(rr.work, "race.out")))
	}
	got = rr.calls[0].argv
	want = []string{"go_test_leg", got[1], got[2], "-race", "-timeout", "10m", "-run", joinedRegex(unique), chunkCellPkg}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Errorf("race argv\n%q\nwant the base form\n%q", got, want)
	}
}

// FAIL-ALL-RUN: a failing chunk does not stop the job: every later chunk still runs, and the job ends nonzero.
func TestSplitJobRunsEveryChunkAfterOneFails(t *testing.T) {
	t.Parallel()
	names := syntheticNames("Fail", 3000)
	for _, fail := range []string{"1", "3"} {
		t.Run("call "+fail+" fails", func(t *testing.T) {
			t.Parallel()
			r := runSplitJob(t, names, false, fail)
			if len(r.calls) < 3 {
				t.Fatalf("want every chunk to run (3 or more calls for %d bytes of names), got %d call(s)", len(joinedRegex(names)), len(r.calls))
			}
			for _, d := range chunkDefects(t, r.calls, names) {
				t.Error(d)
			}
			if strings.Contains(r.stdout, "JOB-RC 0\n") || !strings.Contains(r.stdout, "JOB-RC ") {
				t.Errorf("a failed chunk must fail the job: %s", r.stdout)
			}
			if fin := readCellFile(t, filepath.Join(r.work, "finish.out")); strings.Contains(fin, " rc 0\n") {
				t.Errorf("test_split_jobs_finish must name the job's nonzero rc: %s", fin)
			}
		})
	}
}

// The race split part (check_race_shard) takes the same chunks: RED at base for an over-limit part, every name once, every chunk
// run after a failure, the leg nonzero.
func TestRaceSplitPartOverTheArgLimitRunsInRunChunksOfTheBudget(t *testing.T) {
	t.Parallel()
	names := syntheticNames("Race", 3000)
	for _, fail := range []string{"", "1"} {
		t.Run("fail calls "+fail, func(t *testing.T) {
			t.Parallel()
			r := runRaceSplitPart(t, names, fail)
			out := readCellFile(t, filepath.Join(r.work, "race.out"))
			for _, d := range chunkDefects(t, r.calls, names) {
				t.Error(d)
			}
			for _, d := range sameArgvButRun(t, r.calls) {
				t.Error(d)
			}
			if len(r.calls) < 3 {
				t.Errorf("want every chunk to run (3 or more calls), got %d", len(r.calls))
			}
			wantRC := "RACE-RC 0\n"
			if fail != "" {
				if strings.Contains(r.stdout, wantRC) {
					t.Errorf("a failed chunk must fail the race leg: %s\n%s", r.stdout, out)
				}
			} else if !strings.Contains(r.stdout, wantRC) {
				t.Errorf("a green race part must leave the leg rc 0: %s\n%s", r.stdout, out)
			}
			events := readCellFile(t, filepath.Join(r.work, "race-legjson"))
			for i := range r.calls {
				if strings.Count(events, fmt.Sprintf(`"Test":"TestCall%d"`, i+1)) != 1 {
					t.Errorf("call %d: its event must reach the race leg's -json file exactly once", i+1)
				}
			}
		})
	}
}
