//go:build !race

package checkgo_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// These tests run ci/check_go.sh in child processes; -race adds nothing to them. ci/race_excluded_tests.tsv runs each one without -race.

// The test and race legs run go test -json and rebuild the plain log from it. The converter never decides the verdict: the leg's rc
// is go test's own, whatever the converter and the tee do, and a line that is not a JSON event passes through raw.

const legFakeGo = `#!/usr/bin/env bash
# go list is this stand-in's slow part and gives the same answer for every case: one answer per directory and arguments.
if [ "$1" = "list" ] && [ -n "${FAKE_GO_LIST_CACHE:-}" ]; then
  key="$({ printf '%s\0' "${PWD}" "$@"; } | sha256sum | cut -c1-64)"
  if [ -f "${FAKE_GO_LIST_CACHE}/${key}" ]; then cat "${FAKE_GO_LIST_CACHE}/${key}"; exit 0; fi
  tmp="$(mktemp "${FAKE_GO_LIST_CACHE}/.${key}.XXXXXX")" || exit 1
  "${FAKE_REAL_GO}" "$@" > "${tmp}"; rc=$?
  cat "${tmp}"
  if [ "${rc}" -eq 0 ]; then mv -f "${tmp}" "${FAKE_GO_LIST_CACHE}/${key}"; else rm -f "${tmp}"; fi
  exit "${rc}"
fi
if [ "$1" != "test" ]; then exec "${FAKE_REAL_GO}" "$@"; fi
case " $* " in *" -list "*) cat "${FAKE_GO_LISTING}"; exit 0 ;; esac
if [ -n "${DEV_HEALTH_MULTI_REPLICA_PROOF-}" ]; then printf 3 > "${DEV_HEALTH_MULTI_REPLICA_PROOF}"; exit 0; fi
mode="${FAKE_MODE-}"
sliced=0
case " $* " in *" -run "*) mode=""; sliced=1 ;; esac
mod="github.com/full-chaos/dev-health-ops"
pkgs=()
for a in "$@"; do
  case "$a" in
    "${mod}"*) pkgs+=("$a") ;;
    ./*) pkgs+=("${mod}/${a#./}") ;;
  esac
done
rc=0
for p in "${pkgs[@]}"; do
  rel="${p#"${mod}/"}"
  if grep -q "^${rel}"$'\t' "${FAKE_CENSUS}"; then mode_here=""; else mode_here="${mode}"; fi
  case "${mode_here}" in
    mixed-fail|mixed-green) printf 'RAWLINE-NOT-JSON %s\n' "${rel}" ;;
  esac
  if { [ "${mode_here}" = mixed-fail ] || [ "${mode_here}" = mixed-green ]; } && [ "${done_fail-0}" = 0 ]; then
    done_fail=1
    printf '{"Action":"output","Package":"%s","Test":"TestPasses","Output":"PASSING-OUTPUT\\n"}\n' "${p}"
    printf '{"Action":"pass","Package":"%s","Test":"TestPasses","Elapsed":0.25}\n' "${p}"
    printf '{"Action":"output","Package":"%s","Test":"TestBreaks","Output":"FAILING-OUTPUT\\n"}\n' "${p}"
    printf '{"Action":"output","Package":"%s","Test":"TestBreaks","Output":"--- PASS: this is real output from a failing test\\n"}\n' "${p}"
    printf '{"Action":"fail","Package":"%s","Test":"TestBreaks","Elapsed":0.5}\n' "${p}"
    printf '{"Action":"output","Package":"%s","Output":"FAIL\\t%s\\t0.500s\\n"}\n' "${p}" "${p}"
    printf '{"Action":"fail","Package":"%s","Elapsed":0.5}\n' "${p}"
    [ "${mode_here}" = mixed-fail ] && rc=1
    continue
  fi
  printf '{"Action":"output","Package":"%s","Output":"PASS\\n"}\n' "${p}"
  printf '{"Action":"output","Package":"%s","Output":"ok  \\t%s\\t0.010s\\n"}\n' "${p}" "${p}"
  while IFS=$'\t' read -r rp name; do
    [ "${rp}" = "${rel}" ] && printf '{"Action":"pass","Package":"%s","Test":"%s"}\n' "${p}" "${name}"
  done < <(grep -v '^#' "${FAKE_CENSUS}" | grep -v '^$')
  if [ "${sliced}" = 1 ]; then printf '{"Action":"pass","Package":"%s","Test":"TestFake","Elapsed":0.01}\n' "${p}"; fi
  printf '{"Action":"pass","Package":"%s","Elapsed":0.01}\n' "${p}"
done
exit "${rc}"
`

// legGoListCache is the directory of go list answers the stand-in go shares between the cases of the one parent test.
var legGoListCache string

func runLegWithFakeGo(t *testing.T, leg string, shard int, mode string) (cmdResult, string) {
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
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(legFakeGo), 0o755); err != nil {
		t.Fatal(err)
	}
	legDir := filepath.Join(work, "leg-json")
	count := "3"
	if leg == "race" {
		count = "4"
	}
	goMode := mode
	switch mode {
	case "jq-and-raw-fail":
		// The leg's converter call (jq -nR ...) fails the way a jq out of memory does; every other jq call is the real jq.
		realJQ, err := exec.LookPath("jq")
		if err != nil {
			t.Skip("jq is not installed")
		}
		fakeJQ := "#!/usr/bin/env bash\ncase \" $* \" in *\" -nR \"*) echo 'jq: error: cannot allocate memory' >&2; exit 5 ;; esac\nexec " + realJQ + " \"$@\"\n"
		if err := os.WriteFile(filepath.Join(bin, "jq"), []byte(fakeJQ), 0o755); err != nil {
			t.Fatal(err)
		}

		// The plain log's tee fails with rc 9 after it copied its input: the pipeline's own tee and the converter-failure fallback both end in rc 9.
		realTee, err := exec.LookPath("tee")
		if err != nil {
			t.Fatal(err)
		}
		fakeTee := "#!/usr/bin/env bash\ncase \"$1 $2\" in \"-a \"*dev-health-test-shard.*) " + realTee + " \"$@\"; exit 9 ;; esac\nexec " + realTee + " \"$@\"\n"
		if err := os.WriteFile(filepath.Join(bin, "tee"), []byte(fakeTee), 0o755); err != nil {
			t.Fatal(err)
		}

		// The leg's raw file is a symlink to /dev/full: every write fails with ENOSPC.
		if err := os.MkdirAll(legDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("/dev/full", filepath.Join(legDir, leg+"-"+shard1(shard)+"-of-"+count+".json")); err != nil {
			t.Fatal(err)
		}
		goMode = ""
	}
	weights, err := os.ReadFile(filepath.Join(repo, "ci", "go_providersync_race_weights.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var listing []string
	for _, l := range strings.Split(string(weights), "\n") {
		if l != "" && !strings.HasPrefix(l, "#") {
			listing = append(listing, strings.SplitN(l, "\t", 2)[0])
		}
	}
	listPath := filepath.Join(work, "listing.txt")
	if err := os.WriteFile(listPath, []byte(strings.Join(listing, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	env := inheritedEnv([]string{"PATH", "TMPDIR", "GO_RACE_EXCLUDED_TESTS", "GO_QUALITY_SELECTION_FILE", "DEV_HEALTH_GO_UNWEIGHTED_REPORT_ONLY"},
		"DEV_HEALTH_GO_UNWEIGHTED_REPORT_ONLY=1",
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "TMPDIR="+work, "FAKE_REAL_GO="+realGo,
		"FAKE_CENSUS="+filepath.Join(repo, "ci", "race_excluded_tests.tsv"), "FAKE_MODE="+goMode, "FAKE_GO_LISTING="+listPath, "FAKE_GO_LIST_CACHE="+legGoListCache,
		"GO_TEST_LEG_JSON_DIR="+legDir, "GO_TEST_JSON_DIR="+filepath.Join(work, "ps-json"))
	// A stage that never ends is a failure of the leg under test, not a hang of this test.
	res := cmdSpec{dir: repo, env: env}.run("timeout", "150", "bash", script, "ci-leg", leg, shard1(shard), count)
	if mode == "jq-and-raw-fail" {
		return res, "" // the raw file is /dev/full: reading it never ends
	}
	raw, _ := os.ReadFile(filepath.Join(legDir, leg+"-"+shard1(shard)+"-of-"+count+".json"))
	return res, string(raw)
}

func shard1(n int) string { return itoa(n) }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

// One stand-in leg run per case (each run is a whole `ci-leg` stage, about 4 s, and the package runs them one at a time): the
// two legs differ in their stage function, so the stream cases that depend on it run on both; the helper is one function, so the
// two failure-plumbing cases run on the test leg only (the call-site guard below pins the race sites).
func TestTheLegStageKeepsGoTestsVerdictAndEveryOutputLine(t *testing.T) {
	t.Parallel()
	legGoListCache = t.TempDir()
	const passShaped = "--- PASS: this is real output from a failing test"
	for _, c := range []struct {
		name, leg, mode string
		check           func(t *testing.T, res cmdResult, legJSON string)
	}{
		{"test leg, non-JSON line + fail events, go exits 1: red, every line kept, Elapsed in the leg file", "test", "mixed-fail", everyLineAndVerdict(true, passShaped)},
		{"race leg, the same stream, go exits 0: green (the verdict is go's, not the events'), every line kept", "race", "mixed-green", everyLineAndVerdict(false, passShaped)},
		{"test leg, converter fails and the raw file is unwritable: red, both named, the raw stream is in the plain log", "test", "jq-and-raw-fail", func(t *testing.T, res cmdResult, _ string) {
			if res.code == 0 {
				t.Fatalf("the JSON converter failed, the raw file could not be written, and the leg exited 0: %s", tail(res.stderr, 1200))
			}
			if !strings.Contains(res.stderr, "go_test_leg: JSON converter failed") {
				t.Errorf("the failure does not name the converter: %s", tail(res.stderr, 800))
			}
			if !strings.Contains(res.stderr, "go_test_leg: the raw file write failed") {
				t.Errorf("the failure does not name the raw file: %s", tail(res.stderr, 800))
			}
			if !strings.Contains(res.stdout, `"Action":"output"`) {
				t.Errorf("the raw stream is not in the plain log after the converter failed: %s", tail(res.stdout, 600))
			}
			if !strings.Contains(res.stderr, "go_test_leg: plain log write failed (tee rc 9)") {
				t.Errorf("the failure does not name the plain log write of the converter-failure fallback: %s", tail(res.stderr, 800))
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, legJSON := runLegWithFakeGo(t, c.leg, 1, c.mode)
			c.check(t, res, legJSON)
		})
	}
}

// everyLineAndVerdict checks one stand-in leg run whose stream holds a non-JSON line, a passing test, and a failing test: the leg's
// status is go test's own (red when it exited 1, green when it exited 0), and the plain log holds every line.
func everyLineAndVerdict(red bool, passShaped string) func(t *testing.T, res cmdResult, legJSON string) {
	return func(t *testing.T, res cmdResult, legJSON string) {
		t.Helper()
		if red && res.code == 0 {
			t.Fatalf("go test exited 1 and the leg exited 0: %s", tail(res.stderr, 1200)+"\n--- stdout:\n"+tail(res.stdout, 800))
		}
		if !red && res.code != 0 {
			t.Fatalf("go test exited 0 and the leg exited %d (the verdict is go test's, not the events'): %s", res.code, tail(res.stderr, 1200)+"\n--- stdout:\n"+tail(res.stdout, 800))
		}
		if !strings.Contains(res.stdout, "RAWLINE-NOT-JSON") {
			t.Errorf("the non-JSON line is not in the plain log")
		}
		if !strings.Contains(legJSON, `"Action":"fail"`) || !strings.Contains(legJSON, `"Test":"TestBreaks"`) || !strings.Contains(legJSON, `"Elapsed":0.5`) || !strings.Contains(legJSON, `"Elapsed":0.25`) {
			t.Errorf("the leg file lacks the fail event or a passing test's Elapsed: %q", tail(legJSON, 600))
		}
		pass, fail := strings.Index(res.stdout, "PASSING-OUTPUT"), strings.Index(res.stdout, "FAILING-OUTPUT")
		if pass < 0 || fail < 0 || pass > fail {
			t.Errorf("a passing test's output (at %d) must be in the plain log before the failing test's (at %d), as the stream has them", pass, fail)
		}
		if !strings.Contains(res.stdout, passShaped) {
			t.Errorf("a line of a failing test that starts with \"--- PASS:\" is missing from the plain log")
		}
		if !strings.Contains(res.stdout, "FAIL\t") {
			t.Errorf("the package FAIL line is not in the plain log")
		}
	}
}

func legConverter(t *testing.T) string {
	t.Helper()
	script, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	const open = "GO_TEST_LEG_JQ='"
	i := strings.Index(string(script), open)
	if i < 0 {
		t.Fatal("ci/check_go.sh has no GO_TEST_LEG_JQ")
	}
	rest := string(script)[i+len(open):]
	j := strings.Index(rest, "'\n")
	if j < 0 {
		t.Fatal("GO_TEST_LEG_JQ is not closed")
	}
	return rest[:j]
}

func TestTheLegConverterKeepsEveryOutputVerbatimInStreamOrder(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed")
	}
	events := strings.Join([]string{
		`{"Action":"start","Package":"p"}`,
		`{"Action":"run","Package":"p","Test":"TestA"}`,
		`{"Action":"output","Package":"p","Test":"TestA","Output":"=== RUN   TestA\n"}`,
		`{"Action":"output","Package":"p","Test":"TestA","Output":"    a_test.go:1: passing log\n"}`,
		`{"Action":"output","Package":"p","Test":"TestA","Output":"--- PASS: TestA (0.00s)\n"}`,
		`{"Action":"pass","Package":"p","Test":"TestA","Elapsed":0}`,
		`{"Action":"output","Package":"p","Test":"TestB","Output":"=== RUN   TestB\n"}`,
		`{"Action":"output","Package":"p","Test":"TestB","Output":"    b_test.go:2: failing log\n"}`,
		`{"Action":"output","Package":"p","Test":"TestB","Output":"--- FAIL: TestB (0.00s)\n"}`,
		`{"Action":"fail","Package":"p","Test":"TestB","Elapsed":0}`,
		`{"Action":"output","Package":"p","Test":"TestC","Output":"never ended\n"}`,
		"a line that is not an event",
		`42`,
		`{"x":1}`,
		`{"Action":"output","Package":"p","Output":"PASS\n"}`,
		`{"Action":"output","Package":"p","Output":"testing: warning: no tests to run\n"}`,
		`{"Action":"output","Package":"p","Output":"FAIL\tp\t0.010s\n"}`,
		`{"Action":"fail","Package":"p","Elapsed":0.01}`,
	}, "\n") + "\n"
	cmd := exec.Command("jq", "-nR", "--unbuffered", "-rj", legConverter(t))
	cmd.Stdin = strings.NewReader(events)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("the converter failed: %v", err)
	}
	want := "=== RUN   TestA\n    a_test.go:1: passing log\n--- PASS: TestA (0.00s)\n=== RUN   TestB\n    b_test.go:2: failing log\n--- FAIL: TestB (0.00s)\nnever ended\n" +
		"a line that is not an event\n42\n{\"x\":1}\nPASS\ntesting: warning: no tests to run\nFAIL\tp\t0.010s\n"
	if string(out) != want {
		t.Fatalf("converter output:\n%q\nwant:\n%q", out, want)
	}
}

// legSiteViolations: every go test of the two leg stages goes through go_test_leg, and the helper reads go test's own rc.
func legSiteViolations(script string) []string {
	var out []string
	body := func(name string) string {
		i := strings.Index(script, "\n"+name+"() {")
		if i < 0 {
			out = append(out, "no "+name)
			return ""
		}
		j := strings.Index(script[i+1:], "\n}\n")
		if j < 0 {
			out = append(out, name+" has no end")
			return ""
		}
		return script[i : i+1+j]
	}
	for _, c := range []struct{ fn, logVar string }{{"check_test_shard", "log"}, {"check_race_shard", "racelog"}} {
		b := body(c.fn)
		if n := strings.Count(b, `go_test_leg "${`+c.logVar+`}" "${legjson}" `); n != 3 {
			out = append(out, c.fn+": "+itoa(n)+" go_test_leg call(s) with the leg log and ${legjson}, want 3")
		}
		if strings.Contains(b, "go test -mod=readonly") {
			out = append(out, c.fn+" still has a go test call that does not go through go_test_leg")
		}
		if strings.Contains(b, `2>&1 | tee -a "${`+c.logVar+`}"`) {
			out = append(out, c.fn+" still pipes a go test through tee into the leg log")
		}
	}
	h := body("go_test_leg")
	for _, need := range []string{" -json ", `rc="${ps[0]}"`, `"${ps[1]}"`, `"${ps[2]}"`, `"${ps[3]}"`, `return "${rc}"`, "go_test_leg: JSON converter failed", "go_test_leg: the raw file write failed"} {
		if !strings.Contains(h, need) {
			out = append(out, "go_test_leg lacks "+need)
		}
	}
	if !strings.Contains(script, "fromjson?") {
		out = append(out, "the converter does not pass non-JSON lines through (fromjson?)")
	}
	if !strings.Contains(h, "| tee -a \"${log}\"\n  ps=(\"${PIPESTATUS[@]}\")\n") {
		out = append(out, "go_test_leg does not copy every PIPESTATUS element right after the last tee")
	}
	return out
}

func TestEveryLegStageGoesThroughTheJSONHelperAndKeepsGoTestsRC(t *testing.T) {
	t.Parallel()
	script, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if v := legSiteViolations(string(script)); len(v) > 0 {
		t.Fatalf("%s", strings.Join(v, "\n"))
	}
	text := string(script)
	for name, mut := range map[string][2]string{
		"a race site back to the plain pipe": {`go_test_leg "${racelog}" "${legjson}" -race -timeout "${DEV_HEALTH_GO_RACE_TEST_TIMEOUT:-10m}" "${pkgs_sec[@]}"`, `"${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -race -timeout "${DEV_HEALTH_GO_RACE_TEST_TIMEOUT:-10m}" "${pkgs_sec[@]}"`},
		"no explicit status copy":            {`ps=("${PIPESTATUS[@]}")`, `ps=($?)`},
		"a converter failure is not named":   {"go_test_leg: JSON converter failed", "go_test_leg: converter note"},
		"a raw file failure is not named":    {"go_test_leg: the raw file write failed", "go_test_leg: raw file note"},
	} {
		if len(legSiteViolations(strings.Replace(text, mut[0], mut[1], 1))) == 0 {
			t.Errorf("the guard misses: %s", name)
		}
	}
}
