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
    raw-ok|raw-fail) printf 'RAWLINE-NOT-JSON %s\n' "${rel}" ;;
  esac
  if [ "${mode_here}" = raw-fail ]; then rc=1; continue; fi
  if [ "${mode_here}" = fail-event ] && [ "${done_fail-0}" = 0 ]; then
    done_fail=1
    printf '{"Action":"output","Package":"%s","Test":"TestPasses","Output":"PASSING-OUTPUT\\n"}\n' "${p}"
    printf '{"Action":"pass","Package":"%s","Test":"TestPasses","Elapsed":0.25}\n' "${p}"
    printf '{"Action":"output","Package":"%s","Test":"TestBreaks","Output":"FAILING-OUTPUT\\n"}\n' "${p}"
    printf '{"Action":"fail","Package":"%s","Test":"TestBreaks","Elapsed":0.5}\n' "${p}"
    printf '{"Action":"output","Package":"%s","Output":"FAIL\\t%s\\t0.500s\\n"}\n' "${p}" "${p}"
    printf '{"Action":"fail","Package":"%s","Elapsed":0.5}\n' "${p}"
    rc=1
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
		"FAKE_CENSUS="+filepath.Join(repo, "ci", "race_excluded_tests.tsv"), "FAKE_MODE="+mode, "FAKE_GO_LISTING="+listPath,
		"GO_TEST_LEG_JSON_DIR="+legDir, "GO_TEST_JSON_DIR="+filepath.Join(work, "ps-json"))
	count := "3"
	if leg == "race" {
		count = "4"
	}
	res := cmdSpec{dir: repo, env: env}.run("bash", script, "ci-leg", leg, shard1(shard), count)
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

func TestALegKeepsANonJSONLineAndStaysGreenOnExitZero(t *testing.T) {
	t.Parallel()
	for _, leg := range []string{"test", "race"} {
		res, _ := runLegWithFakeGo(t, leg, 1, "raw-ok")
		if res.code != 0 {
			t.Fatalf("%s leg: go test printed a non-JSON line and exited 0, the leg exited %d: %s", leg, res.code, tail(res.stderr, 1200)+"\n--- stdout:\n"+tail(res.stdout, 800))
		}
		if !strings.Contains(res.stdout, "RAWLINE-NOT-JSON") {
			t.Errorf("%s leg: the non-JSON line is not in the plain log: %s", leg, tail(res.stdout, 800))
		}
	}
}

func TestALegStaysRedWhenGoTestExitsOneAfterANonJSONLine(t *testing.T) {
	t.Parallel()
	for _, leg := range []string{"test", "race"} {
		res, _ := runLegWithFakeGo(t, leg, 1, "raw-fail")
		if res.code == 0 {
			t.Fatalf("%s leg: go test exited 1 and the leg exited 0 (the converter decided the verdict): %s", leg, tail(res.stderr, 1200)+"\n--- stdout:\n"+tail(res.stdout, 800))
		}
		if !strings.Contains(res.stdout, "RAWLINE-NOT-JSON") {
			t.Errorf("%s leg: the non-JSON line is not in the plain log", leg)
		}
	}
}

func TestALegWithAFailEventIsRedAndKeepsElapsedInTheJSON(t *testing.T) {
	t.Parallel()
	for _, leg := range []string{"test", "race"} {
		res, legJSON := runLegWithFakeGo(t, leg, 1, "fail-event")
		if res.code == 0 {
			t.Fatalf("%s leg: go test exited 1 with a fail event and the leg exited 0: %s", leg, tail(res.stderr, 1200)+"\n--- stdout:\n"+tail(res.stdout, 800))
		}
		if !strings.Contains(legJSON, `"Action":"fail"`) || !strings.Contains(legJSON, `"Test":"TestBreaks"`) || !strings.Contains(legJSON, `"Elapsed":0.5`) {
			t.Errorf("%s leg: the leg JSON lacks the fail event with its Elapsed: %q", leg, tail(legJSON, 600))
		}
		if !strings.Contains(legJSON, `"Elapsed":0.25`) {
			t.Errorf("%s leg: the leg JSON lacks the passing test's Elapsed: %q", leg, tail(legJSON, 600))
		}
		if !strings.Contains(res.stdout, "FAILING-OUTPUT") {
			t.Errorf("%s leg: the failing test's output is not in the plain log: %s", leg, tail(res.stdout, 800))
		}
		if strings.Contains(res.stdout, "PASSING-OUTPUT") {
			t.Errorf("%s leg: a passing test's output reached the plain log (plain go test hides it)", leg)
		}
		if !strings.Contains(res.stdout, "FAIL\t") {
			t.Errorf("%s leg: the package FAIL line is not in the plain log", leg)
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

func TestTheLegConverterRebuildsThePlainLog(t *testing.T) {
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
	want := "    b_test.go:2: failing log\n--- FAIL: TestB (0.00s)\na line that is not an event\n42\nFAIL\tp\t0.010s\nnever ended\n"
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
	for _, need := range []string{" -json ", `rc="${PIPESTATUS[0]}"`, `return "${rc}"`, `fromjson?`, "|| cat >/dev/null", `tee -a "${legjson}"`} {
		if !strings.Contains(script, need) || (need != "fromjson?" && !strings.Contains(h, need)) {
			out = append(out, "go_test_leg lacks "+need)
		}
	}
	if !strings.Contains(h, "| tee -a \"${log}\"\n  rc=\"${PIPESTATUS[0]}\"\n") {
		out = append(out, "go_test_leg does not read PIPESTATUS right after the last tee")
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
		"a race site back to the plain pipe": {`go_test_leg "${racelog}" "${legjson}" -race -count=1 `, `"${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -race -count=1 `},
		"no explicit rc":                     {`rc="${PIPESTATUS[0]}"`, `rc=$?`},
		"a converter that can end early":     {"|| cat >/dev/null", "|| true"},
	} {
		if len(legSiteViolations(strings.Replace(text, mut[0], mut[1], 1))) == 0 {
			t.Errorf("the guard misses: %s", name)
		}
	}
}
