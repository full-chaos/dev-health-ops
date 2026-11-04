package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// ci/pdp_load_k3s.sh is run by the lead on bigboy k3s, never in CI. These cells hold what can be checked without a
// cluster: the refusals before any cluster call, the --print-plan render of the chart, and the shape of the script (the
// ticket numbers, the 300 s workload token rule, the namespace it deletes, no kind, no secret on argv).

const (
	pdpScript = "ci/pdp_load_k3s.sh"
	pdpPod    = "ci/pdp_load_k3s/pod.sh"
)

func pdpDigest(name string, c string) string {
	return "registry.example/" + name + "@sha256:" + strings.Repeat(c, 64)
}

func pdpImages() []string {
	return []string{
		"AUTH_PDP_IMAGE_SERVICE=" + pdpDigest("dev-health-go-auth-service", "a"),
		"AUTH_PDP_IMAGE_MIGRATE=" + pdpDigest("dev-health-go-auth-migrate", "b"),
		"AUTH_PDP_IMAGE_DHO=" + pdpDigest("dev-health-go-dho", "c"),
	}
}

// pdpRun runs the script with a kubectl that refuses every call and a kubeconfig that does not exist, so no defect of the
// script can reach the cluster of a host that has /etc/rancher/k3s/k3s.yaml.
func pdpRun(t *testing.T, env []string, args ...string) cmdResult {
	t.Helper()
	root := rootOf(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte("#!/usr/bin/env bash\necho 'kubectl is denied in this cell' >&2\nexit 99\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	base := []string{strings.Replace(shimBasePath(), "PATH=", "PATH="+dir+":", 1), "AUTH_PDP_KUBECONFIG=" + filepath.Join(dir, "no-such-k3s.yaml")}
	return cmdSpec{dir: root, env: append(base, env...)}.run("bash", append([]string{filepath.Join(root, pdpScript)}, args...)...)
}

func TestPDPLoadK3sPrintPlanStatesTheTicketNumbersAndRendersTheChart(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	res := pdpRun(t, nil, "--print-plan")
	if res.code != 0 {
		t.Fatalf("--print-plan exit %d\n%s\n%s", res.code, res.stdout, res.stderr)
	}
	for _, want := range []string{
		"plan: gated SLO      p95=25ms,p99=75ms,err=0",
		"is capped at 240 s",
		"gated SLO = PDP with the limiter lifted; the default limiter caps each PDP route at about 20 rps per pod",
		"cache OFF",
		"plan: the namespace is deleted on exit",
		"PASS: plan: the knee form renders the caller subject and the eddsa class with the default route limiter",
		"PASS: plan: the gated form renders the lifted route limiter (per second and burst)",
		"PASS: plan: the knee form renders the default PDP caller and route limiters",
		"PASS: plan: the gated form renders the lifted PDP caller and route limiters (per second and burst)",
		"PASS: plan: the migrate hook Secret carries CLICKHOUSE_URI (the ClickHouse step of dho migrate upgrade)",
	} {
		if !strings.Contains(res.stdout, want) {
			t.Errorf("--print-plan lacks %q\n%s", want, res.stdout)
		}
	}
}

func TestPDPLoadK3sRefusesBeforeAnyClusterCall(t *testing.T) {
	good := pdpImages()
	for name, env := range map[string][]string{
		"no images":    nil,
		"a tag":        {"AUTH_PDP_IMAGE_SERVICE=registry.example/auth-service:ci", good[1], good[2]},
		"a short hash": {good[0], "AUTH_PDP_IMAGE_MIGRATE=registry.example/auth-migrate@sha256:abc", good[2]},
		"a local name": {good[0], good[1], "AUTH_PDP_IMAGE_DHO=dho:ci"},
	} {
		t.Run(name, func(t *testing.T) {
			res := pdpRun(t, env)
			if res.code == 0 || !strings.Contains(res.stderr, "is not a registry digest reference") {
				t.Fatalf("exit %d, stderr %q; want a refusal naming the digest rule", res.code, res.stderr)
			}
		})
	}
	kind := append(append([]string{}, good...), "AUTH_PDP_KUBECONFIG=/dev/null")
	if res := pdpRun(t, kind); res.code == 0 || !strings.Contains(res.stderr, "must name a k3s kubeconfig") {
		t.Errorf("non-k3s kubeconfig: exit %d, stderr %q", res.code, res.stderr)
	}
	if res := pdpRun(t, good, "--keep"); res.code == 0 || !strings.Contains(res.stderr, "usage:") {
		t.Errorf("--keep: exit %d, stderr %q; want it refused as an unknown argument", res.code, res.stderr)
	}
}

func TestPDPLoadK3sNeedsTheBinaryAndANewOutputDirectory(t *testing.T) {
	root := rootOf(t)
	dir := t.TempDir()
	kubeconfig := filepath.Join(dir, "k3s-test.yaml")
	if err := os.WriteFile(kubeconfig, []byte("apiVersion: v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	kubectl := "#!/usr/bin/env bash\ncase \"$*\" in *\"get nodes\"*) printf 'v1.30.2' ;; *) echo \"kubectl is denied: $*\" >&2; exit 99 ;; esac\n"
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(kubectl), 0o755); err != nil {
		t.Fatal(err)
	}
	run := func(extra ...string) cmdResult {
		env := append([]string{strings.Replace(shimBasePath(), "PATH=", "PATH="+dir+":", 1), "AUTH_PDP_KUBECONFIG=" + kubeconfig}, pdpImages()...)
		return cmdSpec{dir: root, env: append(env, extra...)}.run("bash", filepath.Join(root, pdpScript))
	}
	if res := run(); res.code == 0 || !strings.Contains(res.stderr, "AUTH_PDP_LOADTEST_BIN") {
		t.Errorf("no binary: exit %d, stderr %q", res.code, res.stderr)
	}
	bin := filepath.Join(dir, "loadtest")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if res := run("AUTH_PDP_LOADTEST_BIN=" + bin); res.code == 0 || !strings.Contains(res.stderr, "AUTH_PDP_OUT_DIR") {
		t.Errorf("no output directory: exit %d, stderr %q", res.code, res.stderr)
	}
	if res := run("AUTH_PDP_LOADTEST_BIN="+bin, "AUTH_PDP_OUT_DIR="+dir); res.code == 0 || !strings.Contains(res.stderr, "exists") {
		t.Errorf("an existing output directory: exit %d, stderr %q", res.code, res.stderr)
	}
	res := run("AUTH_PDP_LOADTEST_BIN="+bin, "AUTH_PDP_OUT_DIR="+filepath.Join(dir, "new"))
	if res.code == 0 || !strings.Contains(res.stderr, "is not a k3s cluster") || strings.Contains(res.stderr, "kubectl is denied") {
		t.Errorf("a kind-like cluster behind a file named k3s: exit %d, stderr %q", res.code, res.stderr)
	}
}

func TestPDPLoadK3sKeepsTheTicketNumbersTheCapAndTheReportEvidence(t *testing.T) {
	text := readText(t, filepath.Join(rootOf(t), pdpScript))
	for _, want := range []string{
		`readonly SLO_P95="25ms" SLO_P99="75ms" SLO_ERR="0"`,
		"readonly RUN_CAP_SECONDS=240",
		`--slo "${name}:p95=${SLO_P95},p99=${SLO_P99},err=${SLO_ERR}"`,
		`--out "/work/${report}"`,
		"sha256sum",
		"trap cleanup EXIT",
		"kc delete namespace",
		`pod mint >/dev/null`,
		"authorize-batch-100",
		"the run stops here, nothing is loosened",
		"preflight",
		"dump_failure",
		"--all-containers --previous",
		"describe job",
		`K rollout status "statefulset/${FULL}-clickhouse"`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the script lacks %q", want)
		}
	}
	// The lifted limiter exists only in the gated form of the per-leg values, and never as a default.
	if n := strings.Count(text, "AUTH_SERVICE_RATE_LIMIT_PER_SECOND"); n != 2 {
		t.Errorf("AUTH_SERVICE_RATE_LIMIT_PER_SECOND appears %d times; want the lift line and the plan text", n)
	}
	if !regexp.MustCompile(`(?m)^\s+if \[ "\$3" = lift \]; then$`).MatchString(text) {
		t.Error("the route limiter override is not conditional on the lift form")
	}
	for _, banned := range []string{"kind create", "kind load", "docker ", "ctr images", "sudo ", "--from-literal", "--password", "kubectl delete -A", "--all-namespaces"} {
		if strings.Contains(text, banned) {
			t.Errorf("the script contains %q", banned)
		}
	}
}

func TestPDPLoadK3sPodScriptKeepsValuesOffArgvAndInFiles(t *testing.T) {
	text := readText(t, filepath.Join(rootOf(t), pdpPod))
	for _, want := range []string{"umask 077", `exec timeout "${PDP_RUN_CAP:-240}"`, `grep -q "\"allowed\":$6"`, "expect low analytics.read", "expect deny admin.users"} {
		if !strings.Contains(text, want) {
			t.Errorf("the pod script lacks %q", want)
		}
	}
	for _, banned := range []string{"curl ", "wget ", "echo \"$(cat", "set -x"} {
		if strings.Contains(text, banned) {
			t.Errorf("the pod script contains %q", banned)
		}
	}
}

// The runner pod is a YAML template (envsubst IMG_MIG and RUNNER_IMAGE): the bootstrap value is written in the pod's own
// volume by an init container and read by nothing outside the pod.
func TestPDPLoadK3sRunnerPodWritesTheBootstrapValueIntoItsOwnVolumeOnly(t *testing.T) {
	text := readText(t, filepath.Join(rootOf(t), "ci/pdp_load_k3s/runner-pod.tpl"))
	text = strings.NewReplacer("${IMG_MIG}", "registry.example/m@sha256:"+strings.Repeat("a", 64), "${RUNNER_IMAGE}", "registry.example/r@sha256:"+strings.Repeat("b", 64)).Replace(text)
	var pod struct {
		Spec struct {
			InitContainers []struct {
				Name string   `yaml:"name"`
				Args []string `yaml:"args"`
				Env  []struct {
					Name string `yaml:"name"`
				} `yaml:"env"`
			} `yaml:"initContainers"`
			Containers []struct {
				Name string `yaml:"name"`
			} `yaml:"containers"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal([]byte(text), &pod); err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.InitContainers) != 2 || len(pod.Spec.Containers) != 1 {
		t.Fatalf("%d init containers and %d containers, want 2 and 1", len(pod.Spec.InitContainers), len(pod.Spec.Containers))
	}
	if got := strings.Join(pod.Spec.InitContainers[0].Args, " "); got != "workloads seed -apply" {
		t.Errorf("first init container args %q", got)
	}
	if got := strings.Join(pod.Spec.InitContainers[1].Args, " "); got != "workloads bootstrap-create -workload api -out /work/api.bootstrap -apply" {
		t.Errorf("second init container args %q", got)
	}
	for _, ic := range pod.Spec.InitContainers {
		if len(ic.Env) != 2 {
			t.Errorf("init container %s has %d env entries, want the migration DSN and the runtime role", ic.Name, len(ic.Env))
		}
	}
	for _, banned := range []string{"hostPath", "hostNetwork", "privileged: true"} {
		if strings.Contains(text, banned) {
			t.Errorf("the runner pod contains %q", banned)
		}
	}
}

// pdpFakeKubectl stands in for the cluster: the script runs for real against it. An exec of pod.sh run calls the fake
// loadtest, which logs "profile load" and exits 1 for the one cell named in PDP_FAIL.
const pdpFakeKubectl = `#!/usr/bin/env bash
args=("$@")
for i in "${!args[@]}"; do
  if [ "${args[$i]##*/}" = "pod.sh" ] && [ "${args[$((i-1))]}" = sh ]; then
    verb="${args[$((i+1))]}"
    if [ "${verb}" = run ]; then exec loadtest "${args[@]:$((i+2))}"; fi
    if [ "${verb}" = mint ]; then
      echo m >>"${PDP_FAKE_DIR}/mint.log"
      [ "$(wc -l <"${PDP_FAKE_DIR}/mint.log")" -ne "${PDP_FAIL_MINT:-0}" ] || { echo 'fake mint failed' >&2; exit 42; }
    fi
    exit 0
  fi
done
case "$*" in
  *"get nodes"*) printf 'v1.30.2+k3s1'; exit 0 ;;
  *"get namespace"*) exit 1 ;;
  *" cp "*"pdp-runner:/work/"*.json*)
    dest="${args[$((${#args[@]}-1))]}"; src="${args[$((${#args[@]}-2))]}"
    [ "$(basename "${src}")" != "${PDP_FAIL_COPY:-}" ] || { echo 'fake kubectl cp failed' >&2; exit 42; }
    cp "${PDP_FAKE_DIR}/$(basename "${src}")" "${dest}"; exit 0 ;;
  *" psql "*) cat >/dev/null; printf 'prn_AAAAAAAAAAAAAAAAAAAAAA'; exit 0 ;;
esac
cat >/dev/null
exit 0
`

const pdpFakeLoadtest = `#!/usr/bin/env bash
cfg="" prof="" out="" rate=""
while [ $# -gt 0 ]; do
  case "$1" in
    --config) cfg="$2"; shift ;;
    --profile) prof="$2"; shift ;;
    --out) out="$2"; shift ;;
    --rate) rate="$2"; shift ;;
  esac
  shift
done
name="$(basename "${cfg}" .json)"
load="${prof}"; [ -z "${rate}" ] || load="${prof}:${rate}"
printf '%s %s\n' "${name}" "${load}" >>"${PDP_FAKE_DIR}/ran.log"
printf '{"profile":"%s","load":"%s"}\n' "${name}" "${load}" >"${PDP_FAKE_DIR}/$(basename "${out}")"
if [ "${name} ${load}" = "${PDP_FAIL}" ]; then
  n=$(grep -c -x -F "${PDP_FAIL}" "${PDP_FAKE_DIR}/ran.log")
  [ "${n}" -ne "${PDP_FAIL_N}" ] || exit 1
fi
exit 0
`

func pdpFakeRun(t *testing.T, fail string, extra ...string) (res cmdResult, ran []string, summary string) {
	t.Helper()
	root := rootOf(t)
	dir := t.TempDir()
	fake := filepath.Join(dir, "fake")
	if err := os.MkdirAll(fake, 0o755); err != nil {
		t.Fatal(err)
	}
	fakeSum := "#!/usr/bin/env bash\n[ \"$(basename \"${1:-}\")\" != \"${PDP_FAIL_HASH:-}\" ] || { echo 'fake sha256sum failed' >&2; exit 42; }\nexec /usr/bin/sha256sum \"$@\"\n"
	for name, body := range map[string]string{"kubectl": pdpFakeKubectl, "loadtest": pdpFakeLoadtest, "helm": "#!/usr/bin/env bash\nexit 0\n", "sha256sum": fakeSum} {
		if err := os.WriteFile(filepath.Join(fake, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	kubeconfig := filepath.Join(dir, "k3s-fake.yaml")
	if err := os.WriteFile(kubeconfig, []byte("apiVersion: v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out")
	env := append([]string{
		strings.Replace(shimBasePath(), "PATH=", "PATH="+fake+":", 1),
		"TMPDIR=" + dir, "AUTH_PDP_KUBECONFIG=" + kubeconfig, "AUTH_PDP_LOADTEST_BIN=" + filepath.Join(fake, "loadtest"),
		"AUTH_PDP_OUT_DIR=" + out, "PDP_FAKE_DIR=" + fake, "PDP_FAIL=" + fail, "PDP_FAIL_N=1",
	}, pdpImages()...)
	res = cmdSpec{dir: root, env: append(env, extra...)}.run("bash", filepath.Join(root, pdpScript))
	if b, err := os.ReadFile(filepath.Join(fake, "ran.log")); err == nil {
		ran = strings.Split(strings.TrimSpace(string(b)), "\n")
	}
	if b, err := os.ReadFile(filepath.Join(out, "summary.tsv")); err == nil {
		summary = string(b)
	}
	return res, ran, summary
}

// The first breach is reported and the run stops: the failed run keeps its report and its summary row and no run follows it.
func TestPDPLoadK3sStopsAtTheFirstBreachOfEveryRun(t *testing.T) {
	profiles := []string{"authorize-low", "authorize-low-hot", "authorize-high", "authorize-deny", "authorize-mix", "authorize-batch-10", "authorize-batch-100"}
	var order []string
	for _, load := range []string{"normal", "peak"} {
		for _, p := range profiles {
			order = append(order, p+" "+load)
		}
	}
	for i, cell := range order {
		t.Run(strings.ReplaceAll(cell, " ", "_"), func(t *testing.T) {
			res, ran, summary := pdpFakeRun(t, cell)
			if res.code == 0 {
				t.Fatalf("exit 0 after a breach\n%s", res.stdout)
			}
			if len(ran) != i+2 || ran[len(ran)-1] != cell {
				t.Fatalf("ran %d runs %v; want the knee run and the first %d, ending at %q", len(ran), ran, i+1, cell)
			}
			parts := strings.Split(cell, " ")
			row := fmt.Sprintf("\t%s\t%s\t", parts[0], parts[1])
			if n := strings.Count(summary, row); n != 1 || !strings.Contains(summary, "FAILED(exit 1)") {
				t.Errorf("summary rows for %q: %d, want 1 and a FAILED mark\n%s", cell, n, summary)
			}
			if want := "REPORT: a gated objective or assertion failed at " + parts[1] + " load"; !strings.Contains(res.stdout, want) {
				t.Errorf("stdout lacks %q\n%s", want, res.stdout)
			}
		})
	}
	t.Run("knee", func(t *testing.T) {
		res, ran, _ := pdpFakeRun(t, "authorize-knee peak:100")
		if res.code == 0 || len(ran) != 1 {
			t.Errorf("exit %d after %v; want a failure after the knee run alone", res.code, ran)
		}
	})
	for _, load := range []string{"normal", "peak"} {
		t.Run("cacheon_"+load, func(t *testing.T) {
			res, ran, _ := pdpFakeRun(t, "authorize-mix "+load, "AUTH_PDP_CACHE_ON=1", "PDP_FAIL_N=2")
			want := 16
			if load == "peak" {
				want = 17
			}
			if res.code == 0 || len(ran) != want {
				t.Errorf("cache-on breach at %s: exit %d, %d runs; want a failure after %d runs\n%v", load, res.code, len(ran), want, ran)
			}
		})
	}
}

// A step inside run_one that fails (mint, report copy, hash) stops the run at once, names the step, marks the row, and
// never ends in PASS: run_one is called in && chains and || lists, where bash ignores set -e inside it.
func TestPDPLoadK3sAFailedStepInsideRunOneStopsTheRun(t *testing.T) {
	type cell struct{ leg, profile, load, extra string }
	var cells []cell
	cells = append(cells, cell{"knee", "authorize-knee", "peak-100", ""})
	for _, load := range []string{"normal", "peak"} {
		for _, p := range []string{"authorize-low", "authorize-low-hot", "authorize-high", "authorize-deny", "authorize-mix", "authorize-batch-10", "authorize-batch-100"} {
			leg := "gated"
			if p == "authorize-batch-100" {
				leg = "recorded"
			}
			cells = append(cells, cell{leg, p, load, ""})
		}
	}
	cells = append(cells, cell{"cacheon", "authorize-mix", "normal", "AUTH_PDP_CACHE_ON=1"}, cell{"cacheon", "authorize-mix", "peak", "AUTH_PDP_CACHE_ON=1"})
	for i, c := range cells {
		report := fmt.Sprintf("%s-%s-%s.json", c.leg, c.profile, c.load)
		mintNo := 3 + i
		if i == 0 {
			mintNo = 2
		} else if i >= 15 {
			mintNo = 19 + i - 15
		}
		for _, step := range []string{"mint", "copy", "hash"} {
			t.Run(step+"_"+report, func(t *testing.T) {
				extra := []string{"PDP_FAIL_MINT=" + fmt.Sprint(mintNo)}
				ranWant := i + 1
				switch step {
				case "copy":
					extra = []string{"PDP_FAIL_COPY=" + report}
				case "hash":
					extra = []string{"PDP_FAIL_HASH=" + report}
				default:
					ranWant = i
				}
				if c.extra != "" {
					extra = append(extra, c.extra)
				}
				res, ran, summary := pdpFakeRun(t, "", extra...)
				if res.code == 0 || strings.Contains(res.stdout, "RUN PASSED") {
					t.Fatalf("exit %d after a failed %s\n%s", res.code, step, res.stdout)
				}
				if len(ran) != ranWant && !(ranWant == 0 && len(ran) == 0) {
					t.Errorf("ran %d runs %v; want %d", len(ran), ran, ranWant)
				}
				if want := "step " + step + " failed"; !strings.Contains(res.stderr, want) {
					t.Errorf("stderr lacks %q\n%s", want, res.stderr)
				}
				if want := "FAILED(" + step + ")"; !strings.Contains(summary, want) {
					t.Errorf("summary lacks %q\n%s", want, summary)
				}
			})
		}
	}
}

// run_one is called in && chains, || lists and `if !` lists, where bash ignores set -e inside it: every step in its body
// must carry its own check, and every call site must be one of those conditional forms (9 sites in the source, 17 runs).
func TestPDPLoadK3sRunOneChecksEveryStepItself(t *testing.T) {
	lines := strings.Split(readText(t, filepath.Join(rootOf(t), pdpScript)), "\n")
	start, end := -1, -1
	for i, l := range lines {
		if strings.HasPrefix(l, "run_one() {") {
			start = i
		}
		if start >= 0 && end < 0 && i > start && l == "}" {
			end = i
		}
	}
	if start < 0 || end < 0 {
		t.Fatal("run_one body not found")
	}
	steps := regexp.MustCompile(`(^|[^A-Za-z_])(pod |K |kc |sha256sum)|>>"\$\{SUMMARY\}"`)
	seen := map[string]bool{}
	for _, l := range lines[start+1 : end] {
		code := strings.TrimSpace(l)
		if strings.HasPrefix(code, "printf '%s %s %s: ") {
			continue
		}
		m := steps.FindString(l)
		if m == "" {
			continue
		}
		seen[strings.TrimSpace(strings.Trim(m, " ($"))] = true
		if !strings.Contains(l, "||") && !strings.HasPrefix(code, "if !") {
			t.Errorf("run_one step without its own check: %q", code)
		}
	}
	for _, want := range []string{"pod", "K", "sha256sum", `>>"${SUMMARY}"`} {
		if !seen[want] {
			t.Errorf("the census saw no %q step in run_one", want)
		}
	}
	sites := 0
	call := regexp.MustCompile(`^\s*(for .*; do )?run_one `)
	for i, l := range lines {
		if i > start && i < end || !strings.Contains(l, "run_one ") || strings.HasPrefix(strings.TrimSpace(l), "#") {
			continue
		}
		sites++
		prev := ""
		if i > 0 {
			prev = strings.TrimSpace(lines[i-1])
		}
		if !call.MatchString(l) || !(strings.Contains(l, "&&") || strings.Contains(l, "||") || strings.HasSuffix(prev, "&&")) {
			t.Errorf("run_one call outside a checked list: %q", strings.TrimSpace(l))
		}
	}
	if sites != 9 {
		t.Errorf("%d run_one call sites; want 9 (knee 1, gated 7, cache-on 1), run 17 times", sites)
	}
}
