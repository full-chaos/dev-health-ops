package scripts_test

import (
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
