package devhealth_test

import (
	"os/exec"
	"strings"
	"testing"
)

func goAPIPodSpec(t *testing.T, sets ...string) map[string]any {
	t.Helper()
	spec, _ := at(goAPIDeployment(t, sets...), "spec", "template", "spec").(map[string]any)
	if spec == nil {
		t.Fatal("no pod spec in the go-api Deployment")
	}
	return spec
}

func goAPIContainer(t *testing.T, spec map[string]any) map[string]any {
	t.Helper()
	containers, _ := spec["containers"].([]any)
	for _, container := range containers {
		if at(container, "name") == "go-api" {
			c, _ := container.(map[string]any)
			return c
		}
	}
	t.Fatal("no go-api container")
	return nil
}

func goAPIArgs(t *testing.T, spec map[string]any) []string {
	t.Helper()
	raw, _ := goAPIContainer(t, spec)["args"].([]any)
	args := make([]string, 0, len(raw))
	for _, arg := range raw {
		args = append(args, arg.(string))
	}
	return args
}

func renderGoAPIFails(t *testing.T, sets ...string) string {
	t.Helper()
	args := append([]string{"template", "go-api-drain", ".", "--set", "goApi.enabled=true"}, sets...)
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err == nil {
		t.Fatalf("render passed, want a refusal (%v)", sets)
	}
	return string(out)
}

// By default the old go-api pod keeps serving for preStopSeconds while the endpoint is removed
// (the dho image has no shell, so the native sleep action), and the grace period covers
// sleep + the shutdown timeout the chart passes + 5 s.
func TestGoAPIDrainsBeforeSigterm(t *testing.T) {
	spec := goAPIPodSpec(t)
	if spec["terminationGracePeriodSeconds"] != 45 {
		t.Errorf("terminationGracePeriodSeconds = %v, want 45", spec["terminationGracePeriodSeconds"])
	}
	if seconds := at(goAPIContainer(t, spec), "lifecycle", "preStop", "sleep", "seconds"); seconds != 10 {
		t.Errorf("preStop sleep seconds = %v, want 10", seconds)
	}
	if exec := at(goAPIContainer(t, spec), "lifecycle", "preStop", "exec"); exec != nil {
		t.Errorf("preStop uses exec %v: the image has no shell", exec)
	}
	found := false
	for _, arg := range goAPIArgs(t, spec) {
		if arg == "--shutdown-timeout=30s" {
			found = true
		}
	}
	if !found {
		t.Errorf("args %v lack --shutdown-timeout=30s: the grace guard reads the value the chart passes", goAPIArgs(t, spec))
	}
}

func TestGoAPIDrainOffRendersNoHook(t *testing.T) {
	spec := goAPIPodSpec(t, "--set", "goApi.drain.preStopSeconds=0")
	if lifecycle := goAPIContainer(t, spec)["lifecycle"]; lifecycle != nil {
		t.Errorf("preStopSeconds=0 rendered lifecycle %v", lifecycle)
	}
}

func TestGoAPIGraceMustCoverSleepShutdownAndFiveSeconds(t *testing.T) {
	ok := goAPIPodSpec(t, "--set", "goApi.drain.preStopSeconds=15", "--set", "goApi.shutdownTimeout=40s", "--set", "goApi.terminationGracePeriodSeconds=60")
	if ok["terminationGracePeriodSeconds"] != 60 {
		t.Errorf("grace = %v, want 60", ok["terminationGracePeriodSeconds"])
	}
	for _, sets := range [][]string{
		{"--set", "goApi.terminationGracePeriodSeconds=40"}, // exactly sleep (10) + shutdown (30)
		{"--set", "goApi.terminationGracePeriodSeconds=44"}, // one second under the +5 margin
		{"--set", "goApi.drain.preStopSeconds=15", "--set", "goApi.shutdownTimeout=40s", "--set", "goApi.terminationGracePeriodSeconds=59"},
		{"--set", "goApi.shutdownTimeout=300s"}, // default grace 45 < 10 + 300 + 5
	} {
		out := renderGoAPIFails(t, sets...)
		for _, key := range []string{"goApi.terminationGracePeriodSeconds", "goApi.drain.preStopSeconds", "goApi.shutdownTimeout"} {
			if !strings.Contains(out, key) {
				t.Errorf("%v: refusal does not name %s:\n%.500s", sets, key, out)
			}
		}
	}
}

func TestGoAPIDrainValuesAreWholeNonNegativeSeconds(t *testing.T) {
	for _, sets := range [][]string{
		{"--set", "goApi.drain.preStopSeconds=-1"},
		{"--set", "goApi.drain.preStopSeconds=1.5"},
		{"--set", "goApi.drain.preStopSeconds=abc"},
		{"--set", "goApi.shutdownTimeout=30"},
		{"--set", "goApi.shutdownTimeout=1m"},
		{"--set", "goApi.shutdownTimeout=0s"},
	} {
		renderGoAPIFails(t, sets...)
	}
}

func TestGoAPIShutdownTimeoutHasOneOwner(t *testing.T) {
	renderGoAPIFails(t, "--set-json", `goApi.extraArgs=["--shutdown-timeout=5m"]`)
	renderGoAPIFails(t, "--set-json", `goApi.extraEnv=[{"name":"DEV_HEALTH_SHUTDOWN_TIMEOUT","value":"5m"}]`)
}

// Only the go-api pod drains: no other workload of the chart gets a preStop hook from this value.
func TestGoAPIDrainDoesNotTouchOtherWorkloads(t *testing.T) {
	out, err := exec.Command("helm", "template", "drain-others", ".", "--set", "goApi.enabled=true", "--set", "authService.enabled=false").CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	for _, document := range strings.Split(string(out), "\n---") {
		if strings.Contains(document, "# Source: dev-health/templates/go-api-deployment.yaml\n") {
			continue
		}
		if strings.Contains(document, "preStop:") && strings.Contains(document, "sleep:") {
			head := document
			if len(head) > 200 {
				head = head[:200]
			}
			t.Errorf("a non-go-api document carries a preStop sleep:\n%s", head)
		}
	}
}
