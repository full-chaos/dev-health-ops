package devhealth_test

import (
	"strconv"
	"strings"
	"testing"
)

// CHAOS-11648: auth-service drains like go-api (CHAOS-10718). The old pod keeps serving for
// preStopSeconds while its endpoint is removed, then gets SIGTERM; kubelet counts the hook inside
// the grace period, so the grace must cover the sleep, the shutdown timeout and 5 s.

func authPod(t *testing.T, sets ...string) map[string]any {
	t.Helper()
	docs, out, err := renderAuth(t, append(append([]string{}, authBase...), sets...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if deployment == nil {
		t.Fatal("no auth-service Deployment")
	}
	pod, _ := at(deployment, "spec", "template", "spec").(map[string]any)
	return pod
}

func authRefused(t *testing.T, sets ...string) string {
	t.Helper()
	_, out, err := renderAuth(t, append(append([]string{}, authBase...), sets...)...)
	if err == nil {
		t.Fatalf("render passed, want a refusal (%v)", sets)
	}
	return out
}

func TestAuthServiceDrainsBeforeSigterm(t *testing.T) {
	pod := authPod(t)
	container := at(pod, "containers", 0)
	if pod["terminationGracePeriodSeconds"] != 35 {
		t.Errorf("terminationGracePeriodSeconds = %v, want 35 (10 + 20 + 5)", pod["terminationGracePeriodSeconds"])
	}
	if seconds := at(container, "lifecycle", "preStop", "sleep", "seconds"); seconds != 10 {
		t.Errorf("preStop sleep seconds = %v, want 10", seconds)
	}
	if exec := at(container, "lifecycle", "preStop", "exec"); exec != nil {
		t.Errorf("preStop uses exec %v: the image has no shell", exec)
	}
	found := false
	for _, arg := range toStrings(at(container, "args")) {
		found = found || arg == "--shutdown-timeout=20s"
	}
	if !found {
		t.Errorf("args lack --shutdown-timeout=20s: the grace guard reads the value the chart passes: %v", at(container, "args"))
	}
}

// Each edge of grace >= preStop + shutdown + 5: the minimum renders, one second less is refused.
func TestAuthServiceGraceBoundAtEachEdge(t *testing.T) {
	for _, c := range []struct {
		name      string
		preStop   int
		hook      bool
		shutdown  int
		minimum   int
		wantInMsg string
	}{
		{"default", 10, true, 20, 35, "authService.drain.preStopSeconds (10) + authService.shutdownTimeout (20) + 5 = 35"},
		{"preStop off", 0, false, 20, 25, "authService.drain.preStopSeconds (0) + authService.shutdownTimeout (20) + 5 = 25"},
		{"preStop at its maximum", 600, true, 20, 625, "authService.drain.preStopSeconds (600) + authService.shutdownTimeout (20) + 5 = 625"},
		{"longer shutdown", 10, true, 90, 105, "authService.drain.preStopSeconds (10) + authService.shutdownTimeout (90) + 5 = 105"},
	} {
		t.Run(c.name, func(t *testing.T) {
			sets := func(grace int) []string {
				return []string{
					"--set", "authService.drain.preStopSeconds=" + strconv.Itoa(c.preStop),
					"--set", "authService.shutdownTimeout=" + strconv.Itoa(c.shutdown) + "s",
					"--set", "authService.terminationGracePeriodSeconds=" + strconv.Itoa(grace),
				}
			}
			pod := authPod(t, sets(c.minimum)...)
			if has := at(at(pod, "containers", 0), "lifecycle", "preStop") != nil; has != c.hook {
				t.Errorf("preStop rendered = %v, want %v", has, c.hook)
			}
			if out := authRefused(t, sets(c.minimum-1)...); !strings.Contains(out, c.wantInMsg) {
				t.Errorf("the refusal does not say %q: %.400s", c.wantInMsg, out)
			}
		})
	}
}

func TestAuthServiceDrainRefusals(t *testing.T) {
	for name, c := range map[string]struct {
		sets []string
		want string
	}{
		"preStop 601":        {[]string{"--set", "authService.drain.preStopSeconds=601", "--set", "authService.terminationGracePeriodSeconds=700"}, "preStopSeconds"},
		"preStop negative":   {[]string{"--set", "authService.drain.preStopSeconds=-1"}, "preStopSeconds"},
		"preStop fractional": {[]string{"--set", "authService.drain.preStopSeconds=1.5"}, "preStopSeconds"},
		"preStop text":       {[]string{"--set-string", "authService.drain.preStopSeconds=abc"}, "preStopSeconds"},
		"preStop huge":       {[]string{"--set-string", "authService.drain.preStopSeconds=99999999999999999999"}, "preStopSeconds"},
		"shutdown zero":      {[]string{"--set", "authService.shutdownTimeout=0s"}, "shutdownTimeout"},
		"shutdown over 3 h":  {[]string{"--set", "authService.shutdownTimeout=10801s", "--set", "authService.terminationGracePeriodSeconds=86400"}, "shutdownTimeout"},
		"grace zero":         {[]string{"--set", "authService.terminationGracePeriodSeconds=0"}, "terminationGracePeriodSeconds"},
		"extraArgs shutdown": {[]string{"--set", "authService.extraArgs[0]=--shutdown-timeout=5s"}, "authService.extraArgs must not set --shutdown-timeout=5s"},
		"extraEnv shutdown":  {[]string{"--set", "authService.extraEnv[0].name=AUTH_SERVICE_SHUTDOWN_TIMEOUT", "--set", "authService.extraEnv[0].value=5s"}, "AUTH_SERVICE_SHUTDOWN_TIMEOUT"},
	} {
		t.Run(name, func(t *testing.T) {
			if out := authRefused(t, c.sets...); !strings.Contains(out, c.want) {
				t.Errorf("the refusal does not say %q: %.400s", c.want, out)
			}
		})
	}
}
