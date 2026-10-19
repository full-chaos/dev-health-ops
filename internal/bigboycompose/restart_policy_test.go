package bigboycompose_test

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-10181: a docker daemon restart (an apt upgrade of docker-ce or containerd) stops every container, and docker
// starts again only the ones with a restart policy. On 2026-10-18 the ops services came back and the acr profile
// services, which had none, stayed down. Every long-running service of the bigboy compose file therefore carries the
// policy the ops services use, and only a one-shot service (it runs to its end: an init, a migrate, a route
// activation, a profile one-off) carries none.

const longRunningRestart = "unless-stopped"

// oneShotServices run to their end and must not restart; every other service is long-running.
var oneShotServices = []string{
	"acr-db-acl", "acr-db-init", "acr-migrate",
	"go-contractcheck", "go-river-provision",
	"go-sync-dispatch-route-activate", "go-sync-finalize-route-activate", "go-sync-post-route-activate", "go-sync-reference-route-activate",
	"go-worker-consumers-ready", "go-worker-migrate", "go-worker-ready", "go-worker-route-activate",
	"migrate",
	"venue-hook", "venue-prove", "venue-river", "venue-tools", "web-smoke",
}

// restartPolicyFindings returns one finding per service whose restart policy does not fit its kind.
func restartPolicyFindings(text string) ([]string, error) {
	var doc struct {
		Services map[string]struct {
			Restart   *string              `yaml:"restart"`
			DependsOn map[string]yaml.Node `yaml:"depends_on"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		return nil, err
	}
	if len(doc.Services) == 0 {
		return nil, fmt.Errorf("no services")
	}
	oneShot := map[string]bool{}
	for _, name := range oneShotServices {
		oneShot[name] = true
	}
	var findings []string
	found := func(format string, args ...any) { findings = append(findings, fmt.Sprintf(format, args...)) }
	names := make([]string, 0, len(doc.Services))
	for name := range doc.Services {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		service := doc.Services[name]
		restart := ""
		if service.Restart != nil {
			restart = *service.Restart
		}
		switch {
		case oneShot[name] && restart != "" && restart != "no":
			found("%s: one-shot service with restart %q: it would run again after it ends", name, restart)
		case !oneShot[name] && restart != longRunningRestart:
			found("%s: long-running service with restart %q, want %q: a docker daemon restart leaves it down (or list it as one-shot)", name, restart, longRunningRestart)
		}
		for waited, condition := range service.DependsOn {
			var spec struct {
				Condition string `yaml:"condition"`
			}
			if condition.Kind == yaml.MappingNode && condition.Decode(&spec) == nil &&
				spec.Condition == "service_completed_successfully" && !oneShot[waited] {
				found("%s: waits for %s to complete, but %s is not listed as one-shot", name, waited, waited)
			}
		}
	}
	for _, name := range oneShotServices {
		if _, ok := doc.Services[name]; !ok {
			found("%s: listed as one-shot but not in the file", name)
		}
	}
	sort.Strings(findings)
	return findings, nil
}

func TestBigboyServicesHaveTheRestartPolicyOfTheirKind(t *testing.T) {
	raw, err := os.ReadFile(toolsPath(t, composeName))
	if err != nil {
		t.Fatal(err)
	}
	findings, err := restartPolicyFindings(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	for _, finding := range findings {
		t.Error(finding)
	}
}

// Each defect planted in the checked-in file (changed as parsed data) gives the finding that names it.
func TestBigboyRestartPolicyCheckSeesEachDefect(t *testing.T) {
	raw, err := os.ReadFile(toolsPath(t, composeName))
	if err != nil {
		t.Fatal(err)
	}
	plant := func(t *testing.T, change func(services map[string]any)) string {
		t.Helper()
		var doc map[string]any
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		change(doc["services"].(map[string]any))
		out, err := yaml.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		return string(out)
	}
	service := func(services map[string]any, name string) map[string]any {
		return services[name].(map[string]any)
	}
	cases := map[string]struct {
		change func(services map[string]any)
		want   string
	}{
		"a long-running service with no policy": {func(s map[string]any) { delete(service(s, "valkey"), "restart") },
			`valkey: long-running service with restart ""`},
		"a long-running service with restart no": {func(s map[string]any) { service(s, "go-api")["restart"] = "no" },
			`go-api: long-running service with restart "no"`},
		"a one-shot service that restarts": {func(s map[string]any) { service(s, "migrate")["restart"] = longRunningRestart },
			`migrate: one-shot service with restart "unless-stopped"`},
		"a new service with no policy": {func(s map[string]any) { s["new-daemon"] = map[string]any{"image": "busybox"} },
			`new-daemon: long-running service with restart ""`},
		"a completion wait on an unlisted service": {func(s map[string]any) {
			s["new-init"] = map[string]any{"image": "busybox", "restart": "no"}
			service(s, "go-api")["depends_on"] = map[string]any{"new-init": map[string]any{"condition": "service_completed_successfully"}}
		}, `go-api: waits for new-init to complete`},
		"a listed one-shot service that is gone": {func(s map[string]any) { delete(s, "acr-migrate") },
			`acr-migrate: listed as one-shot but not in the file`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			findings, err := restartPolicyFindings(plant(t, c.change))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(findings, "\n"), c.want) {
				t.Errorf("findings %q do not name %q", findings, c.want)
			}
		})
	}
}
