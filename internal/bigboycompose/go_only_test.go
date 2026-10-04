package bigboycompose_test

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-8361: the bigboy stack is Go-only. The base compose file of that stack is a host file, so the tracked
// overlays and scripts are where the Python api can come back: an image line, a service in the cut's `up` list,
// a side file in the chain, a one-off in the api container's network namespace, a script that names the api
// container. goOnlyFindings reads the tracked files and returns one finding per defect.
// TestBigboyStackIsGoOnly runs it on the checked-in files; TestBigboyGoOnlyCheckSeesEachDefect plants one
// defect at a time in the text of a checked-in file and requires the finding that names it.

const (
	pythonAPIImage      = "dev-hops-api"
	pythonAPIContainer  = "dev-health-api-1"
	retiredAPIProfile   = "retired-python-api"
	dhoImageRepository  = "ghcr.io/full-chaos/dev-health-go-dho"
	imagesOverlayName   = "compose.bigboy.images.yml"
	smokeOverlayName    = "compose.bigboy.smoke.yml"
	cutScriptName       = "bigboy-cut.sh"
	proveScriptName     = "bigboy-graphql-prove.sh"
	logChecksScriptName = "bigboy-log-checks.sh"
	smokeScriptName     = "web-path-smoke.sh"
)

// goOnlyFiles are the tracked files the check reads, by name.
var goOnlyFiles = []string{imagesOverlayName, smokeOverlayName, cutScriptName, proveScriptName, logChecksScriptName, smokeScriptName}

type bigboyService struct {
	Image       string               `yaml:"image"`
	Profiles    []string             `yaml:"profiles"`
	NetworkMode string               `yaml:"network_mode"`
	Networks    []string             `yaml:"networks"`
	DependsOn   map[string]yaml.Node `yaml:"depends_on"`
	Other       map[string]yaml.Node `yaml:",inline"`
}

func bigboyServices(text string) (map[string]bigboyService, error) {
	var doc struct {
		Services map[string]bigboyService `yaml:"services"`
	}
	// compose's `!override` and `!reset` tags are read as the value they wrap.
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		return nil, err
	}
	if len(doc.Services) == 0 {
		return nil, fmt.Errorf("no services")
	}
	return doc.Services, nil
}

// codeLines are the lines of a shell script that are not comments.
func codeLines(text string) []string {
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") {
			lines = append(lines, line)
		}
	}
	return lines
}

var planesUpLine = regexp.MustCompile(`up -d --no-deps --no-build ([a-z0-9 -]+?) > \$REC/up\.out`)

// goOnlyFindings returns what in the tracked bigboy files still needs the Python api, one finding per defect.
func goOnlyFindings(files map[string]string) ([]string, error) {
	var findings []string
	found := func(format string, args ...any) { findings = append(findings, fmt.Sprintf(format, args...)) }

	for _, overlay := range []string{imagesOverlayName, smokeOverlayName} {
		services, err := bigboyServices(files[overlay])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", overlay, err)
		}
		names := make([]string, 0, len(services))
		for name := range services {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			service := services[name]
			if strings.Contains(service.Image, pythonAPIImage) {
				found("%s: %s runs the Python api image", overlay, name)
			}
			if service.NetworkMode == "service:api" {
				found("%s: %s shares the network namespace of the Python api", overlay, name)
			}
			if _, waits := service.DependsOn["api"]; waits {
				found("%s: %s waits for the Python api", overlay, name)
			}
		}
		if overlay != imagesOverlayName {
			continue
		}
		api, ok := services["api"]
		switch {
		case !ok:
			found("%s: the `api` service of the base file is not retired: the overlay does not name it", overlay)
		case len(api.Profiles) != 1 || api.Profiles[0] != retiredAPIProfile || api.Image != "" || len(api.Other) != 0:
			found("%s: `api` must be the profile %s and nothing else, so `up` never starts it", overlay, retiredAPIProfile)
		}
		if migrate := services["migrate"]; !strings.HasPrefix(migrate.Image, dhoImageRepository+"@sha256:") {
			found("%s: migrate runs %q, want the dho image by digest", overlay, migrate.Image)
		}
		if web := services["web"]; len(web.DependsOn) == 0 {
			found("%s: web states no depends_on, so the base file's wait for the Python api stays", overlay)
		}
	}

	cut := files[cutScriptName]
	chain := ""
	for _, line := range codeLines(cut) {
		if strings.HasPrefix(line, "export COMPOSE_FILE=") {
			chain = line
		}
	}
	switch {
	case chain == "":
		return nil, fmt.Errorf("%s exports no COMPOSE_FILE: nothing was checked", cutScriptName)
	case strings.Contains(chain, "metrics-api"):
		found("%s: the compose chain holds the metrics-api side file, a Python api service", cutScriptName)
	}
	up := planesUpLine.FindStringSubmatch(strings.Join(codeLines(cut), "\n"))
	if up == nil {
		return nil, fmt.Errorf("%s has no `up -d --no-deps --no-build <planes>` line: nothing was checked", cutScriptName)
	}
	if got := strings.Fields(up[1]); strings.Join(got, " ") != "query-api go-api web" {
		found("%s: the planes `up` list is %v, want [query-api go-api web]: the Python api is not recreated", cutScriptName, got)
	}
	for _, script := range []string{cutScriptName, proveScriptName, logChecksScriptName, smokeScriptName} {
		for _, line := range codeLines(files[script]) {
			switch {
			case strings.Contains(line, pythonAPIContainer):
				found("%s: a line names the Python api container %s", script, pythonAPIContainer)
			case strings.Contains(line, "metrics-api"):
				found("%s: a line names metrics-api", script)
			case strings.Contains(line, "pass-bigboy") || strings.Contains(line, "run-rest-bigboy") || strings.Contains(line, "bootstrap-admin-proof"):
				found("%s: a line runs a two-plane leg, which needs the Python api container", script)
			case strings.Contains(line, "localhost:8000"):
				found("%s: a line names the Python edge localhost:8000", script)
			}
		}
	}
	if !strings.Contains(strings.Join(codeLines(files[proveScriptName]), "\n"), "-go-edge -edge-url http://traefik:3000/graphql") {
		found("%s: the prover is not handed the routed /graphql in Go-edge mode", proveScriptName)
	}
	sort.Strings(findings)
	return findings, nil
}

func checkedInGoOnlyFiles(t *testing.T) map[string]string {
	t.Helper()
	files := map[string]string{}
	for _, name := range goOnlyFiles {
		files[name] = read(t, filepath.Join(toolsDir(t), name))
	}
	return files
}

func TestBigboyStackIsGoOnly(t *testing.T) {
	findings, err := goOnlyFindings(checkedInGoOnlyFiles(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, finding := range findings {
		t.Error(finding)
	}
}

func TestBigboyGoOnlyCheckSeesEachDefect(t *testing.T) {
	clean := checkedInGoOnlyFiles(t)
	frozen := "ghcr.io/full-chaos/dev-hops-api@sha256:" + strings.Repeat("a", 64)
	for _, plant := range []struct {
		name, file, old, new, want string
	}{
		{
			name: "the Python api image back on api",
			file: imagesOverlayName,
			old:  "  api:\n    profiles:\n      - retired-python-api\n",
			new:  "  api:\n    image: " + frozen + "\n",
			want: "api runs the Python api image",
		},
		{
			name: "api retired and still given an entrypoint",
			file: imagesOverlayName,
			old:  "  api:\n    profiles:\n      - retired-python-api\n",
			new:  "  api:\n    profiles:\n      - retired-python-api\n    entrypoint: [\"python\"]\n",
			want: "`api` must be the profile retired-python-api and nothing else",
		},
		{
			name: "api not named by the overlay",
			file: imagesOverlayName,
			old:  "  api:\n    profiles:\n      - retired-python-api\n",
			new:  "",
			want: "the `api` service of the base file is not retired",
		},
		{
			name: "migrate from the Python api image",
			file: imagesOverlayName,
			old:  "  migrate:\n    image: " + dhoImageRepository,
			new:  "  migrate:\n    image: ghcr.io/full-chaos/dev-hops-api",
			want: "migrate runs the Python api image",
		},
		{
			name: "venue-prove in the network namespace of the api",
			file: imagesOverlayName,
			old:  "    networks:\n      - dev-health\n      - dho-query-internal\n    environment:\n",
			new:  "    network_mode: \"service:api\"\n    environment:\n",
			want: "venue-prove shares the network namespace of the Python api",
		},
		{
			name: "web waits for the api",
			file: imagesOverlayName,
			old:  "    depends_on: !override\n      valkey:\n",
			new:  "    depends_on: !override\n      api:\n        condition: service_healthy\n      valkey:\n",
			want: "web waits for the Python api",
		},
		{
			name: "web keeps the base file's wait",
			file: imagesOverlayName,
			old:  "    depends_on: !override\n      valkey:\n        condition: service_healthy\n      bugsink:\n        condition: service_started\n",
			new:  "",
			want: "web states no depends_on",
		},
		{
			name: "the smoke runner on the Python api image",
			file: imagesOverlayName,
			old:  "  web-smoke:\n    image: " + dhoImageRepository,
			new:  "  web-smoke:\n    image: ghcr.io/full-chaos/dev-hops-api",
			want: "web-smoke runs the Python api image",
		},
		{
			name: "api back in the cut's up list",
			file: cutScriptName,
			old:  "up -d --no-deps --no-build query-api go-api web ",
			new:  "up -d --no-deps --no-build api query-api go-api web ",
			want: "the planes `up` list is [api query-api go-api web]",
		},
		{
			name: "the metrics-api side file back in the chain",
			file: cutScriptName,
			old:  "export COMPOSE_FILE=compose.yml:compose/compose.go.workers.yml:",
			new:  "export COMPOSE_FILE=compose.yml:compose/compose.go.workers.yml:compose/compose.metrics-api.local.yml:",
			want: "the compose chain holds the metrics-api side file",
		},
		{
			name: "a two-plane leg back in the cut",
			file: cutScriptName,
			old:  "echo \"cut done $(date -u +%T)\"\n",
			new:  "timeout 900 bash run-rest-bigboy.sh > out-rest.txt 2>&1; st rest $?\necho \"cut done $(date -u +%T)\"\n",
			want: "bigboy-cut.sh: a line runs a two-plane leg",
		},
		{
			name: "the Python edge back in the prove harness",
			file: proveScriptName,
			old:  "  \"\"|--go-edge) EDGE_ARGS=\"-go-edge -edge-url http://traefik:3000/graphql\"",
			new:  "  \"\") EDGE_ARGS=\"-edge-url http://localhost:8000/graphql\"",
			want: "bigboy-graphql-prove.sh: a line names the Python edge localhost:8000",
		},
		{
			name: "the api container back in the log checks",
			file: logChecksScriptName,
			old:  "codes=$(for c in $NAMES;",
			new:  "a=$(ip dev-health-api-1); [ -n \"$a\" ] && chk dev-health-api-1 $a:8000/ready\ncodes=$(for c in $NAMES;",
			want: "bigboy-log-checks.sh: a line names the Python api container",
		},
		{
			name: "the metrics-api side file back in the smoke chain",
			file: smokeScriptName,
			old:  "  -f .remember/lanes/team-lead/reconciler-sweep-override.yml \\\n",
			new:  "  -f compose/compose.metrics-api.local.yml \\\n  -f .remember/lanes/team-lead/reconciler-sweep-override.yml \\\n",
			want: "web-path-smoke.sh: a line names metrics-api",
		},
	} {
		t.Run(plant.name, func(t *testing.T) {
			if strings.Count(clean[plant.file], plant.old) != 1 {
				t.Fatalf("the text to change is in %s %d times, want once: the plant is not valid", plant.file, strings.Count(clean[plant.file], plant.old))
			}
			planted := map[string]string{}
			for name, text := range clean {
				planted[name] = text
			}
			planted[plant.file] = strings.Replace(clean[plant.file], plant.old, plant.new, 1)
			findings, err := goOnlyFindings(planted)
			if err != nil {
				t.Fatalf("the planted files do not read: %v", err)
			}
			for _, finding := range findings {
				if strings.Contains(finding, plant.want) {
					return
				}
			}
			t.Fatalf("the check did not see the defect: want a finding with %q, got %q", plant.want, findings)
		})
	}
}
