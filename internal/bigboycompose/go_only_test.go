package bigboycompose_test

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-8361: the bigboy stack is Go-only. The Python api can come back through the one compose file
// (ci/bigboy/compose.bigboy.yml, CHAOS-9962): an image line, a service in the cut's `up` list, a one-off in the api
// container's network namespace, a pin that names the Python image, or a script that names the api container.
// goOnlyFindings reads the tracked files and returns one finding per defect. TestBigboyStackIsGoOnly runs it on the
// checked-in files; TestBigboyGoOnlyCheckSeesEachDefect plants one defect at a time in the checked-in files (the
// compose file is changed as parsed data and written back) and requires the finding that names it.

const (
	pythonAPIImage      = "dev-hops-api"
	pythonAPIContainer  = "dev-health-api-1"
	dhoImageRepository  = "ghcr.io/full-chaos/dev-health-go-dho"
	pinsName            = "images.env"
	cutScriptName       = "bigboy-cut.sh"
	proveScriptName     = "bigboy-graphql-prove.sh"
	logChecksScriptName = "bigboy-log-checks.sh"
	smokeScriptName     = "web-path-smoke.sh"
)

// goOnlyFiles are the tracked files the check reads, by name.
var goOnlyFiles = []string{composeName, pinsName, cutScriptName, proveScriptName, logChecksScriptName, smokeScriptName}

type bigboyService struct {
	Image       string               `yaml:"image"`
	Profiles    []string             `yaml:"profiles"`
	NetworkMode string               `yaml:"network_mode"`
	DependsOn   map[string]yaml.Node `yaml:"depends_on"`
	Other       map[string]yaml.Node `yaml:",inline"`
}

func bigboyServices(text string) (map[string]bigboyService, error) {
	var doc struct {
		Services map[string]bigboyService `yaml:"services"`
	}
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

var planesUpLine = regexp.MustCompile(`up -d --no-deps --no-build ([a-z0-9 -]+?) > "?\$REC"?/up\.out`)

// goOnlyFindings returns what in the tracked bigboy files still needs the Python api, one finding per defect.
func goOnlyFindings(files map[string]string) ([]string, error) {
	var findings []string
	found := func(format string, args ...any) { findings = append(findings, fmt.Sprintf(format, args...)) }

	services, err := bigboyServices(files[composeName])
	if err != nil {
		return nil, fmt.Errorf("%s: %w", composeName, err)
	}
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		service := services[name]
		if strings.Contains(service.Image, pythonAPIImage) {
			found("%s: %s runs the Python api image", composeName, name)
		}
		if service.NetworkMode == "service:api" {
			found("%s: %s shares the network namespace of the Python api", composeName, name)
		}
		if _, waits := service.DependsOn["api"]; waits && name != "api" {
			found("%s: %s waits for the Python api", composeName, name)
		}
	}
	for _, python := range []string{"api", "billing-edge"} {
		if _, back := services[python]; back {
			found("%s: the Python `%s` service is back", composeName, python)
		}
	}
	if migrate := services["migrate"]; !strings.HasPrefix(migrate.Image, "${BIGBOY_DHO_IMAGE:") {
		found("%s: migrate runs %q, want the dho image pin ${BIGBOY_DHO_IMAGE}", composeName, migrate.Image)
	}
	if web := services["web"]; len(web.DependsOn) == 0 {
		found("%s: web states no depends_on, so the Python api wait could come back", composeName)
	}
	for _, line := range codeLines(files[pinsName]) {
		if strings.Contains(line, pythonAPIImage) {
			found("%s: a pin names the Python api image", pinsName)
		}
		if strings.HasPrefix(line, "BIGBOY_DHO_IMAGE=") && !strings.HasPrefix(line, "BIGBOY_DHO_IMAGE="+dhoImageRepository+"@sha256:") {
			found("%s: BIGBOY_DHO_IMAGE is not the dho image by digest", pinsName)
		}
	}

	cut := files[cutScriptName]
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
		files[name] = read(t, toolsPath(t, name))
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

// plantCompose changes the parsed compose file and writes it back.
func plantCompose(t *testing.T, text string, change func(services map[string]map[string]any)) string {
	t.Helper()
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		t.Fatal(err)
	}
	services := map[string]map[string]any{}
	for name, raw := range doc["services"].(map[string]any) {
		services[name] = raw.(map[string]any)
	}
	change(services)
	doc["services"] = services
	out, err := yaml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestBigboyGoOnlyCheckSeesEachDefect(t *testing.T) {
	clean := checkedInGoOnlyFiles(t)
	frozen := "ghcr.io/full-chaos/dev-hops-api@sha256:" + strings.Repeat("a", 64)
	for _, plant := range []struct {
		name, file, want string
		compose          func(services map[string]map[string]any)
		old, new         string
	}{
		{name: "the Python api image back on api", file: composeName, want: "api runs the Python api image",
			compose: func(s map[string]map[string]any) { s["api"] = map[string]any{"image": frozen} }},
		{name: "the api service is back", file: composeName, want: "the Python `api` service is back",
			compose: func(s map[string]map[string]any) { s["api"] = map[string]any{"image": "example.test/other:1"} }},
		{name: "the billing-edge service is back", file: composeName, want: "the Python `billing-edge` service is back",
			compose: func(s map[string]map[string]any) { s["billing-edge"] = map[string]any{"image": "example.test/other:1"} }},
		{name: "migrate from the Python api image", file: composeName, want: "migrate runs",
			compose: func(s map[string]map[string]any) { s["migrate"]["image"] = frozen }},
		{name: "venue-prove in the network namespace of the api", file: composeName, want: "venue-prove shares the network namespace of the Python api",
			compose: func(s map[string]map[string]any) { s["venue-prove"]["network_mode"] = "service:api" }},
		{name: "web waits for the api", file: composeName, want: "web waits for the Python api",
			compose: func(s map[string]map[string]any) {
				s["web"]["depends_on"] = map[string]any{"api": map[string]any{"condition": "service_healthy"}}
			}},
		{name: "web keeps no explicit wait", file: composeName, want: "web states no depends_on",
			compose: func(s map[string]map[string]any) { delete(s["web"], "depends_on") }},
		{name: "the smoke runner on the Python api image", file: composeName, want: "web-smoke runs the Python api image",
			compose: func(s map[string]map[string]any) { s["web-smoke"]["image"] = frozen }},
		{name: "the dho pin on the Python api image", file: pinsName, want: "BIGBOY_DHO_IMAGE is not the dho image by digest",
			old: "BIGBOY_DHO_IMAGE=" + dhoImageRepository, new: "BIGBOY_DHO_IMAGE=ghcr.io/full-chaos/dev-hops-api"},
		{name: "api back in the cut's up list", file: cutScriptName, want: "the planes `up` list is [api query-api go-api web]",
			old: "--no-build query-api go-api web ", new: "--no-build api query-api go-api web "},
		{name: "a two-plane leg back in the cut", file: cutScriptName, want: "bigboy-cut.sh: a line runs a two-plane leg",
			old: "echo \"cut done $(date -u +%T)\"\n", new: "timeout 900 bash run-rest-bigboy.sh > out-rest.txt 2>&1; st rest $?\necho \"cut done $(date -u +%T)\"\n"},
		{name: "the Python edge back in the prove harness", file: proveScriptName, want: "bigboy-graphql-prove.sh: a line names the Python edge localhost:8000",
			old: "  \"\"|--go-edge) EDGE_ARGS=\"-go-edge -edge-url http://traefik:3000/graphql\"", new: "  \"\") EDGE_ARGS=\"-edge-url http://localhost:8000/graphql\""},
		{name: "the api container back in the log checks", file: logChecksScriptName, want: "bigboy-log-checks.sh: a line names the Python api container",
			old: "codes=$(for c in $NAMES;", new: "a=$(ip dev-health-api-1); [ -n \"$a\" ] && chk dev-health-api-1 $a:8000/ready\ncodes=$(for c in $NAMES;"},
	} {
		t.Run(plant.name, func(t *testing.T) {
			planted := map[string]string{}
			for name, text := range clean {
				planted[name] = text
			}
			if plant.compose != nil {
				planted[plant.file] = plantCompose(t, clean[plant.file], plant.compose)
			} else {
				if strings.Count(clean[plant.file], plant.old) != 1 {
					t.Fatalf("the text to change is in %s %d times, want once: the plant is not valid", plant.file, strings.Count(clean[plant.file], plant.old))
				}
				planted[plant.file] = strings.Replace(clean[plant.file], plant.old, plant.new, 1)
			}
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
