package bigboy_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	ccfgCompose      = "compose.yml"
	ccfgManifest     = "deploy/go-workers/deployment.json"
	ccfgHelm         = "deploy/helm/dev-health"
	ccfgInitDBs      = "docker/init-extra-dbs.sh"
	ccfgProvisionSQL = "scripts/worker/provision_river_roles.sql"
)

var ccfgComposeFiles = []string{ccfgCompose, ccfgManifest}

var ccfgServiceByProcess = map[string]string{
	"heavy":            "go-worker-heavy",
	"ops":              "go-worker-ops",
	"sync":             "go-worker-sync",
	"sync-provider":    "go-worker-sync-provider",
	"reconciler":       "go-reconciler",
	"scheduler":        "go-scheduler",
	"stream-external":  "go-stream-external",
	"stream-ingest":    "go-stream-ingest",
	"stream-pagerduty": "go-stream-pagerduty",
}

var (
	ccfgComposeDefaultRe = regexp.MustCompile(`^\$\{[A-Za-z_][A-Za-z0-9_]*:-([^}]*)\}$`)
	ccfgRouteSwitchRe    = regexp.MustCompile(`\bWORKER_[A-Z0-9_]+_ENABLED\b`)
)

func ccfgServices(g *guard) doc {
	services := mp(at(g.yamlFile(ccfgCompose), "services"))
	if services == nil {
		g.failf("%s declares no services", ccfgCompose)
	}
	return services
}

func ccfgService(g *guard, services doc, name string) doc {
	svc, ok := services[name].(map[string]any)
	if !ok {
		g.failf("%s has no service %s", ccfgCompose, name)
		return doc{}
	}
	return svc
}

func ccfgEnv(svc doc) doc { return mp(svc["environment"]) }

func ccfgHas(m doc, key string) bool { _, ok := m[key]; return ok }

func ccfgDeps(svc doc) map[string]bool {
	out := map[string]bool{}
	switch d := svc["depends_on"].(type) {
	case map[string]any:
		for k := range d {
			out[k] = true
		}
	case []any:
		for _, k := range d {
			out[str(k)] = true
		}
	}
	return out
}

func ccfgCondition(svc doc, dep string) string {
	return str(at(svc, "depends_on", dep, "condition"))
}

func ccfgWorkerArgs(g *guard, name string, svc doc) map[string]string {
	args := map[string]string{}
	cmd, ok := svc["command"].([]any)
	if !ok {
		g.failf("%s: Go worker command must use list form, got %v", name, svc["command"])
		return args
	}
	if len(cmd) > 0 && str(cmd[0]) == "worker" {
		cmd = cmd[1:]
	}
	for _, item := range cmd {
		k, v, found := strings.Cut(str(item), "=")
		if !found || !strings.HasPrefix(k, "--") {
			g.failf("%s: invalid Go worker argument: %v", name, item)
			continue
		}
		args[k] = v
	}
	return args
}

func ccfgProcesses(g *guard) []doc {
	var out []doc
	for _, p := range list(g.jsonFile(ccfgManifest)["processes"]) {
		out = append(out, mp(p))
	}
	if len(out) == 0 {
		g.failf("%s must declare at least one process", ccfgManifest)
	}
	return out
}

func ccfgProcessByName(g *guard) map[string]doc {
	out := map[string]doc{}
	for _, p := range ccfgProcesses(g) {
		out[str(p["name"])] = p
	}
	return out
}

func ccfgComposeDefault(v any) string {
	s := str(v)
	if m := ccfgComposeDefaultRe.FindStringSubmatch(s); m != nil {
		return m[1]
	}
	return s
}

func ccfgInt(v any) (int, bool) { i, ok := v.(int); return i, ok }

func ccfgCommand(v any) string {
	if l, ok := v.([]any); ok {
		return strings.Join(strs(l), " ")
	}
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func ccfgPlantAfter(t *testing.T, root, rel, anchor, old, repl string) {
	t.Helper()
	p := rootPath(root, rel)
	text := mustRead(t, p)
	a := strings.Index(text, anchor)
	if a < 0 {
		t.Fatalf("plant %s: anchor %q not found, the defect did not land", rel, anchor)
	}
	i := strings.Index(text[a:], old)
	if i < 0 {
		t.Fatalf("plant %s: text %q not found after %q, the defect did not land", rel, old, anchor)
	}
	i += a
	if err := os.WriteFile(p, []byte(text[:i]+repl+text[i+len(old):]), 0o644); err != nil {
		t.Fatal(err)
	}
}

func ccfgMutAfter(rel, anchor, old, repl string) func(*testing.T, string) {
	return func(t *testing.T, root string) { ccfgPlantAfter(t, root, rel, anchor, old, repl) }
}

// A text guard that accepts any one copy needs every copy gone.
func ccfgMutAll(rel, old, repl string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		t.Helper()
		p := rootPath(root, rel)
		text := mustRead(t, p)
		if !strings.Contains(text, old) {
			t.Fatalf("plant %s: text %q not found, the defect did not land", rel, old)
		}
		if err := os.WriteFile(p, []byte(strings.ReplaceAll(text, old, repl)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func ccfgWrite(g *guard, rel, content string, mode os.FileMode) string {
	p := filepath.Join(g.tmp, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		g.failf("mkdir %s: %v", rel, err)
	}
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		g.failf("write %s: %v", rel, err)
	}
	return p
}
