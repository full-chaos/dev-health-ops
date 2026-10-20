package bigboy_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The deleted Python guards ran `docker compose config`, which rejects a file that breaks the Compose schema. There is no
// docker here, so the same check runs in-process through the Compose spec loader (composeSchemaError): a wrong type, an
// unknown key or a dangling service reference fails the guard whichever service or profile it sits in.

var composeRequiredVar = regexp.MustCompile(`\$\{(\w+):\?`)

func composeSchemaBigboy(g *guard) {
	composeMust(g, composeIn{})
	for _, p := range [][]string{{"acr"}, {"acr-venue"}, {"venue"}, {"retired-python-api"}} {
		composeMust(g, composeIn{profiles: p})
	}
}

func composeSchemaRoot(g *guard) {
	text := g.read("compose.yml")
	env := composeEnv(nil)
	for _, m := range composeRequiredVar.FindAllStringSubmatch(text, -1) {
		env[m[1]] = "x"
	}
	if msg := composeSchemaError(text, env); msg != "" {
		g.failf("compose.yml breaks the Compose schema: %s", msg)
	}
}

// inService edits the first occurrence of old inside the named service block of the bigboy file.
func inService(service, old, repl string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		p := filepath.Join(root, filepath.FromSlash(composeFile))
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		s := string(b)
		head := "\n  " + service + ":\n"
		i := strings.Index(s, head)
		if i < 0 {
			t.Fatalf("service %s not found: the defect did not land", service)
		}
		j := strings.Index(s[i:], old)
		if j < 0 {
			t.Fatalf("service %s has no %q: the defect did not land", service, old)
		}
		at := i + j
		if err := os.WriteFile(p, []byte(s[:at]+repl+s[at+len(old):]), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestComposeSchemaReal(t *testing.T) {
	runReal(t,
		check{"bigboy-file-validates-under-every-profile", composeSchemaBigboy},
		check{"root-compose-validates", composeSchemaRoot},
	)
}

func TestComposeSchemaPlanted(t *testing.T) {
	bb := []string{composeFile, composePins}
	runPlanted(t, []planted{
		{name: "web-restart-is-a-boolean", copy: bb, check: composeSchemaBigboy, want: "restart",
			mutate: inService("web", "    restart: unless-stopped", "    restart: true")},
		{name: "inactive-profile-service-restart-is-a-boolean", copy: bb, check: composeSchemaBigboy, want: "restart",
			mutate: inService("api", "    restart: unless-stopped", "    restart: true")},
		{name: "unknown-service-key", copy: bb, check: composeSchemaBigboy, want: "frobnicate",
			mutate: inService("web", "    restart: unless-stopped", "    restart: unless-stopped\n    frobnicate: 1")},
		{name: "healthcheck-retries-is-not-a-number", copy: bb, check: composeSchemaBigboy, want: "retries",
			mutate: mut(composeFile, "      retries: 3", "      retries: many")},
		{name: "depends-on-names-a-service-that-does-not-exist", copy: bb, check: composeSchemaBigboy, want: "nosuchservice",
			mutate: inService("web", "      bugsink:\n        condition: service_started", "      nosuchservice:\n        condition: service_started")},
		{name: "top-level-key-is-unknown", copy: bb, check: composeSchemaBigboy, want: "frobnicate",
			mutate: mut(composeFile, "\nnetworks:\n", "\nfrobnicate: {}\nnetworks:\n")},
		{name: "root-compose-restart-is-a-boolean", copy: []string{"compose.yml"}, check: composeSchemaRoot, want: "breaks the Compose schema",
			mutate: mut("compose.yml", "restart: unless-stopped", "restart: true")},
	})
}
