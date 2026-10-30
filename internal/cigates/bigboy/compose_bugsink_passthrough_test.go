package bigboy_test

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// composeBugsinkPassThrough returns a finding for every way the file hands a bugsink credential to a service by NAME
// rather than by interpolation: an environment key named for one of the three credentials (mapping form with any value,
// null included, or list form NAME / NAME=value) in a service other than bugsink, or in bugsink itself when bugsink loads
// its own env file; and any service other than bugsink whose env_file names bugsink's env file.
func composeBugsinkPassThrough(services doc) []string {
	isCredential := func(name string) bool {
		for _, v := range composeBugsinkVars {
			if name == v {
				return true
			}
		}
		return false
	}
	isOwnFile := func(p string) bool { return path.Clean(p) == path.Clean(composeBugsinkEnvFile) }
	var findings []string
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		svc := mp(services[name])
		ownFile := false
		ef := svc["env_file"]
		if _, one := ef.(string); one {
			ef = []any{ef}
		}
		for _, e := range list(ef) {
			p := str(e)
			if m := mp(e); m != nil {
				p = str(m["path"])
			}
			if isOwnFile(p) {
				ownFile = true
			}
		}
		if ownFile && name != "bugsink" {
			findings = append(findings, fmt.Sprintf("service %s loads bugsink's env file %s", name, composeBugsinkEnvFile))
		}
		if name == "bugsink" && !ownFile {
			continue // the old form: bugsink's credentials are interpolated values, checked by composeBugsinkShape
		}
		var keys []string
		switch e := svc["environment"].(type) {
		case map[string]any:
			for k := range e {
				keys = append(keys, k)
			}
		case []any:
			for _, x := range e {
				k, _, _ := strings.Cut(str(x), "=")
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			if isCredential(k) {
				findings = append(findings, fmt.Sprintf("service %s gets the bugsink credential %s by name from the compose environment", name, k))
			}
		}
	}
	return findings
}

func TestComposeBugsinkPassThroughCells(t *testing.T) {
	own := "  bugsink:\n    env_file:\n    - path: " + composeBugsinkEnvFile + "\n      required: true\n    environment:\n      PORT: '8000'\n"
	for _, c := range []struct {
		name, web string
		ok        bool
	}{
		{"web without bugsink names", "    environment:\n      SENTRY_DSN: x\n", true},
		{"web mapping key, null value", "    environment:\n      BUGSINK_SECRET_KEY: null\n", false},
		{"web mapping key, no value", "    environment:\n      BUGSINK_SECRET_KEY:\n", false},
		{"web mapping key, empty value", "    environment:\n      BUGSINK_DB_PASSWORD: ''\n", false},
		{"web mapping key, a value", "    environment:\n      BUGSINK_CREATE_SUPERUSER: a@b.c:x\n", false},
		{"web list entry, name only", "    environment:\n    - BUGSINK_SECRET_KEY\n", false},
		{"web list entry, NAME=", "    environment:\n    - BUGSINK_DB_PASSWORD=\n", false},
		{"web list entry, NAME=value", "    environment:\n    - BUGSINK_CREATE_SUPERUSER=a@b.c:x\n", false},
		{"web BUGSINK_BASE_URL key (not a credential)", "    environment:\n      BUGSINK_BASE_URL: null\n", true},
		{"web a longer name (another variable)", "    environment:\n    - BUGSINK_SECRET_KEY_OLD\n", true},
		{"web env_file string form", "    env_file: " + composeBugsinkEnvFile + "\n", false},
		{"web env_file list of strings", "    env_file:\n    - ./ops/.env\n    - " + composeBugsinkEnvFile + "\n", false},
		{"web env_file path form", "    env_file:\n    - path: " + composeBugsinkEnvFile + "\n      required: false\n", false},
		{"web env_file path without ./", "    env_file:\n    - path: .bugsink-dev/bugsink.env\n", false},
		{"web env_file path with a dot segment", "    env_file:\n    - ./x/../.bugsink-dev/bugsink.env\n", false},
		{"web env_file ops/.env only", "    env_file:\n    - path: ./ops/.env\n      required: true\n", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := composeBugsinkPassThrough(mp(composeYAML(t, "services:\n"+own+"  web:\n"+c.web)["services"]))
			if c.ok != (len(got) == 0) {
				t.Fatalf("want accepted=%v, findings %v", c.ok, got)
			}
		})
	}
	for _, c := range []struct {
		name, bugsink string
		ok            bool
	}{
		{"bugsink own env file, no credential keys", own, true},
		{"bugsink own env file plus a credential key", own + "      BUGSINK_SECRET_KEY: null\n", false},
		{"bugsink old form (no own file) with interpolated values", "  bugsink:\n    environment:\n      SECRET_KEY: ${BUGSINK_SECRET_KEY:?set}\n", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := composeBugsinkPassThrough(mp(composeYAML(t, "services:\n"+c.bugsink)["services"]))
			if c.ok != (len(got) == 0) {
				t.Fatalf("want accepted=%v, findings %v", c.ok, got)
			}
			if !c.ok && !strings.Contains(strings.Join(got, "\n"), "bugsink") {
				t.Fatalf("findings %v do not name the service", got)
			}
		})
	}
}

func composeYAML(t *testing.T, text string) doc {
	t.Helper()
	var v doc
	if err := yaml.Unmarshal([]byte(text), &v); err != nil {
		t.Fatalf("cell YAML: %v", err)
	}
	return v
}
