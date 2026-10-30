package bigboy_test

import (
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// composeBugsinkLeaks returns a finding for every way the compose file can hand one of the three bugsink credentials to a
// service other than bugsink, in both forms.
func composeBugsinkLeaks(f doc) []string {
	var out []string
	add := func(format string, a ...any) { out = append(out, fmt.Sprintf(format, a...)) }
	services := mp(f["services"])
	if f["include"] != nil {
		add("the file has a top-level include: a bugsink credential in an included file cannot be checked")
	}
	for _, top := range []string{"secrets", "configs"} {
		for _, name := range composeSortedKeys(mp(f[top])) {
			e := f[top].(map[string]any)[name]
			if k := str(mp(e)["environment"]); composeBugsinkIsCredential(k) {
				add("%s %s takes the bugsink credential %s from the environment", top, name, k)
			}
			if composeBugsinkInterpolates(e) {
				add("%s %s interpolates a bugsink credential", top, name)
			}
			if f := mp(e)["file"]; f != nil && composeBugsinkExposes(str(f)) {
				add("%s %s reads bugsink's env file or a directory holding it: %s", top, name, str(f))
			}
		}
	}
	bugsinkOwn := false
	for _, p := range composeEnvFilePaths(mp(services["bugsink"])["env_file"]) {
		if composeBugsinkIsEnvFile(p) {
			bugsinkOwn = true
		}
	}
	for _, name := range composeSortedKeys(services) {
		chain, errs := composeExtendsChain(services, name)
		if name == "bugsink" {
			if bugsinkOwn {
				for _, k := range composeEnvKeys(mp(services[name])["environment"]) {
					if composeBugsinkIsCredential(k) {
						add("service bugsink loads its own env file and also gets the bugsink credential %s by name from the compose environment", k)
					}
				}
			}
			continue
		}
		for _, e := range errs {
			add("service %s %s", name, e)
		}
		for _, svc := range chain {
			for _, p := range composeEnvFilePaths(svc["env_file"]) {
				if strings.Contains(p, "$") {
					add("service %s has an interpolated env_file path %s, which may name bugsink's env file", name, p)
				} else if composeBugsinkIsEnvFile(p) {
					add("service %s loads bugsink's env file through %s", name, p)
				}
			}
			keys := composeEnvKeys(svc["environment"])
			if b := mp(svc["build"]); b != nil {
				keys = append(keys, composeEnvKeys(b["args"])...)
			}
			for _, k := range keys {
				if composeBugsinkIsCredential(k) {
					add("service %s gets the bugsink credential %s by name from the compose environment", name, k)
				}
			}
			if composeBugsinkInterpolates(svc) {
				add("service %s interpolates a bugsink credential", name)
			}
			for _, v := range list(svc["volumes"]) {
				src := str(mp(v)["source"])
				if sv, ok := v.(string); ok {
					src, _, _ = strings.Cut(sv, ":")
				}
				if composeBugsinkExposes(src) {
					add("service %s mounts bugsink's env file or a directory holding it: %s", name, src)
				}
			}
		}
	}
	return out
}

func composeBugsinkIsCredential(name string) bool {
	for _, v := range composeBugsinkVars {
		if name == v {
			return true
		}
	}
	return false
}

// composeBugsinkIsEnvFile reports whether an env_file path can be bugsink's env file under any project directory: the same
// cleaned relative path, the file name bugsink.env, or a .bugsink-dev directory anywhere in the path (absolute or relative).
func composeBugsinkIsEnvFile(p string) bool {
	c := path.Clean(p)
	if c == path.Clean(composeBugsinkEnvFile) || path.Base(c) == path.Base(composeBugsinkEnvFile) {
		return true
	}
	for _, part := range strings.Split(c, "/") {
		if part == path.Base(path.Dir(composeBugsinkEnvFile)) {
			return true
		}
	}
	return false
}

var composeDefaultRe = regexp.MustCompile(`\$\{[A-Za-z_][A-Za-z0-9_]*(:?-([^}]*))?[^}]*\}|\$[A-Za-z_][A-Za-z0-9_]*`)

// composeBugsinkExposes reports whether a host path (with every variable at its default, or empty) is bugsink's env file, a
// directory holding it, the project directory, or an absolute ancestor of the documented one (/home/ubuntu/devhealth).
func composeBugsinkExposes(src string) bool {
	if src == "" {
		return false
	}
	p := composeDefaultRe.ReplaceAllString(src, "$2")
	if p == "" {
		return false // a source that is only an empty-default variable is set at run time; it cannot be judged here
	}
	c := path.Clean(p)
	if composeBugsinkIsEnvFile(c) || c == "." {
		return true
	}
	own := "/home/ubuntu/devhealth/" + path.Clean(composeBugsinkEnvFile)
	return path.IsAbs(c) && (c == "/" || strings.HasPrefix(own, c+"/"))
}

func composeBugsinkInterpolates(v any) bool {
	for _, s := range composeScalars(v) {
		for _, n := range composeInterpolatedNames(s) {
			if composeBugsinkIsCredential(n) {
				return true
			}
		}
	}
	return false
}

// composeEnvFilePaths returns the paths of an env_file value in its string, list-of-strings and list-of-{path} forms.
func composeEnvFilePaths(v any) []string {
	if s, ok := v.(string); ok {
		return []string{s}
	}
	var out []string
	for _, e := range list(v) {
		if m := mp(e); m != nil {
			out = append(out, str(m["path"]))
		} else {
			out = append(out, str(e))
		}
	}
	return out
}

// composeEnvKeys returns the variable names of an environment or build-args value in its mapping and list (NAME, NAME=x) forms.
func composeEnvKeys(v any) []string {
	var out []string
	switch e := v.(type) {
	case map[string]any:
		out = composeSortedKeys(e)
	case []any:
		for _, x := range e {
			k, _, _ := strings.Cut(str(x), "=")
			out = append(out, k)
		}
	}
	return out
}

// composeExtendsChain returns the service and every service it extends in this file, and an error for an extends that names bugsink, another file, an unknown service, or loops.
func composeExtendsChain(services doc, name string) ([]doc, []string) {
	var chain []doc
	var errs []string
	seen := map[string]bool{}
	for cur := name; cur != ""; {
		if seen[cur] {
			errs = append(errs, "has an extends loop through "+cur+", so its bugsink credentials cannot be checked")
			break
		}
		seen[cur] = true
		svc := mp(services[cur])
		if svc == nil {
			errs = append(errs, "extends an unknown service "+cur+", so its bugsink credentials cannot be checked")
			break
		}
		chain = append(chain, svc) // a YAML merge key (<<) is already resolved by the YAML decoder (cells: merge key copies bugsink)
		next := ""
		switch e := svc["extends"].(type) {
		case string:
			next = e
		case map[string]any:
			if e["file"] != nil {
				errs = append(errs, "extends a service in another file, so its bugsink credentials cannot be checked")
				break
			}
			next = str(e["service"])
		}
		if next == "bugsink" {
			errs = append(errs, "extends bugsink")
			break
		}
		cur = next
	}
	return chain, errs
}

func composeSortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestComposeBugsinkLeaksCells(t *testing.T) {
	newBugsink := "  bugsink:\n    env_file:\n    - path: " + composeBugsinkEnvFile + "\n      required: true\n    environment:\n      PORT: '8000'\n"
	oldBugsink := "  bugsink:\n    environment:\n      DATABASE_URL: postgresql://devhealth:${BUGSINK_DB_PASSWORD:?set}@postgres:5432/bugsink\n      SECRET_KEY: ${BUGSINK_SECRET_KEY:?set}\n      CREATE_SUPERUSER: ${BUGSINK_CREATE_SUPERUSER:?set}\n"
	type cell struct {
		name, text string
		ok         bool
	}
	both := func(name, rest string, ok bool) []cell {
		return []cell{
			{"new form: " + name, "services:\n" + newBugsink + rest, ok},
			{"old form: " + name, "services:\n" + oldBugsink + rest, ok},
		}
	}
	var cells []cell
	add := func(c ...cell) { cells = append(cells, c...) }
	add(both("bugsink alone", "", true)...)
	add(both("web with its own env", "  web:\n    environment:\n      SENTRY_DSN: x\n    env_file:\n    - ./ops/.env\n", true)...)
	add(both("web key BUGSINK_BASE_URL", "  web:\n    environment:\n      BUGSINK_BASE_URL: null\n", true)...)
	add(both("web extends a service without credentials", "  base:\n    environment:\n      A: b\n  web:\n    extends:\n      service: base\n", true)...)
	add(both("a shared anchor without credentials", "  web:\n    environment: &shared\n      A: b\n  riverui:\n    environment: *shared\n", true)...)
	add(both("secrets from a file", "  web:\n    secrets:\n    - s\nsecrets:\n  s:\n    file: ./s.txt\n", true)...)
	add(cell{"old form: an unreferenced extension field", "x-note: ${BUGSINK_DB_PASSWORD}\nservices:\n" + oldBugsink, true})
	// interpolation outside bugsink
	add(both("web environment value ${VAR}", "  web:\n    environment:\n      LEAK: ${BUGSINK_DB_PASSWORD}\n", false)...)
	add(both("web environment value $VAR", "  web:\n    environment:\n    - LEAK=$BUGSINK_SECRET_KEY\n", false)...)
	add(both("web command", "  web:\n    command: [\"sh\", \"-c\", \"echo ${BUGSINK_SECRET_KEY}\"]\n", false)...)
	add(both("web label", "  web:\n    labels:\n      x: ${BUGSINK_CREATE_SUPERUSER}\n", false)...)
	add(both("web build arg value", "  web:\n    build:\n      context: .\n      args:\n        A: ${BUGSINK_DB_PASSWORD}\n", false)...)
	// pass-through by name outside bugsink
	add(both("web environment key null", "  web:\n    environment:\n      BUGSINK_SECRET_KEY: null\n", false)...)
	add(both("web environment list name", "  web:\n    environment:\n    - BUGSINK_DB_PASSWORD\n", false)...)
	add(both("web build arg list name", "  web:\n    build:\n      context: .\n      args:\n      - BUGSINK_SECRET_KEY\n", false)...)
	add(both("web build arg map key", "  web:\n    build:\n      context: .\n      args:\n        BUGSINK_CREATE_SUPERUSER: null\n", false)...)
	add(both("a profiled service", "  debug:\n    profiles: [debug]\n    environment:\n    - BUGSINK_DB_PASSWORD\n", false)...)
	// bugsink's env file outside bugsink
	for _, p := range []string{"./.bugsink-dev/bugsink.env", ".bugsink-dev/bugsink.env", "/home/ubuntu/devhealth/.bugsink-dev/bugsink.env",
		"/srv/root/.bugsink-dev/bugsink.env", "../devhealth/.bugsink-dev/bugsink.env", "./x/../.bugsink-dev/bugsink.env",
		"/etc/bugsink.env", "${BIGBOY_ROOT}/.bugsink-dev/bugsink.env", "$HOME/other.env"} {
		add(both("web env_file "+p, "  web:\n    env_file: "+p+"\n", false)...)
		add(both("web env_file path: "+p+" required false", "  web:\n    env_file:\n    - ./ops/.env\n    - path: "+p+"\n      required: false\n", false)...)
	}
	// YAML anchors, merge keys, extends, include, top-level secrets and configs
	add(cell{"new form: env_file anchor reused by web", "services:\n  bugsink:\n    env_file: &bf\n    - path: " + composeBugsinkEnvFile + "\n      required: true\n  web:\n    env_file: *bf\n", false})
	add(cell{"new form: merge key copies bugsink into web", "services:\n  bugsink: &b\n    env_file:\n    - path: " + composeBugsinkEnvFile + "\n      required: true\n  web:\n    <<: *b\n    image: x\n", false})
	add(cell{"old form: merge key copies bugsink into web", "services:\n  bugsink: &b\n    environment:\n      SECRET_KEY: ${BUGSINK_SECRET_KEY:?set}\n  web:\n    <<: *b\n    image: x\n", false})
	add(cell{"new form: an extension-field anchor with a credential key", "x-creds: &c\n  BUGSINK_SECRET_KEY: null\nservices:\n" + newBugsink + "  web:\n    environment: *c\n", false},
		cell{"old form: an extension-field anchor with a credential key", "x-creds: &c\n  BUGSINK_SECRET_KEY: null\nservices:\n" + oldBugsink + "  web:\n    environment: *c\n", false})
	add(both("web extends bugsink", "  web:\n    extends:\n      service: bugsink\n", false)...)
	add(both("web extends a service that extends bugsink", "  mid:\n    extends:\n      service: bugsink\n  web:\n    extends:\n      service: mid\n", false)...)
	add(both("web extends a service with a credential key", "  mid:\n    environment:\n    - BUGSINK_SECRET_KEY\n  web:\n    extends:\n      service: mid\n", false)...)
	add(both("web extends a service in another file", "  web:\n    extends:\n      file: other.yml\n      service: x\n", false)...)
	add(both("web extends in the string form", "  web:\n    extends: bugsink\n", false)...)
	add(both("web extends an unknown service", "  web:\n    extends:\n      service: ghost\n", false)...)
	add(both("two services extend each other", "  a:\n    extends: b\n  b:\n    extends: a\n", false)...)
	add(both("a top-level include", "include:\n- other.yml\n", false)...)
	add(both("a top-level secret from the environment", "secrets:\n  s:\n    environment: BUGSINK_DB_PASSWORD\n", false)...)
	add(both("a top-level config from the environment", "configs:\n  c:\n    environment: BUGSINK_SECRET_KEY\n", false)...)
	add(both("a top-level config content", "configs:\n  c:\n    content: ${BUGSINK_SECRET_KEY}\n", false)...)
	add(cell{"new form: bugsink with a credential key", "services:\n" + newBugsink + "      BUGSINK_SECRET_KEY: null\n", false})
	// bugsink's env file reached as a mounted file
	add(both("web binds an unrelated host file", "  web:\n    volumes:\n    - ./_records:/records\n    - type: bind\n      source: ${OPS:-/home/ubuntu/devhealth/ops-bigboy}/x.xml\n      target: /x.xml\n", true)...)
	add(both("web binds a source that is only an empty-default variable", "  web:\n    volumes:\n    - type: bind\n      source: ${SMOKE_DIR:-}\n      target: /smoke\n", true)...)
	add(both("web binds a secret file that is not bugsink's", "  web:\n    secrets:\n    - s\nsecrets:\n  s:\n    file: ${S_FILE:-/var/lib/x/secret}\n", true)...)
	for _, v := range []struct{ name, yaml string }{
		{"the env file, short form", "    volumes:\n    - ./.bugsink-dev/bugsink.env:/run/b.env:ro\n"},
		{"the env file directory, short form", "    volumes:\n    - ./.bugsink-dev:/run/b\n"},
		{"the env file directory, long form, absolute", "    volumes:\n    - type: bind\n      source: /home/ubuntu/devhealth/.bugsink-dev\n      target: /run/b\n"},
		{"the env file, long form, default of a variable", "    volumes:\n    - type: bind\n      source: ${B:-./.bugsink-dev}/bugsink.env\n      target: /run/b.env\n"},
		{"the project directory", "    volumes:\n    - .:/app\n"},
		{"the documented project root", "    volumes:\n    - /home/ubuntu/devhealth:/root-dir\n"},
		{"a parent of the project root", "    volumes:\n    - /home/ubuntu:/home-dir\n"},
		{"the project root as a variable default", "    volumes:\n    - type: bind\n      source: ${ROOT:-/home/ubuntu/devhealth}\n      target: /root-dir\n"},
	} {
		add(both("web mounts "+v.name, "  web:\n"+v.yaml, false)...)
	}
	add(both("a top-level secret from bugsink's env file", "  web:\n    secrets:\n    - s\nsecrets:\n  s:\n    file: ./.bugsink-dev/bugsink.env\n", false)...)
	add(both("a top-level config from bugsink's env file", "configs:\n  c:\n    file: ${C:-/home/ubuntu/devhealth/.bugsink-dev/bugsink.env}\n", false)...)
	names := map[string]bool{}
	for _, c := range cells {
		if names[c.name] {
			t.Fatalf("duplicate cell name %q", c.name)
		}
		names[c.name] = true
		t.Run(c.name, func(t *testing.T) {
			var f doc
			if err := yaml.Unmarshal([]byte(c.text), &f); err != nil {
				t.Fatalf("cell YAML: %v", err)
			}
			got := composeBugsinkLeaks(f)
			if c.ok != (len(got) == 0) {
				t.Fatalf("want accepted=%v, findings %v", c.ok, got)
			}
			if !c.ok && !strings.Contains(strings.Join(got, "\n"), "bugsink") {
				t.Fatalf("findings %v do not name bugsink", got)
			}
		})
	}
}

func TestComposeBugsinkFileFindingsBothForms(t *testing.T) {
	base := []string{"BIGBOY_DHO_IMAGE", "DHO_API_CH_USERS_XML"}
	withVars := []string{"BIGBOY_DHO_IMAGE", "BUGSINK_CREATE_SUPERUSER", "BUGSINK_DB_PASSWORD", "BUGSINK_SECRET_KEY", "DHO_API_CH_USERS_XML"}
	oldHead := "services:\n  bugsink:\n    environment:\n      DATABASE_URL: postgresql://devhealth:${BUGSINK_DB_PASSWORD:?set}@postgres:5432/bugsink\n      SECRET_KEY: ${BUGSINK_SECRET_KEY:?set}\n      CREATE_SUPERUSER: ${BUGSINK_CREATE_SUPERUSER:?set}\n"
	newHead := "services:\n  bugsink:\n    env_file:\n    - path: " + composeBugsinkEnvFile + "\n      required: true\n    environment:\n      PORT: '8000'\n"
	for _, c := range []struct {
		name, text string
		required   []string
		ok         bool
	}{
		{"old form", oldHead, withVars, true},
		{"old form, another service interpolates a credential", oldHead + "  web:\n    environment:\n      LEAK: ${BUGSINK_DB_PASSWORD}\n", withVars, false},
		{"old form, another service passes a credential by name", oldHead + "  web:\n    environment:\n      BUGSINK_SECRET_KEY: null\n", withVars, false},
		{"new form", newHead, base, true},
		{"new form, another service loads bugsink's env file by an absolute path", newHead + "  web:\n    env_file:\n    - /home/ubuntu/devhealth/.bugsink-dev/bugsink.env\n", base, false},
		{"new form, another service extends bugsink", newHead + "  web:\n    extends:\n      service: bugsink\n", base, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			writeFile(t, root, composeFile, c.text)
			g := &guard{root: root, tmp: t.TempDir()}
			got := composeBugsinkFileFindings(g, c.required, base)
			if len(g.v) != 0 {
				t.Fatalf("guard errors: %v", g.v)
			}
			if c.ok != (len(got) == 0) {
				t.Fatalf("want accepted=%v, findings %v", c.ok, got)
			}
		})
	}
}
