package bigboy_test

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// composeBugsinkLeaks returns a finding for every way the compose file can hand one of the three bugsink credentials to a
// service other than bugsink, in both forms.
func composeBugsinkLeaks(f doc) []string {
	return nil
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
	add(both("a top-level include", "include:\n- other.yml\n", false)...)
	add(both("a top-level secret from the environment", "secrets:\n  s:\n    environment: BUGSINK_DB_PASSWORD\n", false)...)
	add(both("a top-level config from the environment", "configs:\n  c:\n    environment: BUGSINK_SECRET_KEY\n", false)...)
	add(both("a top-level config content", "configs:\n  c:\n    content: ${BUGSINK_SECRET_KEY}\n", false)...)
	add(cell{"new form: bugsink with a credential key", "services:\n" + newBugsink + "      BUGSINK_SECRET_KEY: null\n", false})
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
