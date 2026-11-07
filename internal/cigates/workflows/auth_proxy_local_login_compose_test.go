package workflows_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-13107: the sessions-only posture (AUTH_PROXY_SESSIONS=true with AUTH_PROXY_LOCAL_LOGIN not true) picks the
// earliest-join org at login and records the gateway IP (CHAOS-13107 local legs): go-api signs the user in itself and
// asks auth-service only for the session pair. Production forwards local login with sessions, so every compose service
// that turns sessions on must turn local login on too, in compose.yml and in compose.yml under each ci/*.override.yml.
const sessionsOnlyReason = "the sessions-only posture picks the earliest-join org at login and records the gateway IP (CHAOS-13107 local legs)"

// sessionsFlagMirrors are the services that read go-api's AUTH_PROXY_SESSIONS for their own Postgres role posture and
// serve no sign-in route: the flag says who owns sessions, not that they forward anything. A mirror that names an
// AUTH_PROXY_URL forwards and is refused like any other service; a mirror no file sets sessions on for is stale.
var sessionsFlagMirrors = map[string]string{
	"go-river-migrate": "applies the api role's manifest of go-api's AUTH_PROXY_SESSIONS (internal/rivermigrate); serves no route",
	"query-api":        "its role posture holds the auth session read while go-api's AUTH_PROXY_SESSIONS is on; serves no sign-in route",
}

// composeEnv is a service environment in either compose form. A list entry without "=" is a pass-through from the
// caller's environment: its value is not in the file (nil).
type composeEnv map[string]*string

func (e *composeEnv) UnmarshalYAML(node *yaml.Node) error {
	out := composeEnv{}
	switch node.Kind {
	case yaml.MappingNode:
		var m map[string]any
		if err := node.Decode(&m); err != nil {
			return err
		}
		for k, v := range m {
			if v == nil {
				out[k] = nil
				continue
			}
			s := strings.TrimSpace(strings.Trim(yamlScalar(v), `"`))
			out[k] = &s
		}
	case yaml.SequenceNode:
		var list []string
		if err := node.Decode(&list); err != nil {
			return err
		}
		for _, entry := range list {
			if name, value, ok := strings.Cut(entry, "="); ok {
				v := value
				out[name] = &v
			} else {
				out[entry] = nil
			}
		}
	}
	*e = out
	return nil
}

func yamlScalar(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	default:
		raw, _ := yaml.Marshal(x)
		return strings.TrimSpace(string(raw))
	}
}

type envComposeFile struct {
	Services map[string]struct {
		Environment composeEnv `yaml:"environment"`
	} `yaml:"services"`
}

func readEnvCompose(t *testing.T, path string) envComposeFile {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out envComposeFile
	if err := yaml.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return out
}

// mergedServiceEnv is each service's environment of base with over on top (a key of over replaces base's), every
// literal value with its compose default (${VAR:-default}); a pass-through stays nil.
func mergedServiceEnv(base envComposeFile, over *envComposeFile) map[string]map[string]*string {
	out := map[string]map[string]*string{}
	add := func(f envComposeFile) {
		for name, svc := range f.Services {
			if out[name] == nil {
				out[name] = map[string]*string{}
			}
			for k, v := range svc.Environment {
				if v == nil {
					out[name][k] = nil
					continue
				}
				s := expandDefaults(*v, nil)
				out[name][k] = &s
			}
		}
	}
	add(base)
	if over != nil {
		add(*over)
	}
	return out
}

// sessionsOnlyRefusals lists each service of env that turns sessions on without local login, and each mirror that
// forwards. used collects the mirrors that carried sessions on.
func sessionsOnlyRefusals(where string, env map[string]map[string]*string, used map[string]bool) []string {
	var out []string
	is := func(svc map[string]*string, key, want string) bool { v := svc[key]; return v != nil && *v == want }
	for name, svc := range env {
		if !is(svc, "AUTH_PROXY_SESSIONS", "true") {
			continue
		}
		if _, mirror := sessionsFlagMirrors[name]; mirror {
			used[name] = true
			if v := svc["AUTH_PROXY_URL"]; v != nil && *v != "" {
				out = append(out, where+": "+name+" mirrors go-api's AUTH_PROXY_SESSIONS and names AUTH_PROXY_URL: it forwards, so it needs AUTH_PROXY_LOCAL_LOGIN=true: "+sessionsOnlyReason)
			}
			continue
		}
		if !is(svc, "AUTH_PROXY_LOCAL_LOGIN", "true") {
			local := "unset"
			if v := svc["AUTH_PROXY_LOCAL_LOGIN"]; v != nil {
				local = "\"" + *v + "\""
			}
			out = append(out, where+": "+name+" has AUTH_PROXY_SESSIONS=true and AUTH_PROXY_LOCAL_LOGIN "+local+": "+sessionsOnlyReason)
		}
	}
	sort.Strings(out)
	return out
}

func TestComposeRefusesTheSessionsOnlyPosture(t *testing.T) {
	root := repoRoot(t)
	base := readEnvCompose(t, filepath.Join(root, "compose.yml"))
	overrides, err := filepath.Glob(filepath.Join(root, "ci", "*.override.yml"))
	if err != nil || len(overrides) == 0 {
		t.Fatalf("no ci/*.override.yml (%v)", err)
	}
	used := map[string]bool{}
	refusals := sessionsOnlyRefusals("compose.yml", mergedServiceEnv(base, nil), used)
	for _, path := range overrides {
		over := readEnvCompose(t, path)
		refusals = append(refusals, sessionsOnlyRefusals("compose.yml + ci/"+filepath.Base(path), mergedServiceEnv(base, &over), used)...)
	}
	for _, refusal := range refusals {
		t.Error(refusal)
	}
	for name := range sessionsFlagMirrors {
		if !used[name] {
			t.Errorf("sessionsFlagMirrors names %s, and no compose file turns AUTH_PROXY_SESSIONS on for it: drop the entry", name)
		}
	}
}

// The census refuses a planted sessions-only service, a planted false or blank local login, and a mirror that
// forwards; it accepts sessions with local login on and a pass-through it cannot read.
func TestSessionsOnlyCensusRefusesAPlantedSessionsOnlyService(t *testing.T) {
	parse := func(text string) envComposeFile {
		var f envComposeFile
		if err := yaml.Unmarshal([]byte(text), &f); err != nil {
			t.Fatal(err)
		}
		return f
	}
	for name, c := range map[string]struct {
		file    string
		refused bool
	}{
		"sessions only (map)":       {"services:\n  go-api:\n    environment:\n      AUTH_PROXY_SESSIONS: \"true\"\n", true},
		"sessions only (list)":      {"services:\n  go-api:\n    environment:\n      - AUTH_PROXY_SESSIONS=true\n", true},
		"local login false":         {"services:\n  go-api:\n    environment:\n      AUTH_PROXY_SESSIONS: \"true\"\n      AUTH_PROXY_LOCAL_LOGIN: \"false\"\n", true},
		"local login by default":    {"services:\n  go-api:\n    environment:\n      AUTH_PROXY_SESSIONS: ${S:-true}\n      AUTH_PROXY_LOCAL_LOGIN: ${L:-}\n", true},
		"a forwarding mirror":       {"services:\n  query-api:\n    environment:\n      AUTH_PROXY_SESSIONS: \"true\"\n      AUTH_PROXY_URL: http://auth-service:8095\n", true},
		"sessions and local login":  {"services:\n  go-api:\n    environment:\n      AUTH_PROXY_SESSIONS: \"true\"\n      AUTH_PROXY_LOCAL_LOGIN: \"true\"\n", false},
		"a mirror":                  {"services:\n  query-api:\n    environment:\n      AUTH_PROXY_SESSIONS: \"true\"\n", false},
		"a pass-through":            {"services:\n  go-api:\n    environment:\n      - AUTH_PROXY_SESSIONS\n", false},
		"sessions off":              {"services:\n  go-api:\n    environment:\n      AUTH_PROXY_SESSIONS: \"false\"\n", false},
		"override drops local only": {"services:\n  go-api:\n    environment:\n      AUTH_PROXY_LOCAL_LOGIN: \"false\"\n", true},
	} {
		t.Run(name, func(t *testing.T) {
			base := parse("services:\n  go-api:\n    environment:\n      AUTH_PROXY_SESSIONS: \"true\"\n      AUTH_PROXY_LOCAL_LOGIN: \"true\"\n")
			if !strings.HasPrefix(name, "override") {
				base = parse("services: {}\n")
			}
			over := parse(c.file)
			got := sessionsOnlyRefusals("planted", mergedServiceEnv(base, &over), map[string]bool{})
			if refused := len(got) > 0; refused != c.refused {
				t.Fatalf("refused %v, want %v: %v", refused, c.refused, got)
			}
			if c.refused && !strings.Contains(got[0], sessionsOnlyReason) {
				t.Errorf("the refusal does not give the reason: %s", got[0])
			}
		})
	}
}
