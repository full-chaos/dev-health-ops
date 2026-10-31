package workflows_test

import (
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// Compose delivers the workload bootstrap credential through a restricted file: the one-shots that seed the registry
// and write the file, a named volume the file lives in, auth-service with the bootstrap switch off by default, and
// go-api reading the file from a read-only mount. No docker runs here.

type composeFile struct {
	Services map[string]composeService `yaml:"services"`
	Volumes  map[string]any            `yaml:"volumes"`
}

type composeService struct {
	Profiles    []string       `yaml:"profiles"`
	NetworkMode string         `yaml:"network_mode"`
	ReadOnly    bool           `yaml:"read_only"`
	User        string         `yaml:"user"`
	CapDrop     []string       `yaml:"cap_drop"`
	CapAdd      []string       `yaml:"cap_add"`
	SecurityOpt []string       `yaml:"security_opt"`
	Entrypoint  []string       `yaml:"entrypoint"`
	Command     []string       `yaml:"command"`
	Environment map[string]any `yaml:"environment"`
	Volumes     []any          `yaml:"volumes"`
	DependsOn   map[string]struct {
		Condition string `yaml:"condition"`
	} `yaml:"depends_on"`
}

func readComposeFile(t *testing.T, rel string) composeFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	var out composeFile
	if err := yaml.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	return out
}

func envString(t *testing.T, svc composeService, name string) (string, bool) {
	t.Helper()
	v, ok := svc.Environment[name]
	if !ok {
		return "", false
	}
	s, isString := v.(string)
	if !isString {
		t.Fatalf("%s is %T, want a string", name, v)
	}
	return s, true
}

// volumeMount reads a service volume entry in the long form.
func volumeMount(entry any) (source, target string, readOnly bool) {
	m, _ := entry.(map[string]any)
	source, _ = m["source"].(string)
	target, _ = m["target"].(string)
	readOnly, _ = m["read_only"].(bool)
	return
}

const workloadVolumeCommand = `[ "$(stat -c %u:%a /run/workload)" = 65532:700 ] || { chmod 0700 /run/workload && chown 65532:65532 /run/workload; }`

func TestComposeWorkloadBootstrapOneShots(t *testing.T) {
	c := readComposeFile(t, "compose.yml")
	want := map[string][]string{
		"auth-workload-volume":    nil,
		"auth-workload-seed":      {"workloads", "seed", "-apply"},
		"auth-workload-bootstrap": {"workloads", "bootstrap-create", "-workload", "api", "-out", "/run/workload/api.bootstrap", "-apply"},
	}
	for name, command := range want {
		svc, ok := c.Services[name]
		if !ok {
			t.Errorf("compose.yml has no service %s", name)
			continue
		}
		if !reflect.DeepEqual(svc.Profiles, []string{"auth"}) {
			t.Errorf("%s profiles = %v, want [auth]: it must not start with the default stack", name, svc.Profiles)
		}
		if !svc.ReadOnly || !reflect.DeepEqual(svc.CapDrop, []string{"ALL"}) || !reflect.DeepEqual(svc.SecurityOpt, []string{"no-new-privileges:true"}) {
			t.Errorf("%s is not hardened (read_only %v, cap_drop %v, security_opt %v)", name, svc.ReadOnly, svc.CapDrop, svc.SecurityOpt)
		}
		if command != nil && !reflect.DeepEqual(svc.Command, command) {
			t.Errorf("%s command = %v, want %v: the credential goes to a file, never argv", name, svc.Command, command)
		}
		for key := range svc.Environment {
			if regexp.MustCompile(`(?i)bootstrap|secret|token|password`).MatchString(key) && name != "auth-workload-seed" && name != "auth-workload-bootstrap" {
				t.Errorf("%s env %s looks like a credential", name, key)
			}
		}
	}
	vol := c.Services["auth-workload-volume"]
	if vol.NetworkMode != "none" || vol.User != "0:0" || !reflect.DeepEqual(vol.CapAdd, []string{"CHOWN"}) {
		t.Errorf("auth-workload-volume: network_mode %q user %q cap_add %v, want none, 0:0, [CHOWN]", vol.NetworkMode, vol.User, vol.CapAdd)
	}
	if got := strings.Join(vol.Command, " "); got != workloadVolumeCommand {
		t.Errorf("auth-workload-volume command = %q, want %q: the directory must be 0700 and owned by uid 65532, and a second run must change nothing", got, workloadVolumeCommand)
	}
	for _, name := range []string{"auth-workload-seed", "auth-workload-bootstrap"} {
		if user := c.Services[name].User; user != "65532:65532" {
			t.Errorf("%s runs as %q, want 65532:65532", name, user)
		}
	}
	deps := func(name string) map[string]string {
		out := map[string]string{}
		for k, v := range c.Services[name].DependsOn {
			out[k] = v.Condition
		}
		return out
	}
	if got, want := deps("auth-workload-seed"), map[string]string{"auth-grants": "service_completed_successfully"}; !reflect.DeepEqual(got, want) {
		t.Errorf("auth-workload-seed depends_on = %v, want %v", got, want)
	}
	if got, want := deps("auth-workload-bootstrap"), map[string]string{"auth-workload-volume": "service_completed_successfully", "auth-workload-seed": "service_completed_successfully"}; !reflect.DeepEqual(got, want) {
		t.Errorf("auth-workload-bootstrap depends_on = %v, want %v", got, want)
	}
	if _, ok := c.Volumes["workload_bootstrap"]; !ok {
		t.Error("compose.yml declares no volume workload_bootstrap")
	}
	for _, name := range []string{"auth-workload-volume", "auth-workload-bootstrap"} {
		found := false
		for _, e := range c.Services[name].Volumes {
			if s, ok := e.(string); ok && s == "workload_bootstrap:/run/workload" {
				found = true
			}
		}
		if !found {
			t.Errorf("%s does not mount workload_bootstrap at /run/workload", name)
		}
	}
	// The one-shots are never a dependency of a running service: a later `up` would run bootstrap-create again.
	for name, svc := range c.Services {
		for dep := range svc.DependsOn {
			if strings.HasPrefix(dep, "auth-workload-") && name != "auth-workload-bootstrap" && name != "auth-workload-seed" {
				t.Errorf("%s depends on %s: a later up would run it again, and bootstrap-create refuses a workload with a live credential", name, dep)
			}
		}
	}
}

// auth-service never mounts the credential file (it keeps only a hash); go-api reads it from a read-only mount, and
// the path it is told is the path the one-shot writes.
func TestComposeWorkloadBootstrapMounts(t *testing.T) {
	c := readComposeFile(t, "compose.yml")
	for _, e := range c.Services["auth-service"].Volumes {
		if source, _, _ := volumeMount(e); source == "workload_bootstrap" {
			t.Error("auth-service mounts workload_bootstrap")
		}
	}
	mounts := 0
	var target string
	for _, e := range c.Services["go-api"].Volumes {
		source, tgt, ro := volumeMount(e)
		if source != "workload_bootstrap" {
			continue
		}
		mounts++
		target = tgt
		if !ro {
			t.Error("go-api mounts workload_bootstrap read-write")
		}
	}
	if mounts != 1 {
		t.Fatalf("go-api mounts workload_bootstrap %d times, want once", mounts)
	}
	file, _ := envString(t, c.Services["go-api"], "AUTH_WORKLOAD_BOOTSTRAP_FILE")
	cmd := c.Services["auth-workload-bootstrap"].Command
	out := ""
	for i, a := range cmd {
		if a == "-out" && i+1 < len(cmd) {
			out = cmd[i+1]
		}
	}
	if out == "" || file != out || path.Dir(file) != target {
		t.Errorf("go-api reads %q from the mount at %q, the one-shot writes %q: a path that differs leaves go-api unable to read the file, and it refuses to start", file, target, out)
	}
	issuerGo, _ := envString(t, c.Services["go-api"], "AUTH_WORKLOAD_ISSUER")
	issuerAuth, _ := envString(t, c.Services["auth-service"], "AUTH_TOKEN_ISSUER_URL")
	if issuerGo != "${AUTH_TOKEN_ISSUER_URL:-}" || issuerAuth != "${AUTH_TOKEN_ISSUER_URL:-}" {
		t.Errorf("issuer: go-api %q, auth-service %q; both must read ${AUTH_TOKEN_ISSUER_URL:-}: a token carries the issuer it was signed under", issuerGo, issuerAuth)
	}
	// go-api selectors stay out of compose.yml: unset is the default, session_compat.
	for _, name := range []string{"AUTH_SESSION_ISSUE_CREDENTIAL", "AUTH_PROVISIONING_CREDENTIAL"} {
		if v, ok := envString(t, c.Services["go-api"], name); ok && v != "${"+name+":-}" {
			t.Errorf("go-api %s = %q, want blank by default", name, v)
		}
	}
}

var composeDefault = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*):-([^}]*)\}`)

// expandDefaults resolves ${VAR:-default} to its default, as compose does with the variable unset.
func expandDefaults(s string, set map[string]string) string {
	return composeDefault.ReplaceAllStringFunc(s, func(m string) string {
		sub := composeDefault.FindStringSubmatch(m)
		if v, ok := set[sub[1]]; ok {
			return v
		}
		return sub[2]
	})
}

func authServiceLoad(t *testing.T, set map[string]string) error {
	t.Helper()
	c := readComposeFile(t, "compose.yml")
	env := map[string]string{}
	for k, v := range c.Services["auth-service"].Environment {
		if s, ok := v.(string); ok {
			env[k] = expandDefaults(s, set)
		}
	}
	_, err := authconfig.Load(authconfig.Spec{LookupEnv: func(name string) (string, bool) { v, ok := env[name]; return v, ok }})
	return err
}

// With nothing set auth-service's compose environment loads and the exchange is off; the bootstrap switch on without
// an issuer is refused, so a switch alone never opens an exchange.
func TestComposeWorkloadBootstrapIsOffByDefaultAndNeedsAnIssuer(t *testing.T) {
	if err := authServiceLoad(t, nil); err != nil {
		t.Fatalf("auth-service's default compose environment is refused: %v", err)
	}
	err := authServiceLoad(t, map[string]string{"AUTH_WORKLOAD_BOOTSTRAP": "true"})
	if err == nil || !strings.Contains(err.Error(), "AUTH_WORKLOAD_BOOTSTRAP=true needs AUTH_TOKEN_ISSUER_URL") {
		t.Fatalf("the bootstrap switch without an issuer is not refused for it: %v", err)
	}
	if err := authServiceLoad(t, map[string]string{"AUTH_WORKLOAD_BOOTSTRAP": "true", "AUTH_TOKEN_ISSUER_URL": "https://auth.example.test"}); err != nil {
		t.Fatalf("the bootstrap switch with an issuer is refused: %v", err)
	}
}

// The proof override keeps only the proof; the wiring lives in compose.yml.
func TestWorkloadOverrideKeepsOnlyTheProof(t *testing.T) {
	o := readComposeFile(t, "ci/auth_session_issue_workload.override.yml")
	for _, name := range []string{"auth-workload-volume", "auth-workload-seed", "auth-workload-bootstrap"} {
		if _, ok := o.Services[name]; ok {
			t.Errorf("the override still defines %s, which compose.yml owns", name)
		}
	}
	if _, ok := o.Volumes["workload_bootstrap"]; ok {
		t.Error("the override still declares workload_bootstrap")
	}
	for _, name := range []string{"auth-workload-probe", "auth-service", "go-api"} {
		if _, ok := o.Services[name]; !ok {
			t.Errorf("the override lost %s", name)
		}
	}
	for _, name := range []string{"AUTH_WORKLOAD_BOOTSTRAP_FILE", "AUTH_WORKLOAD_ISSUER"} {
		if _, ok := o.Services["go-api"].Environment[name]; ok {
			t.Errorf("the override sets go-api %s: compose.yml owns it", name)
		}
	}
	if len(o.Services["go-api"].Volumes) != 0 {
		t.Error("the override mounts a volume into go-api: compose.yml owns the mount")
	}
	if v, ok := envString(t, o.Services["auth-service"], "AUTH_WORKLOAD_BOOTSTRAP"); !ok || v != "true" {
		t.Errorf("the override must switch the bootstrap on (got %q): it is the proof", v)
	}
}

// Compose starts auth-workload-volume again as a dependency of auth-workload-bootstrap, after the directory is owned
// by uid 65532 with mode 0700. The container has CAP_CHOWN and no CAP_FOWNER, so a chmod then fails with EPERM: the
// second run must see the state and change nothing.
func TestComposeWorkloadVolumeOneShotIsIdempotent(t *testing.T) {
	c := readComposeFile(t, "compose.yml")
	script := strings.Join(c.Services["auth-workload-volume"].Command, " ")
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	state, calls := filepath.Join(dir, "state"), filepath.Join(dir, "calls")
	fakes := map[string]string{
		"stat":  "#!/bin/sh\ncat " + state + "\n",
		"chmod": "#!/bin/sh\necho chmod >> " + calls + "\n",
		"chown": "#!/bin/sh\necho chown >> " + calls + "\necho 65532:700 > " + state + "\n",
	}
	for name, body := range fakes {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(state, []byte("0:755\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(calls, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	run := func() string {
		cmd := exec.Command("/bin/sh", "-eu", "-c", script)
		cmd.Env = []string{"PATH=" + bin + ":/usr/bin:/bin"}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("the one-shot failed: %v\n%s", err, out)
		}
		raw, err := os.ReadFile(calls)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	if got := run(); got != "chmod\nchown\n" {
		t.Fatalf("first run made %q, want chmod then chown", got)
	}
	if got := run(); got != "chmod\nchown\n" {
		t.Errorf("the second run changed the directory again: calls %q", got)
	}
}
