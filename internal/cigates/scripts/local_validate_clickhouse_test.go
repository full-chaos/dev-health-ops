package scripts_test

import (
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-10376: guards ported from tests/test_local_validate_credential_exposure.py and
// tests/test_local_validate_remote_clickhouse.py. Subject: ci/local_validate.sh must reach a remote ClickHouse
// without docker, and the scratch DSN credential must not reach stdout or any child's argv. The guard is
// lvGateViolations: a function of a script path. One test runs it on the real script, a table plants one defect
// per guard in a temp copy and asserts the guard fires naming it. The probes run the real functions of the
// script with an inert curl and docker on PATH; no container starts.

const lvSecret = "sup3rsecret"

// lvFunc returns the body of a top-level shell function of the script.
func lvFunc(src, name string) (string, bool) {
	m := regexp.MustCompile(`(?ms)^` + regexp.QuoteMeta(name) + `\(\) \{.*?^\}`).FindString(src)
	return m, m != ""
}

func lvBash(script string, args ...string) (string, error) {
	cmd := exec.Command("bash", append([]string{"-c", script, "_"}, args...)...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func lvCallFunc(src, name, arg string) (string, error) {
	body, ok := lvFunc(src, name)
	if !ok {
		return "", fmt.Errorf("%s() not found in the gate", name)
	}
	return lvBash(body+"\n"+name+` "$1"`, arg)
}

// lvStubBin writes an inert curl and docker that record their argv.
func lvStubBin(t *testing.T, dir string) (dockerMarker, curlLog string) {
	t.Helper()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	dockerMarker = filepath.Join(dir, "docker-was-called")
	curlLog = filepath.Join(dir, "curl-argv")
	// The curl stub also copies the file named by --config, with its mode.
	stubs := map[string]string{
		"docker": "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"" + dockerMarker + "\"\nexit 0\n",
		"curl": "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"" + curlLog + "\"\nprev=''\nfor a in \"$@\"; do\n" +
			"  if [ \"$prev\" = '--config' ]; then\n    cat \"$a\" > \"" + dir + "/captured-config\"\n" +
			"    ls -l \"$a\" | cut -c1-10 >> \"" + dir + "/captured-config\"\n  fi\n  prev=\"$a\"\ndone\nexit 0\n",
	}
	for name, body := range stubs {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dockerMarker, curlLog
}

func lvRunGate(script, dir string, env map[string]string, unset []string, args ...string) string {
	cmd := exec.Command("bash", append([]string{script}, args...)...)
	cmd.Dir = filepath.Dir(filepath.Dir(script))
	skip := map[string]bool{"CH_TRANSPORT": false}
	for _, u := range unset {
		skip[u] = true
	}
	var out []string
	for _, e := range os.Environ() {
		k := strings.SplitN(e, "=", 2)[0]
		if _, overridden := env[k]; overridden || skip[k] || k == "CH_HTTP_SCHEME" || k == "CH_PASS" || k == "CH_USER" {
			continue
		}
		out = append(out, e)
	}
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	cmd.Env = out
	b, _ := cmd.CombinedOutput()
	return string(b)
}

func lvPathWith(bin string) string { return bin + ":" + os.Getenv("PATH") }

// lvGateViolations runs every guard on the script at path and returns one message per failed claim.
func lvGateViolations(t *testing.T, script string) []string {
	t.Helper()
	raw, err := os.ReadFile(script)
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	var bad []string
	add := func(format string, a ...any) { bad = append(bad, fmt.Sprintf(format, a...)) }

	// Redaction removes the credential and keeps the endpoint.
	for _, uri := range []string{
		"clickhouse://ch:" + lvSecret + "@10.0.0.5:8123/ci_scratch",
		"clickhouse+https://ch:p@" + lvSecret + "@host:8443/db",
		"clickhouse://ch:pa:ss@" + lvSecret + "@1.2.3.4:9000/db",
	} {
		out, err := lvCallFunc(src, "redact_uri", uri)
		if err != nil {
			add("redact_uri failed: %v %s", err, out)
			continue
		}
		if strings.Contains(out, lvSecret) {
			add("credential survived redaction: %s", out)
		}
		if !strings.HasPrefix(out, strings.SplitN(uri, "://", 2)[0]+"://") || !strings.Contains(out, uri[strings.LastIndex(uri, "/")+1:]) {
			add("redaction lost the scheme or the scratch db name: %s", out)
		}
	}

	// Every display site routes the DSN through redact_uri.
	for i, line := range strings.Split(src, "\n") {
		if !strings.Contains(line, "SCRATCH_URI") || !(strings.Contains(line, "printf") || strings.Contains(line, "die ") || strings.Contains(line, "die(")) {
			continue
		}
		if !strings.Contains(line, "redact_uri") {
			add("line %d displays SCRATCH_URI without redaction: %s", i+1, strings.TrimSpace(line))
		}
	}

	// No child receives the DSN as an argument.
	for _, flag := range []string{"--sink", "--clickhouse-uri", "--db ", "--analytics-db"} {
		if regexp.MustCompile(regexp.QuoteMeta(flag) + `\s*"?\$\{SCRATCH_URI\}`).MatchString(src) {
			add("flag %q passes the credential-bearing SCRATCH_URI in argv", flag)
		}
	}

	// Userinfo percent-encoding: reserved characters round-trip, sub-delims stay raw.
	for _, cred := range []string{"sec/ret", "se#cr?et", "p@ssw%rd", "a:b", "sp ace", "plain"} {
		enc, err := lvCallFunc(src, "uri_encode", cred)
		if err != nil {
			add("uri_encode failed: %v %s", err, enc)
			continue
		}
		u, err := url.Parse("clickhouse://ch:" + enc + "@host:8123/db")
		pw, _ := u.User.Password()
		if err != nil || u.Hostname() != "host" || u.Port() != "8123" || pw != cred {
			add("uri_encode: credential did not round-trip: %q -> %q", cred, enc)
		}
	}
	for _, cred := range []string{"sec!ret", "p$w=1", "a,b;c", "it's"} {
		if enc, _ := lvCallFunc(src, "uri_encode", cred); enc != cred {
			add("uri_encode: sub-delims must stay raw: %q -> %q", cred, enc)
		}
	}
	if enc, _ := lvCallFunc(src, "uri_encode", "sec/ret"); !strings.Contains(enc, "%2F") {
		add("uri_encode: '/' must be encoded, got %q", enc)
	} else if red, _ := lvCallFunc(src, "redact_uri", "clickhouse://ch:"+enc+"@host:8123/db"); strings.TrimSpace(red) != "clickhouse://host:8123/db" {
		add("a slash in the password defeats redaction: %q", red)
	}
	if enc, _ := lvCallFunc(src, "uri_encode", "café"); enc != "caf%C3" {
		add("uri_encode of a non-ASCII credential changed (known truncating output caf%%C3 expected): %q", enc)
	}

	// Remote ClickHouse consumers are proxy-neutralised.
	if migrate, ok := lvFunc(src, "ch_migrate"); !ok {
		add("ch_migrate() not found")
	} else {
		n := 0
		for _, line := range strings.Split(migrate, "\n") {
			if strings.Contains(line, "${DHO}") {
				n++
				if !strings.Contains(line, "PROXY_OFF") {
					add("remote-ClickHouse invocation is not proxy-neutralised: %s", strings.TrimSpace(line))
				}
			}
		}
		if n == 0 {
			add("ch_migrate no longer invokes dho")
		}
	}
	if m := regexp.MustCompile(`(?m)^PROXY_OFF=\(.*\)$`).FindString(src); m == "" {
		add("PROXY_OFF array not found")
	} else if !strings.Contains(m, "-u TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX") {
		add("PROXY_OFF no longer isolates TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX")
	}

	// The lock is scoped to CH_CONTAINER, not the host.
	scoped := false
	for _, line := range strings.Split(src, "\n") {
		l := strings.TrimSpace(line)
		if strings.HasPrefix(l, "LOCK_DIR=") && strings.Contains(l, "CH_CONTAINER") {
			scoped = true
		}
	}
	if !scoped {
		add("LOCK_DIR is not scoped to CH_CONTAINER: isolated lanes would serialize on one lock")
	}

	// Probes: drive the real functions with an inert curl and docker.
	dir := t.TempDir()
	marker, curlLog := lvStubBin(t, dir)
	pathEnv := lvPathWith(filepath.Join(dir, "bin"))
	httpEnv := func(extra map[string]string) map[string]string {
		e := map[string]string{"PATH": pathEnv, "CH_TRANSPORT": "http", "CH_HTTP_SCHEME": "http", "CH_HOST": "192.0.2.10",
			"CH_HTTP_PORT": "30501", "CH_USER": "ch", "CH_PASS": "secret", "CH_CONTAINER": "lane-scoped-name"}
		for k, v := range extra {
			e[k] = v
		}
		return e
	}
	endpoints := func(argv string) []string {
		var e []string
		for _, tok := range strings.Fields(argv) {
			if strings.Contains(tok, "://") {
				e = append(e, tok)
			}
		}
		return e
	}
	reset := func() { os.Remove(marker); os.Remove(curlLog); os.Remove(filepath.Join(dir, "captured-config")) }
	read := func(p string) string { b, _ := os.ReadFile(p); return string(b) }

	reset()
	lvRunGate(script, dir, httpEnv(nil), nil, "--ch-query-probe", "SELECT 1")
	if _, err := os.Stat(marker); err == nil {
		add("ch_query shelled out to docker under CH_TRANSPORT=http: %s", read(marker))
	}
	argv := read(curlLog)
	if argv == "" {
		add("ch_query did not invoke curl under CH_TRANSPORT=http")
	} else {
		if e := endpoints(argv); len(e) != 1 || e[0] != "http://192.0.2.10:30501/" {
			add("wrong endpoint: %s", argv)
		}
		if !strings.Contains(argv, "SELECT 1") {
			add("query body missing: %s", argv)
		}
		if !strings.Contains(argv, "--noproxy") {
			add("the ClickHouse POST must bypass ambient proxies (--noproxy missing): %s", argv)
		}
		if strings.Contains(argv, "secret") {
			add("the ClickHouse password appeared in curl's command line: %s", argv)
		}
		if !strings.Contains(argv, "--config") {
			add("credentials must be passed through a private --config file: %s", argv)
		}
	}
	cfg := read(filepath.Join(dir, "captured-config"))
	if !strings.Contains(cfg, `header = "X-ClickHouse-User: ch"`) || !strings.Contains(cfg, `header = "X-ClickHouse-Key: secret"`) {
		add("the credential headers were not handed to curl in the --config file: %s", cfg)
	}
	if !strings.Contains(cfg, "-rw-------") {
		add("the credential file must be owner-only: %s", cfg)
	}

	reset()
	lvRunGate(script, dir, map[string]string{"PATH": pathEnv, "CH_CONTAINER": "dev-health-clickhouse-1"}, []string{"CH_TRANSPORT", "CH_HTTP_SCHEME"}, "--ch-query-probe", "SELECT 1")
	if m := read(marker); !strings.Contains(m, "dev-health-clickhouse-1") {
		add("default (unset CH_TRANSPORT) must still drive ClickHouse via docker exec")
	}

	reset()
	lvRunGate(script, dir, httpEnv(map[string]string{"CH_HTTP_SCHEME": "https", "CH_HOST": "clickhouse.example.com", "CH_HTTP_PORT": "8443"}), nil, "--ch-query-probe", "SELECT 1")
	if e := endpoints(read(curlLog)); len(e) != 1 || e[0] != "https://clickhouse.example.com:8443/" {
		add("CH_HTTP_SCHEME=https must reach a TLS endpoint: %v", e)
	}

	// Sub-delims are not refused, unsupported combinations refuse up front.
	for _, cred := range []string{"sec!ret", "p$w=1", "a,b;c", "it's"} {
		out := lvRunGate(script, dir, map[string]string{"PATH": pathEnv, "CH_PASS": cred}, nil, "--ch-query-probe", "SELECT 1")
		if strings.Contains(out, "may not contain") {
			add("docker mode refused the sub-delim credential %q: %s", cred, out)
		}
	}
	for _, c := range []struct {
		env      map[string]string
		expected string
	}{
		{map[string]string{"CH_HTTP_SCHEME": "https", "CH_HTTP_PORT": "30501"}, "port 443 or 8443"},
		{map[string]string{"CH_PASS": "sec/ret"}, "may not contain"},
		{map[string]string{"CH_USER": "user@host"}, "may not contain"},
	} {
		if out := lvRunGate(script, dir, httpEnv(c.env), nil, "--ch-query-probe", "SELECT 1"); !strings.Contains(out, c.expected) {
			add("expected a refusal mentioning %q for %v; got: %s", c.expected, c.env, out)
		}
	}
	for _, port := range []map[string]string{{"CH_HTTP_SCHEME": "http", "CH_HTTP_PORT": "30501"}, {"CH_HTTP_SCHEME": "https", "CH_HTTP_PORT": "8443"}} {
		out := lvRunGate(script, dir, httpEnv(port), nil, "--ch-query-probe", "SELECT 1")
		if strings.Contains(out, "only supported on port") || strings.Contains(out, "may not contain") {
			add("supported combination %v was refused: %s", port, out)
		}
	}
	if out := lvRunGate(script, dir, map[string]string{"PATH": pathEnv, "CH_PASS": "café"}, nil, "--ch-query-probe", "SELECT 1"); !strings.Contains(out, "may not contain") {
		add("non-ASCII credential must be refused while uri_encode truncates it: %s", out)
	}
	return bad
}

func TestLocalValidateClickhouseCredentialGuardsHoldOnTheRealScript(t *testing.T) {
	script := filepath.Join(repoRoot(t), "ci", "local_validate.sh")
	if v := lvGateViolations(t, script); len(v) > 0 {
		t.Fatalf("ci/local_validate.sh violates its ClickHouse credential contract:\n%s", strings.Join(v, "\n"))
	}
}

func TestLocalValidateClickhouseGuardsFirePerPlantedDefect(t *testing.T) {
	real := filepath.Join(repoRoot(t), "ci", "local_validate.sh")
	raw, err := os.ReadFile(real)
	if err != nil {
		t.Fatal(err)
	}
	anchor := `PYBIN="${ROOT}/.venv/bin/python"`
	cases := []struct{ name, old, new, want string }{
		{"redaction leaks the tail", `"${rest#*"${authority%@*}"@}"`, `"${rest}"`, "credential survived redaction"},
		{"a display site prints the raw DSN", anchor, anchor + "\nprintf '%s\\n' \"${SCRATCH_URI}\"", "displays SCRATCH_URI without redaction"},
		{"a child gets the DSN in argv", anchor, anchor + "\n: --sink \"${SCRATCH_URI}\"", "passes the credential-bearing SCRATCH_URI in argv"},
		{"sub-delims get encoded", `[A-Za-z0-9._~!\$\&\'\(\)\*+,\;=-]) printf`, `[A-Za-z0-9._~-]) printf`, "sub-delims must stay raw"},
		{"dho runs without the proxy guard", `"${PROXY_OFF[@]}" "${DHO}" migrate clickhouse upgrade`, `"${DHO}" migrate clickhouse upgrade`, "not proxy-neutralised"},
		{"PROXY_OFF forgets the testcontainers prefix", ` -u TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX)`, `)`, "TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX"},
		{"the lock goes host-wide", `.${CH_CONTAINER}.lock}"`, `.lock}"`, "LOCK_DIR is not scoped"},
		{"http transport shells out to docker", "if [ \"${CH_TRANSPORT}\" = \"http\" ]; then\n    # The credential goes", "if [ \"${CH_TRANSPORT}\" = \"nohttp\" ]; then\n    # The credential goes", "shelled out to docker"},
		{"curl loses --noproxy", `--fail-with-body --noproxy '*'`, `--fail-with-body`, "--noproxy missing"},
		{"credential file is group-readable", `"${esc_user}" "${esc_pass}" >"${cfg}"`, `"${esc_user}" "${esc_pass}" >"${cfg}"; chmod 644 "${cfg}"`, "owner-only"},
		{"docker default is lost", "  docker exec -i \"${CH_CONTAINER}\" clickhouse-client", "  true -i \"${CH_CONTAINER}\" clickhouse-client", "must still drive ClickHouse via docker exec"},
		{"the TLS port limit is widened wrongly", `[ "${CH_HTTP_PORT}" != "8443" ]`, `[ "${CH_HTTP_PORT}" != "8444" ]`, "was refused"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if strings.Count(string(raw), c.old) != 1 {
				t.Fatalf("anchor for the planted defect is not unique in ci/local_validate.sh: %q", c.old)
			}
			dir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(dir, "ci"), 0o755); err != nil {
				t.Fatal(err)
			}
			planted := filepath.Join(dir, "ci", "local_validate.sh")
			if err := os.WriteFile(planted, []byte(strings.Replace(string(raw), c.old, c.new, 1)), 0o755); err != nil {
				t.Fatal(err)
			}
			v := strings.Join(lvGateViolations(t, planted), "\n")
			if !strings.Contains(v, c.want) {
				t.Fatalf("guard did not fire naming %q; violations:\n%s", c.want, v)
			}
		})
	}
}
