package bigboy_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	composeCHCheck    = "ci/bigboy/check-dho-api-ch-user.sh"
	composeCHRenderer = "ci/bigboy/render-dho-api-ch-users.py"
	composeCHCut      = "ci/bigboy/bigboy-cut.sh"
	composeCHPassword = "the-api-password"
	composeCHSentinel = "copper-lantern-value"
	composeCHOld8     = "aaaaaaaa"
	composeCHPlace    = "API_CH_PASSWORD=placeholder-not-a-credential"
)

var (
	composeCHNew  = strings.Repeat("b", 40)
	composeCHHash = composeSHA(composeCHPassword)
)

const composeCHManifest = `package clickhouse

// func APIPosture is quoted here in comments: password="leaked" clickhouse://u:p@h/db
func APIPosture(database string) Posture {
	return Posture{RequiredTables: []TableGrant{
		{Database: database, Table: "teams", AllowInsert: true, AllowSelect: true, AllowDelete: true},
		// marker-comment: <password>plain</password> Table: "hidden"
		{Database: database, Table: "team_sync_policies", AllowSelect: true},
		{Database: database, Table: "team_memberships", AllowInsert: true},
		{Database: database, Table: "no_privileges"},
	}}
}

func other() {
	_ = TableGrant{Database: database, Table: "outside", AllowSelect: true}
}
`

func composeSHA(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

func composeCHXML(hash string) string {
	return "<clickhouse><users><dho_api_ch><networks><ip>::/0</ip></networks><profile>default</profile><quota>default</quota>" +
		"<access_management>0</access_management><password_sha256_hex>" + hash + "</password_sha256_hex><grants>" +
		"<query>GRANT SELECT, INSERT, ALTER DELETE ON default.teams</query>" +
		"<query>GRANT SELECT ON default.team_sync_policies</query>" +
		"<query>GRANT INSERT ON default.team_memberships</query>" +
		"</grants></dho_api_ch></users></clickhouse>\n"
}

var composeCHCommands = strings.Fields("stat id sed tail tr wc cut awk cmp head dirname mktemp chmod rm sha256sum python3 cat env bash grep sort paste date mkdir git")

func composeExec(dir string, env []string, timeout time.Duration, name string, args ...string) (scriptRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir, cmd.Env = dir, env
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	err := cmd.Run()
	r := scriptRun{stdout: so.String(), stderr: se.String()}
	if ee, ok := err.(*exec.ExitError); ok && ctx.Err() == nil {
		r.code, err = ee.ExitCode(), nil
	}
	return r, err
}

func composeWrite(g *guard, p, body string, mode os.FileMode) {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		g.failf("mkdir %s: %v", filepath.Dir(p), err)
	}
	if err := os.WriteFile(p, []byte(body), mode); err != nil {
		g.failf("write %s: %v", p, err)
	}
	if err := os.Chmod(p, mode); err != nil {
		g.failf("chmod %s: %v", p, err)
	}
}

func composeReadFile(p string) string { b, _ := os.ReadFile(p); return string(b) }

func composeTempDir(g *guard, base, prefix string) string {
	d, err := os.MkdirTemp(base, prefix)
	if err != nil {
		g.failf("mkdtemp under %s: %v", base, err)
	}
	return d
}

func composeOffTmp(p string) bool {
	return p != "" && p != "/tmp" && p != "/var/tmp" && !strings.HasPrefix(p, "/tmp/") && !strings.HasPrefix(p, "/var/tmp/")
}

// composeCHScratch is a private TMPDIR for the check outside /tmp (the check refuses /tmp, so t.TempDir cannot be it).
func composeCHScratch(g *guard, mode os.FileMode) string {
	base := ""
	if t := os.Getenv("TMPDIR"); t != "" {
		if real, err := filepath.EvalSymlinks(t); err == nil && composeOffTmp(real) && syscall.Access(real, 2) == nil {
			base = real
		}
	}
	if base == "" {
		base = filepath.Join(g.root, "tests")
		if syscall.Access("/dev/shm", 2) == nil {
			base = "/dev/shm"
		}
	}
	d := composeTempDir(g, base, "dho-check-scratch-")
	_ = os.Chmod(d, mode)
	return d
}

func composeCHUsers(g *guard, mode os.FileMode, body string) string {
	p := filepath.Join(composeTempDir(g, g.tmp, "users-"), "dho_api_ch.xml")
	composeWrite(g, p, body, mode)
	return p
}

func composeVarTrap(g *guard, dir string) []string {
	trap, log := filepath.Join(dir, "var-trap.sh"), filepath.Join(dir, "vars.log")
	composeWrite(g, trap, "set -o functrace\ntrap 'declare -p >> \"$VARLOG\" 2>/dev/null' DEBUG\n", 0o644)
	composeWrite(g, log, "", 0o600)
	return []string{"BASH_ENV=" + trap, "VARLOG=" + log}
}

func composeRealBin(name string) string {
	for _, d := range []string{"/usr/bin", "/bin"} {
		if p := filepath.Join(d, name); exists(p) {
			return p
		}
	}
	return ""
}

type composeCHOpts struct {
	file      string
	noFile    bool
	count     string
	noCount   bool
	rc        int
	loginFail bool
	creds     string
	scratch   string
	tmpdir    *string
	opsRepo   string
	direct    bool
	posture   string
}

type composeCHRun struct {
	dir string
	scriptRun
}

func (r composeCHRun) log(name string) string { return composeReadFile(filepath.Join(r.dir, name)) }
func (r composeCHRun) has(name string) bool   { return exists(filepath.Join(r.dir, name)) }
func (r composeCHRun) out() string            { return r.stdout + r.stderr }

func composeCHShims(g *guard, dir string, o composeCHOpts) string {
	// Temporary: the check runs the Python renderer through a real python3 until its owner ports it to Go. A missing
	// interpreter or script must fail the guard, never skip it.
	g.expect(composeRealBin("python3") != "", "python3 is not installed: the check's canonical render cannot run, and this guard must not skip")
	g.expect(exists(g.path(composeCHRenderer)), "%s is missing: the check's canonical render cannot run", composeCHRenderer)
	bin := filepath.Join(dir, "bin")
	argv, envLog := filepath.Join(dir, "child-argv.log"), filepath.Join(dir, "child-env.log")
	args, stdin := filepath.Join(dir, "docker-args.log"), filepath.Join(dir, "docker-stdin.log")
	for _, name := range composeCHCommands {
		real := composeRealBin(name)
		if real == "" {
			g.failf("no %s in /usr/bin or /bin", name)
			continue
		}
		composeWrite(g, filepath.Join(bin, name), fmt.Sprintf("#!/bin/bash\n{ echo \"== %s\"; printf \"%%s\\n\" \"$@\"; } >> \"%s\"\n{ echo \"== %s\"; /usr/bin/env; } >> \"%s\"\nexec %s \"$@\"\n",
			name, argv, name, envLog, real), 0o755)
	}
	login := "exit 1"
	if !o.loginFail {
		login = fmt.Sprintf(`if [ "$(cat "%s")" = "<clickhouse><user>dho_api_ch</user><password>%s</password></clickhouse>" ]; then echo dho_api_ch; exit 0; fi; exit 1`, stdin, composeCHPassword)
	}
	answer := "true"
	if !o.noCount {
		c := o.count
		if c == "" {
			c = "1"
		}
		answer = fmt.Sprintf("echo %q", c)
	}
	composeWrite(g, filepath.Join(bin, "docker"), fmt.Sprintf(`#!/bin/bash
PATH=/usr/bin:/bin
echo "$*" >> "%s"
{ echo "== docker"; printf "%%s\n" "$@"; } >> "%s"
{ echo "== docker"; env; } >> "%s"
case "$*" in *"--config-file /dev/stdin"*) cat > "%s" ;; esac
case "$*" in *"--user dho_api_ch"*|*"--config-file /dev/stdin"*)
  %s ;;
esac
%s
exit %d
`, args, argv, envLog, stdin, login, answer, o.rc), 0o755)
	return bin
}

func composeCHRunCheck(g *guard, o composeCHOpts) composeCHRun {
	dir := composeTempDir(g, g.tmp, "run-")
	bin := composeCHShims(g, dir, o)
	scratch := o.scratch
	if scratch == "" {
		scratch = composeCHScratch(g, 0o700)
		defer os.RemoveAll(scratch)
	}
	creds := filepath.Join(dir, "go-api.creds")
	body := o.creds
	if body == "" {
		body = "API_CH_PASSWORD='" + composeCHPassword + "'\n"
	}
	composeWrite(g, creds, body, 0o600)
	check := g.path(composeCHCheck)
	if o.direct {
		env := []string{"PATH=" + bin, "TMPDIR=" + scratch, "DHO_API_CH_POSTURE_GO=" + o.posture}
		r, err := composeExec(dir, env, 30*time.Second, "/bin/bash", check, o.file, creds)
		if err != nil {
			g.failf("run %s: %v", composeCHCheck, err)
		}
		return composeCHRun{dir, r}
	}
	root := filepath.Join(dir, "bigboy-root")
	composeWrite(g, filepath.Join(root, "_records", "images.env"), "", 0o644)
	composeWrite(g, filepath.Join(root, "ops", ".env"), "", 0o644)
	env := append([]string{"PATH=" + bin, "BIGBOY_ROOT=" + root, "XT=" + filepath.Join(dir, "xtrace.log"),
		"BIGBOY_OPERATOR_IMAGE=ghcr.io/full-chaos/dev-health-go-operator@sha256:" + strings.Repeat("a", 64)}, composeVarTrap(g, dir)...)
	tmpdir := scratch
	if o.tmpdir != nil {
		tmpdir = *o.tmpdir
	}
	env = append(env, "TMPDIR="+tmpdir)
	if o.opsRepo == "" {
		manifest := filepath.Join(dir, "authorization.go")
		composeWrite(g, manifest, composeCHManifest, 0o644)
		env = append(env, "DHO_API_CH_POSTURE_GO="+manifest)
	} else {
		env = append(env, "BIGBOY_OPS_REPO="+o.opsRepo)
	}
	args := []string{"-c", `umask 077; exec 9>"$XT"; BASH_XTRACEFD=9 exec /bin/bash -x "$0" "$@"`, check}
	if !o.noFile {
		args = append(args, o.file, creds)
	}
	r, err := composeExec(dir, env, 60*time.Second, "/bin/bash", args...)
	if err != nil {
		g.failf("run %s: %v", composeCHCheck, err)
	}
	return composeCHRun{dir, r}
}

func composeCHCanon(g *guard) string { return composeCHUsers(g, 0o644, composeCHXML(composeCHHash)) }

func composeCHPassesLive(g *guard) {
	r := composeCHRunCheck(g, composeCHOpts{file: composeCHCanon(g)})
	g.expect(r.code == 0, "a canonical file and a live user exit %d, want 0: %s", r.code, r.stderr)
	for _, want := range []string{"ch_api_user_file=ok", "ch_api_user_live=ok", "ch_api_user_auth=ok"} {
		g.expect(strings.Contains(r.stdout, want), "the check does not print %s", want)
	}
	g.expect(!strings.Contains(r.out(), composeCHHash), "the check prints the password hash")
}

func composeCHRefuses0600(g *guard) {
	r := composeCHRunCheck(g, composeCHOpts{file: composeCHUsers(g, 0o600, composeCHXML(composeCHHash))})
	g.expect(r.code == 1 && strings.Contains(r.stderr, "not world-readable") && strings.Contains(r.stderr, "uid 101"),
		"a 0600 users file exits %d, want 1 naming not world-readable and uid 101: %s", r.code, r.stderr)
	g.expect(!strings.Contains(r.out(), composeCHHash), "the check prints the password hash")
}

func composeCHRefusesMissingOrDirectory(g *guard) {
	r := composeCHRunCheck(g, composeCHOpts{file: filepath.Join(g.tmp, "absent.xml")})
	g.expect(r.code == 1, "a missing users file exits %d, want 1", r.code)
	d := composeTempDir(g, g.tmp, "made-by-docker-")
	r = composeCHRunCheck(g, composeCHOpts{file: d})
	g.expect(r.code == 1 && strings.Contains(r.stderr, "not a regular file"), "a directory as users file exits %d, want 1 naming not a regular file: %s", r.code, r.stderr)
	r = composeCHRunCheck(g, composeCHOpts{noFile: true})
	g.expect(r.code == 1, "no users file argument exits %d, want 1", r.code)
}

func composeCHUserNotLive(g *guard) {
	file := composeCHCanon(g)
	for _, o := range []composeCHOpts{{file: file, count: "0"}, {file: file, noCount: true, rc: 1}} {
		r := composeCHRunCheck(g, o)
		g.expect(r.code == 2 && strings.Contains(r.stderr, "CH_API_USER_LIVE_FAIL") && strings.Contains(r.stderr, "516"),
			"a user missing from the live server (count=%q) exits %d, want 2 with CH_API_USER_LIVE_FAIL and 516: %s", o.count, r.code, r.stderr)
	}
}

func composeCHCannotLogIn(g *guard) {
	file := composeCHCanon(g)
	r := composeCHRunCheck(g, composeCHOpts{file: file, loginFail: true})
	g.expect(r.code == 3 && strings.Contains(r.stderr, "CH_API_USER_AUTH_FAIL") && strings.Contains(r.stderr, "516"),
		"a user that cannot log in exits %d, want 3 with CH_API_USER_AUTH_FAIL and 516: %s", r.code, r.stderr)
	r = composeCHRunCheck(g, composeCHOpts{file: file, creds: "OTHER=1\n"})
	g.expect(r.code == 3 && strings.Contains(r.stderr, "no API_CH_PASSWORD value"), "creds without API_CH_PASSWORD exit %d, want 3 naming it: %s", r.code, r.stderr)
}

func composeCHCredentialOnlyOnStdin(g *guard) {
	r := composeCHRunCheck(g, composeCHOpts{file: composeCHCanon(g)})
	g.expect(r.code == 0, "the canonical run exits %d, want 0: %s", r.code, r.stderr)
	argv, childEnv, xtrace, vars, stdin, args := r.log("child-argv.log"), r.log("child-env.log"), r.log("xtrace.log"), r.log("vars.log"), r.log("docker-stdin.log"), r.log("docker-args.log")
	g.expect(strings.Contains(vars, "declare -- CREDS="), "the variable dump is empty: no proof")
	if info, err := os.Stat(filepath.Join(r.dir, "xtrace.log")); err != nil || info.Mode().Perm() != 0o600 {
		g.failf("xtrace.log is not mode 0600")
	}
	for _, c := range []string{"== docker", "== sed", "== sha256sum"} {
		g.expect(strings.Contains(argv, c), "the argv log has no %q: no proof", c)
	}
	g.expect(strings.Contains(xtrace, "+ ") && strings.Contains(xtrace, "api_password"), "xtrace is empty: this guard would prove nothing")
	g.expect(strings.Contains(stdin, composeCHPassword), "the login config never reached docker's stdin")
	for _, c := range []struct{ name, text string }{
		{"argv", argv}, {"child env", childEnv}, {"xtrace", xtrace}, {"shell variables", vars},
		{"stdout", r.stdout}, {"stderr", r.stderr}, {"docker-args", args},
	} {
		g.expect(!strings.Contains(c.text, composeCHPassword), "credential in %s", c.name)
	}
	g.expect(!strings.Contains(childEnv+argv, "CLICKHOUSE_PASSWORD"), "CLICKHOUSE_PASSWORD reaches a child")
	for _, line := range strings.Split(childEnv, "\n") {
		if strings.HasPrefix(line, "API_CH_PASSWORD=") {
			g.expect(line == composeCHPlace, "real password in a child env (API_CH_PASSWORD is not the placeholder)")
		}
	}
	g.expect(strings.Contains(args, "--config-file /dev/stdin") && !strings.Contains(args, " -e "), "docker gets no --config-file /dev/stdin or gets a -e: %s", args)
	for _, line := range strings.Split(strings.TrimSpace(args), "\n") {
		g.expect(strings.HasPrefix(line, "compose -p dev-health "), "docker call is not a compose verb of project dev-health: %s", line)
		if strings.Contains(" "+line+" ", " exec ") {
			g.expect(strings.Contains(line, " exec -T clickhouse clickhouse-client "), "docker exec is not on the service clickhouse: %s", line)
		} else {
			g.expect(strings.Contains(line, " config --images") || strings.Contains(" "+line+" ", " ps "), "unexpected docker call: %s", line)
		}
	}
}

func composeCHScratchUnderTmpdir(g *guard) {
	scratch := composeCHScratch(g, 0o700)
	defer os.RemoveAll(scratch)
	r := composeCHRunCheck(g, composeCHOpts{file: composeCHCanon(g), scratch: scratch})
	g.expect(r.code == 0, "the canonical run exits %d, want 0: %s", r.code, r.stderr)
	real, _ := filepath.EvalSymlinks(scratch)
	var mktemp []string
	var chmods [][]string
	for _, call := range strings.Split(r.log("child-argv.log"), "== ") {
		parts := strings.Split(call, "\n")
		switch parts[0] {
		case "mktemp":
			if mktemp == nil {
				mktemp = parts
			}
		case "chmod":
			chmods = append(chmods, parts)
		}
	}
	g.expect(len(mktemp) >= 4 && equal(mktemp[1:4], []string{"-d", "-p", real}), "mktemp is not run as mktemp -d -p %s: %v", real, mktemp)
	g.expect(len(chmods) > 0, "the scratch dir is never chmod 700")
	for _, c := range chmods {
		g.expect(len(c) > 2 && c[1] == "700" && strings.HasPrefix(c[2], real+"/"), "chmod is not 700 on a dir under the scratch: %v", c)
	}
	left, _ := os.ReadDir(scratch)
	g.expect(len(left) == 0, "scratch not cleaned: %d entries left", len(left))
}

func composeCHRefusesTmpTmpdir(g *guard) {
	file := composeCHCanon(g)
	dir := composeTempDir(g, g.tmp, "spell-")
	under := "/tmp/dho-sub-" + filepath.Base(dir)
	link := filepath.Join(dir, "link-to-tmp")
	_ = os.Symlink("/tmp", link)
	shmOpen := composeCHScratch(g, 0o500)
	holder := composeCHScratch(g, 0o700)
	defer os.RemoveAll(shmOpen)
	defer os.RemoveAll(holder)
	_ = os.Symlink(under, filepath.Join(holder, "lnk"))
	depth := len(strings.Split(strings.Trim(holder, "/"), "/"))
	spellings := []string{
		"/tmp//", "//tmp", "/tmp/.", "/tmp", "/var/tmp", under,
		under + "/../" + filepath.Base(under), link,
		filepath.Join(holder, "lnk"),
		holder + "/" + strings.Repeat("../", depth) + "tmp/" + filepath.Base(under),
		"/" + under, link + "/",
		filepath.Join(dir, "absent"), "", shmOpen, "",
	}
	for _, s := range spellings {
		s := s
		r := composeCHRunCheck(g, composeCHOpts{file: file, tmpdir: &s})
		g.expect(r.code == 1 && strings.Contains(r.stderr, "TMPDIR"), "TMPDIR %q was not refused (exit %d): %s", s, r.code, r.stderr)
		g.expect(!r.has("docker-args.log"), "TMPDIR %q reached the live checks", s)
	}
}

func composeCHDifferences() [][2]string {
	x := composeCHXML(composeCHHash)
	first := "<query>GRANT SELECT, INSERT, ALTER DELETE ON default.teams</query>"
	return [][2]string{
		{"a comment", strings.Replace(x, "<users>", "<!-- note --><users>", 1)},
		{"the credential in a comment", strings.Replace(x, "</clickhouse>", "<!-- "+composeCHPassword+" --></clickhouse>", 1)},
		{"a malformed grant", strings.Replace(x, first, "<query>GRANT SELECT ON</query>", 1)},
		{"an extra well-formed grant", strings.Replace(x, "</grants>", "<query>GRANT SELECT ON default.other</query></grants>", 1)},
		{"a hand edit of the network", strings.Replace(x, "::/0", "127.0.0.1", 1)},
		{"a hash for another password", strings.Replace(x, composeCHHash, composeSHA("another-password"), 1)},
		{"an attribute", strings.Replace(x, "<networks>", `<networks x="attr-value">`, 1)},
		{"text in a container", strings.Replace(x, "<grants>", "<grants>STRAY", 1)},
		{"a plaintext password element", strings.Replace(x, "</dho_api_ch>", "<password>hunter2</password></dho_api_ch>", 1)},
		{"a uri", strings.Replace(x, "</clickhouse>", "<!-- clickhouse://u:pw@h:9000/db --></clickhouse>", 1)},
		{"a truncated file", strings.Replace(x, "</clickhouse>", "", 1)},
		{"a second user", strings.Replace(x, "</users>", "<other><password_sha256_hex>x</password_sha256_hex></other></users>", 1)},
		{"trailing whitespace", x + "\n"},
		{"an empty file", ""},
	}
}

func composeCHOnlyCanonicalIsValid(g *guard) {
	for _, d := range composeCHDifferences() {
		r := composeCHRunCheck(g, composeCHOpts{file: composeCHUsers(g, 0o644, d[1])})
		g.expect(r.code == 4 && strings.Contains(r.stderr, "CH_API_USER_FILE_MISMATCH"), "a users file with %s exits %d, want 4 with CH_API_USER_FILE_MISMATCH: %s", d[0], r.code, r.stderr)
		for _, secret := range []string{composeCHPassword, composeCHHash, "hunter2", "pw@", "attr-value", "STRAY"} {
			g.expect(!strings.Contains(r.out(), secret), "a users file with %s: the output carries %q", d[0], secret)
		}
		g.expect(!r.has("docker-args.log"), "a users file with %s: the live checks ran on a file that is not canonical", d[0])
	}
}

func composeCHCanonicalPrintsNames(g *guard) {
	r := composeCHRunCheck(g, composeCHOpts{file: composeCHCanon(g)})
	g.expect(r.code == 0 && strings.Contains(r.stdout, "canonical=yes"), "the canonical file exits %d without canonical=yes: %s", r.code, r.stdout)
	g.expect(!strings.Contains(r.out(), composeCHHash), "the check prints the password hash")
}

func composeCHManifestRefusedNotSkipped(g *guard) {
	file := composeCHCanon(g)
	creds := "API_CH_PASSWORD=" + composeCHPassword + "\n"
	r := composeCHRunCheck(g, composeCHOpts{direct: true, file: file, creds: creds, posture: filepath.Join(g.tmp, "absent.go")})
	g.expect(r.code == 1 && strings.Contains(r.stderr, "canonical render cannot be computed"), "an absent posture manifest exits %d, want 1 naming canonical render cannot be computed: %s", r.code, r.stderr)
	g.expect(!r.has("docker-args.log"), "an absent posture manifest reached the live checks")
	empty := filepath.Join(composeTempDir(g, g.tmp, "empty-"), "empty.go")
	composeWrite(g, empty, "package clickhouse\n", 0o644)
	r = composeCHRunCheck(g, composeCHOpts{direct: true, file: file, creds: creds, posture: empty})
	g.expect(r.code == 1 && strings.Contains(r.stderr, "canonical render could not be computed"), "a manifest without APIPosture exits %d, want 1 naming canonical render could not be computed: %s", r.code, r.stderr)
	g.expect(!r.has("docker-args.log"), "a manifest without APIPosture reached the live checks")
}

func composeCHManifestFromOpsRepo(g *guard) {
	repo := filepath.Join(composeTempDir(g, g.tmp, "repo-"), "an ops clone")
	composeWrite(g, filepath.Join(repo, "internal", "storage", "clickhouse", "authorization.go"), composeCHManifest, 0o644)
	r := composeCHRunCheck(g, composeCHOpts{file: composeCHCanon(g), opsRepo: repo})
	g.expect(r.code == 0 && strings.Contains(r.stdout, "ch_api_user_file=ok"), "the manifest is not read from BIGBOY_OPS_REPO (exit %d): %s", r.code, r.stderr)
}

func composeCHCloneWithoutManifest(g *guard) {
	repo := filepath.Join(composeTempDir(g, g.tmp, "repo-"), "empty clone")
	_ = os.MkdirAll(repo, 0o755)
	r := composeCHRunCheck(g, composeCHOpts{file: composeCHCanon(g), opsRepo: repo})
	g.expect(r.code == 1 && strings.Contains(r.stderr, repo), "a clone without the manifest exits %d, want 1 naming the clone %s: %s", r.code, repo, r.stderr)
}

func composeCHComposeMount(g *guard) {
	users := filepath.Join(g.tmp, "dho_api_ch.xml")
	r := composeMust(g, composeIn{env: composeEnv(map[string]string{"DHO_API_CH_USERS_XML": users})})
	var mounts []any
	for _, v := range list(r.services["clickhouse"]["volumes"]) {
		if strings.HasSuffix(str(at(v, "target")), "users.d/dho_api_ch.xml") {
			mounts = append(mounts, v)
		}
	}
	if len(mounts) != 1 {
		g.failf("clickhouse mounts %d users.d/dho_api_ch.xml files, want 1", len(mounts))
	} else {
		g.expect(str(at(mounts[0], "source")) == users, "the users.d mount source is %q, want the DHO_API_CH_USERS_XML path", str(at(mounts[0], "source")))
		g.expect(at(mounts[0], "read_only") == true, "the users.d mount is not read_only")
	}
	env := composeEnv(nil)
	delete(env, "DHO_API_CH_USERS_XML")
	missing := composeRender(g, composeIn{env: env, pins: composeDropLine("DHO_API_CH_USERS_XML=")})
	g.expect(missing.err != "" && strings.Contains(missing.err, "DHO_API_CH_USERS_XML"), "without DHO_API_CH_USERS_XML the render does not fail naming it: the path is required, never defaulted")
}

func composeCHCutChecksBeforeUp(g *guard) {
	text := g.read(composeCHCut)
	lines := strings.Split(text, "\n")
	check, up := -1, -1
	for i, l := range lines {
		if check < 0 && strings.Contains(l, "check-dho-api-ch-user.sh") {
			check = i
		}
		if up < 0 && strings.Contains(l, "up -d --no-deps --no-build query-api go-api web") {
			up = i
		}
	}
	if check < 0 || up < 0 || check > up {
		g.failf("the dho_api_ch check (line %d) must run before go-api is recreated (line %d)", check+1, up+1)
		return
	}
	gate := strings.Join(lines[check:min(check+3, len(lines))], "\n")
	g.expect(strings.Contains(gate, "exit 1") && strings.Contains(gate, "ABORTING before go-api"), "the check is not followed by an exit 1 ABORTING before go-api")
	g.expect(!strings.Contains(text, "compose.bigboy.clickhouse-users.yml"), "the cut names the retired clickhouse-users overlay")
	g.expect(strings.Contains(text, `-f "$HERE/compose.bigboy.yml"`), "the cut does not name the one compose file")
	exported := false
	for _, l := range lines {
		exported = exported || strings.HasPrefix(l, "export DHO_API_CH_USERS_XML=")
	}
	g.expect(exported, "the cut does not export DHO_API_CH_USERS_XML")
}

type composeCutOpts struct {
	checkRC       int
	opsRepo       string
	cwd           string
	credsRelative bool
	extraEnv      []string
	fakeGenerator bool
}

type composeCutRun struct {
	dir   string
	calls []string
	scriptRun
}

func (r composeCutRun) log(name string) string { return composeReadFile(filepath.Join(r.dir, name)) }

func composeCutDockerStub(envLog string) string {
	digest := func(c string) string { return "sha256:" + strings.Repeat(c, 64) }
	answer := func(d string) string {
		raw := `{"manifests":[{"digest":"` + d + `","platform":{"os":"linux","architecture":"arm64"}}]}`
		return fmt.Sprintf("  if [[ \"$args\" == *\"--raw\"* ]]; then\n    echo '%s'; exit 0\n  fi\n  echo '{\"digest\":\"%s\"}'; exit 0\n", raw, d)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "#!/usr/bin/env bash\nenv >> %q\nset -u\nargs=\"$*\"\n", envLog)
	b.WriteString("[ -z \"${DOCKER_STUB_ARGS_LOG:-}\" ] || printf \"%s\\n\" \"$*\" >> \"$DOCKER_STUB_ARGS_LOG\"\n")
	b.WriteString("if [[ \"$args\" == *\"--no-deps migrate\"* ]]; then\n  exit 9\nfi\n")
	fmt.Fprintf(&b, "if [[ \"$args\" == *\"dev-health-go-dho:sha-%s\"* ]]; then\n%sfi\n", composeCHOld8[:7], answer(digest("1")))
	for _, img := range []struct{ name, d string }{
		{"dev-health-go-dho", digest("2")}, {"dev-health-go-api-tools", digest("2")}, {"dev-health-go-operator", digest("2")}, {"dev-health-web", digest("4")},
	} {
		fmt.Fprintf(&b, "if [[ \"$args\" == *\"%s:sha-\"* ]]; then\n%sfi\n", img.name, answer(img.d))
	}
	b.WriteString("if [[ \"$args\" == *\"venue-tools\"* ]]; then\n  cat <<'EOF_BODY'\nGOAPI_ROUTING_JSON {\"reason\": \"digest_unchanged\"}\nEOF_BODY\n  exit 0\nfi\nexit 0\n")
	return b.String()
}

func composeCutRunReal(g *guard, o composeCutOpts) composeCutRun {
	dir := composeTempDir(g, g.tmp, "cut-")
	root := filepath.Join(dir, "bigboy-root")
	old := "sha256:" + strings.Repeat("1", 64)
	composeWrite(g, filepath.Join(root, "_records", "images.env"),
		"BIGBOY_DHO_IMAGE=ghcr.io/full-chaos/dev-health-go-dho@"+old+"\nBIGBOY_API_TOOLS_IMAGE=ghcr.io/full-chaos/dev-health-go-api-tools@"+old+
			"\nBIGBOY_OPERATOR_IMAGE=ghcr.io/full-chaos/dev-health-go-operator@"+old+"\nBIGBOY_WEB_IMAGE=ghcr.io/full-chaos/dev-health-web@sha256:"+strings.Repeat("3", 64)+"\n", 0o644)
	composeWrite(g, filepath.Join(root, "ops", ".env"), "", 0o644)
	composeWrite(g, filepath.Join(root, "_records", "bigboy-"+composeCHOld8, "bigboy-rest-commands.sh"), "#!/usr/bin/env bash\nSTEP=placeholder\nB="+strings.Repeat("0", 40)+"\n", 0o644)
	creds := filepath.Join(root, ".go-api-dev", "go-api.creds")
	composeWrite(g, creds, "API_CH_PASSWORD='"+composeCHSentinel+"'\n", 0o600)
	tools := filepath.Join(dir, "tools")
	_ = os.MkdirAll(tools, 0o755)
	entries, _ := os.ReadDir(g.path("ci/bigboy"))
	for _, e := range entries {
		if e.Type().IsRegular() && e.Name() != filepath.Base(composeCHCheck) {
			_ = os.Symlink(g.path("ci/bigboy/"+e.Name()), filepath.Join(tools, e.Name()))
		}
	}
	composeWrite(g, filepath.Join(tools, filepath.Base(composeCHCheck)),
		fmt.Sprintf("#!/usr/bin/env bash\necho fake-check\n{ echo \"args=$*\"; env; } >> %q\nexit %d\n", filepath.Join(dir, "fake-check.log"), o.checkRC), 0o755)
	stub := filepath.Join(dir, "stubbin")
	composeWrite(g, filepath.Join(stub, "docker"), composeCutDockerStub(filepath.Join(dir, "docker-env.log")), 0o755)
	composeWrite(g, filepath.Join(stub, "gh"), "#!/usr/bin/env bash\necho "+strings.Repeat("c", 40)+"\n", 0o755)
	// The cut waits 45 s for go-api after `up`; nothing asserted here depends on the wait.
	composeWrite(g, filepath.Join(stub, "sleep"), "#!/usr/bin/env bash\nexit 0\n", 0o755)
	if o.fakeGenerator {
		composeWrite(g, filepath.Join(stub, "python3"), fmt.Sprintf("#!/usr/bin/env bash\ncase \"$1\" in */generate-plane-split-router.py) shift; printf '%%s\\n' \"$@\" > %q; exit 1 ;; esac\nexec /usr/bin/python3 \"$@\"\n",
			filepath.Join(dir, "generator-args.log")), 0o755)
	}
	opsRepo := o.opsRepo
	if opsRepo == "" {
		opsRepo = g.root
	}
	cwd := o.cwd
	if cwd == "" {
		cwd = dir
	}
	args := filepath.Join(dir, "docker-args.log")
	env := append([]string{"PATH=" + stub + ":/usr/bin:/bin", "BIGBOY_ROOT=" + root, "BIGBOY_OPS_REPO=" + opsRepo,
		"BIGBOY_TOOLS_DIR=" + tools, "DOCKER_STUB_ARGS_LOG=" + args, "TMPDIR=" + dir}, composeVarTrap(g, dir)...)
	if o.credsRelative {
		rel, _ := filepath.Rel(cwd, creds)
		env = append(env, "DHO_API_CH_CREDS="+rel)
	}
	env = append(env, o.extraEnv...)
	r, err := composeExec(cwd, env, 120*time.Second, "bash", g.path(composeCHCut), composeCHOld8, composeCHNew)
	if err != nil {
		g.failf("run %s: %v", composeCHCut, err)
	}
	var calls []string
	if t := strings.TrimSpace(composeReadFile(args)); t != "" {
		calls = strings.Split(t, "\n")
	}
	return composeCutRun{dir, calls, r}
}

func composeCutUpsGoAPI(calls []string) bool {
	for _, c := range calls {
		words := composeSet(strings.Fields(c)...)
		if words["up"] && words["go-api"] {
			return true
		}
	}
	return false
}

func composeCutCredentialToNoChild(g *guard) {
	r := composeCutRunReal(g, composeCutOpts{})
	g.expect(strings.Contains(r.stdout, "STEP ch-api-user rc=0"), "the cut did not reach the ch-api-user STEP: %s%s", r.stdout, r.stderr)
	check := r.log("fake-check.log")
	g.expect(strings.Contains(check, "args=") && strings.Contains(check, "go-api.creds"), "the check is not handed the credentials path")
	g.expect(strings.Contains(r.log("vars.log"), "declare -- R="), "no variable dump: no proof")
	for _, name := range []string{"fake-check.log", "docker-env.log", "vars.log"} {
		text := r.log(name)
		g.expect(strings.TrimSpace(text) != "", "%s is empty: this guard would prove nothing", name)
		g.expect(!strings.Contains(text, composeCHSentinel), "credential in %s", name)
	}
	g.expect(!strings.Contains(r.stdout+r.stderr, composeCHSentinel), "credential in the cut's output")
}

func composeCutAbortsOnFailedCheck(g *guard) {
	r := composeCutRunReal(g, composeCutOpts{checkRC: 2})
	g.expect(strings.Contains(r.stdout, "STEP ch-api-user rc=2"), "the cut did not report STEP ch-api-user rc=2: %s", r.stderr)
	g.expect(r.code != 0, "the cut exits 0 after a failed check")
	g.expect(strings.Contains(r.stderr, "ABORTING before go-api is recreated"), "the cut does not say ABORTING before go-api is recreated")
	g.expect(!composeCutUpsGoAPI(r.calls), "the cut recreated go-api (compose up) after a failed check")
	g.expect(!strings.Contains(r.stdout, "STEP up rc="), "the cut ran STEP up after a failed check")
}

func composeCutRecreatesOnPassedCheck(g *guard) {
	r := composeCutRunReal(g, composeCutOpts{})
	g.expect(strings.Contains(r.stdout, "STEP ch-api-user rc=0"), "the cut did not report STEP ch-api-user rc=0: %s", r.stderr)
	if !strings.Contains(r.stdout, "STEP up rc=") {
		g.failf("the cut did not run STEP up after a passed check: %s", r.stdout)
		return
	}
	g.expect(composeCutUpsGoAPI(r.calls), "the cut issued no compose up of go-api after a passed check")
}

func composeCutAbsoluteOpsRepo(g *guard) {
	r := composeCutRunReal(g, composeCutOpts{opsRepo: filepath.Base(g.root), cwd: filepath.Dir(g.root)})
	g.expect(strings.Contains(r.stdout, "STEP ch-api-user rc=0"), "the cut did not reach the ch-api-user STEP: %s%s", r.stdout, r.stderr)
	var seen []string
	for _, l := range strings.Split(r.log("fake-check.log"), "\n") {
		if strings.HasPrefix(l, "BIGBOY_OPS_REPO=") {
			seen = append(seen, l)
		}
	}
	g.expect(equal(seen, []string{"BIGBOY_OPS_REPO=" + g.root}), "the check sees %v, want the absolute BIGBOY_OPS_REPO=%s", seen, g.root)
}

func composeCutAbsoluteCreds(g *guard) {
	dir := composeTempDir(g, g.tmp, "cwd-")
	r := composeCutRunReal(g, composeCutOpts{credsRelative: true, cwd: dir})
	g.expect(strings.Contains(r.stdout, "STEP ch-api-user rc=0"), "the cut did not reach the ch-api-user STEP: %s%s", r.stdout, r.stderr)
	var args []string
	for _, l := range strings.Split(r.log("fake-check.log"), "\n") {
		if strings.HasPrefix(l, "args=") {
			args = append(args, l)
		}
	}
	if len(args) != 1 {
		g.failf("the check ran %d times, want 1: %v", len(args), args)
		return
	}
	for _, p := range strings.Fields(strings.TrimPrefix(args[0], "args=")) {
		g.expect(strings.HasPrefix(p, "/"), "the check is handed a path that is not absolute: %s", p)
	}
}

func composeCutAbsoluteDeployRepo(g *guard) {
	dir := composeTempDir(g, g.tmp, "cwd-")
	r := composeCutRunReal(g, composeCutOpts{cwd: dir, fakeGenerator: true,
		extraEnv: []string{"DEPLOY_REPO=deploy checkout", "DEPLOY_SHA=" + strings.Repeat("d", 40)}})
	g.expect(strings.Contains(r.stdout, "STEP deploy-pin"), "the cut did not run STEP deploy-pin: %s", r.stderr)
	seen := strings.Split(r.log("generator-args.log"), "\n")
	got := ""
	for i, a := range seen {
		if a == "--deploy-repo" && i+1 < len(seen) {
			got = seen[i+1]
		}
	}
	want := filepath.Join(dir, "deploy checkout")
	if !exists(filepath.Join(r.dir, "generator-args.log")) {
		g.failf("the cut never ran the plane-split generator: %s", r.stdout)
		return
	}
	g.expect(got == want, "the generator gets --deploy-repo %q, want the absolute %q", got, want)
}

var composeCHChecks = []check{
	{"a_readable_declared_file_and_a_live_user_pass", composeCHPassesLive},
	{"a_0600_file_is_refused_because_clickhouse_would_exit_on_it", composeCHRefuses0600},
	{"a_missing_file_or_a_directory_is_refused", composeCHRefusesMissingOrDirectory},
	{"a_user_missing_from_the_live_server_fails_with_its_own_code", composeCHUserNotLive},
	{"the_compose_file_mounts_the_host_file_into_users_d_and_requires_the_path", composeCHComposeMount},
	{"the_cut_checks_dho_api_ch_before_it_recreates_go_api", composeCHCutChecksBeforeUp},
	{"a_user_that_cannot_log_in_with_the_api_credential_is_refused", composeCHCannotLogIn},
	{"the_api_credential_is_only_ever_on_dockers_stdin", composeCHCredentialOnlyOnStdin},
	{"the_scratch_is_made_under_the_callers_tmpdir_with_mode_0700", composeCHScratchUnderTmpdir},
	{"a_tmpdir_in_or_resolving_into_tmp_or_not_private_is_refused", composeCHRefusesTmpTmpdir},
	{"the_cut_hands_the_credentials_path_to_the_check_and_the_password_to_no_child", composeCutCredentialToNoChild},
	{"the_cut_aborts_before_go_api_up_when_the_check_fails", composeCutAbortsOnFailedCheck},
	{"the_cut_goes_on_to_recreate_go_api_when_the_check_passes", composeCutRecreatesOnPassedCheck},
	{"the_file_is_valid_only_if_it_equals_the_canonical_render", composeCHOnlyCanonicalIsValid},
	{"the_canonical_file_passes_and_prints_only_names", composeCHCanonicalPrintsNames},
	{"a_missing_posture_manifest_or_credential_is_refused_not_skipped", composeCHManifestRefusedNotSkipped},
	{"the_cut_hands_the_check_an_absolute_ops_repo_for_a_relative_setting", composeCutAbsoluteOpsRepo},
	{"the_cut_hands_the_check_absolute_paths_for_relative_settings", composeCutAbsoluteCreds},
	{"the_posture_manifest_is_read_from_bigboy_ops_repo", composeCHManifestFromOpsRepo},
	{"a_clone_without_the_manifest_is_refused_naming_the_clone", composeCHCloneWithoutManifest},
	{"the_cut_hands_the_plane_split_generator_an_absolute_deploy_repo", composeCutAbsoluteDeployRepo},
}

func TestComposeCHUsersReal(t *testing.T) { runReal(t, composeCHChecks...) }

func TestComposeCHUsersPlanted(t *testing.T) {
	chk := []string{composeCHCheck, composeCHRenderer}
	cut := []string{"ci/bigboy"}
	gitDir := func(t *testing.T, r string) {
		writeFile(t, r, ".git/HEAD", "ref: refs/heads/main\n")
		for _, p := range walkFiles(rootPath(r, "ci/bigboy")) {
			if err := os.Chmod(p, 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}
	m := func(old, repl string) func(*testing.T, string) { return mut(composeCHCheck, old, repl) }
	c := func(old, repl string) func(*testing.T, string) { return all(gitDir, mut(composeCHCut, old, repl)) }
	runPlanted(t, []planted{
		{name: "renderer script missing fails loudly", copy: []string{composeCHCheck}, check: composeCHPassesLive, want: "render-dho-api-ch-users.py is missing"},
		{name: "check prints the hash", copy: chk, check: composeCHPassesLive, want: "prints the password hash",
			mutate: m(`mode=$mode canonical=yes"`, `mode=$mode canonical=yes hash=$(cat "$canon_dir/real.hash")"`)},
		{name: "0600 accepted", copy: chk, check: composeCHRefuses0600, want: "0600",
			mutate: m("  *[4-7]) ;;", "  *[0-7]) ;;")},
		{name: "directory accepted", copy: chk, check: composeCHRefusesMissingOrDirectory, want: "directory",
			mutate: m(`[ -f "$FILE" ] && [ ! -L "$FILE" ] ||`, `[ -e "$FILE" ] ||`)},
		{name: "count 0 accepted", copy: chk, check: composeCHUserNotLive, want: `count="0"`,
			mutate: m(`[ "$count" = "1" ] ||`, `[ -n "$count" ] ||`)},
		{name: "login not proven", copy: chk, check: composeCHCannotLogIn, want: "cannot log in",
			mutate: m(`[ "$who" = "dho_api_ch" ] ||`, "true ||")},
		{name: "password in a shell variable", copy: chk, check: composeCHCredentialOnlyOnStdin, want: "credential in",
			mutate: m(`api_password | sha256sum | cut -d' ' -f1 > "$canon_dir/real.hash"`, `pw=$(api_password); printf %s "$pw" | sha256sum | cut -d' ' -f1 > "$canon_dir/real.hash"`)},
		{name: "scratch not under TMPDIR", copy: chk, check: composeCHScratchUnderTmpdir, want: "mktemp",
			mutate: m(`canon_dir=$(mktemp -d -p "$tmp_root")`, `canon_dir=$(mktemp -d)`)},
		{name: "tmp accepted", copy: chk, check: composeCHRefusesTmpTmpdir, want: `TMPDIR "/tmp`,
			mutate: all(m(`""|/|/tmp|/tmp/*|/var/tmp|/var/tmp/*)`, `"")`), m(`if [ "$(stat -c '%a %u' "$tmp_root")" != "700 $(id -u)" ]; then`, "if false; then"))},
		{name: "file grepped not compared", copy: chk, check: composeCHOnlyCanonicalIsValid, want: "a comment",
			mutate: m(`cmp -s "$FILE" "$canon_dir/canonical.xml" ||`, `grep -q dho_api_ch "$FILE" ||`)},
		{name: "canonical marker dropped", copy: chk, check: composeCHCanonicalPrintsNames, want: "canonical=yes",
			mutate: m(`mode=$mode canonical=yes"`, `mode=$mode"`)},
		{name: "manifest presence not checked", copy: chk, check: composeCHManifestRefusedNotSkipped, want: "cannot be computed",
			mutate: m(`[ -f "$POSTURE" ] ||`, "true ||")},
		{name: "ops repo ignored", copy: chk, check: composeCHManifestFromOpsRepo, want: "BIGBOY_OPS_REPO",
			mutate: m(`${DHO_API_CH_POSTURE_GO:-${BIGBOY_OPS_REPO:-$HERE/../..}/internal`, `${DHO_API_CH_POSTURE_GO:-$HERE/../../internal`)},
		{name: "refusal does not name the clone", copy: chk, check: composeCHCloneWithoutManifest, want: "empty clone",
			mutate: m("the posture manifest $POSTURE is not a file", "the posture manifest is not a file")},
		{name: "users path defaulted", copy: composeFiles, check: composeCHComposeMount, want: "never defaulted",
			mutate: mut(composeFile, "${DHO_API_CH_USERS_XML:?set DHO_API_CH_USERS_XML to the host path of dho_api_ch.xml}", "${DHO_API_CH_USERS_XML:-./.go-api-dev/dho_api_ch.xml}")},
		{name: "users mount writable", copy: composeFiles, check: composeCHComposeMount, want: "read_only",
			mutate: mut(composeFile, "    - read_only: true\n      source: ${DHO_API_CH_USERS_XML", "    - read_only: false\n      source: ${DHO_API_CH_USERS_XML")},
		{name: "cut gate lost its abort text", copy: []string{composeCHCut}, check: composeCHCutChecksBeforeUp, want: "ABORTING",
			mutate: mut(composeCHCut, "ABORTING before go-api is recreated", "stopping")},
		{name: "cut hands the password to the check", copy: cut, check: composeCutCredentialToNoChild, want: "credential in fake-check.log",
			mutate: c(`"$HERE/check-dho-api-ch-user.sh" "$DHO_API_CH_USERS_XML"`, `API_CH_PASSWORD="$(sed -n "s/^API_CH_PASSWORD='\(.*\)'$/\1/p" "$DHO_API_CH_CREDS")" "$HERE/check-dho-api-ch-user.sh" "$DHO_API_CH_USERS_XML"`)},
		{name: "cut goes on after a failed check", copy: cut, check: composeCutAbortsOnFailedCheck, want: "recreated go-api",
			mutate: c(`ABORTING before go-api is recreated" >&2; cat "$REC"/ch-api-user.out >&2; exit 1; }`, `continuing" >&2; }`)},
		{name: "cut never recreates go-api", copy: cut, check: composeCutRecreatesOnPassedCheck, want: "no compose up of go-api",
			mutate: c(`"${DC[@]}" up -d --no-deps --no-build query-api go-api web`, `true up -d --no-deps --no-build query-api go-api web`)},
		{name: "cut leaves the ops repo relative", copy: cut, check: composeCutAbsoluteOpsRepo, want: "sees [BIGBOY_OPS_REPO=",
			mutate: c("export BIGBOY_OPS_REPO=$OPS_REPO   #", "#")},
		{name: "cut leaves the creds path relative", copy: cut, check: composeCutAbsoluteCreds, want: "path that is not absolute: ",
			mutate: c(`DHO_API_CH_CREDS=$(abs_path "${DHO_API_CH_CREDS:-$R/.go-api-dev/go-api.creds}")`, `DHO_API_CH_CREDS=${DHO_API_CH_CREDS:-$R/.go-api-dev/go-api.creds}`)},
		{name: "cut leaves the deploy repo relative", copy: cut, check: composeCutAbsoluteDeployRepo, want: `--deploy-repo "deploy checkout"`,
			mutate: c(`DEPLOY_REPO=$(abs_path "${DEPLOY_REPO:-$R/deploy}")`, `DEPLOY_REPO=${DEPLOY_REPO:-$R/deploy}`)},
	})
}
