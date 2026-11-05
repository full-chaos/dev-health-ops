package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-13039: ci/lib/auth_workload_session_issue_k3s.sh is the k3s form of ci/lib/auth_workload_session_issue.sh. These cells run it
// with a fake kubectl function and a fake psql function: no cluster, no docker. The bootstrap credential is a fixture string; no cell
// may find it in the output, on a command line or in the pod manifest.

const (
	wlLib        = "ci/lib/auth_workload_session_issue_k3s.sh"
	wlCredential = "fixture-bootstrap-credential-value"
	wlSubject    = "prn_AAAAAAAAAAAAAAAAAAAAAA"
)

const wlHarness = `set -u
source "${ROOT}/ci/lib/auth_workload_session_issue_k3s.sh"
K() {
  printf 'K %s\n' "$*" >>"${CALLS}"
  case "$*" in
    "apply -f -") cat >"${MANIFEST}" ;;
    exec*) printf '%s' "${CREDENTIAL}" ;;
    "create secret"*) cat >"${SECRET_STDIN}" ;;
    *) [ -t 0 ] || cat >/dev/null ;;
  esac
  return 0
}
pgq() { cat >"${SQL}"; printf '%s\n' "${SUBJECT}"; }
auth_workload_session_issue_k3s K pgq registry.example/auth-migrate@sha256:` + "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" + ` registry.example/postgres:18 go-api-workload
rc=$?
printf 'rc=%s subject=%s\n' "${rc}" "${AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT:-}"
`

type wlRun struct {
	res                                cmdResult
	calls, manifest, secretIn, sqlText string
}

func wlExec(t *testing.T, subject string) wlRun {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{"calls": "", "manifest": "", "secret": "", "sql": ""}
	for name := range files {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	env := []string{shimBasePath(), "ROOT=" + rootOf(t), "CALLS=" + filepath.Join(dir, "calls"), "MANIFEST=" + filepath.Join(dir, "manifest"),
		"SECRET_STDIN=" + filepath.Join(dir, "secret"), "SQL=" + filepath.Join(dir, "sql"), "CREDENTIAL=" + wlCredential, "SUBJECT=" + subject}
	res := cmdSpec{dir: rootOf(t), env: env}.run("bash", "-c", wlHarness)
	return wlRun{res, readText(t, filepath.Join(dir, "calls")), readText(t, filepath.Join(dir, "manifest")), readText(t, filepath.Join(dir, "secret")), readText(t, filepath.Join(dir, "sql"))}
}

func TestWorkloadSessionIssueK3sSeedsMakesTheSecretAndExportsTheSubject(t *testing.T) {
	r := wlExec(t, wlSubject)
	if !strings.Contains(r.res.stdout, "rc=0 subject="+wlSubject) {
		t.Fatalf("stdout %q, stderr %q: want rc=0 and the exported subject", r.res.stdout, r.res.stderr)
	}
	for _, want := range []string{"workloads, seed, -apply", "workloads, bootstrap-create, -workload, api, -out, /work/api.bootstrap, -apply",
		"runAsNonRoot: true", "emptyDir: {medium: Memory}", "name: auth-migrate-db", "registry.example/postgres:18", "automountServiceAccountToken: false"} {
		if !strings.Contains(r.manifest, want) {
			t.Errorf("the pod manifest lacks %q\n%s", want, r.manifest)
		}
	}
	if r.secretIn != wlCredential {
		t.Errorf("the Secret got %q on stdin, want the credential the pod file holds", r.secretIn)
	}
	for _, want := range []string{"K create secret generic go-api-workload --from-file=api.bootstrap=/dev/stdin", "K delete pod fb-wl-bootstrap"} {
		if !strings.Contains(r.calls, want) {
			t.Errorf("the calls lack %q\n%s", want, r.calls)
		}
	}
	if !strings.Contains(r.sqlText, "auth.workloads") || !strings.Contains(r.sqlText, "name = 'api'") {
		t.Errorf("the subject query is not ci/pdp_load_k3s/subject.sql: %q", r.sqlText)
	}
	for name, text := range map[string]string{"stdout": r.res.stdout, "stderr": r.res.stderr, "the calls": r.calls, "the manifest": r.manifest} {
		if strings.Contains(text, wlCredential) {
			t.Errorf("the credential appears in %s", name)
		}
	}
}

func TestWorkloadSessionIssueK3sRefusesASubjectThatIsNotAWirePrincipalId(t *testing.T) {
	for _, subject := range []string{"", "prn_short", "00000000-0000-0000-0000-000000000000", wlSubject + "x"} {
		r := wlExec(t, subject)
		if !strings.Contains(r.res.stdout, "rc=1 subject=") || strings.Contains(r.res.stdout, "subject="+wlSubject) || !strings.Contains(r.res.stderr, "ERROR:") {
			t.Errorf("subject %q: stdout %q, stderr %q: want rc=1, an ERROR line and no exported subject", subject, r.res.stdout, r.res.stderr)
		}
	}
}

func TestWorkloadSessionIssueK3sShape(t *testing.T) {
	text := readText(t, filepath.Join(rootOf(t), wlLib))
	for _, banned := range []string{"docker ", "nohup", "pkill", "--from-literal", "svc_authsess_", "envsubst", "--from-file=api.bootstrap=\""} {
		if strings.Contains(text, banned) {
			t.Errorf("the library contains %q", banned)
		}
	}
	for _, want := range []string{"PIPESTATUS", "pdp_load_k3s/subject.sql", "^prn_[A-Za-z0-9_-]{22}$"} {
		if !strings.Contains(text, want) {
			t.Errorf("the library lacks %q", want)
		}
	}
}
