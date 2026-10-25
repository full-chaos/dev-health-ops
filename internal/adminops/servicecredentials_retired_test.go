package adminops

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

const retiredServiceID = "00000000-0000-4000-8000-0000000000aa"

func requireRetiredRefusal(t *testing.T, code int, stdout, stderr string) {
	t.Helper()
	if code != cli.ExitFailure || stdout != "" {
		t.Fatalf("exit %d stdout %q stderr %q: want a refusal with nothing on stdout", code, stdout, stderr)
	}
	if !strings.Contains(stderr, `"service_credential_refused"`) || !strings.Contains(stderr, "retired") {
		t.Fatalf("stderr %q: want the retired-service refusal", stderr)
	}
	if regexp.MustCompile(`svc_worker_[A-Za-z0-9_-]{43}`).MatchString(stderr) {
		t.Fatalf("stderr %q names a token", stderr)
	}
}

func TestServiceCredentialCreateRefusesWorkerOperator(t *testing.T) {
	for _, scopes := range [][]string{
		{"--scope", "workers:read"},
		{"--scope", "workers:read", "--scope", "workers:operate"},
		{"--scope", "entitlements:read"},
		{"--scope", "nope"},
	} {
		code, stdout, stderr := runCredentialVerb(t, "create", append([]string{"--service", "worker-operator"}, scopes...)...)
		requireRetiredRefusal(t, code, stdout, stderr)
	}
}

func TestServiceCredentialRotateRefusesWorkerOperator(t *testing.T) {
	for _, extra := range [][]string{
		{"--scope", "workers:read"},
		{"--scope", "workers:read", "--scope", "workers:operate", "--overlap-seconds", "60"},
		{"--scope", "entitlements:read"},
	} {
		code, stdout, stderr := runCredentialVerb(t, "rotate", append([]string{retiredServiceID, "--service", "worker-operator"}, extra...)...)
		requireRetiredRefusal(t, code, stdout, stderr)
	}
}

func TestServiceCredentialRetiredRefusalComesAfterIDAndOverlapChecks(t *testing.T) {
	code, _, stderr := runCredentialVerb(t, "rotate", "xyz", "--service", "worker-operator", "--scope", "workers:read")
	if code != cli.ExitFailure || !strings.Contains(stderr, "badly formed hexadecimal UUID string") {
		t.Fatalf("bad id: exit %d %q", code, stderr)
	}
	code, _, stderr = runCredentialVerb(t, "rotate", retiredServiceID, "--service", "worker-operator", "--scope", "workers:read", "--overlap-seconds", "3601")
	if code != cli.ExitFailure || !strings.Contains(stderr, "--overlap-seconds must be between 0 and 3600") {
		t.Fatalf("bad overlap: exit %d %q", code, stderr)
	}
}

func TestServiceCredentialListAndRevokeOfWorkerOperatorReachTheDatabase(t *testing.T) {
	code, _, stderr := runCredentialVerb(t, "list", "--service", "worker-operator")
	if code != cli.ExitFailure || strings.Contains(stderr, "retired") {
		t.Fatalf("list: exit %d %q: list must not refuse worker-operator", code, stderr)
	}
	code, _, stderr = runCredentialVerb(t, "revoke", retiredServiceID)
	if code != cli.ExitFailure || strings.Contains(stderr, "retired") {
		t.Fatalf("revoke: exit %d %q: revoke must not refuse", code, stderr)
	}
}

func TestRefusalTableRowForWorkerOperatorMintIsAnsweredLive(t *testing.T) {
	raw, err := os.ReadFile("testdata/refusals.tsv")
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Split(line, "\t")
		if strings.HasPrefix(line, "#") || len(fields) != 4 || !strings.HasPrefix(fields[0], "service-credentials ") {
			continue
		}
		rows++
		for _, tc := range []struct {
			verb string
			args []string
		}{
			{"create", []string{"--service", "worker-operator", "--scope", "workers:read"}},
			{"rotate", []string{retiredServiceID, "--service", "worker-operator", "--scope", "workers:read"}},
		} {
			code, _, stderr := runCredentialVerb(t, tc.verb, tc.args...)
			if code != cli.ExitFailure || !strings.Contains(stderr, fields[2]) {
				t.Errorf("%s: exit %d stderr %q does not carry the table text %q", tc.verb, code, stderr, fields[2])
			}
		}
		if fields[3] != "CHAOS-10044" || !strings.Contains(fields[1], "src@a4847c5e") {
			t.Errorf("row %q lacks its ruling or its Python reference", fields[0])
		}
	}
	if rows != 1 {
		t.Fatalf("%d service-credentials rows, want 1", rows)
	}
}
