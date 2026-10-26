package devhealth_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-11606: the D2 release (phase 3 after stage goApi) defers the narrowing only while it carries the
// acknowledgement (dev-health.authDeferGrantNarrowing needs $ack). Without it no Job passes -defer-narrowing, so
// auth-migrate would narrow while old owned pods may still serve, and the release is refused.
func TestPhase3WithoutTheAcknowledgementRendersNoDefer(t *testing.T) {
	docs := renderPhase(t, 3)
	for _, job := range []string{"a-dev-health-auth-migrate", "a-dev-health-auth-grants"} {
		for _, arg := range jobArgs(t, authDoc(docs, "Job", job)) {
			if strings.HasPrefix(arg, "-ack-grant-narrowing") || arg == "-defer-narrowing" {
				t.Errorf("phase 3 without the acknowledgement: %s carries %s", job, arg)
			}
		}
	}
}

func runbookStep(t *testing.T, prefix string) string {
	t.Helper()
	raw, err := os.ReadFile("../../../docs/operate/runbooks/auth-users-owned-flip-and-rollback.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, prefix) {
			return line
		}
	}
	t.Fatalf("the runbook has no step %q", prefix)
	return ""
}

// The rollback steps D1, D2 and D3 are named in one form, and D2 and D3 say what the acknowledgement does.
func TestRollbackRunbookStepsD1D2D3(t *testing.T) {
	for step, wants := range map[string][]string{
		"**D1.**": {"usersOwnedStage: goApi", "acknowledgeGrantNarrowing"},
		"**D2.**": {"authService.phase: 3", "keep the acknowledgement", "-defer-narrowing", "refused"},
		"**D3.**": {"authService.phase: 3", "narrowAfterFlipBack: true", "acknowledgeGrantNarrowing"},
	} {
		line := runbookStep(t, step)
		for _, want := range wants {
			if !strings.Contains(line, want) {
				t.Errorf("runbook step %s lacks %q: %.200s", step, want, line)
			}
		}
	}
	raw, _ := os.ReadFile("../../../docs/operate/runbooks/auth-users-owned-flip-and-rollback.md")
	if !strings.Contains(string(raw), "false,false,false") {
		t.Error("the runbook does not give the expected readback false,false,false")
	}
}

// No credential on argv: a database readback never puts a DSN or password in a command line (ps, shell history). The
// runbook reads the grants with the one named read-only exec, SQL on stdin inside BEGIN READ ONLY ... ROLLBACK.
func TestUsersOwnedDocsPutNoDSNOnArgv(t *testing.T) {
	for _, path := range []string{
		"../../../docs/operate/runbooks/auth-users-owned-flip-and-rollback.md",
		"../../../docs/contribute/architecture/auth-control-plane-deployment.md",
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			if regexp.MustCompile(`psql\s+("?\$|.*(postgres(ql)?://|DSN|PASSWORD))`).MatchString(line) {
				t.Errorf("%s:%d puts a DSN or password on a psql command line: %.120s", path, i+1, line)
			}
		}
	}
	raw, _ := os.ReadFile("../../../docs/operate/runbooks/auth-users-owned-flip-and-rollback.md")
	for _, want := range []string{"exec -i", "psql -X -q -v ON_ERROR_STOP=1 -At", "BEGIN READ ONLY;", "ROLLBACK;"} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("the D3 readback lacks %q", want)
		}
	}
}
