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

// sixStubRoutes reads the routes the not-owned auth-service answers with 503 + Retry-After 1 from the Go source
// (authapi.NotOwnedCredentialRoutes and the Pattern constants it names), so the runbook is pinned to the code.
func sixStubRoutes(t *testing.T) []string {
	t.Helper()
	src, err := os.ReadFile("../../../internal/auth/authapi/credentials_unowned.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	start := strings.Index(body, "func NotOwnedCredentialRoutes()")
	end := strings.Index(body, "// UnavailableRetry")
	if start < 0 || end < start {
		t.Fatal("NotOwnedCredentialRoutes not found in credentials_unowned.go")
	}
	names := regexp.MustCompile(`Pattern\w+`).FindAllString(body[start:end], -1)
	var paths []string
	for _, file := range []string{"credentials.go", "register.go"} {
		raw, err := os.ReadFile("../../../internal/auth/authapi/" + file)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			if m := regexp.MustCompile(`\b` + name + `\s*=\s*"([^"]+)"`).FindStringSubmatch(string(raw)); m != nil {
				paths = append(paths, m[1])
			}
		}
	}
	if len(paths) != 6 {
		t.Fatalf("want six stub routes from the source, got %v", paths)
	}
	return paths
}

// During the whole goApi stage the credential writes answer 503 + Retry-After 1 (go-api forwards them, and the not-owned
// auth-service serves the stub). The runbook names the six routes, says login and refresh are not affected, and bounds the
// stage with a time budget, a low-traffic window and a rollback when over budget.
func TestStageOutageIsNamedAndBounded(t *testing.T) {
	raw, err := os.ReadFile("../../../docs/operate/runbooks/auth-users-owned-flip-and-rollback.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, path := range sixStubRoutes(t) {
		if !strings.Contains(text, "`"+path+"`") {
			t.Errorf("the runbook does not name the stub route %s", path)
		}
	}
	for _, want := range []string{
		"credentials_unowned.go",
		"whole goApi stage",
		"a valid request answers `503` with `Retry-After: 1`",
		"go-api validates the body first and answers `422` to an invalid one",
		"internal/apiservice/authproxy.go:147-149",
		"Login and refresh are not affected",
		"internal/apiservice/authproxy.go",
		"stage time budget of 15 minutes",
		"over budget",
		"low-traffic window",
		"status notice",
		"up to 60 seconds",
		"roll windows",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the runbook lacks %q", want)
		}
	}
	if strings.Contains(text, "six credential routes answer `503`") {
		t.Error("the stage outage line still says the routes simply answer 503: an invalid body is answered 422 by go-api first")
	}
	if strings.Contains(text, "- Expect up to 60 seconds of `503`") {
		t.Error("the old rule that put the 60 s text on the whole roll is still there")
	}
	over := runbookStep(t, "over budget")
	for _, want := range []string{"D1", "D2", "roll back"} {
		if !strings.Contains(over, want) {
			t.Errorf("the over-budget line lacks %q: %.200s", want, over)
		}
	}
}
