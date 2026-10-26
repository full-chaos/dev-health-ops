package devhealth_test

import (
	"strings"
	"testing"
)

// CHAOS-11606: phase 4 rolls in two steps. Stage goApi: go-api forwards the credential routes and carries
// AUTH_USERS_OWNED while auth-service, the auth Jobs and the grants stay in their phase 3 form (stubs, nothing owned).
// Stage owned: every reader carries the switch. A phase 4 render without a stage does not exist, so no one-shot roll.

func stageArgs(phase, stage string, extra ...string) []string {
	sets := []string{"--set", "authService.phase=" + phase}
	if stage != "" {
		sets = append(sets, "--set", "authService.usersOwnedStage="+stage)
	}
	return append(goAPIWithAuth(append(sets, phase3Secrets...)...), extra...)
}

var usersOwnedReaders = []struct{ kind, name string }{
	{"Deployment", "a-dev-health-auth-service"},
	{"Job", "a-dev-health-auth-migrate"},
	{"Job", "a-dev-health-auth-grants"},
}

func TestPhase4NeedsAUsersOwnedStage(t *testing.T) {
	_, out, err := renderAuth(t, stageArgs("4", "")...)
	if err == nil || !strings.Contains(out, "authService.usersOwnedStage") {
		t.Fatalf("phase 4 without a stage rendered: err=%v out=%.300s", err, out)
	}
}

func TestUsersOwnedStageIsRefusedBelowPhase4(t *testing.T) {
	for _, phase := range []string{"0", "1", "2", "3"} {
		_, out, err := renderAuth(t, stageArgs(phase, "goApi")...)
		if err == nil || !strings.Contains(out, "authService.usersOwnedStage") {
			t.Errorf("phase %s with a stage rendered: err=%v out=%.300s", phase, err, out)
		}
	}
}

func TestUsersOwnedStageValueIsClosed(t *testing.T) {
	for _, bad := range []string{"GoApi", "go-api", "both", "true"} {
		_, out, err := renderAuth(t, stageArgs("4", bad)...)
		if err == nil || !strings.Contains(out, "usersOwnedStage") {
			t.Errorf("stage %q rendered: err=%v out=%.300s", bad, err, out)
		}
	}
}

func TestStageGoAPIForwardsAndOwnsNothingElse(t *testing.T) {
	docs, out, err := renderAuth(t, stageArgs("4", "goApi")...)
	if err != nil {
		t.Fatalf("stage goApi: render: %v\n%.400s", err, out)
	}
	goAPI := authDoc(docs, "Deployment", "a-dev-health-go-api")
	if value, set := containerEnvValue(t, goAPI, "AUTH_USERS_OWNED"); !set || value != "true" {
		t.Errorf("stage goApi: go-api AUTH_USERS_OWNED=%q set=%v; want true", value, set)
	}
	if got := proxyEnv(t, docs)["AUTH_PROXY_CREDENTIALS"]; got != "true" {
		t.Errorf("stage goApi: go-api AUTH_PROXY_CREDENTIALS=%q; want true", got)
	}
	for _, target := range usersOwnedReaders {
		doc := authDoc(docs, target.kind, target.name)
		if doc == nil {
			t.Fatalf("stage goApi: no %s %s", target.kind, target.name)
		}
		if value, set := containerEnvValue(t, doc, "AUTH_USERS_OWNED"); set {
			t.Errorf("stage goApi: %s sets AUTH_USERS_OWNED=%q; want it unset", target.name, value)
		}
	}
}

func TestStageOwnedGivesEveryReaderTheSwitch(t *testing.T) {
	docs, out, err := renderAuth(t, stageArgs("4", "owned")...)
	if err != nil {
		t.Fatalf("stage owned: render: %v\n%.400s", err, out)
	}
	for _, target := range append(usersOwnedReaders, struct{ kind, name string }{"Deployment", "a-dev-health-go-api"}) {
		doc := authDoc(docs, target.kind, target.name)
		if doc == nil {
			t.Fatalf("stage owned: no %s %s", target.kind, target.name)
		}
		if value, set := containerEnvValue(t, doc, "AUTH_USERS_OWNED"); !set || value != "true" {
			t.Errorf("stage owned: %s AUTH_USERS_OWNED=%q set=%v; want true", target.name, value, set)
		}
	}
}

// Owned auth-service never renders beside a go-api that does not forward (go-api off in this release).
func TestStageOwnedIsRefusedWithoutGoAPIForward(t *testing.T) {
	_, out, err := renderAuth(t, stageArgs("4", "owned", "--set", "goApi.enabled=false")...)
	if err == nil || !strings.Contains(out, "authService.usersOwnedStage=owned needs go-api") {
		t.Fatalf("stage owned without go-api rendered: err=%v out=%.300s", err, out)
	}
	if _, out, err := renderAuth(t, stageArgs("4", "goApi", "--set", "goApi.enabled=false")...); err == nil {
		t.Errorf("stage goApi without go-api rendered (nothing to forward): %.300s", out)
	}
}

// Across every phase and stage the pair (auth-service owned, go-api not on forward) never renders.
func TestNoRenderPairsOwnedAuthServiceWithANonForwardingGoAPI(t *testing.T) {
	for _, c := range []struct{ phase, stage string }{{"3", ""}, {"4", "goApi"}, {"4", "owned"}} {
		docs, out, err := renderAuth(t, stageArgs(c.phase, c.stage)...)
		if err != nil {
			t.Fatalf("phase %s stage %q: render: %v\n%.300s", c.phase, c.stage, err, out)
		}
		owned, _ := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-auth-service"), "AUTH_USERS_OWNED")
		forward := proxyEnv(t, docs)["AUTH_PROXY_CREDENTIALS"] == "true"
		if owned == "true" && !forward {
			t.Errorf("phase %s stage %q: auth-service owned, go-api not forwarding", c.phase, c.stage)
		}
	}
}

// The first backfill (stage goApi) renders the phase 3 form of the Job: the verb refuses when the switch is on.
func TestStageGoAPIBackfillJobCarriesNoSwitch(t *testing.T) {
	args := append(stageArgs("4", "goApi"), "--set", "authService.backfill.enabled=true", "--set", "authService.backfill.verb=users-backfill",
		"--set", "authService.backfill.runId=r2", "--set", "authService.backfill.dryRun=false")
	_, job := renderBackfill(t, args)
	if value, set := containerEnvValue(t, job, "AUTH_USERS_OWNED"); set {
		t.Errorf("stage goApi: backfill Job AUTH_USERS_OWNED=%q; want it unset", value)
	}
}

// legacy-link-purge needs users owned: stage goApi refuses it, as phase 3 does.
func TestStageGoAPIRefusesLegacyLinkPurge(t *testing.T) {
	args := append(stageArgs("4", "goApi"), "--set", "authService.backfill.enabled=true", "--set", "authService.backfill.verb=legacy-link-purge",
		"--set", "authService.backfill.runId=r3")
	_, out, err := renderAuth(t, args...)
	if err == nil || !strings.Contains(out, "legacy-link-purge needs users owned") {
		t.Fatalf("legacy-link-purge at stage goApi rendered: err=%v out=%.300s", err, out)
	}
}

// users-reconcile is the post-flip detection net: a read-only verb, so no --dry-run flag, whatever dryRun says.
func TestBackfillJobRendersUsersReconcileWithoutDryRunFlag(t *testing.T) {
	for _, dry := range []string{"true", "false"} {
		args := append(stageArgs("4", "owned"), "--set", "authService.backfill.enabled=true", "--set", "authService.backfill.verb=users-reconcile",
			"--set", "authService.backfill.runId=rc1", "--set", "authService.backfill.dryRun="+dry)
		_, job := renderBackfill(t, args)
		got := jobArgs(t, job)
		if len(got) != 3 || got[0] != "auth" || got[1] != "users-reconcile" || !strings.HasPrefix(got[2], "--batch=") {
			t.Errorf("dryRun=%s: users-reconcile Job args = %v, want [auth users-reconcile --batch=N]", dry, got)
		}
	}
}
