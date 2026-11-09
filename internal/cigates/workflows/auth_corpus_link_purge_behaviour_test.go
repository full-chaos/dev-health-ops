package workflows_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const legacyLinkPurgeCall = "run --rm --no-deps migrate auth legacy-link-purge"

// Both owned hand-offs run the switch's purge of go-api's legacy link-token rows (dho auth legacy-link-purge), as a real
// users-owned switch does: leg acp after the owned grants (the auth role's DELETE on the two tables is switch-gated), leg
// go-api after the seeded users' copy, both before the corpus is handed the flip. Never with the flag set off.
func TestAuthCorpusOwnedHandOffsPurgeTheLegacyLinkRows(t *testing.T) {
	metrics := metric("sessions") + "\n" + metric("local_login") + "\n" + metric("credentials") + "\n" + metric("social")
	fake, out, err := runAcpOwnedLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_SERVED_OWNED="+requiredServed+","+credentialsRoutes, "FAKE_METRICS="+metrics)
	if err != nil || !strings.Contains(out, "AUTH CORPUS EXTERNAL (acp) PASSED") {
		t.Fatalf("want a pass, got err=%v:\n%s", err, out)
	}
	calls := fake.calls(t)
	grants, purge, flipped := strings.Index(calls, "auth-grants"), strings.Index(calls, legacyLinkPurgeCall), strings.Index(calls, "FLIPPED")
	if purge < 0 || purge < grants || flipped < purge || strings.Count(calls, legacyLinkPurgeCall) != 1 {
		t.Fatalf("leg acp: want auth-grants, then one legacy-link-purge, then the hand-off; got positions %d %d %d:\n%s", grants, purge, flipped, calls)
	}
	if strings.Contains(calls, "-e AUTH_USERS_OWNED=false "+strings.TrimPrefix(legacyLinkPurgeCall, "run --rm --no-deps ")) {
		t.Errorf("leg acp: the purge runs with the flag off:\n%s", calls)
	}

	fake, out, err = runAcpLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_METRICS="+metric("sessions")+"\n"+metric("local_login"))
	if err != nil || !strings.Contains(out, "AUTH CORPUS EXTERNAL (acp) PASSED") {
		t.Fatalf("want a pass, got err=%v:\n%s", err, out)
	}
	if calls := fake.calls(t); strings.Contains(calls, "legacy-link-purge") {
		t.Errorf("a run with users not owned purged the legacy rows:\n%s", calls)
	}

	const owned = `{"services":{"go-api":{"environment":{"AUTH_PROXY_SESSIONS":"true","AUTH_PROXY_LOCAL_LOGIN":"true","AUTH_PROXY_CREDENTIALS":"true","AUTH_PROXY_SOCIAL":"true","AUTH_USERS_OWNED":"true"}},"auth-service":{"environment":{"AUTH_TOKEN_ISSUER_URL":"https://auth.corpus.example","AUTH_SIGNING_KEY_ID":"auth-signing-corpus"}}}}`
	fake = newFakeDocker(t)
	fake.withCurl(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(authProofRoot(t), "ci", "auth_corpus_external.sh"), "go-api")
	cmd.Env = fake.env("FAKE_COMPOSE_CONFIG=" + owned)
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("leg go-api failed: %v\n%s", err, raw)
	}
	calls = fake.calls(t)
	backfill, purge, flipped := strings.Index(calls, "run --rm --no-deps auth-users-backfill"), strings.Index(calls, legacyLinkPurgeCall), strings.Index(calls, "FLIPPED")
	if backfill < 0 || purge < backfill || flipped < purge || strings.Count(calls, legacyLinkPurgeCall) != 1 {
		t.Fatalf("leg go-api: want the users' copy, then one legacy-link-purge, then the hand-off; got positions %d %d %d:\n%s", backfill, purge, flipped, calls)
	}
}
