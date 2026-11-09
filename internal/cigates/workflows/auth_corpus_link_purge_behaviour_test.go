package workflows_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The legacy link-token purge verb is gone with go-api's link-token code: no owned hand-off runs it, and the corpus
// compares the seeded legacy rows that stay as they are.
func TestAuthCorpusOwnedHandOffsRunNoLegacyLinkPurge(t *testing.T) {
	metrics := metric("sessions") + "\n" + metric("local_login") + "\n" + metric("credentials") + "\n" + metric("social")
	fake, out, err := runAcpOwnedLeg(t, "FAKE_SERVED="+requiredServed, "FAKE_SERVED_OWNED="+requiredServed+","+credentialsRoutes, "FAKE_METRICS="+metrics)
	if err != nil || !strings.Contains(out, "AUTH CORPUS EXTERNAL (acp) PASSED") {
		t.Fatalf("want a pass, got err=%v:\n%s", err, out)
	}
	calls := fake.calls(t)
	if !strings.Contains(calls, "auth-grants") || !strings.Contains(calls, "FLIPPED") {
		t.Fatalf("leg acp: the owned hand-off did not run:\n%s", calls)
	}
	if strings.Contains(calls, "legacy-link-purge") {
		t.Errorf("leg acp runs the retired purge verb:\n%s", calls)
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
	if !strings.Contains(calls, "run --rm --no-deps auth-users-backfill") || !strings.Contains(calls, "FLIPPED") {
		t.Fatalf("leg go-api: the owned hand-off did not run:\n%s", calls)
	}
	if strings.Contains(calls, "legacy-link-purge") {
		t.Errorf("leg go-api runs the retired purge verb:\n%s", calls)
	}
}
