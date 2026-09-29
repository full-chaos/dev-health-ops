//go:build integration

package server

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// The write-proof route end to end against a real Postgres: the allowlist row
// is written by the real audited add verb, the org check reads it through the
// production pool, and a registered mutation is dispatched to the real resolver,
// which writes a real saved_reports row. The handlers under test are the ones
// newQueryHandler builds, reached with a signed envelope, so a miswired route, an
// allowlist lookup that always denies, or an org check that is skipped each fail here.
func TestProofWriteRunsARegisteredMutationForAnAllowlistedOrgOnly(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(writeTestJWKS(t, pub), itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	_, _, proofWrite, _, err := newQueryHandler(emptyCHClient{}, pool, verifier, itTestSchemaDigest, os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	if err := goapiproof.AddProofOrg(t.Context(), pool, goapiproof.ProofOrgRequest{
		OrgID: "org-allowed", RecordedBy: "test", ReviewEvidence: "proof-write success path",
	}); err != nil {
		t.Fatal(err)
	}

	create := func(org string) (int, string) {
		token := signDataHealthEnvelope(t, priv, principal.Claims{OrgID: org, Role: "admin"}, time.Now().Add(time.Hour))
		rec := postGraphQLWithVariables(t, proofWrite, registeredCreateSavedReportDocument, token, map[string]any{
			"orgId": org, "input": map[string]any{"name": "proof-write " + org},
		})
		return rec.Code, rec.Body.String()
	}
	rows := func(org string) int {
		var n int
		if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM saved_reports WHERE org_id = $1`, org).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	code, raw := create("org-allowed")
	var body map[string]any
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("an allowlisted org's response is not JSON: status %d: %s", code, raw)
	}
	created, _ := body["data"].(map[string]any)
	report, _ := created["createSavedReport"].(map[string]any)
	if code != http.StatusOK || body["errors"] != nil || report["name"] != "proof-write org-allowed" {
		t.Fatalf("an allowlisted org's mutation was not served: status %d body %s", code, raw)
	}
	if got := rows("org-allowed"); got != 1 {
		t.Fatalf("the allowlisted org's mutation wrote %d saved_reports rows, want exactly 1", got)
	}

	code, raw = create("org-refused")
	if code != http.StatusForbidden {
		t.Fatalf("an org not on the allowlist got status %d body %s, want 403", code, raw)
	}
	if got := rows("org-refused"); got != 0 {
		t.Fatalf("the refused org's mutation wrote %d saved_reports rows, want 0", got)
	}
}
