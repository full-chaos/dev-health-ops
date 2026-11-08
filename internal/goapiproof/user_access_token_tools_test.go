package goapiproof

import (
	"context"
	"strings"
	"testing"
)

// The prove tools present a user access token: no allowlisted helper mints the effective-principal envelope.
func TestMintHelperAllowlistRefusesMintEnvelope(t *testing.T) {
	_, err := MintViaAllowlistedHelper(context.Background(), "mint-envelope", []string{"-org", "00000000-0000-4000-8000-000000000001"})
	if err == nil || !strings.Contains(err.Error(), "not an allowlisted minting helper") {
		t.Fatalf("mint-envelope through the allowlist: %v, want it refused as not allowlisted", err)
	}
}

func userTokenAudit(class, principal string) RoutingAudit {
	return RoutingAudit{
		Action: AuditActionEnable, CredentialClass: class, PrincipalID: principal,
		RecordedBy: "cell", ReviewEvidence: "routing audit class cell", SchemaDigest: "sha256:s",
		Entries: []RoutingAuditEntry{{DocumentDigest: "sha256:d", Operation: "mcp:hotspots", CandidateBuildAfter: "b", ModeAfter: "canary"}},
	}
}

// seed and enable record the class they present, the user access token, with its subject; disable keeps
// operator_direct; no new row records the retired envelope.
func TestRoutingAuditRecordsTheUserAccessTokenWithItsSubject(t *testing.T) {
	for name, c := range map[string]struct {
		audit RoutingAudit
		ok    bool
		want  string
	}{
		"user access token with its subject":    {userTokenAudit("user_access_token", "prn_subject"), true, ""},
		"user access token with no subject":     {userTokenAudit("user_access_token", ""), false, "must name the subject"},
		"operator_direct with no subject":       {userTokenAudit("operator_direct", ""), true, ""},
		"operator_direct naming a subject":      {userTokenAudit("operator_direct", "prn_subject"), false, "must NOT name a principal"},
		"the retired envelope class":            {userTokenAudit("effective_principal_envelope", "prn_subject"), false, "is not a credential class"},
		"a class outside the routing audit set": {userTokenAudit("edge_token", "prn_subject"), false, "is not a credential class"},
	} {
		err := c.audit.validate()
		if (err == nil) != c.ok || (err != nil && !strings.Contains(err.Error(), c.want)) {
			t.Errorf("%s: validate() = %v, want ok=%v naming %q", name, err, c.ok, c.want)
		}
	}
}
