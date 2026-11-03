package authruntime

import (
	"context"
	"crypto/ed25519"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// noKeySet is a signer that cannot read its key set.
type noKeySet struct{ authapi.AccessSigner }

func TestTheSessionIssueWorkloadPathIsBuiltOnlyWhenTheSubjectIsSet(t *testing.T) {
	got, err := sessionIssueWorkload(authconfig.Config{TokenIssuerURL: "https://auth.example.com"}, noKeySet{})
	if err != nil || got != nil {
		t.Fatalf("subject unset: got %v, %v; want nil, nil", got, err)
	}
	cfg := authconfig.Config{TokenIssuerURL: "https://auth.example.com", SessionIssueWorkloadSubject: "prn_AAAAAAAAAAAAAAAAAAAAAA"}
	_, err = sessionIssueWorkload(cfg, noKeySet{})
	if err == nil || !strings.Contains(err.Error(), authconfig.EnvSessionIssueWorkloadSubject) {
		t.Fatalf("a subject with a signer that reads no key set was not refused by name: %v", err)
	}
	if strings.Contains(err.Error(), cfg.SessionIssueWorkloadSubject) {
		t.Fatalf("the error names the subject value: %v", err)
	}
}

// keySetSigner is a signer that reads a fixed key set.
type keySetSigner struct {
	authapi.AccessSigner
	set token.KeySet
}

func (k keySetSigner) KeySet(context.Context) (token.KeySet, error) { return k.set, nil }

// A configured subject and a signer that reads its key set give the route a
// workload verifier.
func TestTheSessionIssueWorkloadPathIsBuiltWithACapableSigner(t *testing.T) {
	public, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	set, err := token.NewKeySet([]token.Key{{KeyID: "k1", State: token.State("active"), Algorithm: token.Algorithm, PublicKey: public}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := authconfig.Config{TokenIssuerURL: "https://auth.example.com", SessionIssueWorkloadSubject: session.WirePrincipalID(uuid.New())}
	got, err := sessionIssueWorkload(cfg, keySetSigner{set: set})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("a configured subject and a signer with a key set gave no workload verifier")
	}
	cfg.TokenIssuerURL = "http://not-https.example.com"
	if _, err := sessionIssueWorkload(cfg, keySetSigner{set: set}); err == nil || !strings.Contains(err.Error(), authconfig.EnvSessionIssueWorkloadSubject) {
		t.Fatalf("an issuer the verifier refuses was not reported by variable name: %v", err)
	}
}
