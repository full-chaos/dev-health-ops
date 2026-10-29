package authruntime

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
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
