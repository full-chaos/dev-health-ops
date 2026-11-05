package authapi

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

type identityGrantSnapshot struct {
	authorization.GrantSnapshot
}

func (identityGrantSnapshot) RepositoryID(org, path string) (string, bool) {
	return "github:42", org == "org-a" && path == "acme/api"
}

func TestDelegatedSnapshotForwardsRepositoryID(t *testing.T) {
	d := delegatedSnapshot{GrantSnapshot: identityGrantSnapshot{}}
	if id, ok := d.RepositoryID("org-a", "acme/api"); !ok || id != "github:42" {
		t.Errorf("forwarded: %q %v", id, ok)
	}
	if _, ok := (delegatedSnapshot{}).RepositoryID("org-a", "acme/api"); ok {
		t.Error("a snapshot without identities resolved a path")
	}
}
