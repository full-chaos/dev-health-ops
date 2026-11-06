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

func (identityGrantSnapshot) PathForID(org, id string) (string, bool) {
	return "acme/api", org == "org-a" && id == "github:42"
}

func TestDelegatedSnapshotForwardsRepositoryID(t *testing.T) {
	d := delegatedSnapshot{GrantSnapshot: identityGrantSnapshot{}}
	if id, ok := d.RepositoryID("org-a", "acme/api"); !ok || id != "github:42" {
		t.Errorf("forwarded: %q %v", id, ok)
	}
	if p, ok := d.PathForID("org-a", "github:42"); !ok || p != "acme/api" {
		t.Errorf("forwarded path: %q %v", p, ok)
	}
	if _, ok := (delegatedSnapshot{}).PathForID("org-a", "github:42"); ok {
		t.Error("a snapshot without identities resolved an id")
	}
	if _, ok := (delegatedSnapshot{}).RepositoryID("org-a", "acme/api"); ok {
		t.Error("a snapshot without identities resolved a path")
	}
}
