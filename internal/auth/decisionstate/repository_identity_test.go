package decisionstate

import (
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

func TestSnapshotRepositoryID(t *testing.T) {
	s := &snapshot{org: "org-a", repoIDs: map[string]string{"acme/api": "github:42", "acme/dup": ""}}
	if id, ok := s.RepositoryID("org-a", "acme/api"); !ok || id != "github:42" {
		t.Errorf("resolved path: %q %v", id, ok)
	}
	for name, c := range map[string][2]string{
		"ambiguous path": {"org-a", "acme/dup"},
		"unknown path":   {"org-a", "acme/none"},
		"other org":      {"org-b", "acme/api"},
		"empty org":      {"", "acme/api"},
	} {
		if id, ok := s.RepositoryID(c[0], c[1]); ok || id != "" {
			t.Errorf("%s: resolved %q", name, id)
		}
	}
	if _, ok := (&snapshot{org: "org-a"}).RepositoryID("org-a", "acme/api"); ok {
		t.Error("a snapshot that read no identities resolved a path")
	}
}

func TestSnapshotPathForID(t *testing.T) {
	s := &snapshot{org: "org-a",
		repoIDs:   map[string]string{"acme/api": "github:42", "acme/dup": ""},
		repoPaths: map[string]string{"github:42": "acme/api", "github:1": "acme/dup", "github:2": "acme/dup"}}
	if p, ok := s.PathForID("org-a", "github:42"); !ok || p != "acme/api" {
		t.Errorf("resolved id: %q %v", p, ok)
	}
	for name, c := range map[string][2]string{
		"id on an ambiguous path": {"org-a", "github:1"},
		"unknown id":              {"org-a", "github:9"},
		"other org":               {"org-b", "github:42"},
		"empty org":               {"", "github:42"},
	} {
		if p, ok := s.PathForID(c[0], c[1]); ok || p != "" {
			t.Errorf("%s: resolved %q", name, p)
		}
	}
}

// CHAOS-13618: the snapshot reads the identity rows for an id-bearing grant, and for more grant rows than an allowlist
// holds (the read listing's recount); 256 or fewer path-only rows read none.
func TestNeedsIdentities(t *testing.T) {
	paths := func(n int) []authorization.Grant {
		rows := make([]authorization.Grant, n)
		for i := range rows {
			rows[i] = authorization.Grant{ResourceKind: "repository", ResourceKey: fmt.Sprintf("acme/repo-%03d", i)}
		}
		return rows
	}
	withID := append(paths(3), authorization.Grant{ResourceKind: "repository", ResourceKey: "acme/x", RepoID: "github:1"})
	for name, tc := range map[string]struct {
		grants []authorization.Grant
		want   bool
	}{
		"no grant":                  {nil, false},
		"256 path-only grants":      {paths(256), false},
		"257 path-only grants":      {paths(257), true},
		"3 path-only and 1 with id": {withID, true},
	} {
		if got := needsIdentities(tc.grants); got != tc.want {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
	}
}
