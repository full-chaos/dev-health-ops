package decisionstate

import "testing"

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
