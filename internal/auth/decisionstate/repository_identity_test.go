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
