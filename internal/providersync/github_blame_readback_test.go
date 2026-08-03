package providersync

import (
	"testing"
	"time"
)

func TestCompareGitBlameVersionCoversEveryPersistedColumn(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	email, name, hash, line := "ada@example.com", "Ada", "abc123", "package main"
	authored := now.Add(-time.Hour)
	expected := gitBlameRow{
		RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: "src/main.go", LineNo: 7,
		AuthorEmail: &email, AuthorName: &name, AuthorWhen: &authored,
		CommitHash: &hash, Line: &line, LastSynced: now, OrgID: "org-a",
	}
	if got := compareGitBlameVersion(expected, gitBlameVersion{}); got != EffectAbsent {
		t.Fatalf("missing=%s", got)
	}
	stale := gitBlameVersion{Row: expected, LastSynced: now.Add(-time.Second), Found: true}
	if got := compareGitBlameVersion(expected, stale); got != EffectAbsent {
		t.Fatalf("stale=%s", got)
	}
	newer := gitBlameVersion{Row: expected, LastSynced: now.Add(time.Second), Found: true}
	if got := compareGitBlameVersion(expected, newer); got != EffectConflict {
		t.Fatalf("newer=%s", got)
	}
	exact := gitBlameVersion{Row: expected, LastSynced: now, Found: true}
	if got := compareGitBlameVersion(expected, exact); got != EffectExact {
		t.Fatalf("exact=%s", got)
	}

	tests := map[string]func(*gitBlameVersion){
		"repo_id":      func(v *gitBlameVersion) { v.Row.RepoID = "00000000-0000-0000-0000-000000000001" },
		"path":         func(v *gitBlameVersion) { v.Row.Path = "other.go" },
		"line_no":      func(v *gitBlameVersion) { v.Row.LineNo++ },
		"author_email": func(v *gitBlameVersion) { value := "other@example.com"; v.Row.AuthorEmail = &value },
		"author_name":  func(v *gitBlameVersion) { value := "Grace"; v.Row.AuthorName = &value },
		"author_when":  func(v *gitBlameVersion) { value := authored.Add(time.Second); v.Row.AuthorWhen = &value },
		"commit_hash":  func(v *gitBlameVersion) { value := "def456"; v.Row.CommitHash = &value },
		"line":         func(v *gitBlameVersion) { value := "package other"; v.Row.Line = &value },
		"org_id":       func(v *gitBlameVersion) { v.Row.OrgID = "org-b" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			actual := exact
			mutate(&actual)
			if got := compareGitBlameVersion(expected, actual); got != EffectConflict {
				t.Fatalf("comparison=%s", got)
			}
		})
	}
}

func TestCompareGitBlameVersionDistinguishesNullFromEmpty(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	empty := ""
	expected := gitBlameRow{
		RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: "src/main.go", LineNo: 1,
		AuthorEmail: &empty, AuthorName: &empty, CommitHash: &empty,
		LastSynced: now, OrgID: "org-a",
	}
	actual := gitBlameVersion{Row: expected, LastSynced: now, Found: true}
	actual.Row.AuthorEmail = nil
	if got := compareGitBlameVersion(expected, actual); got != EffectConflict {
		t.Fatalf("NULL versus empty comparison=%s", got)
	}
}
