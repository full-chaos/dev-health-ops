package identityalias

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func writeAliasFile(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "identity_mapping.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestLoadMissingFileReturnsEmptyMap(t *testing.T) {
	resolver := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if len(resolver.AliasToCanonical) != 0 {
		t.Fatalf("AliasToCanonical = %v, want empty", resolver.AliasToCanonical)
	}
}

func TestLoadParsesAliasesCaseInsensitively(t *testing.T) {
	path := writeAliasFile(t, `
version: 1
identities:
  - canonical: "Lead@Example.com"
    aliases:
      - "github:octocat"
      - "  Jira:AccountID:acct-999  "
  - canonical: "   "
    aliases:
      - "ignored"
`)
	resolver := Load(path)
	want := map[string]string{
		"lead@example.com":        "lead@example.com",
		"github:octocat":          "lead@example.com",
		"jira:accountid:acct-999": "lead@example.com",
	}
	if !reflect.DeepEqual(resolver.AliasToCanonical, want) {
		t.Fatalf("AliasToCanonical = %v, want %v", resolver.AliasToCanonical, want)
	}
}

func TestResolveEmailShortCircuitsAndIsCaseInsensitive(t *testing.T) {
	resolver := &Resolver{AliasToCanonical: map[string]string{}}
	got := resolver.Resolve("github", "Jane.Doe@Example.com", "octocat", "", "")
	if got != "jane.doe@example.com" {
		t.Fatalf("Resolve = %q, want lowercased email (email always wins over username)", got)
	}
}

func TestResolveEmailAliasOverridesNormalizedEmail(t *testing.T) {
	resolver := &Resolver{AliasToCanonical: map[string]string{
		"old.email@example.com": "new.email@example.com",
	}}
	got := resolver.Resolve("github", "Old.Email@Example.com", "", "", "")
	if got != "new.email@example.com" {
		t.Fatalf("Resolve = %q, want alias-mapped canonical", got)
	}
}

func TestResolveUsernameAliasExactAndCaseDifferent(t *testing.T) {
	resolver := &Resolver{AliasToCanonical: map[string]string{
		"github:octocat": "lead@example.com",
	}}
	for _, login := range []string{"octocat", "OctoCat", "OCTOCAT"} {
		if got := resolver.Resolve("github", "", login, "", ""); got != "lead@example.com" {
			t.Fatalf("Resolve(login=%q) = %q, want lead@example.com (case-insensitive alias match)", login, got)
		}
	}
}

func TestResolveUnmappedIdentityFallsBackToQualifiedID(t *testing.T) {
	resolver := &Resolver{AliasToCanonical: map[string]string{}}
	got := resolver.Resolve("gitlab", "", "monalisa", "", "")
	if got != "gitlab:monalisa" {
		t.Fatalf("Resolve = %q, want provider-qualified fallback", got)
	}
}

func TestResolveAccountIDAliasMapsToDifferentCanonicalPerson(t *testing.T) {
	resolver := &Resolver{AliasToCanonical: map[string]string{
		"jira:accountid:acct-999": "person-b@example.com",
	}}
	aliased := resolver.Resolve("jira", "", "", "acct-999", "")
	unaliased := resolver.Resolve("jira", "", "", "acct-111", "")
	if aliased != "person-b@example.com" {
		t.Fatalf("Resolve(acct-999) = %q, want person-b@example.com", aliased)
	}
	if unaliased != "jira:accountid:acct-111" {
		t.Fatalf("Resolve(acct-111) = %q, want raw qualified id (no alias)", unaliased)
	}
	if aliased == unaliased {
		t.Fatalf("aliased and unaliased identities must resolve to different people")
	}
}

func TestResolveNoIdentityAtAllReturnsUnknown(t *testing.T) {
	resolver := &Resolver{AliasToCanonical: map[string]string{}}
	if got := resolver.Resolve("github", "", "", "", ""); got != "unknown" {
		t.Fatalf("Resolve = %q, want unknown", got)
	}
}

func TestMembershipFacetsOrderAndDedup(t *testing.T) {
	resolver := &Resolver{AliasToCanonical: map[string]string{
		"github:octocat": "lead@example.com",
	}}
	facets := resolver.MembershipFacets("github", "octocat", "", "Lead@Example.com")
	want := []string{"lead@example.com", "github:octocat"}
	if !reflect.DeepEqual(facets, want) {
		t.Fatalf("MembershipFacets = %v, want %v", facets, want)
	}
}

func TestMembershipFacetsUnmappedUsernameOnly(t *testing.T) {
	resolver := &Resolver{AliasToCanonical: map[string]string{}}
	facets := resolver.MembershipFacets("gitlab", "monalisa", "", "")
	want := []string{"gitlab:monalisa"}
	if !reflect.DeepEqual(facets, want) {
		t.Fatalf("MembershipFacets = %v, want %v", facets, want)
	}
}
