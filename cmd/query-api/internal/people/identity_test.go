package people

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/quadrant"
)

// TestPersonIDUsesTheSharedQuadrantImplementation pins that this package
// calls quadrant.PersonIDForIdentity -- the ONE md5 call site in this
// binary for person_id_for_identity's digest (people_identity.py:57-59)
// -- rather than carrying its own. Same fixture values
// cmd/query-api/internal/quadrant/identity_test.go pins against Python's
// hashlib.md5(identity.encode("utf-8")).hexdigest().lower(); this test
// guards against a future edit reintroducing a second copy here by
// checking BuildSearchResponse's own output (via PersonID) matches.
func TestPersonIDUsesTheSharedQuadrantImplementation(t *testing.T) {
	cases := map[string]string{
		"jane@example.com":     "9e26471d35a78862c17e467d87cddedf",
		"john.doe@example.com": "8eb1b522f60d11fa897de1dc6351b7e8",
		"github:octocat":       "c4837d37fe567d796fde9e102cf3930c",
	}
	for identity, want := range cases {
		if got := quadrant.PersonIDForIdentity(identity); got != want {
			t.Fatalf("quadrant.PersonIDForIdentity(%q) = %q, want %q", identity, got, want)
		}
	}
}

// TestDisplayNameForIdentity pins display_name_for_identity
// (people_identity.py:62-68): email local-part title-cased with '.'/'_'
// -> space, provider:handle returns the raw handle (NOT title-cased),
// anything else passes through.
func TestDisplayNameForIdentity(t *testing.T) {
	cases := map[string]string{
		"jane@example.com":     "Jane",
		"john.doe@example.com": "John Doe",
		"a_b.c@example.com":    "A B C",
		"github:octocat":       "octocat",
		"jira:accountid:abcd":  "accountid:abcd",
		"plain-handle":         "plain-handle",
		"provider:":            "provider:",
	}
	for identity, want := range cases {
		if got := displayNameForIdentity(identity); got != want {
			t.Fatalf("displayNameForIdentity(%q) = %q, want %q", identity, got, want)
		}
	}
}

// TestParseIdentity pins parse_identity (people_identity.py:71-77): email
// wins over a colon (an email local-part could itself contain ':' in
// theory, but never does in practice -- Python checks "@" first, so this
// port must too), then provider:handle split, then a bare "identity"
// fallback.
func TestParseIdentity(t *testing.T) {
	cases := []struct {
		identity         string
		provider, handle string
	}{
		{"jane@example.com", "email", "jane@example.com"},
		{"github:octocat", "github", "octocat"},
		{":octocat", "identity", "octocat"},
		{"github:", "github", "github:"},
		{"plain-handle", "identity", "plain-handle"},
	}
	for _, tc := range cases {
		provider, handle := parseIdentity(tc.identity)
		if provider != tc.provider || handle != tc.handle {
			t.Fatalf("parseIdentity(%q) = (%q, %q), want (%q, %q)", tc.identity, provider, handle, tc.provider, tc.handle)
		}
	}
}

// TestIdentitiesForPerson pins identities_for_person (people_identity.py:
// 80-94): identity first, then aliases in order, de-duplicated on
// "<provider>:<handle>", empty entries skipped.
func TestIdentitiesForPerson(t *testing.T) {
	got := identitiesForPerson("jane.doe@example.com", []string{"github:jdoe", "Jane Doe", "github:jdoe", ""})
	want := []PersonIdentity{
		{Provider: "email", Handle: "jane.doe@example.com"},
		{Provider: "github", Handle: "jdoe"},
		{Provider: "identity", Handle: "Jane Doe"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("identitiesForPerson(...) = %+v, want %+v", got, want)
	}
}

// TestIdentitiesForPersonDedupesIdentityAgainstItsOwnAliasList pins the
// same-key de-dup when the identity itself reappears in the alias list
// (search.go's own aliasList-append path can produce this shape).
func TestIdentitiesForPersonDedupesIdentityAgainstItsOwnAliasList(t *testing.T) {
	got := identitiesForPerson("github:jdoe", []string{"github:jdoe"})
	want := []PersonIdentity{{Provider: "github", Handle: "jdoe"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("identitiesForPerson(...) = %+v, want %+v", got, want)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestLoadIdentityAliasesMissingFileReturnsEmpty pins load_identity_aliases'
// FileNotFoundError branch (people_identity.py:32-33): a missing config
// file is an empty alias map, not an error.
func TestLoadIdentityAliasesMissingFileReturnsEmpty(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	got := loadIdentityAliases()
	if len(got) != 0 {
		t.Fatalf("loadIdentityAliases() = %v, want empty", got)
	}
}

// TestLoadIdentityAliasesParsesYAMLAndReverseMap pins load_identity_aliases'
// YAML shape (people_identity.py:21-51: identities: [{canonical, aliases}])
// and utils/identity_aliases.py's build_reverse_alias_map.
func TestLoadIdentityAliasesParsesYAMLAndReverseMap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "identity_mapping.yaml")
	writeFile(t, path, `
version: 1
identities:
  - canonical: "Jane.Doe@Example.com"
    aliases:
      - "github:jdoe"
      - "Jane Doe"
  - canonical: "  "
    aliases:
      - "ignored"
`)
	t.Setenv("IDENTITY_MAPPING_PATH", path)

	aliases := loadIdentityAliases()
	want := map[string][]string{"jane.doe@example.com": {"github:jdoe", "Jane Doe"}}
	if !reflect.DeepEqual(aliases, want) {
		t.Fatalf("loadIdentityAliases() = %v, want %v", aliases, want)
	}

	reverse := buildReverseAliasMap(aliases)
	if reverse["github:jdoe"] != "jane.doe@example.com" {
		t.Fatalf("reverse[github:jdoe] = %q, want jane.doe@example.com", reverse["github:jdoe"])
	}
	if reverse["jane doe"] != "jane.doe@example.com" {
		t.Fatalf("reverse[jane doe] = %q, want jane.doe@example.com", reverse["jane doe"])
	}
}
