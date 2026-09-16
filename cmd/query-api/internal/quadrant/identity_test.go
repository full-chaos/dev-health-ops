package quadrant

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// TestPersonIDForIdentityMatchesPythonMD5 pins PersonIDForIdentity against
// hashlib.md5(identity.encode("utf-8")).hexdigest().lower() (people_identity.py:
// 57-59) for a handful of identity shapes.
func TestPersonIDForIdentityMatchesPythonMD5(t *testing.T) {
	cases := map[string]string{
		"jane@example.com":     "9e26471d35a78862c17e467d87cddedf",
		"john.doe@example.com": "8eb1b522f60d11fa897de1dc6351b7e8",
		"github:octocat":       "c4837d37fe567d796fde9e102cf3930c",
	}
	for identity, want := range cases {
		if got := PersonIDForIdentity(identity); got != want {
			t.Fatalf("PersonIDForIdentity(%q) = %q, want %q", identity, got, want)
		}
	}
}

// TestDisplayNameForIdentity pins display_name_for_identity (people_identity.py:
// 62-68): email local-part title-cased with '.'/'_' -> space, provider:handle
// returns the raw handle (NOT title-cased), anything else passes through.
func TestDisplayNameForIdentity(t *testing.T) {
	cases := map[string]string{
		"jane@example.com":     "Jane",
		"john.doe@example.com": "John Doe",
		"a_b.c@example.com":    "A B C",
		"github:octocat":       "octocat",
		"jira:accountid:abcd":  "accountid:abcd",
		"plain-handle":         "plain-handle",
	}
	for identity, want := range cases {
		if got := displayNameForIdentity(identity); got != want {
			t.Fatalf("displayNameForIdentity(%q) = %q, want %q", identity, got, want)
		}
	}
}

// TestIdentityVariants pins identity_variants (people_identity.py:97-109):
// the canonical identity plus its aliases, plus (for an email) the local
// part and the normalized email, plus (for a provider:handle) the bare
// handle.
func TestIdentityVariants(t *testing.T) {
	cases := []struct {
		identity string
		aliases  []string
		want     []string
	}{
		{"jane@example.com", nil, []string{"jane", "jane@example.com"}},
		{"github:octocat", []string{"Jane Octocat"}, []string{"github:octocat", "octocat", "Jane Octocat"}},
		{"plain-handle", []string{""}, []string{"plain-handle"}},
	}
	for _, tc := range cases {
		got := identityVariants(tc.identity, tc.aliases)
		sort.Strings(got)
		want := append([]string(nil), tc.want...)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("identityVariants(%q, %v) = %v, want %v", tc.identity, tc.aliases, got, want)
		}
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

// TestResolveIdentityVariantsFoundIdentity pins _resolve_identity_variants'
// happy path (quadrant.py:339-346) with the alias config empty (this
// package's practical reality, see identity.go's doc comment): the
// resolved identity's own email-derived variants are returned.
func TestResolveIdentityVariantsFoundIdentity(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", filepath.Join(t.TempDir(), "missing.yaml"))
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "WITH identities AS") {
			return &fixtureRowScanner{rows: [][]any{{"jane@example.com"}}}, nil
		}
		t.Fatalf("unexpected query:\n%s", query)
		return nil, nil
	}}

	got, err := resolveIdentityVariants(context.Background(), client, "9e26471d35a78862c17e467d87cddedf", "org-1")
	if err != nil {
		t.Fatalf("resolveIdentityVariants: %v", err)
	}
	sort.Strings(got)
	want := []string{"jane", "jane@example.com"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resolveIdentityVariants() = %v, want %v", got, want)
	}
}

// TestResolveIdentityVariantsNotFound pins the fallback loop over an empty
// alias map (quadrant.py:348-355): nothing to fall back to, so an
// unresolved person id yields no variants at all.
func TestResolveIdentityVariantsNotFound(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", filepath.Join(t.TempDir(), "missing.yaml"))
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "WITH identities AS") {
			return &fixtureRowScanner{}, nil
		}
		t.Fatalf("unexpected query:\n%s", query)
		return nil, nil
	}}

	got, err := resolveIdentityVariants(context.Background(), client, "deadbeef", "org-1")
	if err != nil {
		t.Fatalf("resolveIdentityVariants: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("resolveIdentityVariants() = %v, want empty", got)
	}
}

// TestFetchPersonTeamIDEmptyIdentitiesSkipsQuery pins that an empty
// identity list never reaches ClickHouse (Python's fetch_person_team_id
// only ever runs after _resolve_identity_variants already found at least
// one, but the guard mirrors _sql_params' own falsy-filtering intent).
func TestFetchPersonTeamIDEmptyIdentitiesSkipsQuery(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatal("fetchPersonTeamID must not query ClickHouse for an empty identity list")
		return nil, nil
	}}
	got, err := fetchPersonTeamID(context.Background(), client, []string{"", ""}, "org-1")
	if err != nil {
		t.Fatalf("fetchPersonTeamID: %v", err)
	}
	if got != "" {
		t.Fatalf("fetchPersonTeamID() = %q, want \"\"", got)
	}
}

// TestFetchPersonTeamIDReturnsResolvedTeam pins fetch_person_team_id's
// happy path (queries/people.py:65-79).
func TestFetchPersonTeamIDReturnsResolvedTeam(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM identities FINAL") {
			return &fixtureRowScanner{rows: [][]any{{"team-x"}}}, nil
		}
		t.Fatalf("unexpected query:\n%s", query)
		return nil, nil
	}}
	got, err := fetchPersonTeamID(context.Background(), client, []string{"jane@example.com", "jane"}, "org-1")
	if err != nil {
		t.Fatalf("fetchPersonTeamID: %v", err)
	}
	if got != "team-x" {
		t.Fatalf("fetchPersonTeamID() = %q, want team-x", got)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
