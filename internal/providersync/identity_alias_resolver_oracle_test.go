package providersync

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/identityalias"
)

// identity_alias_resolver_oracle_test.go proves internal/identityalias's Go
// port of providers/identity.py's IdentityResolver matches the LIVE Python
// resolver under a SEEDED, non-empty alias config. The existing
// github/team-catalog/facets pair (github_team_catalog_generic_oracle_test.go)
// only ever runs against this deployment's checked-in EMPTY
// identity_mapping.yaml, so it proves the unaliased fallback ladder matches
// but says nothing about alias resolution itself. This test seeds a temp
// config, points IDENTITY_MAPPING_PATH at it (inherited by the Python
// subprocess exec.Command spawns, since Cmd.Env is nil), and compares both
// sides under the SAME config. See testdata/oracle_pairs/identity_alias_resolve.py.
func seedIdentityAliasMapping(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "identity_mapping.yaml")
	contents := `
version: 1
identities:
  - canonical: "lead@example.com"
    aliases:
      - "github:octocat"
  - canonical: "new.email@example.com"
    aliases:
      - "old.email@example.com"
  - canonical: "person-b@example.com"
    aliases:
      - "jira:accountid:acct-999"
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write seeded identity_mapping.yaml: %v", err)
	}
	t.Setenv("IDENTITY_MAPPING_PATH", path)
}

type identityAliasResolveProducerRow struct {
	Resolved string   `json:"resolved"`
	Facets   []string `json:"facets"`
}

// TestIdentityAliasResolverMatchesLivePythonResolverWithSeededAliases is a
// red-first parity proof: five scenarios the ported Go resolver
// must match the live Python IdentityResolver on -- exact login alias,
// case-different login alias (normKey lowercases before lookup), an email
// alias, an unmapped identity (falls back to the provider-qualified id),
// and a provider id that maps via alias to a DIFFERENT canonical person than
// its raw qualified form would suggest.
func TestIdentityAliasResolverMatchesLivePythonResolverWithSeededAliases(t *testing.T) {
	seedIdentityAliasMapping(t)
	compareRowsAgainstPythonOracle(
		t, "identity/alias/resolve",
		[]oracleCase{
			{ID: "exact_login_alias", Input: map[string]any{
				"provider": "github", "username": "octocat",
			}},
			{ID: "case_different_login_alias", Input: map[string]any{
				"provider": "github", "username": "OctoCat",
			}},
			{ID: "email_alias", Input: map[string]any{
				"provider": "github", "email": "old.email@example.com",
			}},
			{ID: "unmapped_identity", Input: map[string]any{
				"provider": "gitlab", "username": "monalisa",
			}},
			{ID: "provider_id_alias_to_different_person", Input: map[string]any{
				"provider": "jira", "account_id": "acct-999",
			}},
		},
		func(t *testing.T, input map[string]any) identityAliasResolveProducerRow {
			t.Helper()
			resolver := identityalias.LoadDefault()
			provider, _ := input["provider"].(string)
			email, _ := input["email"].(string)
			username, _ := input["username"].(string)
			accountID, _ := input["account_id"].(string)
			displayName, _ := input["display_name"].(string)
			return identityAliasResolveProducerRow{
				Resolved: resolver.Resolve(provider, email, username, accountID, displayName),
				Facets:   resolver.MembershipFacets(provider, username, accountID, email),
			}
		},
		nil,
	)
}
