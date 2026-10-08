package teamsidentity

import "testing"

// An import writes the id the provider's own catalog writes: every provider
// id carries its provider's prefix (teamid.Of), once.
func TestImportedTeamIDPrefixesEveryProvider(t *testing.T) {
	for _, c := range []struct{ provider, id, want string }{
		{"github", "platform", "gh:platform"},
		{"github", "gh:platform", "gh:platform"},
		{"gitlab", "acme/ops", "gl:acme/ops"},
		{"jira", "0a1b2c3d-platform", "jira:0a1b2c3d-platform"},
		{"jira", "jira:0a1b2c3d-platform", "jira:0a1b2c3d-platform"},
		{"linear", "ENG", "linear:ENG"},
		{"linear", "linear:ENG", "linear:ENG"},
		{"ms-teams", "abc", "ms-teams:abc"},
	} {
		if got := importedTeamID(c.provider, c.id); got != c.want {
			t.Errorf("importedTeamID(%q, %q) = %q, want %q", c.provider, c.id, got, c.want)
		}
	}
}
