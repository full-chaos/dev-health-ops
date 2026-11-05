//go:build integration

package adminops

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestLLMSettingsSetRefusesAProviderChangeWithoutAKey is CHAOS-13096 on the
// operator path: `admin llm-settings set` runs the route's write, so a provider
// change with no --api-key is refused and changes no row, and a change to a
// keyless provider deletes the old key row. Go-only: the Python verb kept the key.
func TestLLMSettingsSetRefusesAProviderChangeWithoutAKey(t *testing.T) {
	db := startDatabase(t)
	db.reset(t)
	ctx := context.Background()
	if _, err := db.conn.Exec(ctx, "TRUNCATE settings, feature_flags CASCADE"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ('%s', 'team-org', 'Team', 'team', 'manual', true, now(), now())`, teamOrg)); err != nil {
		t.Fatal(err)
	}
	set := func(args ...string) (int, string) {
		return goVerbEnv(t, db, llmEnv(false), append([]string{"llm-settings", "set", "--org", teamOrg}, args...))
	}
	if code, stdout := set("--provider", "openai", "--model", "gpt-5", "--api-key", apiKeyLong); code != 0 {
		t.Fatalf("seed: exit %d, stdout %q", code, stdout)
	}
	before := db.llmState(t)
	code, stdout := set("--provider", "anthropic", "--model", "claude-haiku-5-5")
	t.Logf("openai -> anthropic, no key: exit=%d stdout=%q", code, stdout)
	if code != 1 || !strings.HasPrefix(stdout, "Error: ") || strings.Contains(stdout, apiKeyLong) {
		t.Fatalf("exit %d, stdout %q: want the refusal", code, stdout)
	}
	if after := db.llmState(t); after != before {
		t.Fatalf("a refused set changed rows:\n%s\n---\n%s", before, after)
	}
	if code, stdout := set("--provider", "ollama", "--model", "llama"); code != 0 {
		t.Fatalf("openai -> ollama: exit %d, stdout %q", code, stdout)
	}
	var keys int
	if err := db.conn.QueryRow(ctx, `SELECT count(*) FROM settings WHERE org_id = $1 AND category = 'llm' AND key = 'api_key'`, teamOrg).Scan(&keys); err != nil {
		t.Fatal(err)
	}
	if keys != 0 {
		t.Fatal("a switch to a keyless provider kept the old vendor's key row")
	}
}
