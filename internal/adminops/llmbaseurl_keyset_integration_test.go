//go:build integration

package adminops

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestLLMSettingsSetRefusesAKeyStoredPurposeBaseURLsCarry is CHAOS-11368 on the
// operator path: `admin llm-settings set` runs the route's write, so a new api
// key that a purpose override's stored base_url already carries is refused and
// changes no row.
func TestLLMSettingsSetRefusesAKeyStoredPurposeBaseURLsCarry(t *testing.T) {
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
	newKey := "sk-" + "carried-" + "0123456789abcdef"
	for _, purpose := range []string{"investigate", "explain", "categorize"} {
		if _, err := db.conn.Exec(ctx, `DELETE FROM settings WHERE org_id = $1 AND key LIKE 'purpose:%'`, teamOrg); err != nil {
			t.Fatal(err)
		}
		if _, err := db.conn.Exec(ctx, `
INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'llm', $2, $3, false, 'seeded', now(), now())`,
			teamOrg, "purpose:"+purpose+":base_url", "https://p.example.invalid/"+newKey); err != nil {
			t.Fatal(err)
		}
		before := db.llmState(t)
		code, stdout := set("--provider", "openai", "--model", "gpt-5", "--api-key", newKey)
		t.Logf("%s base_url carries the new key: exit=%d stdout=%q", purpose, code, stdout)
		if code != 1 || !strings.HasPrefix(stdout, "Error: ") || strings.Contains(stdout, newKey) || strings.Contains(stdout, "example.invalid") {
			t.Errorf("%s: exit %d, stdout %q: want the refusal", purpose, code, stdout)
		}
		if after := db.llmState(t); after != before {
			t.Errorf("%s: a refused set changed rows:\n%s\n---\n%s", purpose, before, after)
		}
	}
	venueoracle.WriteGoOnlyProof(t, "CHAOS-11368 operator llm-settings set: a new key that a stored purpose base_url carries is refused")
}
