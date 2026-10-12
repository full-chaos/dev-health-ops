//go:build integration

package adminops

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

const (
	entOrgA = "cccccccc-0000-4000-8000-00000000000a"
	entOrgB = "cccccccc-0000-4000-8000-00000000000b"
)

func entVerb(t *testing.T, db *database, verb string, args ...string) (int, string) {
	t.Helper()
	runs := map[string]func(context.Context, cli.Env) int{
		"list": runEntitlementsList, "grant": runEntitlementsGrant, "revoke": runEntitlementsRevoke,
	}
	env := map[string]string{"MIGRATION_DATABASE_URI": db.uri}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := runs[verb](context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	if strings.Contains(stdout.String()+stderr.String(), "postgres://") {
		t.Fatal("the output carries the DSN")
	}
	return code, stdout.String()
}

func entLines(t *testing.T, out string) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, text := range strings.Split(strings.TrimSpace(out), "\n") {
		if text == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(text), &line); err != nil {
			t.Fatalf("line %q is not JSON: %v", text, err)
		}
		lines = append(lines, line)
	}
	return lines
}

func TestOrgEntitlementsGrantListRevoke(t *testing.T) {
	db := startDatabase(t)
	ctx := context.Background()
	for _, org := range []string{entOrgA, entOrgB} {
		if _, err := db.conn.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1::uuid, 'org-' || $1::text, 'org', 'community', true, now(), now())`, org); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.conn.Exec(ctx, `DELETE FROM feature_flags WHERE key IN ('ent_x','ent_y')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec(ctx, `INSERT INTO feature_flags (id, key, name, min_tier, is_enabled, created_at, updated_at)
VALUES (gen_random_uuid(), 'ent_x', 'x', 'community', true, now(), now()), (gen_random_uuid(), 'ent_y', 'y', 'community', true, now(), now())`); err != nil {
		t.Fatal(err)
	}

	code, out := entVerb(t, db, "grant", "--org-id", entOrgA, "--feature-key", "ent_x", "--reason", "support", "--expires-at", "2099-01-02T03:04:05Z")
	if code != 0 {
		t.Fatalf("grant A/x exit %d: %s", code, out)
	}
	if got := entLines(t, out); len(got) != 1 || got[0]["action"] != "grant" || got[0]["expires_at"] != "2099-01-02T03:04:05+00:00" {
		t.Fatalf("grant line %v", got)
	}
	if code, out := entVerb(t, db, "grant", "--org-id", entOrgB, "--feature-key", "ent_x"); code != 0 {
		t.Fatalf("grant B/x exit %d: %s", code, out)
	}
	if code, out := entVerb(t, db, "grant", "--org-id", entOrgA, "--feature-key", "ent_x"); code != 1 || !strings.HasPrefix(out, "Error: ") {
		t.Fatalf("second grant of one feature: exit %d %q", code, out)
	}
	if code, out := entVerb(t, db, "grant", "--org-id", entOrgA, "--feature-key", "no_such"); code != 1 || !strings.Contains(out, "not found") {
		t.Fatalf("unknown feature: exit %d %q", code, out)
	}

	_, out = entVerb(t, db, "list", "--org-id", entOrgA)
	if got := entLines(t, out); len(got) != 1 || got[0]["feature_key"] != "ent_x" || got[0]["reason"] != "support" || got[0]["is_enabled"] != true {
		t.Fatalf("list A %v", got)
	}

	// Revoke of a feature the org does not hold touches nothing, in this org or the other.
	code, out = entVerb(t, db, "revoke", "--org-id", entOrgA, "--feature-key", "ent_y")
	if got := entLines(t, out); code != 0 || len(got) != 1 || got[0]["result"] != "not_present" {
		t.Fatalf("revoke absent: exit %d %v", code, got)
	}
	// Org B's grant of ent_x survives org A's revoke.
	code, out = entVerb(t, db, "revoke", "--org-id", entOrgA, "--feature-key", "ent_x")
	if got := entLines(t, out); code != 0 || len(got) != 1 || got[0]["result"] != "removed" || got[0]["org_id"] != entOrgA {
		t.Fatalf("revoke A/x: exit %d %v", code, got)
	}
	if _, out = entVerb(t, db, "list", "--org-id", entOrgA); len(entLines(t, out)) != 0 {
		t.Fatalf("list A after revoke: %q", out)
	}
	if _, out = entVerb(t, db, "list", "--org-id", entOrgB); len(entLines(t, out)) != 1 {
		t.Fatalf("org B lost its override: %q", out)
	}
	// Second revoke is idempotent.
	code, out = entVerb(t, db, "revoke", "--org-id", entOrgA, "--feature-key", "ent_x")
	if got := entLines(t, out); code != 0 || got[0]["result"] != "not_present" {
		t.Fatalf("second revoke: exit %d %q", code, out)
	}
	// bundles assign-org goes through the same grant.
	if code, out := goVerb(t, db, []string{"bundles", "assign-org", "--org-id", entOrgA, "--feature-key", "ent_y"}); code != 0 {
		t.Fatalf("assign-org: exit %d %q", code, out)
	}
	if _, out = entVerb(t, db, "list", "--org-id", entOrgA); len(entLines(t, out)) != 1 {
		t.Fatalf("assign-org row is not listed: %q", out)
	}
}
