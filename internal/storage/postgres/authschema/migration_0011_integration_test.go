//go:build integration

package authschema

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// CHAOS-10194: 0011 closes credential_registry.revoked_reason to the vocabulary
// of contracts/auth/v1/vocabulary/reason-codes.json. The 0008 CHECK admitted
// any lowercase code, so a secret-shaped lowercase value was storable.

const m11Principal = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"

// m11SecretShaped is a synthetic lowercase value shaped like a secret. It is
// built from two parts so the secret scanner does not read the source line as a
// credential.
const m11SecretShaped = "secret_" + "0123456789abcdef0123"

var (
	m11Listed = []string{"compromised", "owner_request"}
	// Each of these fits the 0008 pattern ^[a-z][a-z0-9_]{2,63}$.
	m11Unlisted = []string{
		m11SecretShaped,   // lowercase and secret-shaped
		"key_compromised", // a plausible code no producer emits yet
		"compromise",
		"owner_request_",
		"revoked",
	}
)

var m11Counter int

// m11Insert writes one credential_registry row as the audit commit would. A nil
// reason writes an unrevoked row.
func m11Insert(ctx context.Context, exec func(context.Context, string, ...any) error, reason *string) error {
	m11Counter++
	hash := fmt.Sprintf(`\x%02x%s`, m11Counter, strings.Repeat("0", 62))
	if reason == nil {
		return exec(ctx, `INSERT INTO auth.credential_registry
			(principal_id, credential_class, secret_hash, hash_algorithm, fingerprint)
			VALUES ($1, 'workload_token', $2::bytea, 'sha256', '0123456789abcdef')`, m11Principal, hash)
	}
	return exec(ctx, `INSERT INTO auth.credential_registry
		(principal_id, credential_class, secret_hash, hash_algorithm, fingerprint, revoked_at, revoked_reason)
		VALUES ($1, 'workload_token', $2::bytea, 'sha256', '0123456789abcdef', now(), $3)`, m11Principal, hash, *reason)
}

func TestMigration0011RevokedReasonIsAClosedVocabulary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	if _, err := env.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'm11')`, m11Principal); err != nil {
		t.Fatal(err)
	}
	exec := func(ctx context.Context, sql string, args ...any) error {
		return guardedExec(ctx, env.admin, sql, args...)
	}
	if err := m11Insert(ctx, exec, nil); err != nil {
		t.Errorf("an unrevoked row (NULL reason) was refused: %v", err)
	}
	for _, reason := range m11Listed {
		reason := reason
		if err := m11Insert(ctx, exec, &reason); err != nil {
			t.Errorf("listed revoked_reason %q was refused: %v", reason, err)
		}
	}
	for _, reason := range m11Unlisted {
		reason := reason
		if err := m11Insert(ctx, exec, &reason); err == nil {
			t.Errorf("revoked_reason %q is outside the vocabulary but was stored", reason)
		}
	}
}

// 0011 over rows that already hold listed codes changes nothing about them.
func TestMigration0011AppliesOverPopulatedSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthMigrationOnly(t, ctx)
	eleventh := m11ApplyBefore(t, ctx, env)
	if _, err := env.conn.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'm11')`, m11Principal); err != nil {
		t.Fatal(err)
	}
	exec := m11MigrationExec(env)
	if err := m11Insert(ctx, exec, nil); err != nil {
		t.Fatal(err)
	}
	for _, reason := range m11Listed {
		reason := reason
		if err := m11Insert(ctx, exec, &reason); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := func() string {
		var doc string
		if err := env.conn.QueryRow(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY t.created_at, t.id::text), '[]')::text FROM auth.credential_registry t`).Scan(&doc); err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if doc == "[]" {
			t.Fatal("the snapshot is empty; the control proves nothing")
		}
		return doc
	}
	before := snapshot()
	if err := applyOne(ctx, env.conn, env.options, *eleventh); err != nil {
		t.Fatalf("0011 over populated rows: %v", err)
	}
	if after := snapshot(); before != after {
		t.Errorf("0011 changed existing rows:\nbefore %s\nafter  %s", before, after)
	}
}

// A database that already holds a legacy-valid but unlisted code refuses 0011,
// names how many rows, and keeps the old constraint: the migration neither
// rewrites nor hides what an operator has to look at.
func TestMigration0011RefusesLegacyUnlistedCodesAndRollsBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthMigrationOnly(t, ctx)
	eleventh := m11ApplyBefore(t, ctx, env)
	if _, err := env.conn.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'm11')`, m11Principal); err != nil {
		t.Fatal(err)
	}
	exec := m11MigrationExec(env)
	for _, reason := range []string{"key_compromised", m11SecretShaped, "compromised"} {
		reason := reason
		if err := m11Insert(ctx, exec, &reason); err != nil {
			t.Fatalf("seed %q under the 0008 constraint: %v", reason, err)
		}
	}
	err := applyOne(ctx, env.conn, env.options, *eleventh)
	if err == nil {
		t.Fatal("0011 applied over rows with unlisted revoked_reason values")
	}
	if !strings.Contains(err.Error(), "2 credential_registry row") {
		t.Errorf("the refusal does not name the count of offending rows (want %q): %v", "2 credential_registry row", err)
	}
	if strings.Contains(err.Error(), m11SecretShaped) || strings.Contains(err.Error(), "key_compromised") {
		t.Errorf("the refusal echoes a stored reason value: %v", err)
	}
	other := "another_legacy_code"
	if err := m11Insert(ctx, exec, &other); err != nil {
		t.Errorf("after the refused 0011 the 0008 constraint must still hold (rollback), but a legacy-valid code was refused: %v", err)
	}
}
