//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Migration 0007 adds the TRD section 9/12 columns to credential_registry and
// two triggers that carry the lifecycle rules. Every rule below is executed
// against a real PostgreSQL through the commit guard's transaction setting,
// because credential_registry is a guarded table.

const (
	credPrincipal = "00000000-0000-4000-8000-0000000000a1"
	credOther     = "00000000-0000-4000-8000-0000000000a2"
	credParent    = "00000000-0000-4000-8000-0000000000c1"
	credChild     = "00000000-0000-4000-8000-0000000000c2"
	credRevoked   = "00000000-0000-4000-8000-0000000000c3"
)

func seedCredentialPrincipals(t *testing.T, ctx context.Context, env *authFixture) {
	t.Helper()
	for _, id := range []string{credPrincipal, credOther} {
		if err := guardedExec(ctx, env.runtime,
			`INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'cred')`, id); err != nil {
			t.Fatalf("seed principal: %v", err)
		}
	}
}

func insertCredential(ctx context.Context, env *authFixture, id, principal, extraColumns, extraValues string, args ...any) error {
	sql := `INSERT INTO auth.credential_registry (id, principal_id, credential_class, secret_hash, hash_algorithm` +
		extraColumns + `) VALUES ($1, $2, 'cred-class', decode(md5(random()::text || clock_timestamp()::text), 'hex'), 'sha256'` + extraValues + `)`
	return guardedExec(ctx, env.runtime, sql, append([]any{id, principal}, args...)...)
}

func mustFail(t *testing.T, name string, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: accepted, want a refusal containing %q", name, want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("%s: refused for another reason: %v (want %q)", name, err, want)
	}
}

func TestCredentialRegistryTRDColumnsAndLifecycleRules(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	seedCredentialPrincipals(t, ctx, env)

	t.Run("defaults keep the 0003 insert valid", func(t *testing.T) {
		if err := insertCredential(ctx, env, credParent, credPrincipal, `, scopes, audiences`, `, ARRAY['read','write']::auth.credential_token_name[], ARRAY['api','acr']::auth.credential_token_name[]`); err != nil {
			t.Fatalf("parent: %v", err)
		}
		var owner string
		var parent *string
		if err := env.admin.QueryRow(ctx,
			`SELECT domain_owner, rotation_parent_id::text FROM auth.credential_registry WHERE id = $1`, credParent).Scan(&owner, &parent); err != nil {
			t.Fatal(err)
		}
		if owner != "platform" || parent != nil {
			t.Fatalf("defaults = %q, %v", owner, parent)
		}
	})

	t.Run("rotation may preserve or narrow", func(t *testing.T) {
		if err := insertCredential(ctx, env, credChild, credPrincipal, `, rotation_parent_id, scopes, audiences`,
			`, $3::uuid, ARRAY['read']::auth.credential_token_name[], ARRAY['api']::auth.credential_token_name[]`, credParent); err != nil {
			t.Fatalf("narrowing rotation refused: %v", err)
		}
	})

	t.Run("rotation never widens", func(t *testing.T) {
		const id = "00000000-0000-4000-8000-0000000000d1"
		mustFail(t, "wider scope", insertCredential(ctx, env, id, credPrincipal, `, rotation_parent_id, scopes`,
			`, $3::uuid, ARRAY['read','admin']::auth.credential_token_name[]`, credParent), "never widen")
		mustFail(t, "wider audience", insertCredential(ctx, env, id, credPrincipal, `, rotation_parent_id, audiences`,
			`, $3::uuid, ARRAY['api','billing']::auth.credential_token_name[]`, credParent), "never widen")
	})

	t.Run("rotation keeps principal, parent must exist", func(t *testing.T) {
		const id = "00000000-0000-4000-8000-0000000000d2"
		mustFail(t, "other principal", insertCredential(ctx, env, id, credOther, `, rotation_parent_id`, `, $3::uuid`, credParent), "keeps its parent")
		mustFail(t, "missing parent", insertCredential(ctx, env, id, credPrincipal, `, rotation_parent_id`,
			`, $3::uuid`, "00000000-0000-4000-8000-0000000000ff"), "does not exist")
	})

	t.Run("revocation is idempotent and monotonic", func(t *testing.T) {
		if err := insertCredential(ctx, env, credRevoked, credPrincipal, ``, ``); err != nil {
			t.Fatal(err)
		}
		revoke := `UPDATE auth.credential_registry SET revoked_at = '2026-10-18T00:00:00Z', revoked_reason = 'compromised' WHERE id = $1`
		if err := guardedExec(ctx, env.runtime, revoke, credRevoked); err != nil {
			t.Fatalf("first revoke: %v", err)
		}
		if err := guardedExec(ctx, env.runtime, revoke, credRevoked); err != nil {
			t.Fatalf("revoking again with the same values must be a no-op: %v", err)
		}
		mustFail(t, "later revoked_at", guardedExec(ctx, env.runtime,
			`UPDATE auth.credential_registry SET revoked_at = '2026-10-19T00:00:00Z' WHERE id = $1`, credRevoked), "monotonic")
		mustFail(t, "un-revoke", guardedExec(ctx, env.runtime,
			`UPDATE auth.credential_registry SET revoked_at = NULL, revoked_reason = NULL WHERE id = $1`, credRevoked), "monotonic")
		mustFail(t, "reason rewrite", guardedExec(ctx, env.runtime,
			`UPDATE auth.credential_registry SET revoked_reason = 'owner_request' WHERE id = $1`, credRevoked), "monotonic")
	})

	t.Run("a revoked parent cannot be rotated", func(t *testing.T) {
		mustFail(t, "rotate revoked", insertCredential(ctx, env, "00000000-0000-4000-8000-0000000000d3", credPrincipal,
			`, rotation_parent_id`, `, $3::uuid`, credRevoked), "is revoked")
	})

	t.Run("privilege narrows in place, never widens; identity is immutable", func(t *testing.T) {
		if err := guardedExec(ctx, env.runtime,
			`UPDATE auth.credential_registry SET scopes = ARRAY['read']::auth.credential_token_name[] WHERE id = $1`, credParent); err != nil {
			t.Fatalf("narrowing in place refused: %v", err)
		}
		mustFail(t, "widen in place", guardedExec(ctx, env.runtime,
			`UPDATE auth.credential_registry SET scopes = ARRAY['read','write']::auth.credential_token_name[] WHERE id = $1`, credParent), "never widened")
		mustFail(t, "change principal", guardedExec(ctx, env.runtime,
			`UPDATE auth.credential_registry SET principal_id = $2 WHERE id = $1`, credParent, credOther), "immutable")
		mustFail(t, "re-parent", guardedExec(ctx, env.runtime,
			`UPDATE auth.credential_registry SET rotation_parent_id = $2::uuid WHERE id = $1`, credParent, credChild), "immutable")
	})

	t.Run("column constraints", func(t *testing.T) {
		const id = "00000000-0000-4000-8000-0000000000d4"
		mustFail(t, "bad element shape", insertCredential(ctx, env, id, credPrincipal, `, scopes`,
			`, ARRAY['Bad Scope']::auth.credential_token_name[]`), "credential_token_name_shape")
		var many []string
		for i := 0; i < 17; i++ {
			many = append(many, "'a"+strings.Repeat("b", i)+"'")
		}
		mustFail(t, "17 audiences", insertCredential(ctx, env, id, credPrincipal, `, audiences`,
			`, ARRAY[`+strings.Join(many, ",")+`]::auth.credential_token_name[]`), "audiences_bounded")
		mustFail(t, "null element", insertCredential(ctx, env, id, credPrincipal, `, scopes`,
			`, ARRAY['a', NULL]::auth.credential_token_name[]`), "scopes_bounded")
		mustFail(t, "unknown owner", insertCredential(ctx, env, id, credPrincipal, `, domain_owner`, `, 'nobody'`), "domain_owner_closed")
		mustFail(t, "projected without reference", insertCredential(ctx, env, id, credPrincipal, `, domain_owner`, `, 'acr'`), "matches_owner")
		mustFail(t, "platform with reference", insertCredential(ctx, env, id, credPrincipal, `, external_reference`, `, 'ext-1'`), "matches_owner")
		mustFail(t, "bad fingerprint", insertCredential(ctx, env, id, credPrincipal, `, fingerprint`, `, 'NOT-HEX'`), "fingerprint_shape")
		if err := insertCredential(ctx, env, id, credPrincipal, `, domain_owner, external_reference, fingerprint`,
			`, 'acr', 'ext-1', '0123456789abcdef'`); err != nil {
			t.Fatalf("a well-formed projected credential was refused: %v", err)
		}
	})
}
