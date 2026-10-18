//go:build integration

package audit

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// Migration 0007 adds the TRD section 9/12 columns to credential_registry and
// the triggers that carry the lifecycle rules. Every rule below is executed
// against a real PostgreSQL THROUGH audit.Commit, the way a real caller writes
// credential_registry (a guarded table: the commit guard refuses any other
// path), so a rule that only holds for a hand-armed transaction cannot pass.
// The pool connects without the auth schema on its search_path, which is what
// the runtime role has.

const (
	credPrincipal = "00000000-0000-4000-8000-0000000000a1"
	credOther     = "00000000-0000-4000-8000-0000000000a2"
	credParent    = "00000000-0000-4000-8000-0000000000c1"
	credChild     = "00000000-0000-4000-8000-0000000000c2"
	credRevoked   = "00000000-0000-4000-8000-0000000000c3"
	fp            = "0123456789abcdef"
)

// credEnv separates the two pools on purpose. admin is the fixture's
// superuser connection, used only to seed unguarded rows and to read catalog
// state. writer is the pool every credential write goes through, and it MUST be
// the runtime role: that role's grants, search_path and lack of bypass are what
// the production caller has, so a rule that only holds for a superuser cannot
// pass.
type credEnv struct {
	admin  *pgxpool.Pool
	writer *pgxpool.Pool
	schema authschema.ValidatedIdentifier
}

// runtimeRolePool returns a pool that connects AS the fixture's runtime role.
// newAuditFixture creates that role (NOLOGIN, with the runtime grants) but hands
// back the superuser's pool, so the role is given a login here and the pool is
// built from the superuser pool's own connection settings with only the user
// and password replaced.
func runtimeRolePool(t *testing.T, ctx context.Context, admin *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	var role string
	if err := admin.QueryRow(ctx,
		`SELECT rolname::text FROM pg_roles WHERE rolname LIKE 'audit\_runtime\_%' ORDER BY rolname LIMIT 1`).Scan(&role); err != nil {
		t.Fatalf("finding the runtime role: %v", err)
	}
	const password = "credential-lifecycle-test-only"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`ALTER ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatalf("giving the runtime role a login: %v", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("runtime-role pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

var commitSeq atomic.Int64

// commitCredential runs one statement against credential_registry through
// audit.Commit.
func commitCredential(ctx context.Context, env *credEnv, sql string, args ...any) error {
	n := commitSeq.Add(1)
	return Commit(ctx, env.writer, env.schema, Mutation{
		Apply: func(ctx context.Context, tx TxOps) error {
			_, err := tx.Exec(ctx, sql, args...)
			return err
		},
		Audit: AuditEvent{EventType: "credential.test", Outcome: OutcomeAllowed},
		Event: OutboxEvent{
			AggregateType: "credential", AggregateID: fmt.Sprintf("c%d", n),
			EventType: "credential.test", IdempotencyKey: fmt.Sprintf("credential-test-%d", n),
		},
	})
}

func insertCredential(ctx context.Context, env *credEnv, id, principal, extraColumns, extraValues string, args ...any) error {
	sql := `INSERT INTO auth.credential_registry (id, principal_id, credential_class, secret_hash, hash_algorithm, fingerprint` +
		extraColumns + `) VALUES ($1, $2, 'cred-class', decode(md5(random()::text || clock_timestamp()::text), 'hex'), 'sha256', '` + fp + `'` + extraValues + `)`
	return commitCredential(ctx, env, sql, append([]any{id, principal}, args...)...)
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
	fx := newAuditFixture(t, ctx)
	env := &credEnv{admin: fx.pool, writer: runtimeRolePool(t, ctx, fx.pool), schema: fx.schema}
	// principals is an unguarded table; seed it directly.
	for _, id := range []string{credPrincipal, credOther} {
		if _, err := env.admin.Exec(ctx,
			`INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'cred')`, id); err != nil {
			t.Fatalf("seed principal: %v", err)
		}
	}
	const (
		scopes2   = `ARRAY['api:read','api:write']::auth.credential_scope[]`
		scopes1   = `ARRAY['api:read']::auth.credential_scope[]`
		audiences = `ARRAY['api','acr']::auth.credential_token_name[]`
	)

	t.Run("every write below runs as the runtime role, not a superuser (the control)", func(t *testing.T) {
		var user string
		var super bool
		if err := env.writer.QueryRow(ctx,
			`SELECT current_user::text, (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user)`).Scan(&user, &super); err != nil {
			t.Fatal(err)
		}
		if super {
			t.Fatalf("the write pool connects as %q, a superuser or BYPASSRLS role: the rules below would be tested with privileges the runtime role does not have", user)
		}
		if !strings.Contains(user, "runtime") {
			t.Fatalf("the write pool connects as %q, not the runtime role", user)
		}
	})

	t.Run("the pool's search_path does not name the auth schema (the control for every write below)", func(t *testing.T) {
		// Migration SQL runs with search_path set to the schema; a runtime
		// session does not. A function body naming a table unqualified passes
		// under the migration and fails here, which is how round 1 found it.
		var path string
		if err := env.writer.QueryRow(ctx, `SHOW search_path`).Scan(&path); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(path, "auth") {
			t.Fatalf("search_path %q names the auth schema: the writes below would not exercise the trigger bodies' name resolution", path)
		}
	})

	t.Run("defaults keep the 0003 insert shape valid apart from the fingerprint", func(t *testing.T) {
		if err := insertCredential(ctx, env, credParent, credPrincipal, `, scopes, audiences`,
			`, `+scopes2+`, `+audiences); err != nil {
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

	t.Run("a new credential must carry its fingerprint", func(t *testing.T) {
		mustFail(t, "no fingerprint", commitCredential(ctx, env,
			`INSERT INTO auth.credential_registry (id, principal_id, credential_class, secret_hash, hash_algorithm)
			 VALUES ('00000000-0000-4000-8000-0000000000f1', $1, 'cred-class', decode(md5(random()::text), 'hex'), 'sha256')`, credPrincipal),
			"must carry its fingerprint")
	})

	t.Run("scopes have the principal.v1 shape and are a set", func(t *testing.T) {
		const id = "00000000-0000-4000-8000-0000000000d0"
		mustFail(t, "bare scope", insertCredential(ctx, env, id, credPrincipal, `, scopes`,
			`, ARRAY['read']::auth.credential_scope[]`), "credential_scope_shape")
		mustFail(t, "uppercase scope", insertCredential(ctx, env, id, credPrincipal, `, scopes`,
			`, ARRAY['Api:Read']::auth.credential_scope[]`), "credential_scope_shape")
		mustFail(t, "duplicate scopes", insertCredential(ctx, env, id, credPrincipal, `, scopes`,
			`, ARRAY['api:read','api:read']::auth.credential_scope[]`), "scopes_bounded")
		mustFail(t, "duplicate audiences", insertCredential(ctx, env, id, credPrincipal, `, audiences`,
			`, ARRAY['api','api']::auth.credential_token_name[]`), "audiences_bounded")
	})

	t.Run("rotation may preserve or narrow", func(t *testing.T) {
		if err := insertCredential(ctx, env, credChild, credPrincipal, `, rotation_parent_id, scopes, audiences`,
			`, $3::uuid, `+scopes1+`, ARRAY['api']::auth.credential_token_name[]`, credParent); err != nil {
			t.Fatalf("narrowing rotation refused: %v", err)
		}
	})

	t.Run("rotation may keep scopes and audiences equal", func(t *testing.T) {
		if err := insertCredential(ctx, env, "00000000-0000-4000-8000-0000000000e1", credPrincipal, `, rotation_parent_id, scopes, audiences`,
			`, $3::uuid, `+scopes2+`, `+audiences, credParent); err != nil {
			t.Fatalf("an equal-privilege rotation was refused: %v", err)
		}
	})

	t.Run("rotation never widens", func(t *testing.T) {
		const id = "00000000-0000-4000-8000-0000000000d1"
		mustFail(t, "wider scope", insertCredential(ctx, env, id, credPrincipal, `, rotation_parent_id, scopes`,
			`, $3::uuid, ARRAY['api:read','api:admin']::auth.credential_scope[]`, credParent), "never widen")
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
		if err := commitCredential(ctx, env, revoke, credRevoked); err != nil {
			t.Fatalf("first revoke: %v", err)
		}
		if err := commitCredential(ctx, env, revoke, credRevoked); err != nil {
			t.Fatalf("revoking again with the same values must be a no-op: %v", err)
		}
		mustFail(t, "later revoked_at", commitCredential(ctx, env,
			`UPDATE auth.credential_registry SET revoked_at = '2026-10-19T00:00:00Z' WHERE id = $1`, credRevoked), "monotonic")
		mustFail(t, "un-revoke", commitCredential(ctx, env,
			`UPDATE auth.credential_registry SET revoked_at = NULL, revoked_reason = NULL WHERE id = $1`, credRevoked), "monotonic")
		mustFail(t, "reason rewrite", commitCredential(ctx, env,
			`UPDATE auth.credential_registry SET revoked_reason = 'owner_request' WHERE id = $1`, credRevoked), "monotonic")
	})

	t.Run("a revocation reason is a reason code, never prose or an address", func(t *testing.T) {
		const id = "00000000-0000-4000-8000-0000000000f3"
		if err := insertCredential(ctx, env, id, credPrincipal, ``, ``); err != nil {
			t.Fatal(err)
		}
		for name, reason := range map[string]string{
			"prose":         "Revoked because bob left the company",
			"an address":    "leaked-by-bob@example.test",
			"uppercase":     "Compromised",
			"too short":     "ab",
			"leading digit": "1compromised",
		} {
			mustFail(t, "reason as "+name, commitCredential(ctx, env,
				`UPDATE auth.credential_registry SET revoked_at = now(), revoked_reason = $2 WHERE id = $1`, id, reason),
				"credential_registry_revoked_reason_code")
		}
		if err := commitCredential(ctx, env,
			`UPDATE auth.credential_registry SET revoked_at = now(), revoked_reason = 'owner_request' WHERE id = $1`, id); err != nil {
			t.Fatalf("a reason code was refused: %v", err)
		}
	})

	t.Run("a legacy row without a fingerprint can still be revoked", func(t *testing.T) {
		// Rows that predate 0007 have no fingerprint; requiring one on UPDATE
		// would make them impossible to revoke. Simulate one by clearing the
		// column under the migration-level bypass the superuser pool has.
		const id = "00000000-0000-4000-8000-0000000000f2"
		if err := insertCredential(ctx, env, id, credPrincipal, ``, ``); err != nil {
			t.Fatal(err)
		}
		if _, err := env.admin.Exec(ctx, `ALTER TABLE auth.credential_registry DISABLE TRIGGER USER`); err != nil {
			t.Fatal(err)
		}
		if _, err := env.admin.Exec(ctx, `UPDATE auth.credential_registry SET fingerprint = NULL WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
		if _, err := env.admin.Exec(ctx, `ALTER TABLE auth.credential_registry ENABLE TRIGGER USER`); err != nil {
			t.Fatal(err)
		}
		if err := commitCredential(ctx, env,
			`UPDATE auth.credential_registry SET revoked_at = now(), revoked_reason = 'compromised' WHERE id = $1`, id); err != nil {
			t.Fatalf("a legacy row could not be revoked: %v", err)
		}
	})

	t.Run("a revoked parent cannot be rotated", func(t *testing.T) {
		mustFail(t, "rotate revoked", insertCredential(ctx, env, "00000000-0000-4000-8000-0000000000d3", credPrincipal,
			`, rotation_parent_id`, `, $3::uuid`, credRevoked), "is revoked")
	})

	t.Run("privilege narrows in place, never widens; identity and verifier material are immutable", func(t *testing.T) {
		if err := commitCredential(ctx, env,
			`UPDATE auth.credential_registry SET scopes = `+scopes1+` WHERE id = $1`, credParent); err != nil {
			t.Fatalf("narrowing in place refused: %v", err)
		}
		mustFail(t, "widen in place", commitCredential(ctx, env,
			`UPDATE auth.credential_registry SET scopes = `+scopes2+` WHERE id = $1`, credParent), "never widened")
		mustFail(t, "widen audiences in place", commitCredential(ctx, env,
			`UPDATE auth.credential_registry SET audiences = ARRAY['api','acr','billing']::auth.credential_token_name[] WHERE id = $1`, credParent), "never widened")
		for name, set := range map[string]string{
			"id":             `id = '00000000-0000-4000-8000-0000000000ee'`,
			"secret_hash":    `secret_hash = '\x0102'::bytea`,
			"hash_algorithm": `hash_algorithm = 'md5'`,
			"display_prefix": `display_prefix = 'other_'`,
			"fingerprint":    `fingerprint = 'fedcba9876543210'`,
			"created_at":     `created_at = now() + interval '1 day'`,
		} {
			mustFail(t, "replace "+name+" in place", commitCredential(ctx, env,
				`UPDATE auth.credential_registry SET `+set+` WHERE id = $1`, credChild), "immutable")
		}
		mustFail(t, "change principal", commitCredential(ctx, env,
			`UPDATE auth.credential_registry SET principal_id = $2 WHERE id = $1`, credParent, credOther), "immutable")
		mustFail(t, "re-parent", commitCredential(ctx, env,
			`UPDATE auth.credential_registry SET rotation_parent_id = $2::uuid WHERE id = $1`, credParent, credChild), "immutable")
	})

	t.Run("column constraints", func(t *testing.T) {
		const id = "00000000-0000-4000-8000-0000000000d4"
		mustFail(t, "bad element shape", insertCredential(ctx, env, id, credPrincipal, `, audiences`,
			`, ARRAY['Bad Audience']::auth.credential_token_name[]`), "credential_token_name_shape")
		var many []string
		for i := 0; i < 17; i++ {
			many = append(many, "'a"+strings.Repeat("b", i)+"'")
		}
		mustFail(t, "17 audiences", insertCredential(ctx, env, id, credPrincipal, `, audiences`,
			`, ARRAY[`+strings.Join(many, ",")+`]::auth.credential_token_name[]`), "audiences_bounded")
		mustFail(t, "null element", insertCredential(ctx, env, id, credPrincipal, `, scopes`,
			`, ARRAY['api:read', NULL]::auth.credential_scope[]`), "scopes_bounded")
		mustFail(t, "unknown owner", insertCredential(ctx, env, id, credPrincipal, `, domain_owner`, `, 'nobody'`), "domain_owner_closed")
		mustFail(t, "projected without reference", insertCredential(ctx, env, id, credPrincipal, `, domain_owner`, `, 'acr'`), "matches_owner")
		mustFail(t, "platform with reference", insertCredential(ctx, env, id, credPrincipal, `, external_reference`, `, 'ext-1'`), "matches_owner")
		mustFail(t, "bad fingerprint", commitCredential(ctx, env,
			`INSERT INTO auth.credential_registry (id, principal_id, credential_class, secret_hash, hash_algorithm, fingerprint)
			 VALUES ($1, $2, 'cred-class', decode(md5(random()::text), 'hex'), 'sha256', 'NOT-HEX')`, id, credPrincipal), "fingerprint_shape")
		if err := commitCredential(ctx, env,
			`INSERT INTO auth.credential_registry (id, principal_id, credential_class, secret_hash, hash_algorithm, fingerprint, domain_owner, external_reference)
			 VALUES ($1, $2, 'cred-class', decode(md5(random()::text), 'hex'), 'sha256', '0123456789abcdef', 'acr', 'ext-1')`, id, credPrincipal); err != nil {
			t.Fatalf("a well-formed projected credential was refused: %v", err)
		}
	})
}
