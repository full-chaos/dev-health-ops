//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"
)

const (
	kindOrg      = "00000000-0000-4000-8000-000000108961"
	kindSource   = "00000000-0000-4000-8000-000000108962"
	kindUser     = "00000000-0000-4000-8000-000000108963"
	kindSession  = "00000000-0000-4000-8000-000000108964"
	kindFinger   = "0123456789abcdef"
	kindSecret   = `'\x0a'::bytea`
	kindFPSecret = `decode('0123456789abcdef', 'hex')`
)

func kindSessionInsert(principal string) string {
	return `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at, legacy_token_version)
		VALUES (gen_random_uuid(), '` + principal + `', ` + kindSecret + `, now() + interval '1 hour', 0)`
}

func kindMembershipInsert(principal string) string {
	return `INSERT INTO auth.memberships (organization_id, principal_id, role) VALUES ('` + kindOrg + `', '` + principal + `', 'member')`
}

func kindCredentialInsert(principal, owner string) string {
	if owner == "platform" {
		return `INSERT INTO auth.credential_registry (principal_id, credential_class, secret_hash, hash_algorithm, fingerprint)
			VALUES ('` + principal + `', 'workload_token', ` + kindSecret + `, 'sha256', '` + kindFinger + `')`
	}
	return `INSERT INTO auth.credential_registry (principal_id, credential_class, secret_hash, hash_algorithm, fingerprint, domain_owner, external_reference)
		VALUES ('` + principal + `', 'external_push_ingest_token', ` + kindFPSecret + `, 'fingerprint', '` + kindFinger + `', 'external_push', 'cred-1')`
}

// TestMigrationPrincipalKindAdmitsExternalSource (CHAOS-10896): before the
// migration an external_source principal is refused; after it the row inserts,
// the CHECK is validated, any other kind is still refused and a stored user
// survives. The migration is found by name, so a renumber needs no edit here.
func TestMigrationPrincipalKindAdmitsExternalSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newAuthMigrationOnly(t, ctx)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	insert := func(id, kind string) string {
		return `INSERT INTO auth.principals (id, kind, display_name) VALUES ('` + id + `', '` + kind + `', 'kind cell')`
	}
	applied := false
	for _, m := range migrations {
		if m.Name == principalKindMigration {
			if err := domainOwnedExec(t, ctx, env, insert(kindUser, "user")); err != nil {
				t.Fatalf("seed: %v", err)
			}
			m10Fail(t, domainOwnedExec(t, ctx, env, insert(kindSource, "external_source")), "an external_source principal before the migration")
		}
		if err := applyOne(ctx, env.conn, env.options, m); err != nil {
			t.Fatalf("apply %04d_%s: %v", m.Version, m.Name, err)
		}
		if m.Name == principalKindMigration {
			applied = true
			break
		}
	}
	if !applied {
		t.Fatalf("the lineage lacks the migration named %s", principalKindMigration)
	}
	if err := domainOwnedExec(t, ctx, env, insert(kindSource, "external_source")); err != nil {
		t.Fatalf("an external_source principal after the migration: %v", err)
	}
	var validated bool
	if err := env.conn.QueryRow(ctx, `SELECT convalidated FROM pg_constraint
		WHERE conname = 'principals_kind_check' AND conrelid = 'auth.principals'::regclass`).Scan(&validated); err != nil || !validated {
		t.Fatalf("kind CHECK validated = %v (%v), want true", validated, err)
	}
	for i, kind := range []string{"human", "External_Source", "external_source ", "client_credential", "infrastructure", ""} {
		m10Fail(t, domainOwnedExec(t, ctx, env, insert("00000000-0000-4000-8000-00000010897"+string(rune('0'+i)), kind)), "a principal of kind "+kind)
	}
	var users int
	if err := env.conn.QueryRow(ctx, `SELECT count(*) FROM auth.principals WHERE id = $1 AND kind = 'user'`, kindUser).Scan(&users); err != nil || users != 1 {
		t.Fatalf("the stored user principal = %d (%v), want 1", users, err)
	}
}

// TestPrincipalKindExternalSourceHoldsNoSessionMembershipOrPlatformCredential:
// the three tables refuse the principal in the database, on insert and on a
// move of principal_id; a user principal still holds all three, and the
// external source keeps its domain-owned (external_push) registry row.
func TestPrincipalKindExternalSourceHoldsNoSessionMembershipOrPlatformCredential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newAuthMigrationOnly(t, ctx)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if err := applyOne(ctx, env.conn, env.options, m); err != nil {
			t.Fatalf("apply %04d_%s: %v", m.Version, m.Name, err)
		}
		if m.Name == principalKindMigration {
			break
		}
	}
	if err := domainOwnedExec(t, ctx, env,
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+kindUser+`', 'user', 'user')`,
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+kindSource+`', 'external_source', 'source')`,
		`INSERT INTO auth.organizations (id, slug, name) VALUES ('`+kindOrg+`', 'kind-org', 'Kind Org')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for _, ok := range []string{
		kindSessionInsert(kindUser), kindMembershipInsert(kindUser), kindCredentialInsert(kindUser, "platform"),
		kindCredentialInsert(kindSource, "external_push"),
	} {
		if err := domainOwnedExec(t, ctx, env, ok); err != nil {
			t.Fatalf("a row that must be admitted was refused: %v\n%s", err, ok)
		}
	}
	m10Fail(t, domainOwnedExec(t, ctx, env, kindSessionInsert(kindSource)), "a session of an external_source principal")
	m10Fail(t, domainOwnedExec(t, ctx, env, kindMembershipInsert(kindSource)), "a membership of an external_source principal")
	m10Fail(t, domainOwnedExec(t, ctx, env, kindCredentialInsert(kindSource, "platform")), "a platform-class credential of an external_source principal")
	for _, move := range []string{
		`UPDATE auth.sessions SET principal_id = '` + kindSource + `' WHERE principal_id = '` + kindUser + `'`,
		`UPDATE auth.memberships SET principal_id = '` + kindSource + `' WHERE principal_id = '` + kindUser + `'`,
		`UPDATE auth.credential_registry SET principal_id = '` + kindSource + `' WHERE principal_id = '` + kindUser + `' AND domain_owner = 'platform'`,
	} {
		m10Fail(t, domainOwnedExec(t, ctx, env, move), "moving a row onto an external_source principal: "+move)
	}
}
