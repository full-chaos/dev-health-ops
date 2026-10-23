//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

func domainOwnedApply(t *testing.T, ctx context.Context, seed func(env *migrationOnlyFixture)) (*migrationOnlyFixture, error) {
	t.Helper()
	env := newAuthMigrationOnly(t, ctx)
	target := domainOwnedMigration(t).Version
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.Version == target {
			if seed != nil {
				seed(env)
			}
			return env, applyOne(ctx, env.conn, env.options, migration)
		}
		if err := applyOne(ctx, env.conn, env.options, migration); err != nil {
			t.Fatalf("apply %04d: %v", migration.Version, err)
		}
	}
	t.Fatal("the lineage lacks the domain-owned migration")
	return nil, nil
}

const domainOwnedPrincipal = "00000000-0000-4000-8000-0000000893a1"

func domainOwnedExec(t *testing.T, ctx context.Context, env *migrationOnlyFixture, statements ...string) error {
	t.Helper()
	for _, statement := range statements {
		tx, err := env.conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true)`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, statement); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func domainOwnedRow(class, owner, ref, algorithm, hashBytes, fingerprint string) string {
	return `INSERT INTO auth.credential_registry (principal_id, credential_class, secret_hash, hash_algorithm, fingerprint, domain_owner, external_reference)
		VALUES ('` + domainOwnedPrincipal + `', '` + class + `', ` + hashBytes + `, '` + algorithm + `', ` + fingerprint + `, '` + owner + `', ` + ref + `)`
}

// TestMigrationDomainOwnedRefusesAViolatingRow: a stored external_push row with
// a real hash, or a platform row using the fingerprint form, makes the
// migration refuse with the constraint name and the count; nothing is recorded.
func TestMigrationDomainOwnedRefusesAViolatingRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cases := map[string]string{
		"external_push row with a sha256 hash": domainOwnedRow("external_push_ingest_token", "external_push", `'src-1'`, "sha256", `decode(md5('x'), 'hex')`, `'0123456789abcdef'`),
		"platform row in the fingerprint form": domainOwnedRow("workload_token", "platform", `NULL`, "fingerprint", `decode('0123456789abcdef', 'hex')`, `'0123456789abcdef'`),
	}
	for name, row := range cases {
		t.Run(name, func(t *testing.T) {
			env, err := domainOwnedApply(t, ctx, func(env *migrationOnlyFixture) {
				if err := domainOwnedExec(t, ctx, env,
					`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+domainOwnedPrincipal+`', 'workload', 'w10-4')`, row); err != nil {
					t.Fatalf("seed: %v", err)
				}
			})
			want := "credential_registry_domain_owned_fingerprint_only has 1 violating row(s)"
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("migration over a violating row = %v, want %q", err, want)
			}
			if strings.Contains(err.Error(), "is violated by some row") || strings.Contains(err.Error(), "0123456789abcdef") {
				t.Fatalf("raw or row-bearing error: %v", err)
			}
			var recorded bool
			if err := env.conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM auth.schema_migrations WHERE version = $1)`, domainOwnedMigration(t).Version).Scan(&recorded); err != nil || recorded {
				t.Fatalf("a refused migration was recorded: %v, %v", recorded, err)
			}
		})
	}
}

// TestMigrationDomainOwnedRegistryRowHoldsNoSecretHash: after the migration only
// the fingerprint-only form is accepted for acr and external_push, and a
// platform row keeps a real hash.
func TestMigrationDomainOwnedRegistryRowHoldsNoSecretHash(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env, err := domainOwnedApply(t, ctx, nil)
	if err != nil {
		t.Fatalf("migration over an empty registry: %v", err)
	}
	if err := domainOwnedExec(t, ctx, env,
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+domainOwnedPrincipal+`', 'workload', 'w10-4')`); err != nil {
		t.Fatal(err)
	}
	const fp = `'0123456789abcdef'`
	good := domainOwnedRow("external_push_ingest_token", "external_push", `'src-1'`, "fingerprint", `decode('0123456789abcdef', 'hex')`, fp)
	if err := domainOwnedExec(t, ctx, env, good); err != nil {
		t.Fatalf("the fingerprint-only external_push row was refused: %v", err)
	}
	refused := map[string]string{
		"external_push with a sha256 hash":     domainOwnedRow("external_push_ingest_token", "external_push", `'src-2'`, "sha256", `decode(md5('a'), 'hex')`, `'1123456789abcdef'`),
		"acr with a sha256 hash":               domainOwnedRow("acr_client_credential", "acr", `'src-3'`, "sha256", `decode(md5('b'), 'hex')`, `'2123456789abcdef'`),
		"fingerprint form with other bytes":    domainOwnedRow("external_push_ingest_token", "external_push", `'src-4'`, "fingerprint", `decode(md5('c'), 'hex')`, `'3123456789abcdef'`),
		"platform row in the fingerprint form": domainOwnedRow("workload_token", "platform", `NULL`, "fingerprint", `decode('4123456789abcdef', 'hex')`, `'4123456789abcdef'`),
	}
	for name, stmt := range refused {
		err := domainOwnedExec(t, ctx, env, stmt)
		if err == nil || !strings.Contains(err.Error(), "credential_registry_domain_owned_fingerprint_only") {
			t.Errorf("%s: err = %v, want the domain-owned CHECK", name, err)
		}
	}
	platform := domainOwnedRow("workload_token", "platform", `NULL`, "sha256", `decode(md5('d'), 'hex')`, `'5123456789abcdef'`)
	if err := domainOwnedExec(t, ctx, env, platform); err != nil {
		t.Fatalf("a platform row with a real hash was refused: %v", err)
	}
	var validated bool
	if err := env.conn.QueryRow(ctx, `SELECT convalidated FROM pg_constraint WHERE conname = 'credential_registry_domain_owned_fingerprint_only'`).Scan(&validated); err != nil || !validated {
		t.Fatalf("constraint validated = %v, %v", validated, err)
	}
}
