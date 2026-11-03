//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

const (
	serviceAccountKeyMigrationName = "credential_class_service_account_key"
	saKeyPrincipal                 = "00000000-0000-4000-8000-000000122731"
)

// TestMigrationAdmitsTheServiceAccountKeyClass: after the migration a registry
// row of class service_account_key inserts, a class nobody defined is still
// refused, the CHECK is validated, and it is the prior list plus exactly that
// one value, compared as sets. The migration is found by name: its ordinal is
// the next free one at the merge.
func TestMigrationAdmitsTheServiceAccountKeyClass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newAuthMigrationOnly(t, ctx)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var before string
	applied := false
	for _, m := range migrations {
		target := m.Name == serviceAccountKeyMigrationName
		if target {
			before = classCheckDef(t, ctx, env)
			if err := domainOwnedExec(t, ctx, env,
				`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+saKeyPrincipal+`', 'service_account', 'sakey')`); err != nil {
				t.Fatalf("seed principal: %v", err)
			}
			m10Fail(t, domainOwnedExec(t, ctx, env, saKeyCredential("service_account_key", "a")), "a service_account_key row before the migration")
		}
		if err := applyOne(ctx, env.conn, env.options, m); err != nil {
			t.Fatalf("apply %04d: %v", m.Version, err)
		}
		if target {
			applied = true
			break
		}
	}
	if !applied {
		t.Fatalf("the lineage lacks migration %s", serviceAccountKeyMigrationName)
	}
	if err := domainOwnedExec(t, ctx, env, saKeyCredential("service_account_key", "b")); err != nil {
		t.Fatalf("a service_account_key row after the migration: %v", err)
	}
	m10Fail(t, domainOwnedExec(t, ctx, env, saKeyCredential("service_account_token", "c")), "a row of a class nobody defined")
	var validated bool
	if err := env.conn.QueryRow(ctx, `SELECT convalidated FROM pg_constraint
		WHERE conname = 'credential_registry_credential_class_closed' AND conrelid = 'auth.credential_registry'::regclass`).Scan(&validated); err != nil || !validated {
		t.Fatalf("class CHECK validated = %v (%v), want true", validated, err)
	}
	want := classSet(before)
	if len(want) == 0 || want["service_account_key"] {
		t.Fatalf("the prior CHECK read %v; the comparison would prove nothing", want)
	}
	want["service_account_key"] = true
	got := classSet(classCheckDef(t, ctx, env))
	var wantList []string
	for class := range want {
		wantList = append(wantList, class)
	}
	for _, problem := range classCheckDifferences(wantList, got) {
		t.Error(problem)
	}
}

func saKeyCredential(class, n string) string {
	return `INSERT INTO auth.credential_registry (principal_id, credential_class, secret_hash, hash_algorithm, fingerprint)
		VALUES ('` + saKeyPrincipal + `', '` + class + `', decode('` + strings.Repeat(n, 64) + `', 'hex'), 'sha256', '` + strings.Repeat(n, 16) + `')`
}
