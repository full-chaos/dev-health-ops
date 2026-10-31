//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

const (
	acrBrowserRefreshTokenMigration = 37
	m37Principal                    = "00000000-0000-4000-8000-000000121411"
)

// TestMigration0037AdmitsTheACRBrowserRefreshTokenClass (CHAOS-12141): after
// 0037 a registry row of class acr_browser_refresh_token inserts, a class
// nobody defined is still refused, the CHECK is validated, and it is the 0036
// list plus exactly that one value, compared as sets.
func TestMigration0037AdmitsTheACRBrowserRefreshTokenClass(t *testing.T) {
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
		if m.Version == acrBrowserRefreshTokenMigration {
			before = classCheckDef(t, ctx, env)
			if err := domainOwnedExec(t, ctx, env,
				`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+m37Principal+`', 'workload', 'm37')`); err != nil {
				t.Fatalf("seed principal: %v", err)
			}
			m10Fail(t, domainOwnedExec(t, ctx, env, m37Credential("acr_browser_refresh_token", "a")), "an acr_browser_refresh_token row before 0037")
		}
		if err := applyOne(ctx, env.conn, env.options, m); err != nil {
			t.Fatalf("apply %04d: %v", m.Version, err)
		}
		if m.Version == acrBrowserRefreshTokenMigration {
			applied = true
			break
		}
	}
	if !applied {
		t.Fatalf("the lineage lacks migration %04d", acrBrowserRefreshTokenMigration)
	}
	if err := domainOwnedExec(t, ctx, env, m37Credential("acr_browser_refresh_token", "b")); err != nil {
		t.Fatalf("an acr_browser_refresh_token row after 0037: %v", err)
	}
	m10Fail(t, domainOwnedExec(t, ctx, env, m37Credential("acr_refresh_token", "c")), "a row of a class nobody defined")
	var validated bool
	if err := env.conn.QueryRow(ctx, `SELECT convalidated FROM pg_constraint
		WHERE conname = 'credential_registry_credential_class_closed' AND conrelid = 'auth.credential_registry'::regclass`).Scan(&validated); err != nil || !validated {
		t.Fatalf("class CHECK validated = %v (%v), want true", validated, err)
	}
	want := classSet(before)
	if len(want) == 0 || want["acr_browser_refresh_token"] {
		t.Fatalf("the 0036 CHECK read %v; the comparison would prove nothing", want)
	}
	want["acr_browser_refresh_token"] = true
	got := classSet(classCheckDef(t, ctx, env))
	var wantList []string
	for class := range want {
		wantList = append(wantList, class)
	}
	for _, problem := range classCheckDifferences(wantList, got) {
		t.Error(problem)
	}
}

func m37Credential(class, n string) string {
	return `INSERT INTO auth.credential_registry (principal_id, credential_class, secret_hash, hash_algorithm, fingerprint)
		VALUES ('` + m37Principal + `', '` + class + `', decode('` + strings.Repeat(n, 64) + `', 'hex'), 'sha256', '` + strings.Repeat(n, 16) + `')`
}
