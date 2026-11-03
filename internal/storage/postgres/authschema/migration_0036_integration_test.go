//go:build integration

package authschema

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"
)

const (
	retireLegacyIngestMigration = 36
	m36Principal                = "00000000-0000-4000-8000-000000118201"
)

var retiredLegacyIngestClasses = []string{"legacy_ingest_api_key", "legacy_ingest_hmac_signature"}

func applyThroughRetireLegacyIngest(t *testing.T, ctx context.Context, seed func(*migrationOnlyFixture)) (*migrationOnlyFixture, error) {
	t.Helper()
	env := newAuthMigrationOnly(t, ctx)
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.Version == retireLegacyIngestMigration {
			if seed != nil {
				seed(env)
			}
			return env, applyOne(ctx, env.conn, env.options, m)
		}
		if err := applyOne(ctx, env.conn, env.options, m); err != nil {
			t.Fatalf("apply %04d: %v", m.Version, err)
		}
	}
	t.Fatalf("the lineage lacks migration %04d", retireLegacyIngestMigration)
	return nil, nil
}

func seedM36Principal(t *testing.T, ctx context.Context, env *migrationOnlyFixture) {
	t.Helper()
	if err := domainOwnedExec(t, ctx, env,
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+m36Principal+`', 'workload', 'm36')`); err != nil {
		t.Fatalf("seed principal: %v", err)
	}
}

func m36Credential(class, n string) string {
	return `INSERT INTO auth.credential_registry (principal_id, credential_class, secret_hash, hash_algorithm, fingerprint)
		VALUES ('` + m36Principal + `', '` + class + `', decode('` + strings.Repeat(n, 64) + `', 'hex'), 'sha256', '` + strings.Repeat(n, 16) + `')`
}

func classCheckDef(t *testing.T, ctx context.Context, env *migrationOnlyFixture) string {
	t.Helper()
	var def string
	if err := env.conn.QueryRow(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conname = 'credential_registry_credential_class_closed'
		  AND conrelid = 'auth.credential_registry'::regclass`).Scan(&def); err != nil {
		t.Fatalf("credential_class CHECK: %v", err)
	}
	return def
}

var checkValue = regexp.MustCompile(`'([a-z0-9_]+)'::text`)

func classSet(def string) map[string]bool {
	set := map[string]bool{}
	for _, m := range checkValue.FindAllStringSubmatch(def, -1) {
		set[m[1]] = true
	}
	return set
}

// migration0032Classes is the class list 0032 wrote, read from its SQL.
func migration0032Classes(t *testing.T) []string {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if m.Version != 32 {
			continue
		}
		body := m.SQL[strings.Index(m.SQL, "IN (")+len("IN ("):]
		body = body[:strings.Index(body, ")")]
		var out []string
		for _, part := range strings.Split(body, ",") {
			out = append(out, strings.Trim(strings.TrimSpace(part), "'"))
		}
		return out
	}
	t.Fatal("the lineage lacks migration 0032")
	return nil
}

// (a) A stored row of a retired class stops the migration with the count only;
// nothing is applied or recorded and the CHECK is unchanged.
func TestMigration0036RefusesWhileALegacyIngestRowExists(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	var before string
	env, err := applyThroughRetireLegacyIngest(t, ctx, func(env *migrationOnlyFixture) {
		seedM36Principal(t, ctx, env)
		if err := domainOwnedExec(t, ctx, env, m36Credential("legacy_ingest_api_key", "a")); err != nil {
			t.Fatalf("seed a legacy_ingest_api_key row: %v", err)
		}
		before = classCheckDef(t, ctx, env)
	})
	if err == nil || !strings.Contains(err.Error(), "1 row(s)") || strings.Contains(err.Error(), m36Principal) {
		t.Fatalf("migration over a legacy_ingest row = %v, want a refusal with the count and no row data", err)
	}
	var recorded bool
	if err := env.conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM auth.schema_migrations WHERE version = $1)`, retireLegacyIngestMigration).Scan(&recorded); err != nil || recorded {
		t.Fatalf("a refused migration was recorded: %v, %v", recorded, err)
	}
	if after := classCheckDef(t, ctx, env); after != before {
		t.Fatalf("a refused migration changed the class CHECK:\n%s\n%s", before, after)
	}
}

// (b) With no such row the migration applies; both retired classes are refused
// at insert and a kept class still inserts. (d) The CHECK is the 0032 list
// minus exactly the two retired classes, compared as sets.
func TestMigration0036ClosesTheLegacyIngestClasses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env, err := applyThroughRetireLegacyIngest(t, ctx, func(env *migrationOnlyFixture) {
		seedM36Principal(t, ctx, env)
		if err := domainOwnedExec(t, ctx, env, m36Credential("workload_bootstrap_credential", "b")); err != nil {
			t.Fatalf("seed a kept-class row: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("migration with no legacy_ingest row: %v", err)
	}
	for i, class := range retiredLegacyIngestClasses {
		m10Fail(t, domainOwnedExec(t, ctx, env, m36Credential(class, []string{"c", "d"}[i])), "a "+class+" row after 0036")
	}
	if err := domainOwnedExec(t, ctx, env, m36Credential("workload_bootstrap_credential", "e")); err != nil {
		t.Fatalf("a kept class no longer inserts: %v", err)
	}
	var validated bool
	if err := env.conn.QueryRow(ctx, `SELECT convalidated FROM pg_constraint
		WHERE conname = 'credential_registry_credential_class_closed' AND conrelid = 'auth.credential_registry'::regclass`).Scan(&validated); err != nil || !validated {
		t.Fatalf("class CHECK validated = %v (%v), want true", validated, err)
	}
	want := map[string]bool{}
	for _, class := range migration0032Classes(t) {
		want[class] = true
	}
	for _, class := range retiredLegacyIngestClasses {
		if !want[class] {
			t.Fatalf("0032 did not list %s: the comparison would prove nothing", class)
		}
		delete(want, class)
	}
	got := classSet(classCheckDef(t, ctx, env))
	if len(got) == 0 {
		t.Fatal("no class read from the CHECK definition")
	}
	var wantList []string
	for class := range want {
		wantList = append(wantList, class)
	}
	for _, problem := range classCheckDifferences(wantList, got) {
		t.Error(problem)
	}
}
