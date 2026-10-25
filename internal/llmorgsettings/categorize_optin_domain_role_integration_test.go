//go:build integration

package llmorgsettings_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const optInDomainPassword = "categorize-optin-domain"

// permissionDenials records every statement the domain connection had refused
// with SQLSTATE 42501.
type permissionDenials struct {
	mu         sync.Mutex
	statements []string
}

type deniedStatementKey struct{}

func (denials *permissionDenials) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, deniedStatementKey{}, data.SQL)
}

func (denials *permissionDenials) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	var pgErr *pgconn.PgError
	if errors.As(data.Err, &pgErr) && pgErr.Code == "42501" {
		statement, _ := ctx.Value(deniedStatementKey{}).(string)
		denials.mu.Lock()
		denials.statements = append(denials.statements, statement)
		denials.mu.Unlock()
	}
}

func (denials *permissionDenials) all() []string {
	denials.mu.Lock()
	defer denials.mu.Unlock()
	return append([]string(nil), denials.statements...)
}

// startDomainRoleDatabase builds the REAL migrated schema, applies the REAL
// go-river-migrate runtime grants to a fresh domain role, and returns an admin
// pool plus a pool authenticated as that role. Seeding goes through admin only:
// the domain pool is what the heavy worker holds (workerservice: pools.Domain).
func startDomainRoleDatabase(t *testing.T, ctx context.Context) (*pgxpool.Pool, *pgxpool.Pool, *permissionDenials, string) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)

	domainRole, err := containers.RoleName("optin_domain_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("optin_queue_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, domainRole, t.Logf) })
	t.Cleanup(func() { containers.DropRole(admin, queueRole, t.Logf) })
	for _, statement := range []string{
		"CREATE ROLE " + domainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + optInDomainPassword + "'",
		"CREATE ROLE " + queueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + optInDomainPassword + "'",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}

	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = domainRole, optInDomainPassword
	denials := &permissionDenials{}
	config.ConnConfig.Tracer = denials
	domain, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domain.Close)
	return admin, domain, denials, domainRole
}

// The heavy worker reads an org's categorization opt-in on the domain role
// (CHAOS-11335: it had no grant and every "auto" categorization run failed with
// 42501). This runs the production read as that role, under the grants
// go-river-migrate really applies, and pins the value rule the definer function
// carries: on only for a row that says exactly "true".
func TestTheDomainRoleReadsTheCategorizeOptInThroughTheDefinerFunctionOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, domain, denials, domainRole := startDomainRoleDatabase(t, ctx)

	if err := postgresstore.CheckDomainAuthorization(ctx, domain, domainRole, "river"); err != nil {
		gaps, _ := postgresstore.DiagnoseRolePosture(ctx, admin, domainRole, postgresstore.DomainPosture())
		t.Fatalf("domain readiness under the migrated grants: %v (gaps: %v)", err, gaps)
	}

	insert := func(orgID, category, key string, value *string) {
		t.Helper()
		if _, err := admin.Exec(ctx, `
INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, $3, $4, false, now(), now())`, orgID, category, key, value); err != nil {
			t.Fatal(err)
		}
	}
	text := func(s string) *string { return &s }
	cases := []struct {
		org      string
		category string
		value    *string
		want     bool
	}{
		{"org-true", "llm", text("true"), true},
		{"org-false", "llm", text("false"), false},
		{"org-null", "llm", nil, false},
		{"org-empty", "llm", text(""), false},
		{"org-upper", "llm", text("TRUE"), false},
		{"org-one", "llm", text("1"), false},
		{"org-padded", "llm", text(" true"), false},
		{"org-yes", "llm", text("yes"), false},
		{"org-other-category", "general", text("true"), false},
		{"org-no-row", "", nil, false},
	}
	for _, tc := range cases {
		if tc.category != "" {
			insert(tc.org, tc.category, llmorgsettings.KeyCategorizeBYOEnabled, tc.value)
		}
	}
	// Another org's opt-in and another key of the same org never leak in.
	insert("org-no-row", "llm", "own_model_enabled", text("true"))

	for _, tc := range cases {
		got, err := llmorgsettings.CategorizeBYOEnabled(ctx, domain, tc.org)
		if err != nil || got != tc.want {
			t.Fatalf("%s: CategorizeBYOEnabled on the domain role = %v, %v; want %v", tc.org, got, err, tc.want)
		}
	}
	if denied := denials.all(); len(denied) != 0 {
		t.Fatalf("the domain role was refused %d statement(s) on the opt-in path: %q", len(denied), denied)
	}

	// The role holds nothing on settings itself: the encrypted provider keys
	// stay out of its reach.
	var count int
	err := domain.QueryRow(ctx, `SELECT count(*) FROM settings`).Scan(&count)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("SELECT on settings as the domain role = %v (count %d), want SQLSTATE 42501", err, count)
	}
}
