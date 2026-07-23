package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDomainAuthorizationRejectsMissingOrUnavailablePool(t *testing.T) {
	t.Parallel()

	if err := CheckDomainAuthorization(context.Background(), nil, "domain_role", "river"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckDomainAuthorization(nil) error = %v", err)
	}
	if err := CheckDomainAuthorization(context.Background(), nil, "Domain-Bad", "river"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckDomainAuthorization(invalid role) error = %v", err)
	}

	const secret = "domain-readiness-secret"
	config := DefaultConfig("postgres://domain:" + secret + "@127.0.0.1:1/app")
	config.ConnectTimeout = time.Millisecond
	pool, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = CheckDomainAuthorization(ctx, pool, "domain_role", "river")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckDomainAuthorization() error = %v", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), config.URI) {
		t.Fatalf("authorization readiness exposed connection material: %v", err)
	}
}

func TestDomainAuthorizationQueryIsReadOnlyAndChecksExactPrivilegeBoundary(t *testing.T) {
	t.Parallel()

	upperQuery := strings.ToUpper(domainAuthorizationQuery)
	for _, forbidden := range []string{"INSERT INTO", "UPDATE ", "DELETE FROM", "CREATE ", "ALTER ", "DROP ", "GRANT ", "REVOKE "} {
		if strings.Contains(upperQuery, forbidden) {
			t.Fatalf("domain authorization query contains mutating SQL %q", forbidden)
		}
	}
	for _, required := range []string{
		"CURRENT_USER = $1",
		"ROLCANLOGIN",
		"ROLSUPER",
		"ROLCREATEDB",
		"ROLCREATEROLE",
		"ROLREPLICATION",
		"ROLBYPASSRLS",
		"HAS_SCHEMA_PRIVILEGE",
		"HAS_TABLE_PRIVILEGE",
		"HAS_ANY_COLUMN_PRIVILEGE",
		"HAS_SEQUENCE_PRIVILEGE",
		"HAS_DATABASE_PRIVILEGE",
		"PG_HAS_ROLE",
		"'MEMBER'",
		"'MAINTAIN'",
		"RIVER_SEQUENCES",
		"RIVER_FUNCTIONS",
		"PUBLIC_FUNCTIONS",
		"OTHER_PUBLIC_RELATIONS",
		"PUBLIC_SEQUENCES",
		"'SELECT'",
		"'INSERT'",
		"'UPDATE'",
		"'DELETE'",
		"'USAGE'",
		"'CREATE'",
		"'TEMPORARY'",
	} {
		if !strings.Contains(upperQuery, required) {
			t.Fatalf("domain authorization query omits %q", required)
		}
	}
	for _, table := range []string{
		"integrations",
		"integration_sources",
		"integration_datasets",
		"integration_credentials",
		"sync_runs",
		"worker_job_routes",
		"sync_dispatch_transport_routes",
		"sync_run_units",
		"sync_watermarks",
		"sync_dispatch_outbox",
		"worker_job_outbox",
	} {
		if strings.Count(domainAuthorizationQuery, "'"+table+"'") != 1 {
			t.Fatalf("domain authorization query must inventory %q exactly once", table)
		}
	}
}
