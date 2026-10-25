// Package domainroledb starts a PostgreSQL venue with the real migrated schema
// and the real go-river-migrate grants for a fresh domain runtime role, and
// hands a test an admin pool plus a pool authenticated as that role. The domain
// pool records every statement PostgreSQL refused with SQLSTATE 42501.
package domainroledb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const rolePassword = "domain-role-venue"

// Denials records every statement the domain connection had refused with
// SQLSTATE 42501.
type Denials struct {
	mu         sync.Mutex
	statements []string
}

type deniedStatementKey struct{}

func (denials *Denials) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, deniedStatementKey{}, data.SQL)
}

func (denials *Denials) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	var pgErr *pgconn.PgError
	if errors.As(data.Err, &pgErr) && pgErr.Code == "42501" {
		statement, _ := ctx.Value(deniedStatementKey{}).(string)
		denials.mu.Lock()
		denials.statements = append(denials.statements, statement)
		denials.mu.Unlock()
	}
}

// All is every refused statement so far.
func (denials *Denials) All() []string {
	denials.mu.Lock()
	defer denials.mu.Unlock()
	return append([]string(nil), denials.statements...)
}

// Venue is a started database: Admin seeds, Domain is what a worker holds.
type Venue struct {
	Admin      *pgxpool.Pool
	Domain     *pgxpool.Pool
	Denials    *Denials
	DomainRole string
}

// Start builds the venue. rolePrefix names the test's roles.
func Start(t *testing.T, ctx context.Context, rolePrefix string) Venue {
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

	domainRole, err := containers.RoleName(rolePrefix+"_domain_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName(rolePrefix+"_queue_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, domainRole, t.Logf) })
	t.Cleanup(func() { containers.DropRole(admin, queueRole, t.Logf) })
	for _, statement := range []string{
		"CREATE ROLE " + domainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + rolePassword + "'",
		"CREATE ROLE " + queueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + rolePassword + "'",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
		// As go-river-migrate runs it: the digest creates and stamps the
		// posture-manifest table the domain posture requires.
		PostureManifestDigest: postgresstore.PostureManifestDigest(),
	}); err != nil {
		t.Fatal(err)
	}

	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = domainRole, rolePassword
	denials := &Denials{}
	config.ConnConfig.Tracer = denials
	domain, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domain.Close)
	return Venue{Admin: admin, Domain: domain, Denials: denials, DomainRole: domainRole}
}

// RequireReady fails the test unless the domain role passes its readiness
// check under the migrated grants, naming the posture gaps when it does not.
func (venue Venue) RequireReady(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := postgresstore.CheckDomainAuthorization(ctx, venue.Domain, venue.DomainRole, "river"); err != nil {
		gaps, _ := postgresstore.DiagnoseRolePosture(ctx, venue.Admin, venue.DomainRole, postgresstore.DomainPosture())
		t.Fatalf("domain readiness under the migrated grants: %v (gaps: %v)", err, gaps)
	}
}
