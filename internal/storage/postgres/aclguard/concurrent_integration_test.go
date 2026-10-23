//go:build integration

package aclguard_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/storage/roleprovision"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// The three tools that rewrite table privileges on the shared database (auth-migrate grants, dho migrate roles, dho
// migrate river) run at once, round after round, over the real application schema. Any one failing with SQLSTATE XX000
// "tuple concurrently updated" fails the test.
func TestPrivilegeWritersRunConcurrentlyWithoutCatalogConflicts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	}()
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	pgschema.Apply(ctx, t, admin)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA river"); err != nil {
		t.Fatal(err)
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(admin), &rivermigrate.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Schema: "river",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}

	name := func(base string) string {
		role, err := containers.RoleName(base, instance)
		if err != nil {
			t.Fatal(err)
		}
		return role
	}
	authRole, domain, queue, coordinator, queryAPI := name("acl_auth"), name("acl_domain"), name("acl_queue"), name("acl_coord"), name("acl_qapi")
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD 'acl-test-only'`, authRole)); err != nil {
		t.Fatal(err)
	}
	defer containers.DropRole(admin, authRole, t.Logf)
	authOptions := authschema.Options{Schema: "auth", RuntimeRole: authRole}
	if _, err := authschema.Apply(ctx, admin, authOptions); err != nil {
		t.Fatalf("auth-migrate: %v", err)
	}
	provision := roleprovision.Options{
		Domain: roleprovision.Role{Name: domain, Password: "acl-test-only"}, Queue: roleprovision.Role{Name: queue, Password: "acl-test-only"},
		Coordinator: roleprovision.Role{Name: coordinator, Password: "acl-test-only"}, QueryAPI: roleprovision.Role{Name: queryAPI, Password: "acl-test-only"},
		RiverSchema: "river",
	}
	if err := roleprovision.Apply(ctx, admin, provision); err != nil {
		t.Fatalf("dho migrate roles: %v", err)
	}
	for _, role := range []string{domain, queue, coordinator, queryAPI} {
		defer containers.DropRole(admin, role, t.Logf)
	}
	river := riverstore.MigrationOptions{
		Schema: "river", DomainRole: domain, QueueRole: queue,
		CoordinatorRole: coordinator, CoordinatorGrants: []riverstore.TableGrant{{TableName: "worker_job_routes"}},
		QueryAPIRole: queryAPI, QueryAPIGrants: []riverstore.TableGrant{{TableName: "users"}, {TableName: "organizations"}},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	rounds := 40
	if v, err := strconv.Atoi(os.Getenv("ACLGUARD_ROUNDS")); err == nil && v > 0 {
		rounds = v
	}
	grantsOptions := authOptions
	grantsOptions.RequirePublicTables = true
	var failures []string
	for round := 1; round <= rounds; round++ {
		var wg sync.WaitGroup
		var mu sync.Mutex
		start := make(chan struct{})
		run := func(label string, work func() error) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if err := work(); err != nil {
					mu.Lock()
					failures = append(failures, fmt.Sprintf("round %d %s: %v", round, label, err))
					mu.Unlock()
				}
			}()
		}
		run("auth-migrate grants", func() error { _, err := authschema.Grants(ctx, admin, grantsOptions); return err })
		run("dho migrate roles", func() error { return roleprovision.Apply(ctx, admin, provision) })
		run("dho migrate river", func() error { _, err := riverstore.ApplyPinnedMigrations(ctx, admin, river); return err })
		close(start)
		wg.Wait()
	}
	if len(failures) > 0 {
		t.Fatalf("%d of %d writer runs failed:\n%s", len(failures), rounds*3, strings.Join(failures, "\n"))
	}
}
