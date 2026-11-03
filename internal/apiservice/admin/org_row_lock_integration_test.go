//go:build integration

package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// rowProbe takes mode on the organization row in a transaction of its own,
// never waiting, and rolls back. It returns the SQLSTATE of the refusal.
func rowProbe(ctx context.Context, t *testing.T, pool *pgxpool.Pool, org uuid.UUID, mode string) string {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var one int
	err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT 1 FROM organizations WHERE id = $1 %s NOWAIT`, mode), org).Scan(&one)
	var pg *pgconn.PgError
	switch {
	case err == nil:
		return ""
	case errors.As(err, &pg):
		return pg.Code
	default:
		t.Fatal(err)
		return ""
	}
}

// apiManifestPool connects as a new login that holds exactly the api role's
// table manifest.
func apiManifestPool(ctx context.Context, t *testing.T, admin *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	role := "api_org_locks_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	password := uuid.NewString()
	statements := []string{fmt.Sprintf(`CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD '%s'`, role, password)}
	for _, table := range postgres.APIPosture(true).RequiredTables {
		privileges := []string{"SELECT"}
		if table.AllowInsert {
			privileges = append(privileges, "INSERT")
		}
		if table.AllowUpdate {
			privileges = append(privileges, "UPDATE")
		}
		if table.AllowDelete {
			privileges = append(privileges, "DELETE")
		}
		statements = append(statements, fmt.Sprintf(`GRANT %s ON public.%s TO %s`, strings.Join(privileges, ", "), table.TableName, role))
	}
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("role fixture: %v", err)
		}
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP OWNED BY "+role)
		_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
	})
	config := admin.Config().Copy()
	config.ConnConfig.User, config.ConnConfig.Password = role, password
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// While a PATCH holds the organization row across its auth re-read, a foreign
// key's KEY SHARE on the row does not wait, and another writer's lock does.
func TestAnOrgPatchReReadLetsForeignKeysThroughAndHoldsWriters(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	auth := &liveAuth{status: "suspended", rev: 1, holdRead: 2, held: make(chan struct{}), release: make(chan struct{})}
	org := seedOrg(ctx, t, pool, false)
	done := make(chan int, 1)
	go func() {
		code, _ := patchLive(ctx, liveHandlers(pool, auth), org, `{"is_active": true}`)
		done <- code
	}()
	select {
	case <-auth.held:
	case <-time.After(30 * time.Second):
		close(auth.release)
		t.Fatal("the PATCH never reached its re-read")
	}
	keyShare := rowProbe(ctx, t, pool, org, "FOR KEY SHARE")
	writer := rowProbe(ctx, t, pool, org, "FOR NO KEY UPDATE")
	close(auth.release)
	if code := <-done; code != http.StatusOK {
		t.Fatalf("PATCH answered %d, want 200", code)
	}
	if keyShare != "" {
		t.Fatalf("a foreign key's KEY SHARE waits on the re-read (SQLSTATE %s)", keyShare)
	}
	if writer != "55P03" {
		t.Fatalf("another writer's lock answered SQLSTATE %q during the re-read, want 55P03", writer)
	}
}

// The api role's manifest (postgres.APIPosture) holds what the organization
// row locks need: the PATCH's lock, the purge's head lock, and the billing
// lock, each run as that role.
func TestTheAPIRoleTakesTheOrganizationRowLocks(t *testing.T) {
	ctx, pool := startOrgsDB(t)
	runtime := apiManifestPool(ctx, t, pool)
	org := seedOrg(ctx, t, pool, true)

	tx, err := runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT 1 FROM organizations WHERE id = $1 FOR NO KEY UPDATE`, org); err != nil {
		t.Fatalf("the billing lock as the api role: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Fatal(err)
	}

	store := pgStore{Pool: runtime, Now: time.Now}
	active := false
	updated, err := store.updateOrganization(ctx, org, orgUpdate{IsActive: &active}, func(context.Context) (bool, error) { return false, nil })
	if err != nil || updated == nil {
		t.Fatalf("the PATCH lock as the api role: %v, %v", updated, err)
	}

	h := &handlers{store: store}
	result := &orgDeletionResult{postgresTableCount: map[string]int64{"organizations": 1}}
	if err := h.orgDeletionPurgePostgres(ctx, org, org.String(), result); err != nil {
		t.Fatalf("the purge as the api role: %v", err)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM organizations WHERE id = $1`, org).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("%d organization rows left after the purge", left)
	}
}
