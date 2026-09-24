//go:build integration

package syncadmin

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestDeleteConfigReadsTheRowByNameAgain pins deleteConfig against a
// concurrent replacement: the route looked the config up by id, but the
// Python delete finds the org's config by (name, provider) again, so a
// same-name replacement created in between is the row it acts on -- and
// refuses when that row has a child, deleting nothing. It also pins the
// plain delete and the not-found delete (no error, nothing deleted).
func TestDeleteConfigReadsTheRowByNameAgain(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `CREATE TABLE sync_configurations (
id uuid PRIMARY KEY, org_id text NOT NULL, name text NOT NULL, provider text NOT NULL,
parent_id uuid REFERENCES sync_configurations(id) ON DELETE CASCADE,
UNIQUE (org_id, provider, name))`); err != nil {
		t.Fatal(err)
	}
	insert := func(id uuid.UUID, org, name, provider string, parent any) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO sync_configurations (id, org_id, name, provider, parent_id) VALUES ($1, $2, $3, $4, $5)`,
			id, org, name, provider, parent); err != nil {
			t.Fatal(err)
		}
	}
	count := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_configurations`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	writes := store{pool: pool}

	// The route read config stale; a replacement with a child took its name.
	stale, replacement := uuid.New(), uuid.New()
	insert(replacement, "org", "n", "github", nil)
	insert(uuid.New(), "org", "child", "github", replacement)
	err = writes.deleteConfig(ctx, "org", &syncConfig{ID: stale, Name: "n", Provider: "github"})
	if !errors.Is(err, errDeleteWithChildren) || count() != 2 {
		t.Fatalf("replacement with a child: err %v, %d rows, want errDeleteWithChildren and 2 rows", err, count())
	}

	// A plain config goes; another org's same-name config stays.
	plain, other := uuid.New(), uuid.New()
	insert(plain, "org", "p", "gitlab", nil)
	insert(other, "other-org", "p", "gitlab", nil)
	if err := writes.deleteConfig(ctx, "org", &syncConfig{ID: plain, Name: "p", Provider: "gitlab"}); err != nil || count() != 3 {
		t.Fatalf("plain delete: err %v, %d rows, want 3", err, count())
	}
	// Gone already: no error, nothing else deleted.
	if err := writes.deleteConfig(ctx, "org", &syncConfig{ID: plain, Name: "p", Provider: "gitlab"}); err != nil || count() != 3 {
		t.Fatalf("delete of a deleted config: err %v, %d rows, want 3", err, count())
	}
}
