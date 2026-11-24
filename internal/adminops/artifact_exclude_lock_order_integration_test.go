//go:build integration

package adminops

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// Lock order (Codex round 1, deadlock): a sync-config PATCH takes the integration's selection lock, then the
// integration row, then the configuration row. Transaction 1 below takes exactly that order; the verb's write
// (setArtifactExcludesTx, transaction 2) starts once transaction 1 holds the integration row. With one lock order the
// verb waits at the selection lock holding no row, transaction 1 locks the configuration row and commits, and the verb
// then finishes: no error on either side. With the old order (the verb locked the configuration row first, then the
// integration row) transaction 1's configuration UPDATE waits for the verb and the verb waits for transaction 1:
// PostgreSQL reports SQLSTATE 40P01 on one of them. The cell compiles against the old code too
// (setArtifactExcludesTx keeps its signature), so the base run shows the 40P01.
func TestArtifactExcludeVerbAndPatchOrderNeverDeadlock(t *testing.T) {
	ctx := context.Background()
	db := startDatabase(t)
	pool, err := pgxpool.New(ctx, db.uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	org, integration, config := uuid.NewString(), uuid.New(), uuid.New()
	seed := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1::uuid, 'org-' || $1::text, 'org', 'enterprise', true, now(), now())`, []any{org}},
		{`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, 'github', 'fx', '{"fetch_comments": true}'::json, true, now(), now())`, []any{integration, org}},
		{`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES ($1, $2, 'fx', 'github', '["tests"]'::json, '{}'::json, true, true, $3, now(), now())`, []any{config, org, integration}},
	}
	for _, row := range seed {
		if _, err := pool.Exec(ctx, row.sql, row.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	patch, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = patch.Rollback(context.Background()) }()
	if err := providersync.LockIntegrationSelection(ctx, patch, org, integration); err != nil {
		t.Fatal(err)
	}
	if _, err := patch.Exec(ctx, `UPDATE integrations SET updated_at = now() WHERE id = $1`, integration); err != nil {
		t.Fatal(err)
	}

	verbDone := make(chan error, 1)
	go func() {
		tx, err := pool.Begin(ctx)
		if err != nil {
			verbDone <- err
			return
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if _, err := setArtifactExcludesTx(ctx, tx, config, []pyjson.Value{"ci-x-"}, time.Now().UTC()); err != nil {
			verbDone <- err
			return
		}
		verbDone <- tx.Commit(ctx)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var waiting int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE NOT granted`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the verb never waited for a lock transaction 1 holds")
		}
		time.Sleep(20 * time.Millisecond)
	}

	_, patchErr := patch.Exec(ctx, `UPDATE sync_configurations SET updated_at = now() WHERE id = $1`, config)
	if patchErr == nil {
		patchErr = patch.Commit(ctx)
	}
	var verbErr error
	select {
	case verbErr = <-verbDone:
	case <-time.After(30 * time.Second):
		t.Fatal("the verb did not finish after transaction 1 committed")
	}
	for side, err := range map[string]error{"transaction 1 (PATCH order)": patchErr, "the verb": verbErr} {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
			t.Errorf("%s: deadlock (SQLSTATE 40P01): the verb and the PATCH lock in different orders", side)
		} else if err != nil {
			t.Errorf("%s: %v", side, err)
		}
	}
	var stored string
	if err := pool.QueryRow(ctx, `SELECT config::text FROM integrations WHERE id = $1`, integration).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != `{"fetch_comments": true, "ci_artifact_exclude_prefixes": ["ci-x-"]}` {
		t.Errorf("stored %s, want the verb's list after both committed", stored)
	}
}
