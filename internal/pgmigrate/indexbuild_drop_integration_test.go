//go:build integration

package pgmigrate

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A VALID index is never dropped by the post-failure cleanup.
func TestDropInvalidFailureStreakIndexKeepsAValidIndex(t *testing.T) {
	instance, err := containers.StartPostgres(context.Background())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := pgx.Connect(context.Background(), instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, statement := range []string{
		"CREATE TABLE public.sync_run_units (org_id text, integration_id text, source_id text, dataset_key text, updated_at timestamptz, id text, status text)",
		"CREATE INDEX " + FailureStreakIndexBody,
	} {
		if _, err := conn.Exec(context.Background(), statement); err != nil {
			t.Fatal(err)
		}
	}
	var logs bytes.Buffer
	dropInvalidFailureStreakIndex(context.Background(), conn, IndexBuildOptions{LockTimeout: time.Second}, slog.New(slog.NewJSONHandler(&logs, nil)))
	valid, exists, err := failureStreakIndexState(context.Background(), conn)
	if err != nil || !exists || !valid {
		t.Fatalf("valid index after the cleanup: exists=%v valid=%v err=%v", exists, valid, err)
	}
}
