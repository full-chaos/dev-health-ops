//go:build integration

package routeswitch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testSchemaDigest = "sha256:test-schema-digest"

func startRoutingStatePostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	// Minimal shape of go_api_routing_state (alembic 0114) -- only the
	// columns PostgresSwitch actually reads/needs for its PK, not the
	// full FK to go_api_candidate_build (out of scope for this test: it
	// exercises the Switch, not the whole registry schema).
	if _, err := pool.Exec(ctx, `
		CREATE TABLE go_api_routing_state (
			schema_digest TEXT NOT NULL,
			document_digest TEXT NOT NULL,
			selected_operation TEXT NOT NULL,
			mode TEXT NOT NULL,
			PRIMARY KEY (schema_digest, document_digest, selected_operation)
		)
	`); err != nil {
		t.Fatal(err)
	}
	return pool
}

func insertRoutingState(t *testing.T, pool *pgxpool.Pool, documentDigest, operation, mode string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO go_api_routing_state (schema_digest, document_digest, selected_operation, mode)
		VALUES ($1, $2, $3, $4)
	`, testSchemaDigest, documentDigest, operation, mode); err != nil {
		t.Fatal(err)
	}
}

// TestPostgresSwitch_ModeDrivesReachability is the registry-backed
// counterpart to switch_test.go's in-memory-switch reachability tests
// (plan §6 "cited constructor is not proof of capability", applied here
// to a real Postgres-backed registry read): a handler registered in the
// Mux is reachable only when `go_api_routing_state.mode` is `canary` or
// `primary` for that exact (schema_digest, document_digest,
// selected_operation) triple -- table-driven, clause by clause across
// every mode in the plan §5 vocabulary, not a single happy-path check.
func TestPostgresSwitch_ModeDrivesReachability(t *testing.T) {
	pool := startRoutingStatePostgres(t)

	cases := []struct {
		mode          string
		wantReachable bool
	}{
		{mode: "python", wantReachable: false},
		{mode: "shadow", wantReachable: false},
		{mode: "canary", wantReachable: true},
		{mode: "primary", wantReachable: true},
		{mode: "disabled", wantReachable: false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run("mode_"+tc.mode, func(t *testing.T) {
			operation := "op_" + tc.mode
			documentDigest := "doc-" + tc.mode
			insertRoutingState(t, pool, documentDigest, operation, tc.mode)

			sw := NewPostgresSwitch(pool, testSchemaDigest, map[string]string{operation: documentDigest})
			mux := NewMux(sw)
			mux.Register(operation, handlerNamed(operation))

			rec := httptest.NewRecorder()
			mux.Dispatch(operation, rec, httptest.NewRequest(http.MethodGet, "/query", nil))

			gotReachable := rec.Code == http.StatusOK
			if gotReachable != tc.wantReachable {
				t.Errorf("mode=%q: reachable=%v, want %v (status %d)", tc.mode, gotReachable, tc.wantReachable, rec.Code)
			}
		})
	}
}

// TestPostgresSwitch_UnregisteredOperationIsUnreachable: an operation with
// no row in go_api_routing_state at all (never registered) must resolve
// to unreachable, the same safe default as StaticSwitch/DynamicSwitch --
// a broken or empty registry must never fail open.
func TestPostgresSwitch_UnregisteredOperationIsUnreachable(t *testing.T) {
	pool := startRoutingStatePostgres(t)

	sw := NewPostgresSwitch(pool, testSchemaDigest, map[string]string{"neverRegistered": "doc-x"})
	mux := NewMux(sw)
	mux.Register("neverRegistered", handlerNamed("neverRegistered"))

	rec := httptest.NewRecorder()
	mux.Dispatch("neverRegistered", rec, httptest.NewRequest(http.MethodGet, "/query", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("operation with no routing-state row responded %d, want 404", rec.Code)
	}
}

// TestPostgresSwitch_OperationWithNoDocumentDigestIsUnreachable: an
// operation name the caller never mapped to a document digest cannot be
// looked up at all -- this must not panic or default to enabled.
func TestPostgresSwitch_OperationWithNoDocumentDigestIsUnreachable(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	insertRoutingState(t, pool, "doc-x", "hasDigest", "primary")

	sw := NewPostgresSwitch(pool, testSchemaDigest, map[string]string{"hasDigest": "doc-x"})
	mux := NewMux(sw)
	mux.Register("noDigestMapped", handlerNamed("noDigestMapped"))

	rec := httptest.NewRecorder()
	mux.Dispatch("noDigestMapped", rec, httptest.NewRequest(http.MethodGet, "/query", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("operation with no document-digest mapping responded %d, want 404", rec.Code)
	}
}

// TestPostgresSwitch_RollbackRevokesReachabilityImmediately: plan §5
// "rollback is a registry change, not an image rollback" -- flipping mode
// back from primary/canary must revoke reachability on the very next
// read, with no separate deploy or cache to invalidate.
func TestPostgresSwitch_RollbackRevokesReachabilityImmediately(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	insertRoutingState(t, pool, "doc-rb", "rollbackOp", "primary")

	sw := NewPostgresSwitch(pool, testSchemaDigest, map[string]string{"rollbackOp": "doc-rb"})
	mux := NewMux(sw)
	mux.Register("rollbackOp", handlerNamed("rollbackOp"))

	rec := httptest.NewRecorder()
	mux.Dispatch("rollbackOp", rec, httptest.NewRequest(http.MethodGet, "/query", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 while mode=primary, got %d", rec.Code)
	}

	if _, err := pool.Exec(context.Background(), `
		UPDATE go_api_routing_state SET mode = 'disabled'
		WHERE schema_digest = $1 AND document_digest = $2 AND selected_operation = $3
	`, testSchemaDigest, "doc-rb", "rollbackOp"); err != nil {
		t.Fatal(err)
	}

	rec = httptest.NewRecorder()
	mux.Dispatch("rollbackOp", rec, httptest.NewRequest(http.MethodGet, "/query", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 immediately after rollback to mode=disabled, got %d", rec.Code)
	}
}
