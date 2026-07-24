package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestQueueAuthorizationRejectsMissingOrUnavailablePool(t *testing.T) {
	t.Parallel()

	if err := CheckQueueAuthorization(context.Background(), nil, "queue_role", "river"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckQueueAuthorization(nil) error = %v", err)
	}
	if err := CheckQueueAuthorization(context.Background(), nil, "Queue-Bad", "river"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckQueueAuthorization(invalid role) error = %v", err)
	}

	const secret = "queue-readiness-secret"
	config := DefaultConfig("postgres://queue:" + secret + "@127.0.0.1:1/app")
	config.ConnectTimeout = time.Millisecond
	pool, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = CheckQueueAuthorization(ctx, pool, "queue_role", "river")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckQueueAuthorization() error = %v", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), config.URI) {
		t.Fatalf("authorization readiness exposed connection material: %v", err)
	}
}

func TestQueueAuthorizationQueryIsReadOnlyAndChecksExactPrivilegeBoundary(t *testing.T) {
	t.Parallel()

	upperQuery := strings.ToUpper(queueAuthorizationQuery)
	for _, forbidden := range []string{"INSERT INTO", "UPDATE ", "DELETE FROM", "CREATE ", "ALTER ", "DROP ", "GRANT ", "REVOKE "} {
		if strings.Contains(upperQuery, forbidden) {
			t.Fatalf("queue authorization query contains mutating SQL %q", forbidden)
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
		"PG_HAS_ROLE",
		"'MEMBER'",
		"HAS_DATABASE_PRIVILEGE",
		"HAS_ANY_COLUMN_PRIVILEGE",
		"WORKER_JOB_OUTBOX",
		"WORKER_JOB_COMPLETION_FENCES",
		"SYNC_DISPATCH_OUTBOX",
		"SYNC_DISPATCH_TRANSPORT_ROUTES",
		"PUBLIC_FUNCTIONS",
		"PUBLIC_SEQUENCES",
		"RIVER_TABLES",
		"RIVER_SEQUENCES",
		"RIVER_FUNCTIONS",
		"'INSERT'",
		"'TRUNCATE'",
		"'REFERENCES'",
		"'TRIGGER'",
		"'MAINTAIN'",
		"'TEMPORARY'",
	} {
		if !strings.Contains(upperQuery, required) {
			t.Fatalf("queue authorization query omits %q", required)
		}
	}
}
