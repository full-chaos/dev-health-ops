//go:build integration

package audit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

// requestContext is the context the auth service's middleware binds for a
// request with these User-Agent header values (none when empty).
func requestContext(t *testing.T, ctx context.Context, agents ...string) context.Context {
	t.Helper()
	trust, err := clientaddr.ParseTrust("")
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil).WithContext(ctx)
	req.RemoteAddr = "198.51.100.9:4711"
	for _, agent := range agents {
		req.Header.Add("User-Agent", agent)
	}
	var bound context.Context
	clientaddr.Middleware(trust)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { bound = r.Context() })).
		ServeHTTP(httptest.NewRecorder(), req)
	if bound == nil {
		t.Fatal("the middleware did not call the handler")
	}
	return bound
}

// TestAuditRowsCarryTheRequestUserAgent: Append and Commit write the request's
// user agent, the first header value, and NULL when the request sent none.
func TestAuditRowsCarryTheRequestUserAgent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	writer := runtimeRolePool(t, ctx, env.pool)

	agentOf := func(t *testing.T, requestID string) *string {
		t.Helper()
		var agent *string
		if err := env.pool.QueryRow(ctx, `SELECT user_agent FROM auth.security_audit_events WHERE request_id = $1`, requestID).Scan(&agent); err != nil {
			t.Fatalf("reading the row: %v", err)
		}
		return agent
	}
	failedLogin := func(requestID string) AuditEvent {
		return AuditEvent{EventType: EventLoginFailed, Outcome: OutcomeDenied, RequestID: &requestID,
			Attributes: map[string]any{"reason_code": "invalid_credentials", "provider": "local"}}
	}

	t.Run("Append, a failed sign-in with no session", func(t *testing.T) {
		requestID := uuid.NewString()
		if err := Append(requestContext(t, ctx, "venue-oracle/1"), writer, env.schema, failedLogin(requestID)); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if got := agentOf(t, requestID); got == nil || *got != "venue-oracle/1" {
			t.Fatalf("user_agent = %v, want venue-oracle/1", got)
		}
	})
	t.Run("Append, the first of two header values", func(t *testing.T) {
		requestID := uuid.NewString()
		if err := Append(requestContext(t, ctx, "first/1", "second/2"), writer, env.schema, failedLogin(requestID)); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if got := agentOf(t, requestID); got == nil || *got != "first/1" {
			t.Fatalf("user_agent = %v, want first/1", got)
		}
	})
	t.Run("Append, no User-Agent header", func(t *testing.T) {
		requestID := uuid.NewString()
		if err := Append(requestContext(t, ctx), writer, env.schema, failedLogin(requestID)); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if got := agentOf(t, requestID); got != nil {
			t.Fatalf("user_agent = %q, want NULL", *got)
		}
	})
	t.Run("Commit", func(t *testing.T) {
		requestID := uuid.NewString()
		err := Commit(requestContext(t, ctx, "commit-agent/3"), env.pool, env.schema, Mutation{
			Apply: func(ctx context.Context, tx TxOps) error {
				_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES (gen_random_uuid(), $1, $1)`, "ua-"+requestID[:8])
				return err
			},
			Audit:  AuditEvent{EventType: "organization.created", Outcome: OutcomeAllowed, RequestID: &requestID},
			Events: []OutboxEvent{{AggregateType: "organization", AggregateID: keyedOrg("ua-" + requestID), EventType: "organization.created"}},
		})
		if err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if got := agentOf(t, requestID); got == nil || *got != "commit-agent/3" {
			t.Fatalf("user_agent = %v, want commit-agent/3", got)
		}
	})
}
