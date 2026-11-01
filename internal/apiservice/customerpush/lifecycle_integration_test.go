//go:build integration

package customerpush

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type lifecycleEnv struct {
	pool          *pgxpool.Pool
	h             *handlers
	org, source   uuid.UUID
	user          *policy.User
	legacyCounter int
}

func newLifecycleEnv(t *testing.T, ctx context.Context, events bool) *lifecycleEnv {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	runtimeRole, err := containers.RoleName("cp_auth_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE ROLE `+`"`+runtimeRole+`"`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(pool, runtimeRole, t.Logf) })
	if _, err := authschema.Apply(ctx, pool, authschema.Options{Schema: "auth", RuntimeRole: runtimeRole}); err != nil {
		t.Fatalf("auth schema: %v", err)
	}
	env := &lifecycleEnv{pool: pool, org: uuid.New(), source: uuid.New()}
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier) VALUES ($1, 'cp-life', 'CP Life', 'enterprise')`, []any{env.org}},
		{`INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, display_name, mode, enabled,
			webhook_mode, matched_integration_source_id, created_at, updated_at)
			VALUES ($1, $2, 'custom', 'life', 'legacy', NULL, 'customer_push', true, 'disabled', NULL, now(), now())`, []any{env.source, env.org.String()}},
	} {
		if _, err := pool.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	env.user = &policy.User{UserID: uuid.NewString(), OrgID: env.org.String(), Role: "admin"}
	env.user.ID = uuid.MustParse(env.user.UserID)
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, is_superuser, is_active, token_version) VALUES ($1, 'cp-life@x', false, true, 0)`, env.user.ID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	env.h = &handlers{pool: pool, logger: slog.Default(), now: time.Now, lifecycle: newLifecycle(events, "auth")}
	return env
}

// request builds a request for a handler: the user and the body in its context.
func (e *lifecycleEnv) request(body map[string]any, pathKey, pathValue string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("{}"))
	object := pyjson.NewObject()
	for key, value := range body {
		object.Set(key, value)
	}
	ctx := policy.WithUser(r.Context(), e.user)
	ctx = policy.WithBody(ctx, pybody.Body{Value: object})
	r = r.WithContext(ctx)
	if pathKey != "" {
		r.SetPathValue(pathKey, pathValue)
	}
	return r
}

func (e *lifecycleEnv) createToken(t *testing.T) string {
	t.Helper()
	w := httptest.NewRecorder()
	e.h.createSourceToken(w, e.request(map[string]any{"name": "ci", "scopes": []pyjson.Value{"ingest:write"}}, "source_id", e.source.String()))
	if w.Code != http.StatusCreated {
		t.Fatalf("create token: %d %s", w.Code, w.Body.String())
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out.ID
}

// legacyToken inserts a token row the way the routes did before the events
// existed: no outbox row, no revision row.
func (e *lifecycleEnv) legacyToken(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	e.legacyCounter++
	if _, err := e.pool.Exec(ctx, `INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, created_at)
		VALUES ($1, $2, $3, 'legacy', md5(gen_random_uuid()::text), 'fcpush_xxxxx', '["ingest:write"]', now())`, id, e.org.String(), e.source); err != nil {
		t.Fatal(err)
	}
	return id
}

type outboxRow struct {
	eventType string
	revision  int64
}

func (e *lifecycleEnv) outbox(t *testing.T, ctx context.Context, aggregateID string) []outboxRow {
	t.Helper()
	rows, err := e.pool.Query(ctx, `SELECT event_type, (payload->>'revision')::bigint FROM auth.auth_outbox_events
		WHERE aggregate_type = 'external_push_credential' AND aggregate_id = $1 ORDER BY id`, aggregateID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.eventType, &r.revision); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func (e *lifecycleEnv) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestTokenCreateRotateRevokeWriteLifecycleEventsInTheMutationTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newLifecycleEnv(t, ctx, true)

	created := env.createToken(t)
	if got := env.outbox(t, ctx, created); len(got) != 1 || got[0] != (outboxRow{externalpush.Created, 1}) {
		t.Fatalf("after create: %+v", got)
	}

	legacy := env.legacyToken(t, ctx)
	w := httptest.NewRecorder()
	env.h.rotateToken(w, env.request(nil, "token_id", legacy.String()))
	if w.Code != http.StatusOK {
		t.Fatalf("rotate: %d %s", w.Code, w.Body.String())
	}
	var rotatedBody struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &rotatedBody)
	if got := env.outbox(t, ctx, rotatedBody.ID); len(got) != 1 || got[0] != (outboxRow{externalpush.Rotated, 1}) {
		t.Fatalf("new token events: %+v", got)
	}
	if got := env.outbox(t, ctx, legacy.String()); len(got) != 2 || got[0] != (outboxRow{externalpush.Created, 1}) || got[1] != (outboxRow{externalpush.Revoked, 2}) {
		t.Fatalf("legacy parent events: %+v", got)
	}

	t.Run("revoke writes once and a repeat writes nothing", func(t *testing.T) {
		for range 2 {
			w := httptest.NewRecorder()
			env.h.revokeToken(w, env.request(nil, "token_id", created))
			if w.Code != http.StatusOK {
				t.Fatalf("revoke: %d %s", w.Code, w.Body.String())
			}
		}
		if got := env.outbox(t, ctx, created); len(got) != 2 || got[1] != (outboxRow{externalpush.Revoked, 2}) {
			t.Fatalf("created token events: %+v", got)
		}
	})

	t.Run("every mutation also wrote its public audit row", func(t *testing.T) {
		if n := env.count(t, ctx, `SELECT count(*) FROM audit_logs WHERE resource_id = $1 AND action IN ('ingest_token_created','ingest_token_revoked')`, created); n != 2 {
			t.Fatalf("%d audit rows for the created token, want 2", n)
		}
	})

	t.Run("two concurrent rotations of one legacy token emit created once", func(t *testing.T) {
		contested := env.legacyToken(t, ctx)
		var wg sync.WaitGroup
		codes := make([]int, 2)
		for i := range codes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				w := httptest.NewRecorder()
				env.h.rotateToken(w, env.request(nil, "token_id", contested.String()))
				codes[i] = w.Code
			}()
		}
		wg.Wait()
		if !(codes[0] == http.StatusOK && codes[1] == http.StatusBadRequest) && !(codes[0] == http.StatusBadRequest && codes[1] == http.StatusOK) {
			t.Fatalf("codes = %v, want one 200 and one 400", codes)
		}
		if got := env.outbox(t, ctx, contested.String()); len(got) != 2 || got[0] != (outboxRow{externalpush.Created, 1}) || got[1] != (outboxRow{externalpush.Revoked, 2}) {
			t.Fatalf("contested token events: %+v", got)
		}
	})

	t.Run("a failed parent revoked event rolls the rotation back", func(t *testing.T) {
		victim := env.legacyToken(t, ctx)
		if _, err := env.pool.Exec(ctx, `CREATE FUNCTION auth.refuse_revoked() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN IF NEW.event_type = 'external_push_credential.revoked' THEN RAISE EXCEPTION 'test refusal'; END IF; RETURN NEW; END $$`); err != nil {
			t.Fatal(err)
		}
		if _, err := env.pool.Exec(ctx, `CREATE TRIGGER refuse_revoked BEFORE INSERT ON auth.auth_outbox_events
			FOR EACH ROW EXECUTE FUNCTION auth.refuse_revoked()`); err != nil {
			t.Fatal(err)
		}
		defer func() { _, _ = env.pool.Exec(ctx, `DROP TRIGGER refuse_revoked ON auth.auth_outbox_events`) }()
		tokensBefore := env.count(t, ctx, `SELECT count(*) FROM external_ingest_tokens`)
		auditBefore := env.count(t, ctx, `SELECT count(*) FROM audit_logs`)
		outboxBefore := env.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events`)
		revisionsBefore := env.count(t, ctx, `SELECT count(*) FROM auth.aggregate_revisions`)

		w := httptest.NewRecorder()
		env.h.rotateToken(w, env.request(nil, "token_id", victim.String()))
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("rotate with a refused event: %d %s", w.Code, w.Body.String())
		}
		if got := env.count(t, ctx, `SELECT count(*) FROM external_ingest_tokens`); got != tokensBefore {
			t.Errorf("tokens %d, want %d (no new token)", got, tokensBefore)
		}
		if got := env.count(t, ctx, `SELECT count(*) FROM external_ingest_tokens WHERE id = $1 AND revoked_at IS NULL`, victim); got != 1 {
			t.Errorf("the parent was revoked by a rolled-back rotation")
		}
		if got := env.count(t, ctx, `SELECT count(*) FROM audit_logs`); got != auditBefore {
			t.Errorf("audit rows %d, want %d", got, auditBefore)
		}
		if got := env.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events`); got != outboxBefore {
			t.Errorf("outbox rows %d, want %d", got, outboxBefore)
		}
		if got := env.count(t, ctx, `SELECT count(*) FROM auth.aggregate_revisions`); got != revisionsBefore {
			t.Errorf("revision rows %d, want %d", got, revisionsBefore)
		}
	})
}

func TestTokenWritesWithTheSwitchOffWriteNoEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newLifecycleEnv(t, ctx, false)
	id := env.createToken(t)
	w := httptest.NewRecorder()
	env.h.revokeToken(w, env.request(nil, "token_id", id))
	if w.Code != http.StatusOK {
		t.Fatalf("revoke: %d", w.Code)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'external_push_credential'`); n != 0 {
		t.Fatalf("%d outbox rows with the switch off", n)
	}
}
