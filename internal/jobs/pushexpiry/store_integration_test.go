//go:build integration

package pushexpiry

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushevent"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// sweepEnv is the auth schema and the token table, with a sweeper login that
// holds exactly what the deployed worker will hold: SELECT on six token
// columns and EXECUTE on the append function. No UPDATE, no lock.
type sweepEnv struct {
	admin   *pgxpool.Pool
	sweeper *pgxpool.Pool
	schema  authschema.ValidatedIdentifier
	org     uuid.UUID
	source  uuid.UUID
}

func newSweepEnv(t *testing.T, ctx context.Context) *sweepEnv {
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
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	runtimeRole, err := containers.RoleName("sweep_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	sweeperRole, err := containers.RoleName("sweep_login", instance)
	if err != nil {
		t.Fatal(err)
	}
	peerRole, err := containers.RoleName("sweep_peer", instance)
	if err != nil {
		t.Fatal(err)
	}
	writerRole, err := containers.RoleName("sweep_writer", instance)
	if err != nil {
		t.Fatal(err)
	}
	const password = "sweep-test-not-a-real-credential"
	for _, statement := range []string{
		fmt.Sprintf("CREATE ROLE %q", runtimeRole),
		fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", sweeperRole, password),
		fmt.Sprintf("CREATE ROLE %q NOLOGIN", peerRole),
		fmt.Sprintf("CREATE ROLE %q NOLOGIN", writerRole),
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		containers.DropRole(admin, writerRole, t.Logf)
		containers.DropRole(admin, peerRole, t.Logf)
		containers.DropRole(admin, sweeperRole, t.Logf)
		containers.DropRole(admin, runtimeRole, t.Logf)
	})
	if _, err := authschema.Apply(ctx, admin, authschema.Options{
		Schema: "auth", RuntimeRole: runtimeRole, SessionReaderRoles: []string{sweeperRole},
		PushLifecycleWriterRole: writerRole, PushLifecycleCallerRoles: []string{sweeperRole, peerRole},
	}); err != nil {
		t.Fatalf("auth schema: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`GRANT SELECT (id, org_id, source_id, scopes, expires_at, revoked_at) ON external_ingest_tokens TO %q`, sweeperRole)); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(sweeperRole, password)
	sweeper, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sweeper.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	env := &sweepEnv{admin: admin, sweeper: sweeper, schema: schema, org: uuid.New(), source: uuid.New()}
	if _, err := admin.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier) VALUES ($1, 'sweep', 'Sweep', 'enterprise')`, env.org); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, display_name, mode, enabled,
		webhook_mode, matched_integration_source_id, created_at, updated_at)
		VALUES ($1, $2, 'custom', 'sweep', 'legacy', NULL, 'customer_push', true, 'disabled', NULL, now(), now())`, env.source, env.org.String()); err != nil {
		t.Fatal(err)
	}
	return env
}

// token inserts a token row with no event, as a token that predates the stream.
// expires is relative to the database clock.
func (e *sweepEnv) token(t *testing.T, ctx context.Context, expires string, revoked, orgWide bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	var source any = e.source
	if orgWide {
		source = nil
	}
	revokedAt := "NULL"
	if revoked {
		revokedAt = "now() - interval '2 hours'"
	}
	sql := fmt.Sprintf(`INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, expires_at, revoked_at, created_at)
		VALUES ($1, $2, $3, 'SENTINEL-NAME', 'SENTINEL-HASH-'||gen_random_uuid()::text, 'SENTINEL-PFX', '["ingest:write"]', %s, %s, now())`, expires, revokedAt)
	if _, err := e.admin.Exec(ctx, sql, id, e.org.String(), source); err != nil {
		t.Fatal(err)
	}
	return id
}

func (e *sweepEnv) store() PgStore { return PgStore{DB: e.sweeper, Schema: e.schema} }

func (e *sweepEnv) sweep(t *testing.T, ctx context.Context, on bool) error {
	t.Helper()
	var logs strings.Builder
	handler, err := NewHandler(e.store(), on, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Work(ctx, execution(due))
	if strings.Contains(logs.String(), "SENTINEL") {
		t.Fatalf("log holds a token field: %s", logs.String())
	}
	return err
}

type outboxEvent struct {
	eventType, wireType, key string
	revision                 int64
	payload                  []byte
	createdAt                time.Time
}

func (e *sweepEnv) outbox(t *testing.T, ctx context.Context, id uuid.UUID) []outboxEvent {
	t.Helper()
	rows, err := e.admin.Query(ctx, `SELECT event_type, idempotency_key, (payload->>'revision')::bigint, payload, created_at
		FROM auth.auth_outbox_events WHERE aggregate_type = 'external_push_credential' AND aggregate_id = $1 ORDER BY id`, id.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []outboxEvent
	for rows.Next() {
		var r outboxEvent
		if err := rows.Scan(&r.eventType, &r.key, &r.revision, &r.payload, &r.createdAt); err != nil {
			t.Fatal(err)
		}
		r.wireType = r.eventType
		r.eventType = strings.TrimPrefix(r.eventType, externalpush.Aggregate+".")
		out = append(out, r)
	}
	return out
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, _ := moduleroot.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..")
}

// replay applies the aggregate's events through the shared projection core and
// fails on any event it refuses (a parked row).
func (e *sweepEnv) replay(t *testing.T, ctx context.Context, id uuid.UUID) pushlifecycle.State {
	t.Helper()
	state := pushlifecycle.State{}
	for _, row := range e.outbox(t, ctx, id) {
		raw, _ := json.Marshal(map[string]any{
			"schema_version": contracts.ExternalPushLifecycleEventSurface, "aggregate_type": contracts.ExternalPushCredentialAggregate,
			"aggregate_id": id.String(), "event_type": row.wireType, "idempotency_key": row.key,
			"created_at": row.createdAt.UTC().Format(time.RFC3339Nano), "payload": json.RawMessage(row.payload),
		})
		event, err := contracts.ParseExternalPushLifecycleEvent(repoRoot(t), raw)
		if err != nil {
			t.Fatalf("%s: %v", row.eventType, err)
		}
		var outcome pushlifecycle.Outcome
		if state, outcome = pushlifecycle.Apply(state, event, nil); outcome != pushlifecycle.Applied {
			t.Fatalf("a parked row: %s refused with %s", row.eventType, outcome)
		}
	}
	return state
}

func (e *sweepEnv) tx(t *testing.T, ctx context.Context, fn func(pgx.Tx) error) error {
	t.Helper()
	tx, err := e.sweeper.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// routeCreate writes the created event the way the create route does.
func (e *sweepEnv) routeCreate(t *testing.T, ctx context.Context, id uuid.UUID, expires time.Time) {
	t.Helper()
	source := e.source
	change := pushevent.CreatedChange(id, []string{"ingest:write"}, &expires)
	if err := e.tx(t, ctx, func(tx pgx.Tx) error {
		return audit.AppendPushLifecycle(ctx, tx, e.schema, []audit.PushLifecycleEvent{{EventType: externalpush.Created, AggregateID: id.String(),
			OrganizationID: provisioning.WireID("organization", e.org.String()), Binding: externalpush.BindingSource, SourceID: source.String(), Change: change}})
	}); err != nil {
		t.Fatal(err)
	}
}

// routeRevoke writes the revoked event the way the revoke route does, and
// marks the row revoked.
func (e *sweepEnv) routeRevoke(t *testing.T, ctx context.Context, id uuid.UUID, expires time.Time) error {
	t.Helper()
	source := e.source
	snapshot := pushevent.CreatedChange(id, []string{"ingest:write"}, &expires)
	return e.tx(t, ctx, func(tx pgx.Tx) error {
		return audit.AppendPushLifecycle(ctx, tx, e.schema, []audit.PushLifecycleEvent{{EventType: externalpush.Revoked, AggregateID: id.String(),
			OrganizationID: provisioning.WireID("organization", e.org.String()), Binding: externalpush.BindingSource, SourceID: source.String(),
			Change: externalpush.Change{Status: "revoked"}, Snapshot: &snapshot}})
	})
}

func types(rows []outboxEvent) string {
	var out []string
	for _, r := range rows {
		out = append(out, fmt.Sprintf("%s/%d", r.eventType, r.revision))
	}
	return strings.Join(out, ",")
}

func TestSweepIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newSweepEnv(t, ctx)

	t.Run("an expired legacy token gets created then expired, once", func(t *testing.T) {
		id := env.token(t, ctx, "now() - interval '1 hour'", false, false)
		if err := env.sweep(t, ctx, true); err != nil {
			t.Fatal(err)
		}
		rows := env.outbox(t, ctx, id)
		if types(rows) != "created/1,expired/2" || rows[1].key != "external_push_credential:"+id.String()+":2" {
			t.Fatalf("rows = %+v", rows)
		}
		for _, banned := range []string{"SENTINEL", "fcpush"} {
			if strings.Contains(string(rows[0].payload)+string(rows[1].payload), banned) {
				t.Fatalf("payload holds %q", banned)
			}
		}
		if state := env.replay(t, ctx, id); state.Status != pushlifecycle.StatusExpired {
			t.Fatalf("state = %+v", state)
		}
		if err := env.sweep(t, ctx, true); err != nil {
			t.Fatal(err)
		}
		if again := env.outbox(t, ctx, id); types(again) != "created/1,expired/2" {
			t.Fatalf("a second sweep wrote: %+v", again)
		}
	})

	t.Run("an organization-wide token carries no source", func(t *testing.T) {
		id := env.token(t, ctx, "now() - interval '1 hour'", false, true)
		if err := env.sweep(t, ctx, true); err != nil {
			t.Fatal(err)
		}
		rows := env.outbox(t, ctx, id)
		if types(rows) != "created/1,expired/2" || strings.Contains(string(rows[1].payload), "source_id") {
			t.Fatalf("rows = %s", string(rows[1].payload))
		}
		env.replay(t, ctx, id)
	})

	t.Run("a revoked, a live and a never-expiring token get no event", func(t *testing.T) {
		revoked := env.token(t, ctx, "now() - interval '1 hour'", true, false)
		live := env.token(t, ctx, "now() + interval '1 hour'", false, false)
		never := env.token(t, ctx, "NULL", false, false)
		if err := env.sweep(t, ctx, true); err != nil {
			t.Fatal(err)
		}
		for name, id := range map[string]uuid.UUID{"revoked": revoked, "live": live, "never": never} {
			if rows := env.outbox(t, ctx, id); len(rows) != 0 {
				t.Fatalf("%s token got %+v", name, rows)
			}
		}
	})

	t.Run("a token that expired a microsecond ago is expired", func(t *testing.T) {
		id := env.token(t, ctx, "transaction_timestamp() - interval '1 microsecond'", false, false)
		if err := env.sweep(t, ctx, true); err != nil {
			t.Fatal(err)
		}
		if rows := env.outbox(t, ctx, id); types(rows) != "created/1,expired/2" {
			t.Fatalf("rows = %+v", rows)
		}
	})

	t.Run("the flag off writes nothing", func(t *testing.T) {
		id := env.token(t, ctx, "now() - interval '1 hour'", false, false)
		if err := env.sweep(t, ctx, false); err != nil {
			t.Fatal(err)
		}
		if rows := env.outbox(t, ctx, id); len(rows) != 0 {
			t.Fatalf("rows = %+v", rows)
		}
		if err := env.sweep(t, ctx, true); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a token the route created is expired once at revision 2, and a later revoke is a no-op", func(t *testing.T) {
		id := env.token(t, ctx, "now() - interval '1 hour'", false, false)
		expires := time.Now().Add(-time.Hour)
		env.routeCreate(t, ctx, id, expires)
		if err := env.sweep(t, ctx, true); err != nil {
			t.Fatal(err)
		}
		if rows := env.outbox(t, ctx, id); types(rows) != "created/1,expired/2" {
			t.Fatalf("rows = %+v", rows)
		}
		if err := env.routeRevoke(t, ctx, id, expires); err != nil {
			t.Fatal(err)
		}
		if rows := env.outbox(t, ctx, id); types(rows) != "created/1,expired/2" {
			t.Fatalf("revoke after expired wrote: %+v", rows)
		}
		env.replay(t, ctx, id)
	})

	t.Run("a token whose aggregate is already revoked gets no expired", func(t *testing.T) {
		id := env.token(t, ctx, "now() - interval '1 hour'", false, false)
		if err := env.routeRevoke(t, ctx, id, time.Now().Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := env.sweep(t, ctx, true); err != nil {
			t.Fatal(err)
		}
		if rows := env.outbox(t, ctx, id); types(rows) != "created/1,revoked/2" {
			t.Fatalf("rows = %+v", rows)
		}
		env.replay(t, ctx, id)
	})

	t.Run("two sweeps at once write one expired and park nothing", func(t *testing.T) {
		id := env.token(t, ctx, "now() - interval '1 hour'", false, false)
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, 2)
		for i := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				errs[i] = env.sweep(t, ctx, true)
			}()
		}
		close(start)
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		if rows := env.outbox(t, ctx, id); types(rows) != "created/1,expired/2" {
			t.Fatalf("rows = %+v", rows)
		}
		env.replay(t, ctx, id)
	})

	t.Run("a sweep against a revoke writes one terminal event, in either order", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			id := env.token(t, ctx, "now() - interval '1 hour'", false, false)
			var wg sync.WaitGroup
			start := make(chan struct{})
			var sweepErr, revokeErr error
			wg.Add(2)
			go func() { defer wg.Done(); <-start; sweepErr = env.sweep(t, ctx, true) }()
			go func() {
				defer wg.Done()
				<-start
				revokeErr = env.routeRevoke(t, ctx, id, time.Now().Add(-time.Hour))
			}()
			close(start)
			wg.Wait()
			if sweepErr != nil || revokeErr != nil {
				t.Fatalf("sweep=%v revoke=%v", sweepErr, revokeErr)
			}
			rows := env.outbox(t, ctx, id)
			if len(rows) != 2 || rows[0].eventType != "created" || (rows[1].eventType != "expired" && rows[1].eventType != "revoked") {
				t.Fatalf("round %d: rows = %s", round, types(rows))
			}
			env.replay(t, ctx, id)
		}
	})

	t.Run("more tokens than a page with one expires_at are all swept", func(t *testing.T) {
		const n = BatchLimit + 30
		ids := make([]uuid.UUID, 0, n)
		shared := fmt.Sprintf("timestamptz '%s'", time.Now().UTC().Add(-48*time.Hour).Truncate(time.Second).Format("2006-01-02 15:04:05+00"))
		for i := 0; i < n; i++ {
			ids = append(ids, env.token(t, ctx, shared, false, i%2 == 0))
		}
		if err := env.sweep(t, ctx, true); err != nil {
			t.Fatal(err)
		}
		for _, id := range ids {
			if rows := env.outbox(t, ctx, id); types(rows) != "created/1,expired/2" {
				t.Fatalf("%s: rows = %s", id, types(rows))
			}
		}
	})

	t.Run("a token expired outside the window is not swept, one inside is", func(t *testing.T) {
		old := env.token(t, ctx, "now() - interval '31 days'", false, false)
		recent := env.token(t, ctx, "now() - interval '29 days'", false, false)
		if err := env.sweep(t, ctx, true); err != nil {
			t.Fatal(err)
		}
		if rows := env.outbox(t, ctx, old); len(rows) != 0 {
			t.Fatalf("a token outside the window was swept: %s", types(rows))
		}
		if rows := env.outbox(t, ctx, recent); types(rows) != "created/1,expired/2" {
			t.Fatalf("a token inside the window: rows = %s", types(rows))
		}
	})

	t.Run("a second run on one handler skips an ended token and a new handler re-submits it as a no-op", func(t *testing.T) {
		id := env.token(t, ctx, "now() - interval '1 hour'", false, false)
		var logs strings.Builder
		handler, err := NewHandler(env.store(), true, slog.New(slog.NewTextHandler(&logs, nil)))
		if err != nil {
			t.Fatal(err)
		}
		for run := 0; run < 2; run++ {
			if err := handler.Work(ctx, execution(due)); err != nil {
				t.Fatal(err)
			}
		}
		if err := env.sweep(t, ctx, true); err != nil { // a new handler: restart
			t.Fatal(err)
		}
		if rows := env.outbox(t, ctx, id); types(rows) != "created/1,expired/2" {
			t.Fatalf("rows = %s", types(rows))
		}
	})

	t.Run("a sweep leaves every token row as it was", func(t *testing.T) {
		before := tokenDigest(t, ctx, env)
		env.token(t, ctx, "now() - interval '1 hour'", false, false)
		withNew := tokenDigest(t, ctx, env)
		if err := env.sweep(t, ctx, true); err != nil {
			t.Fatal(err)
		}
		if after := tokenDigest(t, ctx, env); after != withNew || before == withNew {
			t.Fatalf("token rows changed by the sweep")
		}
	})

	t.Run("the sweeper holds no UPDATE and no lock privilege", func(t *testing.T) {
		if _, err := env.sweeper.Exec(ctx, `UPDATE external_ingest_tokens SET revoked_at = now()`); err == nil {
			t.Fatal("the sweeper login can UPDATE the token table")
		}
		if _, err := env.sweeper.Exec(ctx, `SELECT id FROM external_ingest_tokens FOR UPDATE`); err == nil {
			t.Fatal("the sweeper login can take a row lock")
		}
	})
}

func tokenDigest(t *testing.T, ctx context.Context, env *sweepEnv) string {
	t.Helper()
	var digest string
	if err := env.admin.QueryRow(ctx, `SELECT coalesce(md5(string_agg(t::text, '|' ORDER BY id)), '') FROM external_ingest_tokens t`).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	return digest
}
