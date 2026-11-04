//go:build integration

package identity

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type statementLog struct {
	mu   sync.Mutex
	sqls []string
}

func (l *statementLog) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sqls = append(l.sqls, data.SQL)
	return ctx
}

func (l *statementLog) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// take returns the statements so far without the transaction's end, which is the one difference by design, and resets.
func (l *statementLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, s := range l.sqls {
		if t := strings.ToLower(strings.TrimSpace(s)); t != "commit" && t != "rollback" {
			out = append(out, s)
		}
	}
	l.sqls = nil
	return out
}

func tracedOwnedStore(t *testing.T, e *env) (*Store, *statementLog) {
	t.Helper()
	log := &statementLog{}
	cfg := e.runtime.Config().Copy()
	cfg.ConnConfig.Tracer = log
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(Config{Pool: pool, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	return store, log
}

// A rehearsed provision makes the statements of a real one, with and without an organization, and stores nothing.
func TestRehearsedProvisionRunsTheStatementsOfARealOneAndStoresNothing(t *testing.T) {
	e := getEnv(t)
	for _, org := range []*NewOrganization{nil, {Name: "Rehearsal Co " + uuid.NewString()[:8]}} {
		store, log := tracedOwnedStore(t, e)
		ctx := context.Background()
		hash := testBcrypt
		run := func(ctx context.Context, email string) {
			t.Helper()
			if _, err := store.ProvisionLocal(ctx, LocalProvisionRequest{Email: email, PasswordHash: &hash, Actor: SelfSignUp(nil), Organization: org}); err != nil {
				t.Fatalf("ProvisionLocal(%s): %v", email, err)
			}
		}
		real, rehearsed := uniqueEmail("real"), uniqueEmail("rehearsed")
		log.take()
		run(ctx, real)
		want := log.take()

		before := e.count(t, ctx, `SELECT (SELECT count(*) FROM auth.principals) + (SELECT count(*) FROM auth.users) + (SELECT count(*) FROM public.users)
 + (SELECT count(*) FROM public.organizations) + (SELECT count(*) FROM auth.auth_outbox_events) + (SELECT count(*) FROM auth.security_audit_events)`)
		run(audit.WithRehearsal(ctx), rehearsed)
		got := log.take()
		after := e.count(t, ctx, `SELECT (SELECT count(*) FROM auth.principals) + (SELECT count(*) FROM auth.users) + (SELECT count(*) FROM public.users)
 + (SELECT count(*) FROM public.organizations) + (SELECT count(*) FROM auth.auth_outbox_events) + (SELECT count(*) FROM auth.security_audit_events)`)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("org=%v: rehearsed statements differ from the real ones:\nreal      %q\nrehearsed %q", org != nil, want, got)
		}
		if before != after {
			t.Errorf("org=%v: a rehearsal changed the stored row count from %d to %d", org != nil, before, after)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM public.users WHERE email = $1`, rehearsed); n != 0 {
			t.Errorf("org=%v: the rehearsed address is stored (%d rows)", org != nil, n)
		}
	}
}
