//go:build integration

package workload

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// env: writes go through the operator pool (the migration role, here the
// container superuser); Resolve reads through the runtime role, which holds
// SELECT only on workload_bindings.
type env struct {
	admin   *pgxpool.Pool
	runtime *pgxpool.Pool
	writer  *Store
	reader  *Store
}

var (
	shared     *env
	sharedErr  error
	sharedOnce sync.Once
	closers    []func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(closers) - 1; i >= 0; i-- {
		closers[i]()
	}
	os.Exit(code)
}

func getEnv(t *testing.T) *env {
	t.Helper()
	sharedOnce.Do(func() { shared, sharedErr = newEnv(t) })
	if sharedErr != nil {
		t.Fatal(sharedErr)
	}
	return shared
}

func newEnv(t *testing.T) (*env, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		return nil, err
	}
	closers = append(closers, func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		return nil, fmt.Errorf("admin pool: %w", err)
	}
	closers = append(closers, admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("workload_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	const password = "workload-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, fmt.Errorf("create role: %w", err)
	}
	closers = append(closers, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		return nil, fmt.Errorf("authschema.Apply: %w", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("runtime pool: %w", err)
	}
	closers = append(closers, runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return nil, err
	}
	writer, err := New(Config{Pool: admin, Schema: schema})
	if err != nil {
		return nil, err
	}
	reader, err := New(Config{Pool: runtime, Schema: schema})
	if err != nil {
		return nil, err
	}
	return &env{admin: admin, runtime: runtime, writer: writer, reader: reader}, nil
}

func (e *env) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func name() string { return "t-" + uuid.NewString()[:8] }

func tuple(ns string) BindingSpec {
	return BindingSpec{Environment: "test", TrustDomain: "cluster.local", Issuer: "https://issuer.test", Namespace: ns, ServiceAccountName: "sa-probe-7x"}
}

func (b BindingSpec) input() ResolveInput {
	return ResolveInput{TrustDomain: b.TrustDomain, Issuer: b.Issuer, Namespace: b.Namespace,
		ServiceAccountName: b.ServiceAccountName, ServiceAccountUID: b.ServiceAccountUID}
}

func registered(t *testing.T, e *env, audiences []string) (string, BindingSpec) {
	t.Helper()
	ctx := ctxT(t)
	n := name()
	if _, err := e.writer.Register(ctx, Spec{Name: n, Audiences: audiences}, Actor{}); err != nil {
		t.Fatal(err)
	}
	b := tuple("ns-" + n)
	if _, err := e.writer.AddBinding(ctx, n, b, Actor{}); err != nil {
		t.Fatal(err)
	}
	return n, b
}

func TestRegisterWritesPrincipalWorkloadAuditAndRevisionOne(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n := name()
	id, err := e.writer.Register(ctx, Spec{Name: n, Audiences: []string{"auth-service"}}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE id = $1 AND kind = 'workload'`, id); c != 1 {
		t.Fatalf("principal rows = %d", c)
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.workload.registered' AND subject_principal_id = $1`, id); c != 1 {
		t.Fatalf("audit rows = %d", c)
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'workload' AND aggregate_id = $1 AND event_type = 'workload.created'`, "prn_"+id.String()); c != 1 {
		t.Fatalf("outbox rows = %d", c)
	}
	if _, err := e.writer.Register(ctx, Spec{Name: n}, Actor{}); !errors.Is(err, ErrNameTaken) {
		t.Fatalf("second Register = %v, want ErrNameTaken", err)
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE display_name = $1`, n); c != 1 {
		t.Fatalf("a refused Register left %d principals", c)
	}
}

func TestSeedIsIdempotent(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	specs := []Spec{
		{Name: name(), Audiences: []string{"auth-service", "dev-health-api"}},
		{Name: name()},
	}
	first, err := e.writer.Seed(ctx, specs, Actor{})
	if err != nil || first.Created != 2 || first.Updated != 0 {
		t.Fatalf("first Seed = %+v, %v", first, err)
	}
	audits := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`)
	outbox := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events`)
	revs := e.count(t, ctx, `SELECT coalesce(sum(revision), 0) FROM auth.aggregate_revisions`)
	second, err := e.writer.Seed(ctx, specs, Actor{})
	if err != nil || second.Created != 0 || second.Updated != 0 || second.Unchanged != 2 {
		t.Fatalf("second Seed = %+v, %v", second, err)
	}
	if a := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`); a != audits {
		t.Fatalf("a repeat Seed wrote audit rows: %d -> %d", audits, a)
	}
	if o := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events`); o != outbox {
		t.Fatalf("a repeat Seed wrote outbox rows: %d -> %d", outbox, o)
	}
	if r := e.count(t, ctx, `SELECT coalesce(sum(revision), 0) FROM auth.aggregate_revisions`); r != revs {
		t.Fatalf("a repeat Seed bumped revisions: %d -> %d", revs, r)
	}
}

func TestSeedChangedContractUpdatesOnceAndNeverDeletes(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	keep, drop := name(), name()
	if _, err := e.writer.Seed(ctx, []Spec{{Name: keep}, {Name: drop}}, Actor{}); err != nil {
		t.Fatal(err)
	}
	changed := []Spec{{Name: keep, Audiences: []string{"auth-service"}, AllowedActions: []string{"org.read"}}}
	r, err := e.writer.Seed(ctx, changed, Actor{})
	if err != nil || r.Updated != 1 {
		t.Fatalf("Seed = %+v, %v", r, err)
	}
	found := false
	for _, u := range r.Unlisted {
		if u == drop {
			found = true
		}
	}
	if !found {
		t.Fatalf("Unlisted = %v, want to include %s", r.Unlisted, drop)
	}
	info, err := e.writer.Lookup(ctx, keep)
	if err != nil || len(info.Audiences) != 1 || len(info.AllowedActions) != 1 {
		t.Fatalf("Lookup = %+v, %v", info, err)
	}
	if _, err := e.writer.Lookup(ctx, drop); err != nil {
		t.Fatalf("Seed deleted an unlisted workload: %v", err)
	}
	again, err := e.writer.Seed(ctx, changed, Actor{})
	if err != nil || again.Updated != 0 || again.Unchanged != 1 {
		t.Fatalf("repeat = %+v, %v", again, err)
	}
	plan, err := e.writer.PlanSeed(ctx, []Spec{{Name: keep}, {Name: name()}})
	if err != nil || plan.Created != 1 || plan.Updated != 1 {
		t.Fatalf("PlanSeed = %+v, %v", plan, err)
	}
	if info, _ := e.writer.Lookup(ctx, keep); len(info.Audiences) != 1 {
		t.Fatal("PlanSeed wrote")
	}
}

func TestResolveAcceptsAnEnabledBindingThroughTheRuntimeRole(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, b := registered(t, e, []string{"auth-service"})
	reg, err := e.reader.Resolve(ctx, b.input())
	if err != nil || reg.Name != n || len(reg.Audiences) != 1 || reg.Revision != 1 {
		t.Fatalf("Resolve = %+v, %v", reg, err)
	}
}

func TestResolveRefusals(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	refused := func(t *testing.T, in ResolveInput, want reason) {
		t.Helper()
		_, err := e.reader.Resolve(ctx, in)
		if !errors.Is(err, ErrRefused) {
			t.Fatalf("Resolve = %v, want ErrRefused", err)
		}
		if got := ReasonOf(err); got != publicReason(want) {
			t.Fatalf("ReasonOf = %q, want %q", got, publicReason(want))
		}
		for _, v := range []string{in.Namespace, in.ServiceAccountName, in.ServiceAccountUID, in.Issuer, in.TrustDomain} {
			if v != "" && strings.Contains(err.Error(), v) {
				t.Fatalf("refusal text %q holds input value %q", err.Error(), v)
			}
		}
		if _, why, err := e.reader.resolve(ctx, in); err != nil || why != want {
			t.Fatalf("internal reason = %q, %v; want %q", why, err, want)
		}
	}
	t.Run("unknown tuple", func(t *testing.T) {
		refused(t, tuple("ns-"+name()).input(), reasonUnknown)
	})
	t.Run("disabled workload", func(t *testing.T) {
		n, b := registered(t, e, []string{"auth-service"})
		if err := e.writer.Disable(ctx, n, Actor{}); err != nil {
			t.Fatal(err)
		}
		refused(t, b.input(), reasonWorkloadDisabled)
		if err := e.writer.Enable(ctx, n, Actor{}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.reader.Resolve(ctx, b.input()); err != nil {
			t.Fatalf("re-enabled workload refused: %v", err)
		}
	})
	t.Run("disabled binding", func(t *testing.T) {
		n, b := registered(t, e, []string{"auth-service"})
		var id uuid.UUID
		if err := e.admin.QueryRow(ctx, `SELECT id FROM auth.workload_bindings WHERE namespace = $1`, "ns-"+n).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if err := e.writer.DisableBinding(ctx, id, Actor{}); err != nil {
			t.Fatal(err)
		}
		refused(t, b.input(), reasonBindingDisabled)
		if err := e.writer.EnableBinding(ctx, id, Actor{}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.reader.Resolve(ctx, b.input()); err != nil {
			t.Fatalf("re-enabled binding refused: %v", err)
		}
	})
	t.Run("disabled principal", func(t *testing.T) {
		n, b := registered(t, e, []string{"auth-service"})
		info, _ := e.writer.Lookup(ctx, n)
		pid := info.PrincipalID.String()
		schema, _ := authschema.NewValidatedIdentifier("auth")
		if err := audit.Commit(ctx, e.admin, schema, audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				_, err := tx.Exec(ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, info.PrincipalID)
				return err
			},
			Audit:     audit.AuditEvent{EventType: "auth.workload.disabled", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &pid},
			OffStream: audit.OffStreamWorkloadRegistry,
		}); err != nil {
			t.Fatal(err)
		}
		refused(t, b.input(), reasonPrincipalOff)
	})
	t.Run("uid pinned and not equal", func(t *testing.T) {
		n := name()
		if _, err := e.writer.Register(ctx, Spec{Name: n, Audiences: []string{"auth-service"}}, Actor{}); err != nil {
			t.Fatal(err)
		}
		b := tuple("ns-" + n)
		b.ServiceAccountUID = uuid.NewString()
		if _, err := e.writer.AddBinding(ctx, n, b, Actor{}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.reader.Resolve(ctx, b.input()); err != nil {
			t.Fatalf("equal uid refused: %v", err)
		}
		other := b.input()
		other.ServiceAccountUID = uuid.NewString()
		refused(t, other, reasonUIDMismatch)
		none := b.input()
		none.ServiceAccountUID = ""
		refused(t, none, reasonUIDMismatch)
	})
	t.Run("uid not pinned accepts any uid", func(t *testing.T) {
		_, b := registered(t, e, []string{"auth-service"})
		in := b.input()
		in.ServiceAccountUID = uuid.NewString()
		if _, err := e.reader.Resolve(ctx, in); err != nil {
			t.Fatalf("unpinned binding refused a uid: %v", err)
		}
	})
	t.Run("empty audience never resolves", func(t *testing.T) {
		_, b := registered(t, e, nil)
		refused(t, b.input(), reasonNoAudience)
	})
	t.Run("malformed input", func(t *testing.T) {
		refused(t, ResolveInput{}, reasonBadInput)
	})
	t.Run("store error fails closed", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := e.reader.Resolve(cancelled, tuple("ns-x").input())
		if !errors.Is(err, ErrRefused) || ReasonOf(err) != ReasonStoreError {
			t.Fatalf("Resolve on a dead context = %v, want ErrRefused / store_error", err)
		}
	})
}

func TestSecondWorkloadCannotBindTheSameServiceAccountTuple(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	a, b := registered(t, e, []string{"auth-service"})
	other := name()
	if _, err := e.writer.Register(ctx, Spec{Name: other, Audiences: []string{"auth-service"}}, Actor{}); err != nil {
		t.Fatal(err)
	}
	before := e.count(t, ctx, `SELECT count(*) FROM auth.workload_bindings`)
	if _, err := e.writer.AddBinding(ctx, other, b, Actor{}); !errors.Is(err, ErrBindingTaken) {
		t.Fatalf("AddBinding of a held tuple = %v, want ErrBindingTaken", err)
	}
	if after := e.count(t, ctx, `SELECT count(*) FROM auth.workload_bindings`); after != before {
		t.Fatalf("a refused binding wrote a row: %d -> %d", before, after)
	}
	reg, err := e.reader.Resolve(ctx, b.input())
	if err != nil || reg.Name != a {
		t.Fatalf("Resolve = %+v, %v; want the first workload", reg, err)
	}
}

func TestAddBindingRepeatIsANoOpAndReturnsTheSameID(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, b := registered(t, e, []string{"auth-service"})
	var id uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT id FROM auth.workload_bindings WHERE namespace = $1`, "ns-"+n).Scan(&id); err != nil {
		t.Fatal(err)
	}
	audits := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.workload.binding_added'`)
	got, err := e.writer.AddBinding(ctx, n, b, Actor{})
	if err != nil || got != id {
		t.Fatalf("repeat AddBinding = %v, %v; want %v", got, err, id)
	}
	if a := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.workload.binding_added'`); a != audits {
		t.Fatalf("a repeat binding wrote an audit row")
	}
	changed := b
	changed.Environment = "other"
	if _, err := e.writer.AddBinding(ctx, n, changed, Actor{}); !errors.Is(err, ErrBindingTaken) {
		t.Fatalf("same tuple, other environment = %v, want ErrBindingTaken", err)
	}
}

func TestRuntimeRoleCannotWriteBindingsOrAnythingOutsideAuditCommit(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, _ := registered(t, e, []string{"auth-service"})
	if _, err := e.runtime.Exec(ctx, `UPDATE auth.workload_bindings SET disabled_at = now() WHERE namespace = $1`, "ns-"+n); err == nil {
		t.Fatal("the runtime role updated workload_bindings")
	}
	if _, err := e.runtime.Exec(ctx, `DELETE FROM auth.workload_bindings`); err == nil {
		t.Fatal("the runtime role deleted workload_bindings")
	}
	if _, err := e.admin.Exec(ctx, `UPDATE auth.workload_bindings SET disabled_at = now() WHERE namespace = $1`, "ns-"+n); err == nil {
		t.Fatal("a write outside audit.Commit was accepted")
	}
	if _, err := e.admin.Exec(ctx, `TRUNCATE auth.workload_bindings`); err == nil {
		t.Fatal("TRUNCATE outside audit.Commit was accepted")
	}
}

func TestDisableDeniesAtOnceAndAuditsTheTransition(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, b := registered(t, e, []string{"auth-service"})
	if err := e.writer.Disable(ctx, n, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.writer.Disable(ctx, n, Actor{}); err != nil {
		t.Fatalf("repeat Disable = %v", err)
	}
	if _, err := e.reader.Resolve(ctx, b.input()); !errors.Is(err, ErrRefused) {
		t.Fatalf("Resolve after Disable = %v", err)
	}
	info, _ := e.writer.Lookup(ctx, n)
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.workload.disabled' AND subject_principal_id = $1`, info.PrincipalID); c != 1 {
		t.Fatalf("disable audit rows = %d", c)
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE id = $1 AND disabled_at IS NOT NULL`, info.PrincipalID); c != 1 {
		t.Fatal("the principal was not disabled with the workload")
	}
}

func TestAllowedActionsAndAudienceChangesAreAuditedAndNarrowOnly(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	n, b := registered(t, e, []string{"auth-service"})
	if err := e.writer.SetAllowedActions(ctx, n, []string{"org.read"}, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.writer.SetAllowedActions(ctx, n, []string{"*"}, Actor{}); !errors.Is(err, ErrActionInvalid) {
		t.Fatalf("wildcard = %v", err)
	}
	if err := e.writer.SetAudiences(ctx, n, []string{"auth-service", "dev-health-api"}, Actor{}); err != nil {
		t.Fatal(err)
	}
	reg, err := e.reader.Resolve(ctx, b.input())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reg.Narrow("dev-health-api", []string{"org.read"}); err != nil || len(got) != 1 {
		t.Fatalf("Narrow = %v, %v", got, err)
	}
	if _, err := reg.Narrow("dev-health-api", []string{"org.write"}); !errors.Is(err, ErrRefused) {
		t.Fatalf("Narrow widened: %v", err)
	}
	for _, ev := range []string{"auth.workload.allowed_actions_changed", "auth.workload.audiences_changed"} {
		if c := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1`, ev); c < 1 {
			t.Fatalf("no audit row for %s", ev)
		}
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'workload.audiences_changed'`); c < 1 {
		t.Fatal("no audiences_changed outbox event")
	}
}
