//go:build integration

package usersbackfill

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type reconEnv struct {
	admin   *pgxpool.Pool
	runtime *pgxpool.Pool
	store   *identity.Store
	schema  authschema.ValidatedIdentifier
}

var (
	reconShared *reconEnv
	reconErr    error
	reconOnce   sync.Once
	reconClose  []func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(reconClose) - 1; i >= 0; i-- {
		reconClose[i]()
	}
	os.Exit(code)
}

func getReconEnv(t *testing.T) *reconEnv {
	t.Helper()
	reconOnce.Do(func() { reconShared, reconErr = newReconEnv(t) })
	if reconErr != nil {
		t.Fatal(reconErr)
	}
	return reconShared
}

func newReconEnv(t *testing.T) (*reconEnv, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		return nil, err
	}
	reconClose = append(reconClose, func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(c)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		return nil, err
	}
	reconClose = append(reconClose, admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("recon_runtime", instance)
	if err != nil {
		return nil, err
	}
	const password = "reconcile-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, err
	}
	reconClose = append(reconClose, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		return nil, fmt.Errorf("authschema.Apply: %w", err)
	}
	cols := "id, email, full_name, password_hash, is_active, is_verified, auth_provider, auth_provider_id, is_superuser, token_version, last_login_at"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`GRANT SELECT (%s) ON public.users TO %q`, cols, role)); err != nil {
		return nil, err
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	reconClose = append(reconClose, runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return nil, err
	}
	store, err := identity.New(identity.Config{Pool: runtime, Schema: schema})
	if err != nil {
		return nil, err
	}
	return &reconEnv{admin: admin, runtime: runtime, store: store, schema: schema}, nil
}

func (e *reconEnv) seed(t *testing.T, ctx context.Context, email string, active, verified bool) uuid.UUID {
	t.Helper()
	return e.seedID(t, ctx, uuid.New(), email, active, verified)
}

func (e *reconEnv) seedID(t *testing.T, ctx context.Context, id uuid.UUID, email string, active, verified bool) uuid.UUID {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, '$2b$12$abcdefghijklmnopqrstuuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0', 'local', $3, $4, false, 0, now(), now())`,
		id, email, active, verified); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
	return id
}

func (e *reconEnv) backfill(t *testing.T, ctx context.Context) {
	t.Helper()
	if _, err := Run(ctx, PGSource{Pool: e.runtime}, e.store, Options{}); err != nil {
		t.Fatal(err)
	}
}

func (e *reconEnv) reconcile(t *testing.T, ctx context.Context) Reconciliation {
	t.Helper()
	tx, err := OpenSnapshot(ctx, e.runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	rep, err := Reconcile(ctx, TxSource{Tx: tx}, TxTarget{Tx: tx, Schema: e.schema}, 2)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func uniq(prefix string) string {
	return fmt.Sprintf("%s-%s@example.test", prefix, uuid.NewString()[:8])
}

// After a backfill the reconciliation is zero-diff; a change made behind the
// backfill's back shows up in exactly its class; a re-run backfill clears it.
func TestReconcileAfterBackfillIsZeroDiffAndSeesPlantedDrift(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Earlier tests in the package share the database; reconcile only counts differences, so start clean.
	addr := uniq("recon")
	id := e.seed(t, ctx, addr, true, true)
	e.seed(t, ctx, uniq("recon"), false, false)
	e.backfill(t, ctx)
	if rep := e.reconcile(t, ctx); rep.GatedTotal() != 0 || rep.Matched != rep.Seen {
		t.Fatalf("after backfill: %+v", rep)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if rep := e.reconcile(t, ctx); rep.Diffs[DiffActive] != 1 || rep.GatedTotal() != 1 {
		t.Fatalf("active drift: %+v", rep.Diffs)
	}
	e.backfill(t, ctx)
	if rep := e.reconcile(t, ctx); rep.GatedTotal() != 0 {
		t.Fatalf("after the second backfill: %+v", rep.Diffs)
	}
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.users WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if rep := e.reconcile(t, ctx); rep.Diffs[DiffExtraInAuth] != 1 {
		t.Fatalf("auth user without a public row: %+v", rep.Diffs)
	}
	// Put the public row back so the shared database stays zero-diff for the next test.
	e.seedID(t, ctx, id, addr, false, true)
	if rep := e.reconcile(t, ctx); rep.GatedTotal() != 0 {
		t.Fatalf("after restoring the public row: %+v", rep.Diffs)
	}
}

// An external identity the public row does not name is a gated diff; unlinking
// it restores zero-diff.
func TestReconcileSeesAnAuthIdentityThePublicRowDoesNotName(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	id := e.seed(t, ctx, uniq("extraid"), true, true)
	e.backfill(t, ctx)
	if rep := e.reconcile(t, ctx); rep.GatedTotal() != 0 {
		t.Fatalf("before linking: %+v", rep.Diffs)
	}
	key := identity.ExternalKey{Issuer: "https://github.com", Provider: identity.ProviderGitHub, Subject: "extra-" + uuid.NewString()}
	if _, err := e.store.LinkIdentity(ctx, key, id, identity.Actor{}); err != nil {
		t.Fatal(err)
	}
	if rep := e.reconcile(t, ctx); rep.Diffs[DiffExtraIdentity] != 1 || rep.GatedTotal() != 1 {
		t.Fatalf("with an extra identity: %+v", rep.Diffs)
	}
	if err := e.store.UnlinkIdentity(ctx, key, id, identity.Actor{}); err != nil {
		t.Fatal(err)
	}
	if rep := e.reconcile(t, ctx); rep.GatedTotal() != 0 {
		t.Fatalf("after unlinking: %+v", rep.Diffs)
	}
}

// The snapshot transaction refuses every write with SQLSTATE 25006.
func TestSnapshotRefusesWritesWithReadOnlyTransaction(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tx, err := OpenSnapshot(ctx, e.runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	for _, stmt := range []string{
		`INSERT INTO auth.principals (id, kind, display_name) VALUES (gen_random_uuid(), 'user', 'x')`,
		`UPDATE auth.principals SET display_name = 'x'`,
		`DELETE FROM auth.platform_role_assignments`,
	} {
		_, err := tx.Exec(ctx, stmt)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "25006" {
			t.Fatalf("%q: error %v, want SQLSTATE 25006", stmt, err)
		}
		// 25006 aborts the transaction; open a fresh snapshot for the next statement.
		_ = tx.Rollback(ctx)
		if tx, err = OpenSnapshot(ctx, e.runtime); err != nil {
			t.Fatal(err)
		}
	}
}

// One snapshot: a row committed after the transaction took its snapshot is not
// seen by a later page of either schema.
func TestSnapshotDoesNotSeeLaterCommits(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tx, err := OpenSnapshot(ctx, e.runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	src := TxSource{Tx: tx}
	before, err := src.Page(ctx, nil, 100000)
	if err != nil {
		t.Fatal(err)
	}
	late := e.seed(t, ctx, uniq("late"), true, true)
	e.backfill(t, ctx)
	after, err := src.Page(ctx, nil, 100000)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("snapshot saw %d rows, then %d after a later commit", len(before), len(after))
	}
	targets, err := (TxTarget{Tx: tx, Schema: e.schema}).Page(ctx, nil, 100000)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range targets {
		if r.ID == late {
			t.Fatal("snapshot saw an auth row committed after it began")
		}
	}
}

// The verb exits 1 on a gated diff and 0 on zero-diff, and prints no address.
func TestVerbExitCodesAndOutput(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	addr := uniq("verb")
	id := e.seed(t, ctx, addr, true, true)
	e.backfill(t, ctx)
	dsn := e.runtime.Config().ConnString()
	run := func() (int, string, string) {
		var stdout, stderr bytes.Buffer
		code := reconcileVerbForTest().Run(ctx, cli.Env{
			Lookup: func(k string) (string, bool) {
				if k == dsnEnv {
					return dsn, true
				}
				return "", false
			}, Stdout: &stdout, Stderr: &stderr})
		return code, stdout.String(), stderr.String()
	}
	if code, out, errText := run(); code != cli.ExitOK || !strings.Contains(out, "gated_diffs=0") {
		t.Fatalf("zero-diff: exit %d\n%s\n%s", code, out, errText)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET token_version = 9 WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	code, out, errText := run()
	if code != cli.ExitFailure || !strings.Contains(out, "diff.token_version_diff=1") {
		t.Fatalf("drift: exit %d\n%s\n%s", code, out, errText)
	}
	for _, text := range []string{out, errText} {
		if strings.Contains(text, addr) || strings.Contains(text, id.String()) {
			t.Fatal("the verb printed an address or an id")
		}
	}
}

func reconcileVerbForTest() cli.Command {
	for _, c := range Command().Children {
		if c.Name == "users-reconcile" {
			return c
		}
	}
	panic("no users-reconcile verb")
}
