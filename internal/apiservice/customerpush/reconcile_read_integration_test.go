//go:build integration

package customerpush

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// workerPool opens a pool as a worker-shaped login: column SELECT of the domain read (six columns of
// external_ingest_tokens) and ProjectionReaderReads, no superuser, no other grant.
func workerPool(t *testing.T, ctx context.Context, e *lifecycleEnv) *pgxpool.Pool {
	t.Helper()
	role := "cp_worker_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	db := e.pool.Config().ConnConfig.Database
	const password = "customerpush-worker-test-only-not-a-credential"
	for _, stmt := range []string{
		fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password),
		fmt.Sprintf(`GRANT CONNECT ON DATABASE %q TO %q`, db, role),
		fmt.Sprintf(`GRANT SELECT (id, org_id, source_id, scopes, expires_at, revoked_at) ON external_ingest_tokens TO %q`, role),
		fmt.Sprintf(`GRANT USAGE ON SCHEMA auth TO %q`, role),
	} {
		if _, err := e.pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}
	for _, g := range authschema.ProjectionReaderReads() {
		cols := ""
		for i, c := range g.Columns {
			if i > 0 {
				cols += ", "
			}
			cols += c
		}
		if _, err := e.pool.Exec(ctx, fmt.Sprintf(`GRANT SELECT (%s) ON auth.%s TO %q`, cols, g.Table, role)); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { containers.DropRole(e.pool, role, t.Logf) })
	cfg := e.pool.Config().ConnConfig
	u := url.URL{Scheme: "postgres", User: url.UserPassword(role, password), Host: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), Path: cfg.Database, RawQuery: "sslmode=disable"}
	pool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func tokenIDs(tokens []pushreconcile.Token) []string {
	out := make([]string, len(tokens))
	for i, tk := range tokens {
		out[i] = tk.ID
	}
	return out
}

// C22 (integration): the worker-shaped login reads the domain page with the six columns and cannot read a secret column.
func TestC22iWorkerShapedLoginReadsTheDomainPageAndNoSecretColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newLifecycleEnv(t, ctx, true)
	env.createToken(t)
	env.createToken(t)
	env.legacyOrgToken(t, ctx)
	worker := workerPool(t, ctx, env)
	ident, _ := authschema.NewValidatedIdentifier("auth")
	var all []string
	after := uuid.Nil
	for {
		pair, err := ReadDomainPage(ctx, worker, ident, after, 2)
		if err != nil {
			t.Fatalf("ReadDomainPage as the worker login: %v", err)
		}
		all = append(all, tokenIDs(pair.Tokens)...)
		if pair.Last {
			break
		}
		after = uuid.MustParse(pair.Next)
	}
	if len(all) != 3 || !slices.IsSorted(all) {
		t.Fatalf("tokens = %v", all)
	}
	if _, err := worker.Exec(ctx, `SELECT token_hash FROM external_ingest_tokens`); err == nil {
		t.Fatal("the worker login reads token_hash")
	}
}

// C23 (integration): a revoke committed between the two reads of one page is seen on both reads or on neither.
func TestC23iPageSnapshotIsConsistentAcrossTheTwoReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newLifecycleEnv(t, ctx, true)
	env.createToken(t)
	worker := workerPool(t, ctx, env)
	ident, _ := authschema.NewValidatedIdentifier("auth")
	gate := &revokeBetween{pool: worker, owner: env.pool}
	pair, err := ReadDomainPage(ctx, gate, ident, uuid.Nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pair.Tokens) != 1 || pair.Tokens[0].Revoked {
		t.Fatalf("the page saw the concurrent revoke: %+v", pair.Tokens)
	}
	again, err := ReadDomainPage(ctx, worker, ident, uuid.Nil, 10)
	if err != nil || len(again.Tokens) != 1 || !again.Tokens[0].Revoked {
		t.Fatalf("a later page does not see the revoke: %+v %v", again.Tokens, err)
	}
}

// revokeBetween revokes every token through the owner pool right after the first query of the transaction it hands out.
type revokeBetween struct {
	pool  *pgxpool.Pool
	owner *pgxpool.Pool
}

func (r *revokeBetween) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx, err := r.pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &revokeTx{Tx: tx, owner: r.owner}, nil
}

type revokeTx struct {
	pgx.Tx
	owner *pgxpool.Pool
	done  bool
}

func (t *revokeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := t.Tx.Query(ctx, sql, args...)
	if err == nil && !t.done {
		t.done = true
		// the snapshot is taken by the first query; commit a revoke after it.
		if _, e := t.owner.Exec(ctx, `UPDATE external_ingest_tokens SET revoked_at = now()`); e != nil {
			return nil, e
		}
	}
	return rows, err
}
