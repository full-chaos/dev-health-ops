//go:build integration

package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// sessionLockTimeout is a session-level lock_timeout no server or role default gives.
const sessionLockTimeout = "7s"

// saveLockPool is a migrated Postgres whose every connection SETs
// sessionLockTimeout after connect, with one org.
func saveLockPool(t *testing.T, ctx context.Context) (*pgxpool.Pool, uuid.UUID) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET lock_timeout = '`+sessionLockTimeout+`'`)
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	org := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'save-lock', 'save-lock', 'team', 'stripe', true, now(), now())`, org); err != nil {
		t.Fatal(err)
	}
	return pool, org
}

func saveLockHandlers(pool *pgxpool.Pool) *handlers {
	return &handlers{
		store:       pgStore{Pool: pool},
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		modelCache:  newLLMModelListCache(),
		validateURL: func(context.Context, string) (bool, string, error) { return true, "", nil },
	}
}

// A session with its own lock_timeout keeps that exact value inside the save
// transaction after the save lock, default and purpose saves alike: only the
// save lock wait is bounded.
func TestLLMSettingsSaveGivesTheSessionLockTimeoutBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool, org := saveLockPool(t, ctx)
	h := saveLockHandlers(pool)
	var serverDefault string
	if err := pool.QueryRow(ctx, `SELECT reset_val FROM pg_settings WHERE name = 'lock_timeout'`).Scan(&serverDefault); err != nil {
		t.Fatal(err)
	}
	if serverDefault == "7000" {
		t.Fatalf("the server default lock_timeout is already %sms: the cell cannot tell the session value apart", serverDefault)
	}
	read := func(got *string) func(context.Context, pgx.Tx) error {
		return func(ctx context.Context, tx pgx.Tx) error {
			return tx.QueryRow(ctx, `SELECT current_setting('lock_timeout')`).Scan(got)
		}
	}
	var inDefault, inPurpose string
	if _, refusal, err := h.applyLLMSettings(ctx, org, llmUpsert{provider: "openai", model: strPtr("m"), requireModel: true, inTx: read(&inDefault)}); err != nil || refusal != nil {
		t.Fatalf("default save: refusal=%v err=%v", refusal, err)
	}
	if _, refusal, err := h.applyLLMPurpose(ctx, org, llmPurposeUpsert{purpose: llmorgsettings.PurposeExplain, provider: "openai", model: "m", inTx: read(&inPurpose)}); err != nil || refusal != nil {
		t.Fatalf("purpose save: refusal=%v err=%v", refusal, err)
	}
	t.Logf("server default lock_timeout %sms; session %s; inside the save after the lock: default %s, purpose %s", serverDefault, sessionLockTimeout, inDefault, inPurpose)
	if inDefault != sessionLockTimeout || inPurpose != sessionLockTimeout {
		t.Fatalf("lock_timeout after the save lock: default %q, purpose %q, want the session's %q", inDefault, inPurpose, sessionLockTimeout)
	}
}

// A default save with a budget limit (save lock, then the budget lock) and BYO
// call admissions (the budget lock only) on the same org never deadlock: with
// the budget lock held while both wait, and in free-running rounds, every one
// finishes within the bound and no 40P01/55P01/55P03 is seen.
func TestLLMSettingsBudgetSaveAndAdmitNeverDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool, org := saveLockPool(t, ctx)
	h := saveLockHandlers(pool)
	store := llmbudget.PgStore{Pool: pool}
	noEnv := func(string) (string, bool) { return "", false }
	const bound = 10 * time.Second
	lockErr := func(err error) string {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "40P01", "55P01", "55P03":
				return pgErr.Code
			}
		}
		return ""
	}
	save := func(limit int64) error {
		_, refusal, err := h.applyLLMSettings(ctx, org, llmUpsert{provider: "openai", model: strPtr("m"), budgetLimitMu: big.NewInt(limit), requireModel: true})
		if refusal != nil {
			return fmt.Errorf("refused: %v", refusal.detail)
		}
		return err
	}
	admit := func(key string) error {
		_, err := store.Admit(ctx, llmbudget.AdmitRequest{OrgID: org.String(), Provider: "openai", Model: "m", IdempotencyKey: key,
			MaxCost: ptrInt64(1), Now: time.Now().UTC(), Lookup: noEnv})
		if code := lockErr(err); code != "" {
			return err
		}
		return nil
	}
	type result struct {
		name string
		err  error
		took time.Duration
	}
	run := func(name string, f func() error, out chan<- result, start <-chan struct{}) {
		<-start
		began := time.Now()
		err := f()
		out <- result{name, err, time.Since(began)}
	}
	collect := func(round string, out <-chan result, n int) {
		t.Helper()
		for range n {
			select {
			case r := <-out:
				if r.err != nil {
					t.Errorf("%s: %s: %v (lock error %q)", round, r.name, r.err, lockErr(r.err))
				}
				if round == "held" {
					t.Logf("held: %s finished in %s, err %v", r.name, r.took, r.err)
				}
				if r.took > bound {
					t.Errorf("%s: %s took %s, bound %s", round, r.name, r.took, bound)
				}
			case <-time.After(bound):
				t.Fatalf("%s: a save or an admission did not finish within %s", round, bound)
			}
		}
	}

	// Held: the budget lock is held (an admission in flight) while the save,
	// already past the save lock, and a second admission both wait on it.
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, llmbudget.LockKey(org.String())); err != nil {
		t.Fatal(err)
	}
	out, start := make(chan result, 2), make(chan struct{})
	go run("budget save", func() error { return save(1000) }, out, start)
	go run("admit", func() error { return admit("held") }, out, start)
	close(start)
	deadline := time.Now().Add(bound)
	for {
		var waiting int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid)) > 0`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting >= 2 {
			break
		}
		if time.Now().After(deadline) {
			_ = holder.Rollback(ctx)
			t.Fatalf("held: the save and the admission did not both wait on the budget lock within %s (%d waiting)", bound, waiting)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	collect("held", out, 2)

	// Free-running: 20 rounds of one budget save and two admissions at once.
	var wg sync.WaitGroup
	for round := range 20 {
		out, start := make(chan result, 3), make(chan struct{})
		wg.Add(3)
		for i, f := range []func() error{
			func() error { return save(int64(1000 + round)) },
			func() error { return admit(fmt.Sprintf("r%d-a", round)) },
			func() error { return admit(fmt.Sprintf("r%d-b", round)) },
		} {
			name := []string{"budget save", "admit a", "admit b"}[i]
			go func() { defer wg.Done(); run(name, f, out, start) }()
		}
		close(start)
		collect(fmt.Sprintf("round %d", round), out, 3)
	}
	wg.Wait()
	t.Logf("20 free-running rounds of one budget save and two admissions finished, no lock error")
	var limit string
	if err := pool.QueryRow(ctx, `SELECT value FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`, org.String(), llmbudget.Category, llmbudget.LimitKey).Scan(&limit); err != nil || limit != "1019" {
		t.Fatalf("stored budget limit %q, %v; want the last round's 1019", limit, err)
	}
}

// An operator save that spells the org id another way (upper case, no hyphens,
// braces) and a save under the canonical id serialize on one save lock: with
// the canonical save held after its lock, the alias save waits on the save
// lock and answers only after the canonical save ends.
func TestLLMSettingsOperatorAliasSaveWaitsOnTheCanonicalSave(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool, org := saveLockPool(t, ctx)
	h := saveLockHandlers(pool)
	operator := Operator{Pool: pool}
	canonicalSave := func() error {
		_, refusal, err := h.applyLLMSettings(ctx, org, llmUpsert{provider: "openai", model: strPtr("m"), baseURL: strPtr("https://8.8.8.8/v1"), requireModel: true})
		if refusal != nil {
			return fmt.Errorf("refused: %v", refusal.detail)
		}
		return err
	}
	if err := canonicalSave(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	waitingOnSaveLock := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	blocked := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid)) > 0`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	upper := strings.ToUpper(org.String())
	for _, alias := range []string{upper, strings.ReplaceAll(org.String(), "-", ""), "{" + org.String() + "}"} {
		holder, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var held int
		if err := holder.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM settings WHERE org_id = $1 AND category = 'llm' AND key = 'base_url' FOR UPDATE) AS l`, org.String()).Scan(&held); err != nil || held != 1 {
			_ = holder.Rollback(ctx)
			t.Fatalf("%q: hold the canonical base_url row: %d, %v", alias, held, err)
		}
		canonical, aliased := make(chan error, 1), make(chan error, 1)
		go func() { canonical <- canonicalSave() }()
		deadline := time.Now().Add(10 * time.Second)
		for blocked() < 1 {
			if time.Now().After(deadline) {
				_ = holder.Rollback(ctx)
				t.Fatalf("%q: the canonical save did not wait on the held row", alias)
			}
			time.Sleep(20 * time.Millisecond)
		}
		go func() {
			_, err := operator.SetLLMSettings(ctx, alias, LLMSettingsInput{Provider: "openai", Model: strPtr("m")}, LLMSettingsConfig{})
			aliased <- err
		}()
		var early error
		answered := false
		deadline = time.Now().Add(10 * time.Second)
		for !answered && waitingOnSaveLock() < 1 {
			select {
			case early = <-aliased:
				answered = true
			default:
				if time.Now().After(deadline) {
					_ = holder.Rollback(ctx)
					t.Fatalf("%q: the alias save neither answered nor waited on the save lock", alias)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		if err := holder.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if answered {
			t.Errorf("%q: the alias save answered (%v) while the canonical save held the save lock: they took different locks", alias, early)
		}
		for name, done := range map[string]chan error{"canonical": canonical, "alias": aliased} {
			select {
			case err := <-done:
				if name == "alias" && answered {
					continue
				}
				if err != nil {
					t.Errorf("%q: %s save: %v", alias, name, err)
				}
			case <-time.After(10 * time.Second):
				if !(name == "alias" && answered) {
					t.Fatalf("%q: the %s save did not finish after the release", alias, name)
				}
			}
		}
		if !answered {
			t.Logf("%q: the alias save waited on the canonical save's lock, then both finished", alias)
		}
	}
}

// An operator save that spells the org id another way writes the rows of the
// canonical id: after each of the 3 forms there is exactly one model row, under
// the canonical id, holding that save's model, and the canonical read finds it.
func TestLLMSettingsOperatorAliasSaveWritesTheCanonicalRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool, org := saveLockPool(t, ctx)
	h := saveLockHandlers(pool)
	operator := Operator{Pool: pool}
	canonical := org.String()
	for i, alias := range []string{strings.ToUpper(canonical), strings.ReplaceAll(canonical, "-", ""), "{" + canonical + "}"} {
		model := fmt.Sprintf("m-%d", i)
		if _, err := operator.SetLLMSettings(ctx, alias, LLMSettingsInput{Provider: "openai", Model: strPtr(model)}, LLMSettingsConfig{}); err != nil {
			t.Fatalf("%q: operator save: %v", alias, err)
		}
		var rows, canonicalRows int
		if err := pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE org_id = $1) FROM settings WHERE category = 'llm' AND key = 'model' AND lower(replace(trim(org_id, '{}'), '-', '')) = replace($1, '-', '')`, canonical).Scan(&rows, &canonicalRows); err != nil {
			t.Fatal(err)
		}
		out, err := h.llmSettingsResponse(ctx, pool, canonical)
		if err != nil {
			t.Fatalf("%q: canonical read: %v", alias, err)
		}
		got, _ := out.Get("model")
		t.Logf("%q: model rows for any spelling of the org %d, under the canonical id %d; canonical read model %v", alias, rows, canonicalRows, got)
		if rows != 1 || canonicalRows != 1 || fmt.Sprint(got) != model {
			t.Errorf("%q: %d model rows (%d canonical), canonical read %v; want exactly one canonical row holding %q", alias, rows, canonicalRows, got, model)
		}
	}
}

func ptrInt64(v int64) *int64 { return &v }
