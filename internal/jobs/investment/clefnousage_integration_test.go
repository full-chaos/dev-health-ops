//go:build integration

package investment

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const clefBudgetOrg = "aaaaaaaa-0000-4000-8000-0000000000c1"

// A clef answer with no usage settles through the real Postgres store at the
// reserved amount as a success; the next admission of the same month passes.
func TestAClefAnswerWithNoUsageSettlesThroughPgStoreAndTheNextAdmitPasses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'clef-budget-org', 'Clef Budget Org', 'team', 'self', true, now(), now())`, clefBudgetOrg); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'llm_budget', 'limit_micro_usd', '100000000', false, now(), now())`, clefBudgetOrg); err != nil {
		t.Fatal(err)
	}

	stub := newClefBYO(clefReplyWith(dropFields("usage", "model")))
	stub.store = llmbudget.PgStore{Pool: pool}
	served, err := stub.build().Build(ctx, clefBudgetOrg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = served.Close() }()
	m := &Materializer{logger: testLogger()}
	m.SetServed(served)
	for index, unit := range []string{"u1", "u2"} {
		if _, err := m.categorizeEntry(ctx, shadowTestConfig(), shadowEntry(index, unit, shadowTestBundle(t, unit, 6))); err != nil {
			t.Fatalf("unit %s: %v", unit, err)
		}
	}
	rows, err := pool.Query(ctx, `SELECT status, count(*), count(actual_micro_usd) FROM byo_llm_budget_reservations WHERE org_id = $1 GROUP BY status`, clefBudgetOrg)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	states := map[string][2]int{}
	for rows.Next() {
		var status string
		var n, priced int
		if err := rows.Scan(&status, &n, &priced); err != nil {
			t.Fatal(err)
		}
		states[status] = [2]int{n, priced}
	}
	if got := states["succeeded"]; got != [2]int{2, 2} || len(states) != 1 {
		t.Fatalf("reservation states = %v, want two succeeded rows, both priced, and no usage_unavailable", states)
	}
	if stub.network.count() != 2 {
		t.Fatalf("requests = %d, want 2", stub.network.count())
	}
}
