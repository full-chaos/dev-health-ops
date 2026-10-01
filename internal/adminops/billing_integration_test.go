//go:build integration

package adminops

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// `admin billing seed|list` are compared with the real Python verbs on one
// scripted session over real PostgreSQL: every step's exit code and stdout, and
// the plan and price rows left after it.

func seedSQL(sql string) bundleStep {
	return bundleStep{args: []string{"billing", "list"}, seed: true, sql: sql}
}

var billingScript = []bundleStep{
	vb("billing", "list"),
	vb("billing", "seed"),
	vb("billing", "seed"),
	vb("billing", "list"),
	seedSQL(`DELETE FROM billing_plans WHERE key = 'community'`),
	vb("billing", "seed"),
	vb("billing", "list"),
	seedSQL(`INSERT INTO billing_plans (id, key, name, description, tier, is_active, display_order, stripe_product_id) VALUES
(gen_random_uuid(), 'a-rather-long-plan-key', 'A rather long plan name', 'd', 'a-long-tier-name', false, 5, 'prod_0123456789012345678901234567890'),
(gen_random_uuid(), 'uni', 'Ünï 日本', NULL, 'team', true, 6, 'prod_uni'),
(gen_random_uuid(), 'noprices', 'No prices', NULL, 'team', true, 7, NULL);
INSERT INTO billing_prices (id, plan_id, interval, amount, currency)
SELECT gen_random_uuid(), id, v.i, v.a, 'usd' FROM billing_plans, (VALUES ('monthly', 1), ('yearly', 999), ('weekly', 1999), ('daily', -150), ('once', 100000), ('odd', 5)) AS v(i, a) WHERE key = 'uni'`),
	vb("billing", "list"),
	seedSQL(`DELETE FROM billing_plans WHERE key IN ('team', 'enterprise')`),
	seedSQL(`ALTER TABLE billing_prices ADD CONSTRAINT oracle_no_prices CHECK (amount < 0 OR amount = 0 OR amount = 1 OR amount = 999 OR amount = 1999 OR amount = 100000 OR amount = 5)`),
	{args: []string{"billing", "seed"}, dbError: true},
	vb("billing", "list"),
	seedSQL(`ALTER TABLE billing_prices DROP CONSTRAINT oracle_no_prices`),
	vb("billing", "seed"),
	vb("billing", "list"),
	vb("billing", "seed", "--extra"),
	vb("billing", "list", "x"),
}

func (db *database) billingState(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	collect := func(sql string) []string {
		rows, err := db.conn.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var text string
			if err := rows.Scan(&text); err != nil {
				t.Fatal(err)
			}
			out = append(out, text)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	state := map[string][]string{
		"plans":  collect(`SELECT concat_ws('|', key, name, coalesce(description, '<null>'), tier, is_active::text, display_order::text, coalesce(stripe_product_id, '<null>'), metadata::text) FROM billing_plans ORDER BY key`),
		"prices": collect(`SELECT concat_ws('|', p.key, r.interval, r.amount::text, r.currency, r.is_active::text, coalesce(r.stripe_price_id, '<null>')) FROM billing_prices r JOIN billing_plans p ON p.id = r.plan_id ORDER BY p.key, r.interval, r.amount`),
	}
	raw, _ := json.Marshal(state)
	return string(raw)
}

func billingSession(t *testing.T, python bool) []bundleResult {
	t.Helper()
	db := startDatabase(t)
	ctx := context.Background()
	if _, err := db.conn.Exec(ctx, "TRUNCATE billing_plans CASCADE"); err != nil {
		t.Fatal(err)
	}
	var out []bundleResult
	for _, s := range billingScript {
		if s.seed {
			if _, err := db.conn.Exec(ctx, s.sql); err != nil {
				t.Fatalf("seed: %v\n%s", err, s.sql)
			}
			out = append(out, bundleResult{Args: []string{"<seed>"}, State: db.billingState(t)})
			continue
		}
		var code int
		var stdout string
		if python {
			code, stdout = pythonVerbFull(t, db, s.env, nil, s.args)
		} else {
			code, stdout = goVerbEnv(t, db, s.env, s.args)
		}
		if s.dbError && strings.HasPrefix(stdout, "Error: ") {
			stdout = "Error: <database error>\n"
		}
		out = append(out, bundleResult{Args: s.args, Exit: code, Stdout: stdout, State: db.billingState(t)})
	}
	return out
}

// TestBillingMatchesTheFrozenPythonOutput runs the script and compares every step (exit, stdout, the plan and
// price rows left) with what the REAL `dev-hops admin billing seed|list` verbs did. The answers were
// executed once on adminPythonBuild and are frozen in testdata/golden/billing.json (the recipe regenerates
// them by execution); the script is part of the golden's key.
func TestBillingMatchesTheFrozenPythonOutput(t *testing.T) {
	golden, root := adminGolden(t, "billing", "a90677055d98c1a8e31aa74c063a16b0cadd8ca666446479234e4aa2011cf733", "TestBillingMatchesTheFrozenPythonOutput")
	script := make([]map[string]any, len(billingScript))
	for index, s := range billingScript {
		script[index] = map[string]any{"args": s.args, "env": s.env, "dbError": s.dbError, "sql": s.sql, "seed": s.seed}
	}
	input, err := json.Marshal(script)
	if err != nil {
		t.Fatal(err)
	}
	raw := adminProduce(t, golden, root, "billing script", input, func() any { return billingSession(t, true) })
	var frozen []bundleResult
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	compareBundles(t, billingSession(t, false), frozen, "frozen Python")
	seeded, listed := 0, 0
	for _, item := range frozen {
		if strings.HasPrefix(item.Stdout, "Seeded") {
			seeded++
		}
		if strings.Contains(item.Stdout, "Stripe Product ID") {
			listed++
		}
	}
	if seeded < 3 || listed < 3 {
		t.Fatalf("the golden has %d seeds and %d listings: it measures too little", seeded, listed)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}
