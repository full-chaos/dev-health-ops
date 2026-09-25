//go:build integration

package adminops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
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

const billingGolden = "testdata/billing_golden.json"

// billingGoldenSHA256 pins testdata/billing_golden.json (R24): what the real
// `dev-hops admin billing seed|list` verbs printed and left for every step. The
// producer is deleted with the Python CLI, so this is a rot guard: the file is
// only rewritten by TestBillingVenueOracleMatchesThePythonProducer with
// DHO_BILLING_GOLDEN_UPDATE=1, then this digest is updated.
const billingGoldenSHA256 = "e4ba12d30d8c861bd6213e8e1d6a2d20691640263b38fd93fd5c8af67abd3ff8"

func TestBillingGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(billingGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != billingGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", billingGolden, got, billingGoldenSHA256)
	}
}

// TestBillingMatchesTheFrozenPythonOutput runs the script and compares every step
// with what the real Python verbs did (frozen; no Python needed).
func TestBillingMatchesTheFrozenPythonOutput(t *testing.T) {
	raw, err := os.ReadFile(billingGolden)
	if err != nil {
		t.Fatal(err)
	}
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
}

// TestBillingVenueOracleMatchesThePythonProducer runs the script through the real
// Python verbs and through dho. With DHO_BILLING_GOLDEN_UPDATE=1 it rewrites the
// frozen file.
func TestBillingVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	py := billingSession(t, true)
	got := billingSession(t, false)
	compareBundles(t, got, py, "python")
	if os.Getenv("DHO_BILLING_GOLDEN_UPDATE") == "1" {
		raw, err := json.MarshalIndent(py, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(billingGolden, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}
