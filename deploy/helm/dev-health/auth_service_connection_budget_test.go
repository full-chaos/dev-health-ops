package devhealth_test

import (
	"strings"
	"testing"
)

const budgetRefusal = "authService.database.connectionBudget"

// prod-shape pool sizes: transaction 16, the default KEDA extra user at its
// own size 4, queue-session 36, coordinator-session 10 = 66 pooler connections.
var budgetPools = []string{
	"--set", "goWorkers.pgbouncer.transaction.poolSize=16",
	"--set", "goWorkers.pgbouncer.queueSession.poolSize=36",
	"--set", "goWorkers.pgbouncer.coordinatorSession.poolSize=10",
}

func budgetWith(budget string, extra ...string) []string {
	args := append(append([]string{}, budgetPools...), "--set", "authService.database.connectionBudget="+budget)
	return authWith(append(args, extra...)...)
}

// 66 pooler + (2 replicas + 1 surge) x 8 = 90.
func TestAuthServiceConnectionBudgetBoundary(t *testing.T) {
	if _, out, err := renderAuth(t, budgetWith("90")...); err != nil {
		t.Fatalf("used == budget must render: %v\n%.400s", err, out)
	}
	_, out, err := renderAuth(t, budgetWith("89")...)
	if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "90") || !strings.Contains(out, "89") {
		t.Fatalf("used == budget+1 must fail with the named message and both figures: err=%v out=%.500s", err, out)
	}
}

// The chart defaults fit the 97 usable connections of a default PostgreSQL.
func TestAuthServiceConnectionBudgetDefaultPostgres(t *testing.T) {
	if _, out, err := renderAuth(t, authWith("--set", "authService.database.connectionBudget=97")...); err != nil {
		t.Fatalf("the default pools plus the auth-service must fit 97: %v\n%.500s", err, out)
	}
}

// The default KEDA extra user without its own size inherits the transaction
// size: 16 + 16 + 36 + 10 + 24 = 102, over 97.
func TestAuthServiceConnectionBudgetKedaPairAtTheDefaultSizeIsOver(t *testing.T) {
	keda := []string{"--set-json", `goWorkers.pgbouncer.transaction.extraUsers=[{"role":"devhealth_keda_readonly","passwordKey":"RIVER_KEDA_READONLY_PASSWORD"}]`}
	_, out, err := renderAuth(t, budgetWith("97", keda...)...)
	if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "102") || !strings.Contains(out, "97") {
		t.Fatalf("a KEDA user at the inherited size must fail the 97 budget with the named message: err=%v out=%.500s", err, out)
	}
	sized := []string{"--set-json", `goWorkers.pgbouncer.transaction.extraUsers=[{"role":"devhealth_keda_readonly","passwordKey":"RIVER_KEDA_READONLY_PASSWORD","poolSize":4}]`}
	if _, out, err := renderAuth(t, budgetWith("97", sized...)...); err != nil {
		t.Fatalf("a KEDA user with its own size 4 must fit: %v\n%.500s", err, out)
	}
}

func TestAuthServiceConnectionBudgetOffByDefault(t *testing.T) {
	if _, out, err := renderAuth(t, authBase...); err != nil {
		t.Fatalf("default render must not check the budget: %v\n%.400s", err, out)
	}
	if _, out, err := renderAuth(t, authWith("--set", "authService.database.connectionBudget=0", "--set", "authService.database.maxConns=32")...); err != nil {
		t.Fatalf("budget 0 is off: %v\n%.400s", err, out)
	}
}

func TestAuthServiceConnectionBudgetCountsEachClause(t *testing.T) {
	cases := map[string][]string{
		"pool size":   {"--set", "authService.database.maxConns=9"},
		"replicas":    {"--set", "authService.replicas=3"},
		"queue pool":  {"--set", "goWorkers.pgbouncer.queueSession.poolSize=37"},
		"coordinator": {"--set", "goWorkers.pgbouncer.coordinatorSession.poolSize=11"},
		"transaction": {"--set", "goWorkers.pgbouncer.transaction.poolSize=17"},
	}
	for name, set := range cases {
		_, out, err := renderAuth(t, budgetWith("90", set...)...)
		if err == nil || !strings.Contains(out, budgetRefusal) {
			t.Errorf("%s: one more connection must fail: err=%v out=%.300s", name, err, out)
		}
	}
}

func TestAuthServiceConnectionBudgetCountsEveryTransactionUser(t *testing.T) {
	users := []string{"--set-json", `goWorkers.pgbouncer.transaction.extraUsers=[{"role":"devhealth_keda_readonly","passwordKey":"RIVER_KEDA_READONLY_PASSWORD","poolSize":4},{"role":"r1","passwordKey":"K1"}]`,
		"--set", "goWorkers.pgbouncer.secret.data.K1=x"}
	// Three transaction pairs: own role 16, keda 4, r1 at the inherited 16: 36 + 36 + 10 + 24 = 106.
	if _, out, err := renderAuth(t, budgetWith("106", users...)...); err != nil {
		t.Fatalf("used == 106 must render: %v\n%.300s", err, out)
	}
	_, out, err := renderAuth(t, budgetWith("105", users...)...)
	if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "106") {
		t.Fatalf("three transaction pairs need 106 over a budget of 105: err=%v out=%.300s", err, out)
	}
	sized := []string{"--set-json", `goWorkers.pgbouncer.transaction.extraUsers=[{"role":"devhealth_keda_readonly","passwordKey":"RIVER_KEDA_READONLY_PASSWORD","poolSize":4},{"role":"r1","passwordKey":"K1","poolSize":2}]`,
		"--set", "goWorkers.pgbouncer.secret.data.K1=x"}
	// r1 at its own size 2: 16 + 4 + 2 + 36 + 10 + 24 = 92.
	if _, out, err := renderAuth(t, budgetWith("92", sized...)...); err != nil {
		t.Fatalf("used == 92 must render: %v\n%.300s", err, out)
	}
	if _, out, err := renderAuth(t, budgetWith("91", sized...)...); err == nil || !strings.Contains(out, "92") {
		t.Fatalf("each user counts at its own size (92 over 91): err=%v out=%.300s", err, out)
	}
}

func TestAuthServiceConnectionBudgetIgnoresAbsentPoolers(t *testing.T) {
	args := budgetWith("24", "--set", "goWorkers.enabled=false")
	if _, out, err := renderAuth(t, args...); err != nil {
		t.Fatalf("no pooler renders: only the auth-service term counts (24): %v\n%.300s", err, out)
	}
}

func TestAuthServiceConnectionBudgetStrict(t *testing.T) {
	for name, set := range map[string][]string{
		"negative": {"--set", "authService.database.connectionBudget=-1"},
		"fraction": {"--set-json", "authService.database.connectionBudget=97.5"},
		"string":   {"--set-string", "authService.database.connectionBudget=97"},
		"bool":     {"--set", "authService.database.connectionBudget=true"},
		"null":     {"--set-json", "authService.database.connectionBudget=null"},
	} {
		_, out, err := renderAuth(t, authWith(set...)...)
		if err == nil || !strings.Contains(out, "authService.database.connectionBudget must be a whole number of 0 or more") {
			t.Errorf("%s: want the named refusal, err=%v out=%.300s", name, err, out)
		}
	}
}

func TestAuthServiceSurgeFigureMatchesTheDeployment(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.300s", err, out)
	}
	if got := at(authDoc(docs, "Deployment", "a-dev-health-auth-service"), "spec", "strategy", "rollingUpdate", "maxSurge"); got != 1 {
		t.Fatalf("maxSurge %v: the budget counts exactly one surge pod", got)
	}
}
