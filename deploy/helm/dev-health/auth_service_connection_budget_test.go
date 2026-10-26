package devhealth_test

import (
	"strings"
	"testing"
)

const budgetRefusal = "authService.database.connectionBudget"

// prod-shape pool sizes (CHAOS-11542): transaction 9, the default KEDA extra
// user at its own size 4, queue-session 36, coordinator-session 10. The
// transaction pools (9 + 4) count twice, once for the surge pod of a roll:
// 2 x 13 + 36 + 10 = 72 pooler connections.
var budgetPools = []string{
	"--set", "goWorkers.pgbouncer.transaction.poolSize=9",
	"--set", "goWorkers.pgbouncer.queueSession.poolSize=36",
	"--set", "goWorkers.pgbouncer.coordinatorSession.poolSize=10",
}

func budgetWith(budget string, extra ...string) []string {
	args := append(append([]string{}, budgetPools...), "--set", "authService.database.connectionBudget="+budget)
	return authWith(append(args, extra...)...)
}

// 72 pooler + (2 replicas + 1 surge) x 8 = 96.
func TestAuthServiceConnectionBudgetBoundary(t *testing.T) {
	if _, out, err := renderAuth(t, budgetWith("96")...); err != nil {
		t.Fatalf("used == budget must render: %v\n%.400s", err, out)
	}
	_, out, err := renderAuth(t, budgetWith("95")...)
	if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "96") || !strings.Contains(out, "95") {
		t.Fatalf("used == budget+1 must fail with the named message and both figures: err=%v out=%.500s", err, out)
	}
}

// The chart defaults are sized for the deployment manifest (every group at
// max_replicas plus a full rolling restart): (16 + 4) x 2 + 50 + 14 + 24 =
// 128, over the 97 usable connections of a default PostgreSQL. The default
// budget is 0, so the default render passes; opting in at 97 names the excess.
func TestAuthServiceConnectionBudgetDefaultPostgres(t *testing.T) {
	_, out, err := renderAuth(t, authWith("--set", "authService.database.connectionBudget=97")...)
	if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "128") || !strings.Contains(out, "97") {
		t.Fatalf("the chart default pools over 97 must fail with the named message: err=%v out=%.500s", err, out)
	}
}

// The production values after CHAOS-11542 (9 / 36 / 10 and KEDA at 4) fit the 97.
func TestAuthServiceConnectionBudgetProdShapeFits97(t *testing.T) {
	if _, out, err := renderAuth(t, budgetWith("97")...); err != nil {
		t.Fatalf("72 pooler + 24 auth-service = 96 must fit 97: %v\n%.500s", err, out)
	}
}

// The default KEDA extra user without its own size inherits the transaction
// size: (9 + 9) x 2 + 36 + 10 + 24 = 106, over 97.
func TestAuthServiceConnectionBudgetKedaPairAtTheDefaultSizeIsOver(t *testing.T) {
	keda := []string{"--set-json", `goWorkers.pgbouncer.transaction.extraUsers=[{"role":"devhealth_keda_readonly","passwordKey":"RIVER_KEDA_READONLY_PASSWORD"}]`}
	_, out, err := renderAuth(t, budgetWith("97", keda...)...)
	if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "106") || !strings.Contains(out, "97") {
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
		"transaction": {"--set", "goWorkers.pgbouncer.transaction.poolSize=10"},
	}
	for name, set := range cases {
		_, out, err := renderAuth(t, budgetWith("96", set...)...)
		if err == nil || !strings.Contains(out, budgetRefusal) {
			t.Errorf("%s: one more connection must fail: err=%v out=%.300s", name, err, out)
		}
	}
}

func TestAuthServiceConnectionBudgetCountsEveryTransactionUser(t *testing.T) {
	users := []string{"--set-json", `goWorkers.pgbouncer.transaction.extraUsers=[{"role":"devhealth_keda_readonly","passwordKey":"RIVER_KEDA_READONLY_PASSWORD","poolSize":4},{"role":"r1","passwordKey":"K1"}]`,
		"--set", "goWorkers.pgbouncer.secret.data.K1=x"}
	// Three transaction pairs: own role 9, keda 4, r1 at the inherited 9, all
	// counted twice (surge pod): 2 x 22 + 36 + 10 + 24 = 114.
	if _, out, err := renderAuth(t, budgetWith("114", users...)...); err != nil {
		t.Fatalf("used == 114 must render: %v\n%.300s", err, out)
	}
	_, out, err := renderAuth(t, budgetWith("113", users...)...)
	if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "114") {
		t.Fatalf("three transaction pairs need 114 over a budget of 113: err=%v out=%.300s", err, out)
	}
	sized := []string{"--set-json", `goWorkers.pgbouncer.transaction.extraUsers=[{"role":"devhealth_keda_readonly","passwordKey":"RIVER_KEDA_READONLY_PASSWORD","poolSize":4},{"role":"r1","passwordKey":"K1","poolSize":2}]`,
		"--set", "goWorkers.pgbouncer.secret.data.K1=x"}
	// r1 at its own size 2: 2 x (9 + 4 + 2) + 36 + 10 + 24 = 100.
	if _, out, err := renderAuth(t, budgetWith("100", sized...)...); err != nil {
		t.Fatalf("used == 100 must render: %v\n%.300s", err, out)
	}
	if _, out, err := renderAuth(t, budgetWith("99", sized...)...); err == nil || !strings.Contains(out, "100") {
		t.Fatalf("each user counts at its own size (100 over 99): err=%v out=%.300s", err, out)
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
